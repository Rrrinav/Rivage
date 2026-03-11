package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"rivage/pkg/config"
	"rivage/pkg/security"
	"rivage/pkg/telemetry"
	"rivage/pkg/transfer"
	pb "rivage/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxStderrBytes       = 64 * 1024
	maxConcurrentUploads = 4
)

type Worker struct {
	id  string
	cfg *config.WorkerConfig
	log *telemetry.Logger
	sem chan struct{}

	activeTasks atomic.Int32
	cpuGauge    atomic.Value

	cancelRunning sync.Map
	sendSem       chan struct{}
	streamMu      sync.Mutex // NEW: Serializes all outgoing gRPC stream.Send calls

	inboundMu     sync.Mutex
	inboundChunks map[string]*inboundAssembler
}

// inboundAssembler streams chunks directly to disk so the worker uses ~0 bytes of RAM
type inboundAssembler struct {
	first *pb.ChunkedTaskSpec
	file  *os.File
	path  string
}

func New(cfg *config.WorkerConfig) (*Worker, error) {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("%s-%d-%04x", hostname, os.Getpid(), time.Now().UnixMilli()%0xFFFF)
	concurrency := cfg.Execution.MaxConcurrentTasks
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	w := &Worker{
		id:            id,
		cfg:           cfg,
		log:           telemetry.New("worker/"+id, telemetry.LevelInfo),
		sem:           make(chan struct{}, concurrency),
		sendSem:       make(chan struct{}, maxConcurrentUploads),
		inboundChunks: make(map[string]*inboundAssembler),
	}
	w.cpuGauge.Store(float32(0))
	return w, nil
}

func (w *Worker) ID() string { return w.id }

func (w *Worker) Run(ctx context.Context) error {
	maxAttempts := w.cfg.Coordinator.MaxReconnectAttempts
	reconnectInterval := w.cfg.Coordinator.ReconnectInterval.Duration
	if reconnectInterval == 0 {
		reconnectInterval = 5 * time.Second
	}
	attempt := 0
	for {
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.log.Warn("Disconnected from coordinator", "err", err, "attempt", attempt)
		}
		attempt++
		if maxAttempts > 0 && attempt >= maxAttempts {
			return fmt.Errorf("max reconnect attempts (%d) reached", maxAttempts)
		}
		backoff := time.Duration(math.Min(float64(reconnectInterval)*math.Pow(1.5, float64(attempt)), float64(60*time.Second)))
		w.log.Info("Reconnecting", "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) error {
	conn, err := w.dial()
	if err != nil {
		return fmt.Errorf("dialing coordinator: %w", err)
	}
	defer conn.Close()

	client := pb.NewWorkerServiceClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("opening stream: %w", err)
	}

	if err := w.register(stream); err != nil {
		return fmt.Errorf("registration: %w", err)
	}

	ackMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("waiting for ack: %w", err)
	}
	ack, ok := ackMsg.Payload.(*pb.CoordinatorMessage_Ack)
	if !ok || !ack.Ack.Accepted {
		return fmt.Errorf("coordinator rejected registration")
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx, stream)

	resultCh := make(chan *taskResult, 64)
	go w.resultSender(hbCtx, stream, resultCh)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.CoordinatorMessage_Task:
			go w.executeTask(ctx, p.Task, resultCh)

		case *pb.CoordinatorMessage_ChunkedTask:
			if spec := w.receiveChunk(p.ChunkedTask); spec != nil {
				go w.executeTask(ctx, spec, resultCh)
			}

		case *pb.CoordinatorMessage_Cancel:
			if cancel, ok := w.cancelRunning.Load(p.Cancel.TaskId); ok {
				cancel.(context.CancelFunc)()
			}

		case *pb.CoordinatorMessage_Drain:
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Drain{Drain: &pb.DrainRequest{WorkerId: w.id}}})
			w.streamMu.Unlock()
			w.waitForIdle()
			return nil
		}
	}
}

// receiveChunk streams gRPC chunks straight to the hard drive
func (w *Worker) receiveChunk(cr *pb.ChunkedTaskSpec) *pb.TaskSpec {
	w.inboundMu.Lock()
	defer w.inboundMu.Unlock()

	asm, exists := w.inboundChunks[cr.TaskId]
	if !exists {
		f, err := os.CreateTemp(w.tempDir(), "rivage-in-*")
		if err != nil {
			w.log.Error("Failed to create inbound temp file", "err", err)
			return nil
		}
		asm = &inboundAssembler{first: cr, file: f, path: f.Name()}
		w.inboundChunks[cr.TaskId] = asm
	}

	if len(cr.Data) > 0 {
		asm.file.Write(cr.Data)
	}

	if !cr.IsFinal {
		return nil
	}

	delete(w.inboundChunks, cr.TaskId)
	asm.file.Close()

	first := asm.first
	if first.Env == nil {
		first.Env = make(map[string]string)
	}
	
	// Pass the disk file to the Python script via Environment Variable
	first.Env["RIVAGE_INPUT_FILE"] = asm.path

	return &pb.TaskSpec{
		TaskId:         first.TaskId,
		JobId:          first.JobId,
		StageId:        first.StageId,
		TaskType:       first.TaskType,
		Command:        first.Command,
		Args:           first.Args,
		Code:           first.Code,
		Env:            first.Env,
		TimeoutSeconds: first.TimeoutSeconds,
		MaxRetries:     first.MaxRetries,
		RetryCount:     first.RetryCount,
		RequiredTags:   first.RequiredTags,
		InputData:      nil, // Zero RAM usage!
	}
}

type taskResult struct {
	proto    *pb.TaskResult
	diskPath string
}

func (w *Worker) executeTask(ctx context.Context, spec *pb.TaskSpec, resultCh chan<- *taskResult) {
	select {
	case w.sem <- struct{}{}:
		defer func() { <-w.sem }()
	case <-ctx.Done():
		return
	}

	w.activeTasks.Add(1)
	defer w.activeTasks.Add(-1)

	start := time.Now()
	timeout := w.cfg.Execution.DefaultTaskTimeout.Duration
	if spec.TimeoutSeconds > 0 {
		timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	w.cancelRunning.Store(spec.TaskId, cancel)
	defer w.cancelRunning.Delete(spec.TaskId)

	result := w.runSubprocess(taskCtx, spec)
	result.proto.DurationMs = time.Since(start).Milliseconds()
	result.proto.WorkerId = w.id
	resultCh <- result
}

func (w *Worker) runSubprocess(ctx context.Context, spec *pb.TaskSpec) *taskResult {
	base := &pb.TaskResult{TaskId: spec.TaskId, JobId: spec.JobId, StageId: spec.StageId}
	fail := func(msg string) *taskResult {
		base.Status = pb.TaskStatus_TASK_STATUS_FAILED
		base.ErrorLog = msg
		return &taskResult{proto: base}
	}

	inputFile := spec.Env["RIVAGE_INPUT_FILE"]
	if inputFile == "" && len(spec.InputData) > 0 {
		f, err := os.CreateTemp(w.tempDir(), "rivage-in-*")
		if err == nil {
			f.Write(spec.InputData)
			f.Close()
			inputFile = f.Name()
			if spec.Env == nil {
				spec.Env = make(map[string]string)
			}
			spec.Env["RIVAGE_INPUT_FILE"] = inputFile
		}
	}
	if inputFile != "" {
		defer os.Remove(inputFile)
	}

	var tempCode string
	if len(spec.Code) > 0 {
		f, _ := os.CreateTemp(w.tempDir(), "rivage-code-*")
		f.Write(spec.Code)
		f.Close()
		os.Chmod(f.Name(), 0700)
		tempCode = f.Name()
		defer os.Remove(tempCode)
	}

	args := make([]string, len(spec.Args))
	for i, a := range spec.Args {
		if a == "{CODE}" && tempCode != "" {
			args[i] = tempCode
		} else {
			args[i] = a
		}
	}
	command := spec.Command
	if command == "{CODE}" && tempCode != "" {
		command = tempCode
	}

	outFile, _ := os.CreateTemp(w.tempDir(), "rivage-out-*")
	outPath := outFile.Name()

	stderrBuf := &cappedBuffer{cap: maxStderrBytes}

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = bytes.NewReader(spec.InputData) // Fallback for small json payloads
	cmd.Stdout = outFile
	cmd.Stderr = stderrBuf
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	runErr := cmd.Run()
	outFile.Close()

	if runErr != nil {
		os.Remove(outPath)
		return fail(fmt.Sprintf("error: %v\nSTDERR:\n%s", runErr, stderrBuf.String()))
	}

	fi, _ := os.Stat(outPath)
	outSize := int64(0)
	if fi != nil {
		outSize = fi.Size()
	}

	base.Status = pb.TaskStatus_TASK_STATUS_COMPLETED
	if outSize <= 4*1024*1024 {
		data, _ := os.ReadFile(outPath)
		os.Remove(outPath)
		base.OutputData = data
		return &taskResult{proto: base}
	}

	return &taskResult{proto: base, diskPath: outPath}
}

func (w *Worker) resultSender(ctx context.Context, stream pb.WorkerService_ConnectClient, ch <-chan *taskResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case tr, ok := <-ch:
			if !ok {
				return
			}
			select {
			case w.sendSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(tr *taskResult) {
				defer func() { <-w.sendSem }()
				w.sendResult(ctx, stream, tr)
			}(tr)
		}
	}
}

func (w *Worker) sendResult(ctx context.Context, stream pb.WorkerService_ConnectClient, tr *taskResult) {
	result := tr.proto
	if tr.diskPath == "" {
		w.streamMu.Lock()
		stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Result{Result: result}})
		w.streamMu.Unlock()
		return
	}
	defer os.Remove(tr.diskPath)
	f, err := os.Open(tr.diskPath)
	if err != nil {
		result.Status = pb.TaskStatus_TASK_STATUS_FAILED
		w.streamMu.Lock()
		stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Result{Result: result}})
		w.streamMu.Unlock()
		return
	}
	defer f.Close()

	seqNum := int32(0)
	cr := transfer.NewChunkReader(f, transfer.DefaultChunkSize)
	for {
		chunk, err := cr.Next()
		isFinal := err == io.EOF || (err != nil)
		
		// BUG FIX: We MUST send the packet if there is data OR if it is the final termination packet
		if len(chunk) > 0 || isFinal {
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_ChunkedResult{
					ChunkedResult: &pb.ChunkedTaskResult{
						TaskId: result.TaskId, JobId: result.JobId, StageId: result.StageId,
						WorkerId: result.WorkerId, ChunkIndex: seqNum, Data: chunk, IsFinal: isFinal,
					},
				},
			})
			w.streamMu.Unlock()
			seqNum++
		}
		if isFinal {
			break
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context, stream pb.WorkerService_ConnectClient) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerId: w.id, ActiveTasks: w.activeTasks.Load(),
						CpuUsage: w.cpuGauge.Load().(float32), TimestampUnix: time.Now().Unix(),
					},
				},
			})
			w.streamMu.Unlock()
		}
	}
}

func (w *Worker) register(stream pb.WorkerService_ConnectClient) error {
	caps := &pb.WorkerCapabilities{
		WorkerId: w.id, CpuCores: int32(runtime.NumCPU()),
		MemoryBytes: 8 * 1024 * 1024 * 1024, Tags: w.cfg.Tags,
	}

	if w.cfg.Security.SharedSecret != "" {
		signer := security.NewTokenSigner(w.cfg.Security.SharedSecret)
		caps.WorkerToken = signer.Issue(w.id, 24*time.Hour)
	}

	hostname, _ := os.Hostname()
	caps.Hostname = hostname
	
	w.streamMu.Lock()
	defer w.streamMu.Unlock()
	return stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Register{Register: caps}})
}

func (w *Worker) dial() (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(16*1024*1024), grpc.MaxCallSendMsgSize(16*1024*1024),
	))
	
	// NEW: 8MB TCP Buffers for extreme network throughput
	opts = append(opts, grpc.WithWriteBufferSize(1024*1024*8), grpc.WithReadBufferSize(1024*1024*8))

	sec := w.cfg.Security
	if sec.TLSCertFile != "" || sec.TLSCAFile != "" {
		tlsCfg, err := security.LoadClientTLS(sec.TLSCertFile, sec.TLSKeyFile, sec.TLSCAFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	//nolint:staticcheck
	return grpc.Dial(w.cfg.Coordinator.Addr, opts...)
}

func (w *Worker) tempDir() string {
	if d := w.cfg.Execution.TempDir; d != "" {
		return d
	}
	return os.TempDir()
}

func (w *Worker) waitForIdle() {
	for w.activeTasks.Load() > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

type cappedBuffer struct {
	cap  int
	data []byte
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	remaining := b.cap - len(b.data)
	if remaining > 0 {
		if len(p) <= remaining {
			b.data = append(b.data, p...)
		} else {
			b.data = append(b.data, p[:remaining]...)
		}
	}
	return len(p), nil
}

func (b *cappedBuffer) String() string { return string(b.data) }
