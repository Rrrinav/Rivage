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

type workerStats struct {
	tasksCompleted atomic.Int32
	tasksFailed    atomic.Int32
	computeMs      atomic.Int64
}

type Worker struct {
	id  string
	cfg *config.WorkerConfig
	log *telemetry.Logger
	sem chan struct{}

	activeTasks atomic.Int32
	cpuGauge    atomic.Value

	cancelRunning sync.Map
	sendSem       chan struct{}
	streamMu      sync.Mutex

	inboundMu     sync.Mutex
	inboundChunks map[string]*inboundAssembler

	stats workerStats
}

// inboundAssembler streams incoming chunks straight to disk — zero RAM overhead.
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
	w.log.Info("Worker initialised",
		"id", id,
		"concurrency", concurrency,
		"tags", cfg.Tags,
		"coordinator", cfg.Coordinator.Addr,
	)
	return w, nil
}

func (w *Worker) ID() string { return w.id }

// ── Connection loop ───────────────────────────────────────────────────────────

func (w *Worker) Run(ctx context.Context) error {
	maxAttempts := w.cfg.Coordinator.MaxReconnectAttempts
	reconnectInterval := w.cfg.Coordinator.ReconnectInterval.Duration
	if reconnectInterval == 0 {
		reconnectInterval = 5 * time.Second
	}

	attempt := 0
	for {
		connectStart := time.Now()
		if err := w.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				w.log.Info("Context cancelled, shutting down")
				return ctx.Err()
			}
			w.log.Warn("Disconnected from coordinator",
				"err", err,
				"uptime", time.Since(connectStart).Round(time.Second),
				"attempt", attempt+1,
			)
		} else {
			w.log.Info("Connection closed cleanly",
				"uptime", time.Since(connectStart).Round(time.Second),
			)
		}

		attempt++
		if maxAttempts > 0 && attempt >= maxAttempts {
			return fmt.Errorf("max reconnect attempts (%d) reached", maxAttempts)
		}

		backoff := time.Duration(math.Min(
			float64(reconnectInterval)*math.Pow(1.5, float64(attempt)),
			float64(60*time.Second),
		))
		w.log.Info("Reconnecting", "in", backoff.Round(time.Millisecond), "attempt", attempt+1)
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
		return fmt.Errorf("coordinator rejected registration: %s", ack.Ack.GetMessage())
	}
	w.log.Info("Registered with coordinator", "msg", ack.Ack.Message)

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
			spec := p.Task
			w.log.Debug("Received task",
				"task", spec.TaskId,
				"stage", spec.StageId,
				"input_bytes", len(spec.InputData),
			)
			go w.executeTask(ctx, spec, resultCh)

		case *pb.CoordinatorMessage_ChunkedTask:
			cr := p.ChunkedTask
			w.log.Debug("Received chunk",
				"task", cr.TaskId,
				"chunk", cr.ChunkIndex,
				"bytes", len(cr.Data),
				"final", cr.IsFinal,
			)
			if spec := w.receiveChunk(cr); spec != nil {
				w.log.Info("Chunked task fully received, executing",
					"task", spec.TaskId,
					"stage", spec.StageId,
				)
				go w.executeTask(ctx, spec, resultCh)
			}

		case *pb.CoordinatorMessage_Cancel:
			taskID := p.Cancel.TaskId
			if cancel, ok := w.cancelRunning.Load(taskID); ok {
				w.log.Info("Cancelling task", "task", taskID)
				cancel.(context.CancelFunc)()
			}

		case *pb.CoordinatorMessage_JobComplete:
			w.log.Info("Job completed globally",
				"job_id", p.JobComplete.JobId,
				"my_completed", w.stats.tasksCompleted.Load(),
				"my_failed", w.stats.tasksFailed.Load(),
				"my_compute_ms", w.stats.computeMs.Load(),
			)

		case *pb.CoordinatorMessage_Drain:
			w.log.Info("Drain requested, finishing in-flight tasks")
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Drain{Drain: &pb.DrainRequest{WorkerId: w.id}}})
			w.streamMu.Unlock()
			w.waitForIdle()
			w.log.Info("Drain complete, disconnecting")
			return nil
		}
	}
}

// ── Chunk assembly ────────────────────────────────────────────────────────────

func (w *Worker) receiveChunk(cr *pb.ChunkedTaskSpec) *pb.TaskSpec {
	w.inboundMu.Lock()
	defer w.inboundMu.Unlock()

	asm, exists := w.inboundChunks[cr.TaskId]
	if !exists {
		f, err := os.CreateTemp(w.tempDir(), "rivage-in-*")
		if err != nil {
			w.log.Error("Failed to create inbound temp file", "task", cr.TaskId, "err", err)
			return nil
		}
		asm = &inboundAssembler{first: cr, file: f, path: f.Name()}
		w.inboundChunks[cr.TaskId] = asm
	}

	if len(cr.Data) > 0 {
		if _, err := asm.file.Write(cr.Data); err != nil {
			w.log.Error("Failed to write chunk to disk", "task", cr.TaskId, "err", err)
		}
	}

	if !cr.IsFinal {
		return nil
	}

	delete(w.inboundChunks, cr.TaskId)
	asm.file.Close()

	fi, _ := os.Stat(asm.path)
	totalBytes := int64(0)
	if fi != nil {
		totalBytes = fi.Size()
	}
	w.log.Debug("Inbound chunk stream assembled",
		"task", cr.TaskId,
		"chunks", cr.ChunkIndex+1,
		"bytes", totalBytes,
	)

	first := asm.first
	if first.Env == nil {
		first.Env = make(map[string]string)
	}
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
		InputData:      nil,
		DatastoreUrl:   first.DatastoreUrl,
	}
}

type taskResult struct {
	proto    *pb.TaskResult
	diskPath string
}

func (w *Worker) executeTask(ctx context.Context, spec *pb.TaskSpec, resultCh chan<- *taskResult) {
	// Acquire concurrency slot.
	select {
	case w.sem <- struct{}{}:
		defer func() { <-w.sem }()
	case <-ctx.Done():
		return
	}

	w.activeTasks.Add(1)
	defer w.activeTasks.Add(-1)

	timeout := w.cfg.Execution.DefaultTaskTimeout.Duration
	if spec.TimeoutSeconds > 0 {
		timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	w.cancelRunning.Store(spec.TaskId, cancel)
	defer w.cancelRunning.Delete(spec.TaskId)

	w.log.Info("Task starting",
		"task", spec.TaskId,
		"stage", spec.StageId,
		"cmd", spec.Command,
		"timeout", timeout,
		"active", w.activeTasks.Load(),
		"slots_free", cap(w.sem)-len(w.sem),
	)

	start := time.Now()
	result := w.runSubprocess(taskCtx, spec)
	durationMs := time.Since(start).Milliseconds()
	result.proto.DurationMs = durationMs
	result.proto.WorkerId = w.id

	if result.proto.Status == pb.TaskStatus_TASK_STATUS_COMPLETED {
		w.stats.tasksCompleted.Add(1)
		w.stats.computeMs.Add(durationMs)
		outBytes := int64(len(result.proto.OutputData))
		if result.diskPath != "" {
			if fi, err := os.Stat(result.diskPath); err == nil {
				outBytes = fi.Size()
			}
		}
		w.log.Info("Task completed",
			"task", spec.TaskId,
			"stage", spec.StageId,
			"duration_ms", durationMs,
			"output_bytes", outBytes,
			"my_total_completed", w.stats.tasksCompleted.Load(),
		)
	} else {
		w.stats.tasksFailed.Add(1)
		w.log.Error("Task failed",
			"task", spec.TaskId,
			"stage", spec.StageId,
			"duration_ms", durationMs,
			"err", result.proto.ErrorLog,
			"my_total_failed", w.stats.tasksFailed.Load(),
		)
	}

	resultCh <- result
}

func (w *Worker) runSubprocess(ctx context.Context, spec *pb.TaskSpec) *taskResult {
	base := &pb.TaskResult{TaskId: spec.TaskId, JobId: spec.JobId, StageId: spec.StageId}
	fail := func(msg string) *taskResult {
		base.Status = pb.TaskStatus_TASK_STATUS_FAILED
		base.ErrorLog = msg
		return &taskResult{proto: base}
	}

	// Write input to disk if it arrived inline (not via chunked path).
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
		} else {
			w.log.Warn("Could not write input to disk, falling back to stdin", "task", spec.TaskId, "err", err)
		}
	}
	if inputFile != "" {
		defer os.Remove(inputFile)
	}

	// Write shipped code to a temp file.
	var tempCode string
	if len(spec.Code) > 0 {
		f, err := os.CreateTemp(w.tempDir(), "rivage-code-*")
		if err != nil {
			return fail(fmt.Sprintf("creating temp code file: %v", err))
		}
		f.Write(spec.Code)
		f.Close()
		os.Chmod(f.Name(), 0700)
		tempCode = f.Name()
		defer os.Remove(tempCode)
	}

	// Substitute {CODE} placeholder in args.
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

	outFile, err := os.CreateTemp(w.tempDir(), "rivage-out-*")
	if err != nil {
		return fail(fmt.Sprintf("creating output temp file: %v", err))
	}
	outPath := outFile.Name()

	stderrBuf := &cappedBuffer{cap: maxStderrBytes}

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = bytes.NewReader(spec.InputData)
	cmd.Stdout = outFile

	// Modify this line to stream live to your terminal
	cmd.Stderr = io.MultiWriter(stderrBuf, os.Stderr)

	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("RIVAGE_WORKER_ID=%s", w.id))

	// Inject the dynamic Datastore URL so polyglot scripts can fetch data over the network
	if spec.DatastoreUrl != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("RIVAGE_DATASTORE_URL=%s", spec.DatastoreUrl))
	}

	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	w.log.Debug("Subprocess starting", "task", spec.TaskId, "cmd", command, "args", args)

	runErr := cmd.Run()
	outFile.Close()

	if runErr != nil {
		os.Remove(outPath)
		stderr := stderrBuf.String()
		errMsg := fmt.Sprintf("subprocess error: %v", runErr)
		if stderr != "" {
			errMsg += "\nSTDERR:\n" + stderr
		}
		w.log.Debug("Subprocess stderr", "task", spec.TaskId, "stderr", stderr)
		return fail(errMsg)
	}

	fi, _ := os.Stat(outPath)
	outSize := int64(0)
	if fi != nil {
		outSize = fi.Size()
	}

	base.Status = pb.TaskStatus_TASK_STATUS_COMPLETED

	// Small outputs: read into memory and clean up.
	if outSize <= 4*1024*1024 {
		data, readErr := os.ReadFile(outPath)
		os.Remove(outPath)
		if readErr != nil {
			return fail(fmt.Sprintf("reading output file: %v", readErr))
		}
		base.OutputData = data
		return &taskResult{proto: base}
	}

	// Large outputs: leave on disk and stream back in chunks.
	w.log.Debug("Large output, will stream back",
		"task", spec.TaskId,
		"output_bytes", outSize,
	)
	return &taskResult{proto: base, diskPath: outPath}
}

// Result sender
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
		w.log.Debug("Sending inline result", "task", result.TaskId, "bytes", len(result.OutputData))
		w.streamMu.Lock()
		stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Result{Result: result}})
		w.streamMu.Unlock()
		return
	}

	defer os.Remove(tr.diskPath)
	f, err := os.Open(tr.diskPath)
	if err != nil {
		w.log.Error("Failed to open output file for streaming", "task", result.TaskId, "err", err)
		result.Status = pb.TaskStatus_TASK_STATUS_FAILED
		result.ErrorLog = fmt.Sprintf("opening output for streaming: %v", err)
		w.streamMu.Lock()
		stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Result{Result: result}})
		w.streamMu.Unlock()
		return
	}
	defer f.Close()

	fi, _ := f.Stat()
	totalBytes := int64(0)
	if fi != nil {
		totalBytes = fi.Size()
	}
	w.log.Debug("Streaming large result", "task", result.TaskId, "bytes", totalBytes)

	seqNum := int32(0)
	cr := transfer.NewChunkReader(f, transfer.DefaultChunkSize)
	for {
		chunk, err := cr.Next()
		isFinal := err == io.EOF || (err != nil)

		if len(chunk) > 0 || isFinal {
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_ChunkedResult{
					ChunkedResult: &pb.ChunkedTaskResult{
						TaskId:     result.TaskId,
						JobId:      result.JobId,
						StageId:    result.StageId,
						WorkerId:   result.WorkerId,
						ChunkIndex: seqNum,
						Data:       chunk,
						IsFinal:    isFinal,
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
	w.log.Debug("Finished streaming result", "task", result.TaskId, "chunks", seqNum)
}

// Heartbeat
func (w *Worker) heartbeatLoop(ctx context.Context, stream pb.WorkerService_ConnectClient) {
	interval := w.cfg.Execution.HeartbeatInterval.Duration
	if interval == 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			active := w.activeTasks.Load()
			w.log.Debug("Heartbeat",
				"active_tasks", active,
				"cpu", w.cpuGauge.Load().(float32),
				"completed", w.stats.tasksCompleted.Load(),
				"failed", w.stats.tasksFailed.Load(),
			)
			w.streamMu.Lock()
			stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerId:      w.id,
						ActiveTasks:   active,
						CpuUsage:      w.cpuGauge.Load().(float32),
						TimestampUnix: time.Now().Unix(),
					},
				},
			})
			w.streamMu.Unlock()
		}
	}
}

// ── Registration & dial ───────────────────────────────────────────────────────

func (w *Worker) register(stream pb.WorkerService_ConnectClient) error {
	concurrency := w.cfg.Execution.MaxConcurrentTasks
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	caps := &pb.WorkerCapabilities{
		WorkerId:    w.id,
		CpuCores:    int32(concurrency),
		MemoryBytes: 8 * 1024 * 1024 * 1024,
		Tags:        w.cfg.Tags,
	}
	if w.cfg.Security.SharedSecret != "" {
		signer := security.NewTokenSigner(w.cfg.Security.SharedSecret)
		caps.WorkerToken = signer.Issue(w.id, 24*time.Hour)
	}
	hostname, _ := os.Hostname()
	caps.Hostname = hostname

	w.log.Info("Registering",
		"cores", caps.CpuCores,
		"tags", caps.Tags,
		"hostname", hostname,
	)

	w.streamMu.Lock()
	defer w.streamMu.Unlock()
	return stream.Send(&pb.WorkerMessage{Payload: &pb.WorkerMessage_Register{Register: caps}})
}

func (w *Worker) dial() (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(16*1024*1024),
		grpc.MaxCallSendMsgSize(16*1024*1024),
	))
	opts = append(opts,
		grpc.WithWriteBufferSize(1024*1024*8),
		grpc.WithReadBufferSize(1024*1024*8),
	)

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

	w.log.Debug("Dialing coordinator", "addr", w.cfg.Coordinator.Addr)
	//nolint:staticcheck
	return grpc.Dial(w.cfg.Coordinator.Addr, opts...)
}

// ── Utilities ─────────────────────────────────────────────────────────────────

func (w *Worker) tempDir() string {
	if d := w.cfg.Execution.TempDir; d != "" {
		return d
	}
	return os.TempDir()
}

func (w *Worker) waitForIdle() {
	if w.activeTasks.Load() > 0 {
		w.log.Info("Waiting for in-flight tasks to finish", "active", w.activeTasks.Load())
	}
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
