// Package worker implements the Rivage worker (executor) node.
//
// Workers connect to the coordinator, receive tasks, execute them as
// subprocesses, and stream results back. The worker is completely
// language-agnostic: it pipes JSON to STDIN and reads JSON from STDOUT.
//
// Usage:
//
//	cfg, _ := config.LoadWorkerConfig("worker.yaml")
//	w, _ := worker.New(cfg)
//	w.Run(ctx)
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
	pb "rivage/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ─────────────────────────────────────────────────────────────────────────────
// Worker
// ─────────────────────────────────────────────────────────────────────────────

// Worker is a single executor node. Create one with New() and call Run().
type Worker struct {
	id   string
	cfg  *config.WorkerConfig
	log  *telemetry.Logger
	sem  chan struct{} // concurrency limiter

	activeTasks atomic.Int32
	cpuGauge    atomic.Value // float32

	// cancelRunning maps taskID → cancel func (for CancelTask messages)
	cancelRunning sync.Map
}

// New creates a Worker from config.
func New(cfg *config.WorkerConfig) (*Worker, error) {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("%s-%d-%04x", hostname, os.Getpid(), time.Now().UnixMilli()%0xFFFF)

	concurrency := cfg.Execution.MaxConcurrentTasks
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	w := &Worker{
		id:  id,
		cfg: cfg,
		log: telemetry.New("worker/"+id, telemetry.LevelInfo),
		sem: make(chan struct{}, concurrency),
	}
	w.cpuGauge.Store(float32(0))
	return w, nil
}

// ID returns the worker's unique identifier.
func (w *Worker) ID() string { return w.id }

// Run connects to the coordinator and serves tasks until ctx is cancelled.
// It reconnects automatically on disconnect (up to MaxReconnectAttempts).
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

		// Exponential backoff capped at 60s
		backoff := time.Duration(math.Min(
			float64(reconnectInterval)*math.Pow(1.5, float64(attempt)),
			float64(60*time.Second),
		))
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
	w.log.Info("Connected to coordinator", "addr", w.cfg.Coordinator.Addr)

	// Register
	if err := w.register(stream); err != nil {
		return fmt.Errorf("registration: %w", err)
	}

	// Wait for ACK
	ackMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("waiting for ack: %w", err)
	}
	ack, ok := ackMsg.Payload.(*pb.CoordinatorMessage_Ack)
	if !ok || !ack.Ack.Accepted {
		msg := "rejected"
		if ok {
			msg = ack.Ack.Message
		}
		return fmt.Errorf("coordinator rejected registration: %s", msg)
	}
	w.log.Info("Registration acknowledged", "msg", ack.Ack.Message)

	// Start heartbeat goroutine
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.heartbeatLoop(hbCtx, stream)

	// Result sender goroutine
	resultCh := make(chan *pb.TaskResult, 64)
	go w.resultSender(hbCtx, stream, resultCh)

	// Main receive loop
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			w.log.Info("Coordinator closed stream")
			return nil
		}
		if err != nil {
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.CoordinatorMessage_Task:
			task := p.Task
			w.log.Info("Received task", "task_id", task.TaskId, "stage", task.StageId)
			go w.executeTask(ctx, task, resultCh)

		case *pb.CoordinatorMessage_Cancel:
			if cancel, ok := w.cancelRunning.Load(p.Cancel.TaskId); ok {
				w.log.Info("Cancelling task", "task_id", p.Cancel.TaskId, "reason", p.Cancel.Reason)
				cancel.(context.CancelFunc)()
			}

		case *pb.CoordinatorMessage_Drain:
			w.log.Info("Drain command received", "reason", p.Drain.Reason)
			// Signal the coordinator we are draining
			stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_Drain{
					Drain: &pb.DrainRequest{WorkerId: w.id, Reason: "drain acknowledged"},
				},
			})
			// Let in-flight tasks finish, then return
			w.waitForIdle()
			return nil
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Task execution
// ─────────────────────────────────────────────────────────────────────────────

func (w *Worker) executeTask(ctx context.Context, spec *pb.TaskSpec, resultCh chan<- *pb.TaskResult) {
	// Acquire concurrency slot
	select {
	case w.sem <- struct{}{}:
		defer func() { <-w.sem }()
	case <-ctx.Done():
		return
	}

	w.activeTasks.Add(1)
	defer w.activeTasks.Add(-1)

	start := time.Now()

	// Per-task cancellable context
	timeout := w.cfg.Execution.DefaultTaskTimeout.Duration
	if spec.TimeoutSeconds > 0 {
		timeout = time.Duration(spec.TimeoutSeconds) * time.Second
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	w.cancelRunning.Store(spec.TaskId, cancel)
	defer w.cancelRunning.Delete(spec.TaskId)

	result := w.runSubprocess(taskCtx, spec)
	result.DurationMs = time.Since(start).Milliseconds()
	result.WorkerId = w.id

	resultCh <- result
}

func (w *Worker) runSubprocess(ctx context.Context, spec *pb.TaskSpec) *pb.TaskResult {
	base := &pb.TaskResult{
		TaskId:  spec.TaskId,
		JobId:   spec.JobId,
		StageId: spec.StageId,
	}

	// Write ephemeral code to temp file if provided
	var tempFile string
	if len(spec.Code) > 0 {
		f, err := os.CreateTemp(w.tempDir(), "rivage-task-*")
		if err != nil {
			return failed(base, fmt.Sprintf("creating temp file: %v", err))
		}
		if _, err := f.Write(spec.Code); err != nil {
			f.Close()
			os.Remove(f.Name())
			return failed(base, fmt.Sprintf("writing code to temp file: %v", err))
		}
		f.Close()
		os.Chmod(f.Name(), 0700)
		tempFile = f.Name()
		defer os.Remove(tempFile)
	}

	// Resolve args (substitute {CODE} placeholder)
	args := make([]string, len(spec.Args))
	for i, a := range spec.Args {
		if a == "{CODE}" && tempFile != "" {
			args[i] = tempFile
		} else {
			args[i] = a
		}
	}

	// Resolve command (if command is {CODE}, run the temp file as a binary)
	command := spec.Command
	if command == "{CODE}" && tempFile != "" {
		command = tempFile
	}

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = bytes.NewReader(spec.InputData)

	// Set environment
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	w.log.Debug("Executing", "cmd", command, "args", args, "task", spec.TaskId)

	if err := cmd.Run(); err != nil {
		errMsg := fmt.Sprintf("subprocess error: %v\nSTDERR:\n%s", err, stderr.String())
		w.log.Warn("Task failed", "task", spec.TaskId, "err", err)
		return failed(base, errMsg)
	}

	return &pb.TaskResult{
		TaskId:     spec.TaskId,
		JobId:      spec.JobId,
		StageId:    spec.StageId,
		Status:     pb.TaskStatus_TASK_STATUS_COMPLETED,
		OutputData: stdout.Bytes(),
	}
}

func failed(base *pb.TaskResult, errLog string) *pb.TaskResult {
	base.Status = pb.TaskStatus_TASK_STATUS_FAILED
	base.ErrorLog = errLog
	return base
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat loop
// ─────────────────────────────────────────────────────────────────────────────

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
			cpu, _ := w.cpuGauge.Load().(float32)
			err := stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						WorkerId:    w.id,
						ActiveTasks: w.activeTasks.Load(),
						CpuUsage:    cpu,
						TimestampUnix: time.Now().Unix(),
					},
				},
			})
			if err != nil {
				w.log.Warn("Heartbeat send failed", "err", err)
				return
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Result sender
// ─────────────────────────────────────────────────────────────────────────────

func (w *Worker) resultSender(ctx context.Context, stream pb.WorkerService_ConnectClient, ch <-chan *pb.TaskResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-ch:
			if !ok {
				return
			}
			w.log.Info("Sending result", "task", result.TaskId, "status", result.Status)
			if err := stream.Send(&pb.WorkerMessage{
				Payload: &pb.WorkerMessage_Result{Result: result},
			}); err != nil {
				w.log.Error("Failed to send result", "task", result.TaskId, "err", err)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Registration
// ─────────────────────────────────────────────────────────────────────────────

func (w *Worker) register(stream pb.WorkerService_ConnectClient) error {
	var memBytes int64
	if si, err := getMemInfo(); err == nil {
		memBytes = si
	}

	caps := &pb.WorkerCapabilities{
		WorkerId:    w.id,
		CpuCores:    int32(runtime.NumCPU()),
		MemoryBytes: memBytes,
		Tags:        w.cfg.Tags,
	}

	// Include auth token if security is configured
	if w.cfg.Security.SharedSecret != "" {
		signer := security.NewTokenSigner(w.cfg.Security.SharedSecret)
		caps.WorkerToken = signer.Issue(w.id, 24*time.Hour)
	}

	hostname, _ := os.Hostname()
	caps.Hostname = hostname

	return stream.Send(&pb.WorkerMessage{
		Payload: &pb.WorkerMessage_Register{Register: caps},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Dial
// ─────────────────────────────────────────────────────────────────────────────

func (w *Worker) dial() (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

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

	//nolint:staticcheck — grpc.Dial is deprecated in newer versions but kept for compat
	return grpc.Dial(w.cfg.Coordinator.Addr, opts...)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func (w *Worker) tempDir() string {
	if d := w.cfg.Execution.TempDir; d != "" {
		return d
	}
	return os.TempDir()
}

func (w *Worker) waitForIdle() {
	for {
		if w.activeTasks.Load() == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func getMemInfo() (int64, error) {
	// Cross-platform approximate: on Linux read /proc/meminfo, fallback to 8GB
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 8 * 1024 * 1024 * 1024, nil
	}
	var total int64
	for _, line := range bytes.Split(data, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("MemTotal:")) {
			fmt.Sscanf(string(line), "MemTotal: %d kB", &total)
			return total * 1024, nil
		}
	}
	return 8 * 1024 * 1024 * 1024, nil
}

