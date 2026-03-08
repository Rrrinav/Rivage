// Package coordinator implements the Rivage coordinator (master) node.
//
// Usage:
//
//	cfg, _ := config.LoadCoordinatorConfig("coordinator.yaml")
//	pipeline, _ := dag.New("my-job").Stage(...).Stage(...).Build()
//	coord, _ := coordinator.New(cfg, pipeline)
//	coord.Start(ctx)
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"rivage/pkg/config"
	"rivage/pkg/dag"
	"rivage/pkg/scheduler"
	"rivage/pkg/security"
	"rivage/pkg/telemetry"
	pb "rivage/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcpeer "google.golang.org/grpc/peer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Coordinator — public API
// ─────────────────────────────────────────────────────────────────────────────

// Coordinator is the central orchestrator. Create one with New().
type Coordinator struct {
	cfg       *config.CoordinatorConfig
	sched     scheduler.Scheduler
	signer    *security.TokenSigner
	log       *telemetry.Logger
	grpcSrv   *grpc.Server
	httpSrv   *http.Server
	startTime time.Time

	// worker registry
	workers sync.Map // workerID -> *workerState

	// job registry
	jobs sync.Map // jobID/stageID -> *job

	// serialises the round-robin index inside the scheduler
	schedMu sync.Mutex
}

// New creates a Coordinator from config. The pipeline is not required here;
// pipelines are submitted per-job via RunJob.
func New(cfg *config.CoordinatorConfig) (*Coordinator, error) {
	sched, err := scheduler.New(cfg.Scheduler.Algorithm)
	if err != nil {
		return nil, err
	}

	level := telemetry.ParseLevel(cfg.Telemetry.LogLevel)
	logger := telemetry.New("coordinator", level)

	c := &Coordinator{
		cfg:       cfg,
		sched:     sched,
		log:       logger,
		startTime: time.Now(),
	}

	if cfg.Security.Enabled {
		if cfg.Security.SharedSecret == "" {
			return nil, fmt.Errorf("security is enabled but shared_secret is empty")
		}
		c.signer = security.NewTokenSigner(cfg.Security.SharedSecret)
	}

	return c, nil
}

// Start launches the gRPC and HTTP servers and blocks until ctx is cancelled.
func (c *Coordinator) Start(ctx context.Context) error {
	// Build gRPC server options
	var grpcOpts []grpc.ServerOption
	if c.cfg.Security.Enabled && c.cfg.Security.TLSCertFile != "" {
		tlsCfg, err := security.LoadServerTLS(
			c.cfg.Security.TLSCertFile,
			c.cfg.Security.TLSKeyFile,
			c.cfg.Security.TLSCAFile,
		)
		if err != nil {
			return fmt.Errorf("loading TLS: %w", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	} else {
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	c.grpcSrv = grpc.NewServer(grpcOpts...)
	pb.RegisterWorkerServiceServer(c.grpcSrv, &grpcServer{coord: c})

	lis, err := net.Listen("tcp", c.cfg.Server.GRPCAddr)
	if err != nil {
		return fmt.Errorf("gRPC listen %s: %w", c.cfg.Server.GRPCAddr, err)
	}
	c.log.Info("gRPC server listening", "addr", c.cfg.Server.GRPCAddr)

	// HTTP admin server
	if c.cfg.Server.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", c.handleHealthz)
		mux.HandleFunc("/status", c.handleStatus)
		mux.HandleFunc("/metrics", c.handleMetrics)
		c.httpSrv = &http.Server{Addr: c.cfg.Server.HTTPAddr, Handler: mux}
		go func() {
			c.log.Info("HTTP admin server listening", "addr", c.cfg.Server.HTTPAddr)
			if err := c.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				c.log.Error("HTTP server error", "err", err)
			}
		}()
	}

	// Start watchdog
	go c.watchdog(ctx)

	// Serve gRPC (blocking) in a goroutine; wait for ctx
	serveErr := make(chan error, 1)
	go func() { serveErr <- c.grpcSrv.Serve(lis) }()

	select {
	case <-ctx.Done():
		c.log.Info("Coordinator shutting down gracefully")
		c.grpcSrv.GracefulStop()
		if c.httpSrv != nil {
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c.httpSrv.Shutdown(shutCtx)
		}
		return nil
	case err := <-serveErr:
		return err
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RunJob — submit and execute a full pipeline job
// ─────────────────────────────────────────────────────────────────────────────

// RunJob executes a full pipeline against the given input chunks and returns
// the final aggregated output as a JSON-marshalled byte slice.
func (c *Coordinator) RunJob(ctx context.Context, jobID string, pipeline *dag.Pipeline, inputChunks [][]byte) ([]byte, error) {
	c.log.Info("Starting job", "job_id", jobID, "pipeline", pipeline.Name, "chunks", len(inputChunks))

	// Load all code files referenced by executors upfront
	codeCache := map[string][]byte{}
	for _, stageID := range pipeline.Order {
		stage := pipeline.StageByID(stageID)
		if cf := stage.Executor.CodeFile; cf != "" {
			if _, loaded := codeCache[cf]; !loaded {
				data, err := os.ReadFile(cf)
				if err != nil {
					return nil, fmt.Errorf("reading code file %q for stage %q: %w", cf, stageID, err)
				}
				codeCache[cf] = data
				c.log.Debug("Loaded code file", "file", cf, "bytes", len(data))
			}
		}
	}

	// Execute stages in topological order
	// stageOutputs accumulates completed outputs keyed by stageID
	stageOutputs := map[string][]dag.TaskOutput{}

	for i, stageID := range pipeline.Order {
		stage := pipeline.StageByID(stageID)
		c.log.Info("Starting stage", "job_id", jobID, "stage", stageID, "index", i)

		// Determine input tasks for this stage
		var tasks []*pb.TaskSpec
		if i == 0 {
			// Source stage: one task per input chunk
			tasks = c.buildSourceTasks(jobID, stage, inputChunks, codeCache)
		} else {
			// Non-source stage: apply shuffle from upstream outputs
			upstreamOutputs := collectUpstreamOutputs(stageOutputs, stage.DependsOn)
			shuffled, err := stage.Shuffle(upstreamOutputs)
			if err != nil {
				return nil, fmt.Errorf("shuffle for stage %q: %w", stageID, err)
			}
			tasks = c.buildShuffledTasks(jobID, stage, shuffled, codeCache)
		}

		if len(tasks) == 0 {
			c.log.Warn("Stage produced zero tasks, skipping", "stage", stageID)
			stageOutputs[stageID] = []dag.TaskOutput{}
			continue
		}

		// Submit and wait for all tasks in this stage
		outputs, err := c.executeStage(ctx, jobID, stage, tasks)
		if err != nil {
			return nil, fmt.Errorf("stage %q failed: %w", stageID, err)
		}
		stageOutputs[stageID] = outputs
		c.log.Info("Stage completed", "job_id", jobID, "stage", stageID, "tasks", len(outputs))
	}

	// The final stage's outputs become the job result
	lastStageID := pipeline.Order[len(pipeline.Order)-1]
	finalOutputs := stageOutputs[lastStageID]

	// Merge final outputs into a single JSON object/array
	return mergeFinalOutputs(finalOutputs)
}

// ─────────────────────────────────────────────────────────────────────────────
// Stage execution
// ─────────────────────────────────────────────────────────────────────────────

func (c *Coordinator) executeStage(
	ctx context.Context,
	jobID string,
	stage *dag.Stage,
	tasks []*pb.TaskSpec,
) ([]dag.TaskOutput, error) {
	j := newJob(jobID+"/"+stage.ID, tasks)
	c.jobs.Store(j.id, j)
	defer c.jobs.Delete(j.id)

	// Start the watchdog for this job
	go c.watchJob(ctx, j)

	// Wait for completion or context cancellation
	select {
	case <-j.allDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Collect results, fail fast on any error
	outputs := make([]dag.TaskOutput, 0, len(tasks))
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, state := range j.tasks {
		if state.result == nil {
			return nil, fmt.Errorf("task %q has no result (internal error)", state.spec.TaskId)
		}
		if state.result.Status == pb.TaskStatus_TASK_STATUS_FAILED {
			return nil, fmt.Errorf("task %q failed: %s", state.spec.TaskId, state.result.ErrorLog)
		}
		outputs = append(outputs, dag.TaskOutput{
			TaskID:  state.spec.TaskId,
			StageID: stage.ID,
			Data:    state.result.OutputData,
		})
	}
	return outputs, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Task builders
// ─────────────────────────────────────────────────────────────────────────────

func (c *Coordinator) buildSourceTasks(
	jobID string,
	stage *dag.Stage,
	chunks [][]byte,
	codeCache map[string][]byte,
) []*pb.TaskSpec {
	maxRetries := int32(c.cfg.Scheduler.MaxGlobalRetries)
	if stage.MaxRetries >= 0 {
		maxRetries = int32(stage.MaxRetries)
	}
	tasks := make([]*pb.TaskSpec, len(chunks))
	for i, chunk := range chunks {
		tasks[i] = c.makeTaskSpec(jobID, stage, fmt.Sprintf("%s/%s-%d", jobID, stage.ID, i), chunk, codeCache, maxRetries)
	}
	return tasks
}

func (c *Coordinator) buildShuffledTasks(
	jobID string,
	stage *dag.Stage,
	shuffled dag.ShuffleResult,
	codeCache map[string][]byte,
) []*pb.TaskSpec {
	maxRetries := int32(c.cfg.Scheduler.MaxGlobalRetries)
	if stage.MaxRetries >= 0 {
		maxRetries = int32(stage.MaxRetries)
	}
	tasks := make([]*pb.TaskSpec, 0, len(shuffled))
	i := 0
	for shuffleKey, payload := range shuffled {
		taskID := fmt.Sprintf("%s/%s-%s", jobID, stage.ID, shuffleKey)
		tasks = append(tasks, c.makeTaskSpec(jobID, stage, taskID, payload, codeCache, maxRetries))
		i++
	}
	return tasks
}

func (c *Coordinator) makeTaskSpec(
	jobID string,
	stage *dag.Stage,
	taskID string,
	inputData []byte,
	codeCache map[string][]byte,
	maxRetries int32,
) *pb.TaskSpec {
	exec := stage.Executor
	timeout := c.cfg.Scheduler.TaskTimeout.Seconds()
	if stage.TimeoutSecs > 0 {
		timeout = float64(stage.TimeoutSecs)
	}

	env := make(map[string]string, len(exec.Env))
	for k, v := range exec.Env {
		env[k] = v
	}

	spec := &pb.TaskSpec{
		TaskId:         taskID,
		JobId:          jobID,
		StageId:        stage.ID,
		Command:        exec.Command,
		Args:           exec.Args,
		InputData:      inputData,
		Env:            env,
		TimeoutSeconds: int64(timeout),
		MaxRetries:     maxRetries,
		RequiredTags:   exec.RequiredTags,
	}
	if exec.CodeFile != "" {
		spec.Code = codeCache[exec.CodeFile]
	}
	return spec
}

// ─────────────────────────────────────────────────────────────────────────────
// Job state machine
// ─────────────────────────────────────────────────────────────────────────────

type taskStatus int32

const (
	statusPending   taskStatus = iota
	statusRunning
	statusCompleted
	statusFailed
)

type taskState struct {
	spec         *pb.TaskSpec
	status       taskStatus
	workerID     string
	dispatchedAt time.Time
	retries      int32
	result       *pb.TaskResult
}

type job struct {
	id         string
	mu         sync.Mutex
	tasks      map[string]*taskState // taskID -> state
	totalTasks int
	doneTasks  atomic.Int32
	allDone    chan struct{}
	closed     atomic.Bool
}

func newJob(id string, specs []*pb.TaskSpec) *job {
	tasks := make(map[string]*taskState, len(specs))
	for _, s := range specs {
		tasks[s.TaskId] = &taskState{spec: s, status: statusPending}
	}
	return &job{
		id:         id,
		tasks:      tasks,
		totalTasks: len(specs),
		allDone:    make(chan struct{}),
	}
}

func (j *job) recordResult(result *pb.TaskResult, maxRetries int32) (retry bool) {
	j.mu.Lock()
	defer j.mu.Unlock()

	state, ok := j.tasks[result.TaskId]
	if !ok || state.status == statusCompleted {
		return false
	}

	if result.Status == pb.TaskStatus_TASK_STATUS_FAILED && state.retries < maxRetries {
		state.retries++
		state.status = statusPending
		state.workerID = ""
		telemetry.Global.TasksRetried.Inc()
		return true
	}

	state.result = result
	if result.Status == pb.TaskStatus_TASK_STATUS_COMPLETED {
		state.status = statusCompleted
		telemetry.Global.TasksCompleted.Inc()
	} else {
		state.status = statusFailed
		telemetry.Global.TasksFailed.Inc()
	}

	done := j.doneTasks.Add(1)
	if int(done) >= j.totalTasks && !j.closed.Swap(true) {
		close(j.allDone)
	}
	return false
}

func (j *job) snapshotPending() []*taskState {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*taskState
	for _, s := range j.tasks {
		if s.status == statusPending {
			out = append(out, s)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// Watchdog
// ─────────────────────────────────────────────────────────────────────────────

// watchdog scans ALL active jobs for stuck tasks.
func (c *Coordinator) watchdog(ctx context.Context) {
	interval := c.cfg.Scheduler.WatchdogInterval.Duration
	if interval == 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.jobs.Range(func(_, v interface{}) bool {
				j := v.(*job)
				c.reapDeadWorkerTasks(j)
				return true
			})
		}
	}
}

// watchJob dispatches pending tasks for a specific job.
func (c *Coordinator) watchJob(ctx context.Context, j *job) {
	interval := c.cfg.Scheduler.WatchdogInterval.Duration
	if interval == 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-j.allDone:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending := j.snapshotPending()
			for _, state := range pending {
				workerID, err := c.pickWorker(state.spec.RequiredTags)
				if err != nil {
					c.log.Warn("No worker available", "task", state.spec.TaskId, "err", err)
					break
				}
				if err := c.dispatch(workerID, state.spec); err != nil {
					c.log.Error("Dispatch failed", "worker", workerID, "task", state.spec.TaskId, "err", err)
					continue
				}
				j.mu.Lock()
				state.status = statusRunning
				state.workerID = workerID
				state.dispatchedAt = time.Now()
				j.mu.Unlock()
				telemetry.Global.TasksDispatched.Inc()
				c.log.Debug("Dispatched task", "task", state.spec.TaskId, "worker", workerID)
			}
		}
	}
}

// reapDeadWorkerTasks marks running tasks as pending when their worker dies.
func (c *Coordinator) reapDeadWorkerTasks(j *job) {
	timeout := c.cfg.Scheduler.HeartbeatTimeout.Duration
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, state := range j.tasks {
		if state.status != statusRunning {
			continue
		}
		wsRaw, ok := c.workers.Load(state.workerID)
		if !ok {
			c.log.Warn("Worker gone, re-queuing task", "worker", state.workerID, "task", state.spec.TaskId)
			state.status = statusPending
			state.workerID = ""
			continue
		}
		ws := wsRaw.(*workerState)
		ws.mu.RLock()
		lastSeen := ws.lastHeartbeat
		ws.mu.RUnlock()
		if time.Since(lastSeen) > timeout {
			c.log.Warn("Worker heartbeat timeout, re-queuing task",
				"worker", state.workerID, "task", state.spec.TaskId)
			state.status = statusPending
			state.workerID = ""
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker registry
// ─────────────────────────────────────────────────────────────────────────────

type workerState struct {
	mu            sync.RWMutex
	id            string
	caps          *pb.WorkerCapabilities
	stream        pb.WorkerService_ConnectServer
	lastHeartbeat time.Time
	activeTasks   atomic.Int32
	cpuUsage      atomic.Value // float32
	draining      bool
}

func (ws *workerState) snapshot() scheduler.WorkerSnapshot {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	cpu, _ := ws.cpuUsage.Load().(float32)
	return scheduler.WorkerSnapshot{
		ID:            ws.id,
		Tags:          ws.caps.Tags,
		ActiveTasks:   int(ws.activeTasks.Load()),
		CPUUsage:      cpu,
		LastHeartbeat: ws.lastHeartbeat,
	}
}

func (c *Coordinator) pickWorker(requiredTags []string) (string, error) {
	timeout := c.cfg.Scheduler.HeartbeatTimeout.Duration
	var snapshots []scheduler.WorkerSnapshot
	c.workers.Range(func(_, v interface{}) bool {
		ws := v.(*workerState)
		ws.mu.RLock()
		alive := time.Since(ws.lastHeartbeat) <= timeout && !ws.draining
		ws.mu.RUnlock()
		if alive {
			snapshots = append(snapshots, ws.snapshot())
		}
		return true
	})
	if len(snapshots) == 0 {
		return "", fmt.Errorf("no workers available")
	}
	return c.sched.Pick(snapshots, requiredTags)
}

func (c *Coordinator) dispatch(workerID string, spec *pb.TaskSpec) error {
	wsRaw, ok := c.workers.Load(workerID)
	if !ok {
		return fmt.Errorf("worker %q not found", workerID)
	}
	ws := wsRaw.(*workerState)
	ws.mu.RLock()
	stream := ws.stream
	ws.mu.RUnlock()
	return stream.Send(&pb.CoordinatorMessage{
		Payload: &pb.CoordinatorMessage_Task{Task: spec},
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// gRPC server implementation
// ─────────────────────────────────────────────────────────────────────────────

type grpcServer struct {
	pb.UnimplementedWorkerServiceServer
	coord *Coordinator
}

func (s *grpcServer) Connect(stream pb.WorkerService_ConnectServer) error {
	c := s.coord

	// First message must be a registration
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	reg, ok := msg.Payload.(*pb.WorkerMessage_Register)
	if !ok {
		return fmt.Errorf("expected registration message")
	}
	caps := reg.Register

	// Token auth (if security enabled)
	if c.cfg.Security.Enabled && c.signer != nil {
		token := caps.GetWorkerToken()
		if _, err := c.signer.Verify(token); err != nil {
			stream.Send(&pb.CoordinatorMessage{
				Payload: &pb.CoordinatorMessage_Ack{Ack: &pb.AckRegistration{
					Accepted: false,
					Message:  "authentication failed: " + err.Error(),
				}},
			})
			return fmt.Errorf("worker auth failed: %w", err)
		}
	}

	p, _ := grpcpeer.FromContext(stream.Context())
	c.log.Info("Worker registered",
		"id", caps.WorkerId, "cores", caps.CpuCores,
		"mem_mb", caps.MemoryBytes/1024/1024, "addr", p.Addr,
		"tags", caps.Tags)

	ws := &workerState{
		id:            caps.WorkerId,
		caps:          caps,
		stream:        stream,
		lastHeartbeat: time.Now(),
	}
	ws.cpuUsage.Store(float32(0))
	c.workers.Store(caps.WorkerId, ws)
	telemetry.Global.ActiveWorkers.Inc()

	// Ack
	stream.Send(&pb.CoordinatorMessage{
		Payload: &pb.CoordinatorMessage_Ack{Ack: &pb.AckRegistration{
			Accepted: true,
			Message:  "welcome",
		}},
	})

	defer func() {
		c.workers.Delete(caps.WorkerId)
		telemetry.Global.ActiveWorkers.Dec()
		c.log.Info("Worker disconnected", "id", caps.WorkerId)
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch p := msg.Payload.(type) {
		case *pb.WorkerMessage_Heartbeat:
			hb := p.Heartbeat
			ws.mu.Lock()
			ws.lastHeartbeat = time.Now()
			ws.mu.Unlock()
			ws.cpuUsage.Store(hb.CpuUsage)
			ws.activeTasks.Store(hb.ActiveTasks)
			c.log.Debug("Heartbeat", "worker", caps.WorkerId,
				"active_tasks", hb.ActiveTasks, "cpu", fmt.Sprintf("%.1f%%", hb.CpuUsage*100))

		case *pb.WorkerMessage_Result:
			result := p.Result
			ws.activeTasks.Add(-1)
			telemetry.Global.ActiveTasks.Dec()
			c.log.Debug("Task result received",
				"task", result.TaskId, "worker", caps.WorkerId,
				"status", result.Status, "duration_ms", result.DurationMs)

			// FIX: Lookup key must include the stage ID to match the store key
			lookupKey := result.JobId + "/" + result.StageId
			if jobRaw, ok := c.jobs.Load(lookupKey); ok {
				j := jobRaw.(*job)
				maxRetries := int32(c.cfg.Scheduler.MaxGlobalRetries)
				j.recordResult(result, maxRetries)
			} else {
				c.log.Warn("Received result for unknown job/stage", "lookup_key", lookupKey)
			}

		case *pb.WorkerMessage_Drain:
			c.log.Info("Worker requesting drain", "id", caps.WorkerId, "reason", p.Drain.Reason)
			ws.mu.Lock()
			ws.draining = true
			ws.mu.Unlock()
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP admin handlers
// ─────────────────────────────────────────────────────────────────────────────

func (c *Coordinator) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

func (c *Coordinator) handleStatus(w http.ResponseWriter, r *http.Request) {
	type workerInfo struct {
		ID          string   `json:"id"`
		Tags        []string `json:"tags"`
		ActiveTasks int32    `json:"active_tasks"`
		CPUUsage    float32  `json:"cpu_usage"`
		LastSeen    string   `json:"last_seen"`
		Alive       bool     `json:"alive"`
	}
	type jobInfo struct {
		ID         string `json:"id"`
		TotalTasks int    `json:"total_tasks"`
		DoneTasks  int32  `json:"done_tasks"`
	}
	type response struct {
		UptimeSeconds int64        `json:"uptime_seconds"`
		Workers       []workerInfo `json:"workers"`
		ActiveJobs    []jobInfo    `json:"active_jobs"`
		Metrics       interface{}  `json:"metrics"`
	}

	timeout := c.cfg.Scheduler.HeartbeatTimeout.Duration
	var workerInfos []workerInfo
	c.workers.Range(func(_, v interface{}) bool {
		ws := v.(*workerState)
		snap := ws.snapshot()
		workerInfos = append(workerInfos, workerInfo{
			ID:          snap.ID,
			Tags:        snap.Tags,
			ActiveTasks: ws.activeTasks.Load(),
			CPUUsage:    snap.CPUUsage,
			LastSeen:    snap.LastHeartbeat.Format(time.RFC3339),
			Alive:       time.Since(snap.LastHeartbeat) <= timeout,
		})
		return true
	})

	var jobInfos []jobInfo
	c.jobs.Range(func(_, v interface{}) bool {
		j := v.(*job)
		jobInfos = append(jobInfos, jobInfo{
			ID:         j.id,
			TotalTasks: j.totalTasks,
			DoneTasks:  j.doneTasks.Load(),
		})
		return true
	})

	resp := response{
		UptimeSeconds: int64(time.Since(c.startTime).Seconds()),
		Workers:       workerInfos,
		ActiveJobs:    jobInfos,
		Metrics: map[string]int64{
			"tasks_dispatched": telemetry.Global.TasksDispatched.Load(),
			"tasks_completed":  telemetry.Global.TasksCompleted.Load(),
			"tasks_failed":     telemetry.Global.TasksFailed.Load(),
			"tasks_retried":    telemetry.Global.TasksRetried.Load(),
			"active_workers":   telemetry.Global.ActiveWorkers.Load(),
			"active_tasks":     telemetry.Global.ActiveTasks.Load(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (c *Coordinator) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Prometheus text format
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "rivage_tasks_dispatched_total %d\n", telemetry.Global.TasksDispatched.Load())
	fmt.Fprintf(w, "rivage_tasks_completed_total %d\n", telemetry.Global.TasksCompleted.Load())
	fmt.Fprintf(w, "rivage_tasks_failed_total %d\n", telemetry.Global.TasksFailed.Load())
	fmt.Fprintf(w, "rivage_tasks_retried_total %d\n", telemetry.Global.TasksRetried.Load())
	fmt.Fprintf(w, "rivage_active_workers %d\n", telemetry.Global.ActiveWorkers.Load())
	fmt.Fprintf(w, "rivage_active_tasks %d\n", telemetry.Global.ActiveTasks.Load())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func collectUpstreamOutputs(stageOutputs map[string][]dag.TaskOutput, deps []string) []dag.TaskOutput {
	var all []dag.TaskOutput
	for _, dep := range deps {
		all = append(all, stageOutputs[dep]...)
	}
	return all
}

func mergeFinalOutputs(outputs []dag.TaskOutput) ([]byte, error) {
	if len(outputs) == 0 {
		return []byte("{}"), nil
	}
	if len(outputs) == 1 {
		return outputs[0].Data, nil
	}
	// Merge all outputs: if they're JSON objects, merge keys; otherwise wrap in array
	merged := map[string]interface{}{}
	for _, out := range outputs {
		var obj map[string]interface{}
		if err := json.Unmarshal(out.Data, &obj); err != nil {
			// Not a JSON object — fall back to array
			items := make([]json.RawMessage, len(outputs))
			for i, o := range outputs {
				items[i] = o.Data
			}
			return json.MarshalIndent(items, "", "  ")
		}
		for k, v := range obj {
			merged[k] = v
		}
	}
	return json.MarshalIndent(merged, "", "  ")
}

// suppress unused import warning
// var _ = log.Printf
