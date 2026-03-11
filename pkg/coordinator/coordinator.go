// Package coordinator implements the Rivage coordinator (master) node.
package coordinator

import (
	"bytes"
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
	"rivage/pkg/store"
	"rivage/pkg/telemetry"
	pb "rivage/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcpeer "google.golang.org/grpc/peer"
)

type Coordinator struct {
	cfg       *config.CoordinatorConfig
	sched     scheduler.Scheduler
	signer    *security.TokenSigner
	log       *telemetry.Logger
	grpcSrv   *grpc.Server
	httpSrv   *http.Server
	startTime time.Time

	workers sync.Map
	jobs    sync.Map
	schedMu sync.Mutex
}

func New(cfg *config.CoordinatorConfig) (*Coordinator, error) {
	sched, err := scheduler.New(cfg.Scheduler.Algorithm)
	if err != nil {
		return nil, err
	}
	level := telemetry.ParseLevel(cfg.Telemetry.LogLevel)
	logger := telemetry.New("coordinator", level)
	c := &Coordinator{cfg: cfg, sched: sched, log: logger, startTime: time.Now()}
	if cfg.Security.Enabled {
		if cfg.Security.SharedSecret == "" {
			return nil, fmt.Errorf("security is enabled but shared_secret is empty")
		}
		c.signer = security.NewTokenSigner(cfg.Security.SharedSecret)
	}
	return c, nil
}

func (c *Coordinator) Start(ctx context.Context) error {
	maxMsgSize := 1024 * 1024 * 16
	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts,
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		grpc.WriteBufferSize(1024*1024*8), // NEW: 8MB TCP Write Buffer for max throughput
		grpc.ReadBufferSize(1024*1024*8),  // NEW: 8MB TCP Read Buffer for max throughput
	)

	if c.cfg.Security.Enabled && c.cfg.Security.TLSCertFile != "" {
		tlsCfg, err := security.LoadServerTLS(c.cfg.Security.TLSCertFile, c.cfg.Security.TLSKeyFile, c.cfg.Security.TLSCAFile)
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

	go c.watchdog(ctx)

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

// RunJob executes a pipeline and merges outputs to JSON.
func (c *Coordinator) RunJob(ctx context.Context, jobID string, pipeline *dag.Pipeline, inputChunks [][]byte) ([]byte, error) {
	outputs, err := c.RunJobRaw(ctx, jobID, pipeline, inputChunks)
	if err != nil {
		return nil, err
	}
	return mergeFinalOutputs(outputs)
}

// RunJobRaw executes a pipeline and returns the raw binary TaskOutputs (bypasses JSON merge).
func (c *Coordinator) RunJobRaw(ctx context.Context, jobID string, pipeline *dag.Pipeline, inputChunks [][]byte) ([]dag.TaskOutput, error) {
	c.log.Info("Starting job", "job_id", jobID, "pipeline", pipeline.Name, "chunks", len(inputChunks))

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

	stageOutputs := map[string][]dag.TaskOutput{}

	for i, stageID := range pipeline.Order {
		stage := pipeline.StageByID(stageID)
		c.log.Info("Starting stage", "job_id", jobID, "stage", stageID, "index", i)

		var tasks []*pb.TaskSpec
		if i == 0 {
			tasks = c.buildSourceTasks(jobID, stage, inputChunks, codeCache)
		} else {
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

		outputs, err := c.executeStage(ctx, jobID, stage, tasks)
		if err != nil {
			return nil, fmt.Errorf("stage %q failed: %w", stageID, err)
		}
		stageOutputs[stageID] = outputs
		c.log.Info("Stage completed", "job_id", jobID, "stage", stageID, "tasks", len(outputs))
	}

	lastStageID := pipeline.Order[len(pipeline.Order)-1]
	return stageOutputs[lastStageID], nil
}

func (c *Coordinator) executeStage(ctx context.Context, jobID string, stage *dag.Stage, tasks []*pb.TaskSpec) ([]dag.TaskOutput, error) {
	rs, err := store.New("")
	if err != nil {
		return nil, fmt.Errorf("creating result store: %w", err)
	}
	defer rs.Close()

	j := newJob(jobID+"/"+stage.ID, tasks, rs)
	c.jobs.Store(j.id, j)
	defer c.jobs.Delete(j.id)

	go c.watchJob(ctx, j)

	select {
	case <-j.allDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	outputs := make([]dag.TaskOutput, 0, len(tasks))
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, state := range j.tasks {
		if state.result == nil {
			return nil, fmt.Errorf("task %q has no result", state.spec.TaskId)
		}
		if state.result.Status == pb.TaskStatus_TASK_STATUS_FAILED {
			return nil, fmt.Errorf("task %q failed: %s", state.spec.TaskId, state.result.ErrorLog)
		}
		data, err := rs.Read(state.spec.TaskId)
		if err != nil {
			return nil, fmt.Errorf("reading result for %q: %w", state.spec.TaskId, err)
		}
		outputs = append(outputs, dag.TaskOutput{
			TaskID:  state.spec.TaskId,
			StageID: stage.ID,
			Data:    data,
		})
	}
	return outputs, nil
}

type taskStatus int32
const (
	statusPending taskStatus = iota
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
	tasks      map[string]*taskState
	totalTasks int
	doneTasks  atomic.Int32
	allDone    chan struct{}
	closed     atomic.Bool
	store      *store.ResultStore

	chunkMu         sync.Mutex
	chunkAssemblers map[string]*chunkAssembler
}

type chunkAssembler struct {
	buf     bytes.Buffer
	lastIdx int32
}

func newJob(id string, specs []*pb.TaskSpec, rs *store.ResultStore) *job {
	tasks := make(map[string]*taskState, len(specs))
	for _, s := range specs {
		tasks[s.TaskId] = &taskState{spec: s, status: statusPending}
	}
	return &job{
		id:              id,
		tasks:           tasks,
		totalTasks:      len(specs),
		allDone:         make(chan struct{}),
		store:           rs,
		chunkAssemblers: make(map[string]*chunkAssembler),
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

	if err := j.store.Write(result.TaskId, result.OutputData); err != nil {
		_ = err
	}
	result.OutputData = nil

	state.result = result
	if result.Status == pb.TaskStatus_TASK_STATUS_COMPLETED {
		state.status = statusCompleted
		telemetry.Global.TasksCompleted.Inc()
	} else {
		state.status = statusFailed
		telemetry.Global.TasksFailed.Inc()
	}

	telemetry.Global.ActiveTasks.Dec()

	done := j.doneTasks.Add(1)
	if int(done) >= j.totalTasks && !j.closed.Swap(true) {
		close(j.allDone)
	}
	return false
}

func (j *job) recordChunk(taskID, jobID, stageID, workerID string, idx int32, data []byte, isFinal bool) (done bool) {
	j.chunkMu.Lock()
	asm, exists := j.chunkAssemblers[taskID]
	if !exists {
		asm = &chunkAssembler{lastIdx: -1}
		j.chunkAssemblers[taskID] = asm
	}
	asm.buf.Write(data)
	asm.lastIdx = idx
	j.chunkMu.Unlock()

	if !isFinal {
		return false
	}

	j.chunkMu.Lock()
	assembled := asm.buf.Bytes()
	delete(j.chunkAssemblers, taskID)
	j.chunkMu.Unlock()

	result := &pb.TaskResult{
		TaskId:     taskID,
		JobId:      jobID,
		StageId:    stageID,
		WorkerId:   workerID,
		Status:     pb.TaskStatus_TASK_STATUS_COMPLETED,
		OutputData: assembled,
	}
	j.recordResult(result, 0)
	return true
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

func (c *Coordinator) buildSourceTasks(jobID string, stage *dag.Stage, chunks [][]byte, codeCache map[string][]byte) []*pb.TaskSpec {
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

func (c *Coordinator) buildShuffledTasks(jobID string, stage *dag.Stage, shuffled dag.ShuffleResult, codeCache map[string][]byte) []*pb.TaskSpec {
	maxRetries := int32(c.cfg.Scheduler.MaxGlobalRetries)
	if stage.MaxRetries >= 0 {
		maxRetries = int32(stage.MaxRetries)
	}
	tasks := make([]*pb.TaskSpec, 0, len(shuffled))
	for shuffleKey, payload := range shuffled {
		taskID := fmt.Sprintf("%s/%s-%s", jobID, stage.ID, shuffleKey)
		tasks = append(tasks, c.makeTaskSpec(jobID, stage, taskID, payload, codeCache, maxRetries))
	}
	return tasks
}

func (c *Coordinator) makeTaskSpec(jobID string, stage *dag.Stage, taskID string, inputData []byte, codeCache map[string][]byte, maxRetries int32) *pb.TaskSpec {
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
				c.reapDeadWorkerTasks(v.(*job))
				return true
			})
		}
	}
}

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
				
				// 1. Optimistically mark as running to prevent double-dispatch
				j.mu.Lock()
				state.status = statusRunning
				state.workerID = workerID
				state.dispatchedAt = time.Now()
				telemetry.Global.TasksDispatched.Inc()
				telemetry.Global.ActiveTasks.Inc()
				j.mu.Unlock()
				c.log.Debug("Dispatched task", "task", state.spec.TaskId, "worker", workerID)

				// 2. NEW: Asynchronous Parallel Dispatching
				// Smashes network bottlenecks by streaming to multiple workers simultaneously
				go func(wID string, tState *taskState) {
					if err := c.dispatch(wID, tState.spec); err != nil {
						c.log.Error("Dispatch failed", "worker", wID, "task", tState.spec.TaskId, "err", err)
						
						// Revert on failure
						j.mu.Lock()
						tState.status = statusPending
						tState.workerID = ""
						telemetry.Global.ActiveTasks.Dec()
						j.mu.Unlock()
					}
				}(workerID, state)
			}
		}
	}
}

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
			telemetry.Global.ActiveTasks.Dec()
			continue
		}
		ws := wsRaw.(*workerState)
		ws.mu.RLock()
		lastSeen := ws.lastHeartbeat
		ws.mu.RUnlock()
		if time.Since(lastSeen) > timeout {
			c.log.Warn("Worker heartbeat timeout, re-queuing task", "worker", state.workerID, "task", state.spec.TaskId)
			state.status = statusPending
			state.workerID = ""
			telemetry.Global.ActiveTasks.Dec()
		}
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

const dispatchChunkSize = 4 * 1024 * 1024

func (c *Coordinator) dispatch(workerID string, spec *pb.TaskSpec) error {
	wsRaw, ok := c.workers.Load(workerID)
	if !ok {
		return fmt.Errorf("worker %q not found", workerID)
	}
	ws := wsRaw.(*workerState)
	ws.mu.RLock()
	stream := ws.stream
	ws.mu.RUnlock()

	if len(spec.InputData) <= dispatchChunkSize {
		ws.sendMu.Lock()
		err := stream.Send(&pb.CoordinatorMessage{
			Payload: &pb.CoordinatorMessage_Task{Task: spec},
		})
		ws.sendMu.Unlock()
		return err
	}

	c.log.Debug("Chunked dispatch", "task", spec.TaskId, "input_bytes", len(spec.InputData))

	input := spec.InputData
	totalSize := int64(len(input))
	chunkIdx := int32(0)
	for offset := 0; offset < len(input); {
		end := offset + dispatchChunkSize
		if end > len(input) {
			end = len(input)
		}
		chunk := input[offset:end]
		isFinal := end == len(input)

		msg := &pb.ChunkedTaskSpec{
			ChunkIndex: chunkIdx,
			Data:       chunk,
			IsFinal:    isFinal,
		}
		if chunkIdx == 0 {
			msg.TaskId = spec.TaskId
			msg.JobId = spec.JobId
			msg.StageId = spec.StageId
			msg.TaskType = spec.TaskType
			msg.Command = spec.Command
			msg.Args = spec.Args
			msg.Code = spec.Code
			msg.Env = spec.Env
			msg.TimeoutSeconds = spec.TimeoutSeconds
			msg.MaxRetries = spec.MaxRetries
			msg.RetryCount = spec.RetryCount
			msg.RequiredTags = spec.RequiredTags
			msg.TotalSize = totalSize
		} else {
			msg.TaskId = spec.TaskId
		}

		ws.sendMu.Lock()
		err := stream.Send(&pb.CoordinatorMessage{
			Payload: &pb.CoordinatorMessage_ChunkedTask{ChunkedTask: msg},
		})
		ws.sendMu.Unlock()
		
		if err != nil {
			return fmt.Errorf("sending chunk %d for task %q: %w", chunkIdx, spec.TaskId, err)
		}
		offset = end
		chunkIdx++
	}
	return nil
}

type workerState struct {
	mu            sync.RWMutex
	sendMu        sync.Mutex // NEW: Serializes gRPC stream.Send to prevent concurrent write panics
	id            string
	caps          *pb.WorkerCapabilities
	stream        pb.WorkerService_ConnectServer
	lastHeartbeat time.Time
	activeTasks   atomic.Int32
	cpuUsage      atomic.Value
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

type grpcServer struct {
	pb.UnimplementedWorkerServiceServer
	coord *Coordinator
}

func (s *grpcServer) Connect(stream pb.WorkerService_ConnectServer) error {
	c := s.coord

	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	reg, ok := msg.Payload.(*pb.WorkerMessage_Register)
	if !ok {
		return fmt.Errorf("expected registration message")
	}
	caps := reg.Register

	if c.cfg.Security.Enabled && c.signer != nil {
		if _, err := c.signer.Verify(caps.GetWorkerToken()); err != nil {
			stream.Send(&pb.CoordinatorMessage{
				Payload: &pb.CoordinatorMessage_Ack{Ack: &pb.AckRegistration{
					Accepted: false, Message: "authentication failed: " + err.Error(),
				}},
			})
			return fmt.Errorf("worker auth failed: %w", err)
		}
	}

	p, _ := grpcpeer.FromContext(stream.Context())
	c.log.Info("Worker registered", "id", caps.WorkerId, "cores", caps.CpuCores, "addr", p.Addr)

	ws := &workerState{
		id:            caps.WorkerId,
		caps:          caps,
		stream:        stream,
		lastHeartbeat: time.Now(),
	}
	ws.cpuUsage.Store(float32(0))
	c.workers.Store(caps.WorkerId, ws)
	telemetry.Global.ActiveWorkers.Inc()

	stream.Send(&pb.CoordinatorMessage{
		Payload: &pb.CoordinatorMessage_Ack{Ack: &pb.AckRegistration{Accepted: true, Message: "welcome"}},
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

		case *pb.WorkerMessage_Result:
			result := p.Result
			ws.activeTasks.Add(-1)
			lookupKey := result.JobId + "/" + result.StageId
			if jobRaw, ok := c.jobs.Load(lookupKey); ok {
				jobRaw.(*job).recordResult(result, int32(c.cfg.Scheduler.MaxGlobalRetries))
			}

		case *pb.WorkerMessage_ChunkedResult:
			cr := p.ChunkedResult
			if cr.IsFinal {
				ws.activeTasks.Add(-1)
			}
			lookupKey := cr.JobId + "/" + cr.StageId
			if jobRaw, ok := c.jobs.Load(lookupKey); ok {
				jobRaw.(*job).recordChunk(cr.TaskId, cr.JobId, cr.StageId, cr.WorkerId, cr.ChunkIndex, cr.Data, cr.IsFinal)
			}

		case *pb.WorkerMessage_Drain:
			ws.mu.Lock()
			ws.draining = true
			ws.mu.Unlock()
		}
	}
}

func (c *Coordinator) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

func (c *Coordinator) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintln(w, `{"status": "ok"}`)
}

func (c *Coordinator) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "rivage_tasks_dispatched_total %d\n", telemetry.Global.TasksDispatched.Load())
	fmt.Fprintf(w, "rivage_tasks_completed_total %d\n", telemetry.Global.TasksCompleted.Load())
	fmt.Fprintf(w, "rivage_tasks_failed_total %d\n", telemetry.Global.TasksFailed.Load())
	fmt.Fprintf(w, "rivage_tasks_retried_total %d\n", telemetry.Global.TasksRetried.Load())
	fmt.Fprintf(w, "rivage_active_workers %d\n", telemetry.Global.ActiveWorkers.Load())
	fmt.Fprintf(w, "rivage_active_tasks %d\n", telemetry.Global.ActiveTasks.Load())
}

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
	merged := map[string]interface{}{}
	for _, out := range outputs {
		var obj map[string]interface{}
		if err := json.Unmarshal(out.Data, &obj); err != nil {
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
