package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "dist-system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const port = ":50051"
const heartbeatTimeout = 10 * time.Second

// -----------------------------------------------------------------
// Job tracking & Fault Tolerance State Machine
// -----------------------------------------------------------------

type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusRunning
	StatusCompleted
)

type TaskState struct {
	task         *pb.Task
	status       TaskStatus
	workerID     string
	dispatchedAt time.Time
	result       *pb.TaskResult
}

type Job struct {
	id         string
	totalTasks int
	tasks      map[string]*TaskState
	mu         sync.Mutex
	allDone    chan struct{}
	isDone     bool
}

func newJob(id string, tasks []*pb.Task) *Job {
	jobTasks := make(map[string]*TaskState, len(tasks))
	for _, t := range tasks {
		jobTasks[t.GetTaskId()] = &TaskState{
			task:   t,
			status: StatusPending,
		}
	}
	return &Job{
		id:         id,
		totalTasks: len(tasks),
		tasks:      jobTasks,
		allDone:    make(chan struct{}),
	}
}

func (j *Job) recordResult(result *pb.TaskResult) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if state, ok := j.tasks[result.GetTaskId()]; ok {
		if state.status != StatusCompleted {
			state.result = result
			state.status = StatusCompleted
		}
	}

	allCompleted := true
	for _, s := range j.tasks {
		if s.status != StatusCompleted {
			allCompleted = false
			break
		}
	}
	
	if allCompleted && !j.isDone {
		j.isDone = true
		close(j.allDone)
	}
}

func (j *Job) results() []*pb.TaskResult {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]*pb.TaskResult, 0, len(j.tasks))
	for _, s := range j.tasks {
		out = append(out, s.result)
	}
	return out
}

// watchAndDispatch uses Worker Health to determine task failure
func (j *Job) watchAndDispatch(s *coordinatorServer) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-j.allDone:
			return 
			
		case <-ticker.C:
			j.mu.Lock()
			var pendingTasks []*TaskState

			for _, state := range j.tasks {
				// 1. Fault Tolerance Check: Is the worker assigned to this task still alive?
				if state.status == StatusRunning {
					lastSeenIntf, ok := s.workerHeartbeats.Load(state.workerID)
					if !ok || time.Since(lastSeenIntf.(time.Time)) > heartbeatTimeout {
						log.Printf("[Watchdog] ⚠️ Worker %s missed heartbeats. Marking Task %s as Pending.", state.workerID, state.task.GetTaskId())
						state.status = StatusPending
						state.workerID = "" 
					}
				}

				// 2. Gather Pending Tasks
				if state.status == StatusPending {
					pendingTasks = append(pendingTasks, state)
				}
			}
			j.mu.Unlock()

			// 3. Dispatch Phase
			for _, state := range pendingTasks {
				workerID, ok := s.pickWorker()
				if !ok {
					break 
				}

				err := s.sendTask(workerID, state.task)
				
				j.mu.Lock()
				if err == nil {
					state.status = StatusRunning
					state.workerID = workerID
					state.dispatchedAt = time.Now()
					log.Printf("[Coordinator] Dispatched task %s to %s", state.task.GetTaskId(), workerID)
				} else {
					log.Printf("[Coordinator] ❌ Failed to dispatch to %s: %v", workerID, err)
					s.removeWorker(workerID)
				}
				j.mu.Unlock()
			}
		}
	}
}

// -----------------------------------------------------------------
// Coordinator server
// -----------------------------------------------------------------

type coordinatorServer struct {
	pb.UnimplementedCoordinatorServer

	workerStreams    sync.Map // workerID -> pb.Coordinator_RegisterAndStreamServer
	workerHeartbeats sync.Map // workerID -> time.Time (NEW)
	jobs             sync.Map // jobID -> *Job

	workerList []string
	rrMutex    sync.Mutex
	rrIndex    int
}

func (s *coordinatorServer) addWorker(id string) {
	s.rrMutex.Lock()
	defer s.rrMutex.Unlock()
	s.workerList = append(s.workerList, id)
}

func (s *coordinatorServer) removeWorker(id string) {
	s.rrMutex.Lock()
	defer s.rrMutex.Unlock()
	for i, w := range s.workerList {
		if w == id {
			s.workerList = append(s.workerList[:i], s.workerList[i+1:]...)
			break
		}
	}
}

func (s *coordinatorServer) RegisterAndStream(stream pb.Coordinator_RegisterAndStreamServer) error {
	regMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	regReq, ok := regMsg.GetPayload().(*pb.ExecutorMessage_Register)
	if !ok {
		return io.ErrUnexpectedEOF
	}

	workerID := regReq.Register.GetWorkerId()
	p, _ := peer.FromContext(stream.Context())
	log.Printf("[Coordinator] Worker registered: %s (Cores: %d) from %s",
		workerID, regReq.Register.GetCpuCores(), p.Addr)

	s.workerStreams.Store(workerID, stream)
	s.workerHeartbeats.Store(workerID, time.Now()) // Initial heartbeat
	s.addWorker(workerID)

	defer func() {
		s.workerStreams.Delete(workerID)
		s.workerHeartbeats.Delete(workerID)
		s.removeWorker(workerID)
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[Coordinator] Worker %s cleanly disconnected.", workerID)
			return nil
		}
		if err != nil {
			log.Printf("[Coordinator] Error from %s: %v", workerID, err)
			return err
		}

		// Handle incoming messages by type
		switch payload := msg.GetPayload().(type) {
		case *pb.ExecutorMessage_Heartbeat:
			// Update the radar!
			s.workerHeartbeats.Store(workerID, time.Now())
			
		case *pb.ExecutorMessage_Result:
			result := payload.Result
			log.Printf("[Coordinator] Result for Task %s from %s: success=%t",
				result.GetTaskId(), workerID, result.GetSuccess())

			jobID := result.GetJobId()
			if jobVal, ok := s.jobs.Load(jobID); ok {
				jobVal.(*Job).recordResult(result)
			}
		}
	}
}

func (s *coordinatorServer) sendTask(workerID string, task *pb.Task) error {
	val, ok := s.workerStreams.Load(workerID)
	if !ok {
		return fmt.Errorf("worker %s not found", workerID)
	}
	stream := val.(pb.Coordinator_RegisterAndStreamServer)
	return stream.Send(&pb.CoordinatorMessage{Task: task})
}

func (s *coordinatorServer) pickWorker() (string, bool) {
	s.rrMutex.Lock()
	defer s.rrMutex.Unlock()

	// Before returning a worker, make sure they haven't ghosted us
	for len(s.workerList) > 0 {
		id := s.workerList[s.rrIndex%len(s.workerList)]
		
		lastSeenIntf, ok := s.workerHeartbeats.Load(id)
		if ok && time.Since(lastSeenIntf.(time.Time)) <= heartbeatTimeout {
			s.rrIndex++
			return id, true
		}
		
		// Worker is dead but hasn't fully disconnected yet, remove them
		s.rrMutex.Unlock() // Unlock temporarily to call removeWorker safely
		s.removeWorker(id)
		s.rrMutex.Lock()   // Re-lock
	}

	return "", false
}

// -----------------------------------------------------------------
// MapReduce orchestration
// -----------------------------------------------------------------

func (s *coordinatorServer) MapReduceJob(jobID string, inputChunks [][]byte, mapScript, reduceScript string) ([]byte, error) {

	log.Printf("[Coordinator] [%s] Starting MAP phase (%d chunks)", jobID, len(inputChunks))

	var mapTasks []*pb.Task
	for i, chunk := range inputChunks {
		mapTasks = append(mapTasks, &pb.Task{
			TaskId:    fmt.Sprintf("%s-map-%d", jobID, i),
			JobId:     jobID + "-map",
			TaskType:  pb.TaskType_MAP,
			Command:   "python3",
			Args:      []string{mapScript},
			InputData: chunk,
		})
	}

	mapJob := newJob(jobID+"-map", mapTasks)
	s.jobs.Store(mapJob.id, mapJob)

	go mapJob.watchAndDispatch(s)

	<-mapJob.allDone
	log.Printf("[Coordinator] [%s] MAP phase complete.", jobID)

	merged := map[string][]interface{}{}
	for _, result := range mapJob.results() {
		if !result.GetSuccess() {
			return nil, fmt.Errorf("map task %s failed: %s", result.GetTaskId(), result.GetErrorLog())
		}
		var partial map[string]interface{}
		if err := json.Unmarshal(result.GetOutputData(), &partial); err != nil {
			return nil, fmt.Errorf("bad map output for %s: %w", result.GetTaskId(), err)
		}
		for k, v := range partial {
			merged[k] = append(merged[k], v)
		}
	}
	log.Printf("[Coordinator] [%s] Shuffle complete. Unique keys: %d", jobID, len(merged))

	log.Printf("[Coordinator] [%s] Starting REDUCE phase", jobID)

	var reduceTasks []*pb.Task
	i := 0
	for k, vals := range merged {
		payload, _ := json.Marshal(map[string]interface{}{
			"key":    k,
			"values": vals,
		})
		reduceTasks = append(reduceTasks, &pb.Task{
			TaskId:    fmt.Sprintf("%s-reduce-%d", jobID, i),
			JobId:     jobID + "-reduce",
			TaskType:  pb.TaskType_REDUCE,
			Command:   "python3",
			Args:      []string{reduceScript},
			InputData: payload,
		})
		i++
	}

	reduceJob := newJob(jobID+"-reduce", reduceTasks)
	s.jobs.Store(reduceJob.id, reduceJob)

	go reduceJob.watchAndDispatch(s)

	<-reduceJob.allDone
	log.Printf("[Coordinator] [%s] REDUCE phase complete.", jobID)

	final := map[string]interface{}{}
	for _, result := range reduceJob.results() {
		if !result.GetSuccess() {
			return nil, fmt.Errorf("reduce task %s failed: %s", result.GetTaskId(), result.GetErrorLog())
		}
		var partial map[string]interface{}
		if err := json.Unmarshal(result.GetOutputData(), &partial); err != nil {
			return nil, fmt.Errorf("bad reduce output: %w", err)
		}
		for k, v := range partial {
			final[k] = v
		}
	}

	return json.MarshalIndent(final, "", "  ")
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[Coordinator] Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	coordServer := &coordinatorServer{}
	pb.RegisterCoordinatorServer(grpcServer, coordServer)

	log.Printf("[Coordinator] Listening on %v", lis.Addr())

	go func() {
		time.Sleep(3 * time.Second)

		lines := []string{
			"the quick brown fox jumps over the lazy dog",
			"the fox ran quickly over the hill",
			"a lazy dog slept all day",
		}

		chunks := make([][]byte, len(lines))
		for i, line := range lines {
			chunks[i], _ = json.Marshal(map[string]interface{}{
				"chunk": []string{line},
			})
		}

		result, err := coordServer.MapReduceJob("job-1", chunks, "example-tasks/map.py", "example-tasks/reduce.py")
		if err != nil {
			log.Printf("[Coordinator] Job failed: %v", err)
			return
		}
		log.Printf("[Coordinator] Final result:\n%s", result)
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("[Coordinator] Failed to serve: %v", err)
	}
}
