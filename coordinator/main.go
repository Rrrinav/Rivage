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

// -----------------------------------------------------------------
// Job tracking
// -----------------------------------------------------------------

type TaskState struct {
	result *pb.TaskResult
	done   bool
}

type Job struct {
	id         string
	totalTasks int
	tasks      map[string]*TaskState // task_id -> state
	mu         sync.Mutex
	allDone    chan struct{}
}

func newJob(id string, taskIDs []string) *Job {
	tasks := make(map[string]*TaskState, len(taskIDs))
	for _, tid := range taskIDs {
		tasks[tid] = &TaskState{}
	}
	return &Job{
		id:         id,
		totalTasks: len(taskIDs),
		tasks:      tasks,
		allDone:    make(chan struct{}),
	}
}

// recordResult stores a task result and closes allDone when every task is in.
func (j *Job) recordResult(result *pb.TaskResult) {
	j.mu.Lock()
	defer j.mu.Unlock()

	if state, ok := j.tasks[result.GetTaskId()]; ok {
		state.result = result
		state.done = true
	}

	for _, s := range j.tasks {
		if !s.done {
			return
		}
	}
	close(j.allDone)
}

// results returns all TaskResult values in task-id order (stable for reassembly).
func (j *Job) results() []*pb.TaskResult {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]*pb.TaskResult, 0, len(j.tasks))
	for _, s := range j.tasks {
		out = append(out, s.result)
	}
	return out
}

// -----------------------------------------------------------------
// Coordinator server
// -----------------------------------------------------------------

type coordinatorServer struct {
	pb.UnimplementedCoordinatorServer

	workerStreams sync.Map // workerID -> pb.Coordinator_RegisterAndStreamServer
	jobs         sync.Map  // jobID -> *Job
}

func (s *coordinatorServer) RegisterAndStream(stream pb.Coordinator_RegisterAndStreamServer) error {
	// First message must be registration
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
	defer s.workerStreams.Delete(workerID)

	// Listen for results
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[Coordinator] Worker %s disconnected.", workerID)
			return nil
		}
		if err != nil {
			log.Printf("[Coordinator] Error from %s: %v", workerID, err)
			return err
		}

		if resultMsg, ok := msg.GetPayload().(*pb.ExecutorMessage_Result); ok {
			result := resultMsg.Result
			log.Printf("[Coordinator] Result for Task %s from %s: success=%t",
				result.GetTaskId(), workerID, result.GetSuccess())

			// Route the result back to its job
			jobID := extractJobID(result.GetTaskId())
			if jobVal, ok := s.jobs.Load(jobID); ok {
				jobVal.(*Job).recordResult(result)
			}
		}
	}
}

// sendTask sends a single task to the given worker.
func (s *coordinatorServer) sendTask(workerID string, task *pb.Task) error {
	val, ok := s.workerStreams.Load(workerID)
	if !ok {
		return fmt.Errorf("worker %s not found", workerID)
	}
	stream := val.(pb.Coordinator_RegisterAndStreamServer)
	return stream.Send(&pb.CoordinatorMessage{Task: task})
}

// pickWorker returns the first available worker (round-robin can be added later).
func (s *coordinatorServer) pickWorker() (string, bool) {
	var id string
	s.workerStreams.Range(func(key, _ interface{}) bool {
		id = key.(string)
		return false // stop at first
	})
	return id, id != ""
}

// -----------------------------------------------------------------
// MapReduce orchestration
// -----------------------------------------------------------------

// MapReduceJob runs a full map → reduce pipeline.
//
//	inputChunks  — one element per map task (each is JSON bytes for the map script)
//	mapScript    — path to the Python map script   e.g. "tasks/map.py"
//	reduceScript — path to the Python reduce script e.g. "tasks/reduce.py"
func (s *coordinatorServer) MapReduceJob(jobID string, inputChunks [][]byte, mapScript, reduceScript string) ([]byte, error) {

	log.Printf("[Coordinator] [%s] Starting MAP phase (%d chunks)", jobID, len(inputChunks))

	mapTaskIDs := make([]string, len(inputChunks))
	for i := range inputChunks {
		mapTaskIDs[i] = fmt.Sprintf("%s-map-%d", jobID, i)
	}

	mapJob := newJob(jobID+"-map", mapTaskIDs)
	s.jobs.Store(mapJob.id, mapJob)

	// Dispatch map tasks — wait for at least one worker
	for {
		_, ok := s.pickWorker()
		if ok {
			break
		}
		log.Println("[Coordinator] Waiting for a worker...")
		time.Sleep(1 * time.Second)
	}

	for i, chunk := range inputChunks {
		workerID, _ := s.pickWorker()
		task := &pb.Task{
			TaskId:    mapTaskIDs[i],
			Command:   "python3",
			Args:      []string{mapScript},
			InputData: chunk,
		}
		if err := s.sendTask(workerID, task); err != nil {
			return nil, fmt.Errorf("failed to send map task %d: %w", i, err)
		}
		log.Printf("[Coordinator] Sent map task %s to %s", task.TaskId, workerID)
	}

	// Wait for all map tasks to finish
	<-mapJob.allDone
	log.Printf("[Coordinator] [%s] MAP phase complete.", jobID)

	// Merge all map outputs into a single JSON object: { key: [v1,v2,...] }
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

	reduceTaskIDs := make([]string, 0, len(merged))
	reduceInputs := make([][]byte, 0, len(merged))

	i := 0
	for k, vals := range merged {
		tid := fmt.Sprintf("%s-reduce-%d", jobID, i)
		reduceTaskIDs = append(reduceTaskIDs, tid)

		payload, _ := json.Marshal(map[string]interface{}{
			"key":    k,
			"values": vals,
		})
		reduceInputs = append(reduceInputs, payload)
		i++
	}

	reduceJob := newJob(jobID+"-reduce", reduceTaskIDs)
	s.jobs.Store(reduceJob.id, reduceJob)

	for j, payload := range reduceInputs {
		workerID, _ := s.pickWorker()
		task := &pb.Task{
			TaskId:    reduceTaskIDs[j],
			Command:   "python3",
			Args:      []string{reduceScript},
			InputData: payload,
		}
		if err := s.sendTask(workerID, task); err != nil {
			return nil, fmt.Errorf("failed to send reduce task %d: %w", j, err)
		}
		log.Printf("[Coordinator] Sent reduce task %s to %s", task.TaskId, workerID)
	}

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

// -----------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------

// extractJobID pulls the job ID from a task ID.
// Convention: task IDs look like "jobID-map-0" or "jobID-reduce-2"
func extractJobID(taskID string) string {
	// Walk backwards past the last two "-" segments
	// e.g. "job-1-map-0" → "job-1-map"  → stored as "job-1-map"
	// We stored the job under jobID+"-map" / jobID+"-reduce"
	for i := len(taskID) - 1; i >= 0; i-- {
		if taskID[i] == '-' {
			return taskID[:i]
		}
	}
	return taskID
}

// -----------------------------------------------------------------
// Main
// -----------------------------------------------------------------

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[Coordinator] Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	coordServer := &coordinatorServer{}
	pb.RegisterCoordinatorServer(grpcServer, coordServer)

	log.Printf("[Coordinator] Listening on %v", lis.Addr())

	// Run a demo word-count job after workers connect
	go func() {
		time.Sleep(3 * time.Second)

		// Split some text into chunks — one per map task
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
