package main

import (
	"io"
	"log"
	"net"
	"sync"
	"time"

	pb "dist-system/proto" // Import our generated code

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	port = ":50051"
)

// coordinatorServer holds the state for our master server
type coordinatorServer struct {
	pb.UnimplementedCoordinatorServer

	// A thread-safe map to store the gRPC stream for each worker
	// The key is the worker_id (string), the value is the stream
	workerStreams sync.Map
}

// RegisterAndStream is the bi-directional streaming RPC handler
func (s *coordinatorServer) RegisterAndStream(stream pb.Coordinator_RegisterAndStreamServer) error {
	// --- 1. Registration Phase ---

	// The very first message from a worker MUST be a registration
	regMsg, err := stream.Recv()
	if err != nil {
		return err
	}

	// We use the "oneof" to check if the payload is a registration
	regReq, ok := regMsg.GetPayload().(*pb.ExecutorMessage_Register)
	if !ok {
		log.Println("[Server] First message from worker was not a registration. Disconnecting.")
		return io.ErrUnexpectedEOF // A simple error to close the stream
	}

	workerID := regReq.Register.GetWorkerId()
	cpuCores := regReq.Register.GetCpuCores()

	// Store the stream in our thread-safe map
	s.workerStreams.Store(workerID, stream)

	p, _ := peer.FromContext(stream.Context())
	log.Printf("[Server] New worker registered: %s (Cores: %d) from %s",
		workerID, cpuCores, p.Addr)

	// --- 2. Task Result Listening Phase ---

	// This is the loop for *receiving* messages (TaskResults) from this worker
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[Server] Worker %s disconnected.", workerID)
			s.workerStreams.Delete(workerID) // Clean up the map
			return nil
		}
		if err != nil {
			log.Printf("[Server] Error receiving from %s: %v", workerID, err)
			s.workerStreams.Delete(workerID)
			return err
		}

		// Check if the message is a TaskResult
		if result, ok := msg.GetPayload().(*pb.ExecutorMessage_Result); ok {
			log.Printf("[Server] Received result for Task %s from %s: Success=%t",
				result.Result.GetTaskId(),
				workerID,
				result.Result.GetSuccess(),
			)
			// In a real app, you'd process this result (e.g., mark task as complete)
		}
	}
}

// sendDemoTask is a helper to test our system
func (s *coordinatorServer) sendDemoTask() {
	// Wait 5 seconds for a worker to connect
	time.Sleep(5 * time.Second)

	log.Println("[Server] Trying to send a demo task...")

	// This creates our first demo task
	task := &pb.Task{
		TaskId:    "task-001",
		Command:   "./my_cpp_task", // The (fake) command the worker should run
		Args:      []string{"--mode=fast"},
		InputData: []byte("Hello from the Coordinator!"),
	}

	// This is how we loop over our sync.Map
	// We'll just send the task to the *first* worker we find.
	s.workerStreams.Range(func(key, value interface{}) bool {
		workerID := key.(string)
		stream := value.(pb.Coordinator_RegisterAndStreamServer)

		log.Printf("[Server] Sending Task %s to worker %s", task.TaskId, workerID)

		// Wrap the task in the CoordinatorMessage
		msg := &pb.CoordinatorMessage{
			Task: task,
		}

		if err := stream.Send(msg); err != nil {
			log.Printf("[Server] Error sending task to %s: %v", workerID, err)
		}

		return false // Stop iterating after sending to one
	})
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("[Server] Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	// Create our server instance
	coordServer := &coordinatorServer{}

	// Register the service
	pb.RegisterCoordinatorServer(s, coordServer)

	log.Printf("[Server] gRPC server listening at %v", lis.Addr())

	// Start the demo task sender in the background
	go coordServer.sendDemoTask()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("[Server] Failed to serve: %v", err)
	}
}
