package main

import (
	"context"
	"io"
	"log"
	"runtime"
	"time"

	pb "dist-system/proto" // Import our generated code

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address  = "localhost:50051"
	workerID = "go-worker-01" // In a real app, this would be dynamic (e.g., hostname)
)

// handleTask is the "Layer 1" stub. It just pretends to work.
func handleTask(task *pb.Task) *pb.TaskResult {
	log.Printf("[Worker] Received Task %s: running command '%s'",
		task.GetTaskId(), task.GetCommand())

	// --- LAYER 1: FAKE WORK ---
	// We just pretend to do the work.
	// In Layer 2, we will replace this with the real os/exec.
	time.Sleep(2 * time.Second)
	// ---

	log.Printf("[Worker] Task %s completed successfully.", task.GetTaskId())

	// We pretend the task succeeded and send back some fake output
	return &pb.TaskResult{
		TaskId:      task.GetTaskId(),
		Success:     true,
		OutputData:  []byte("Task completed successfully! (Fake output)"),
		ErrorLog:    "",
	}
}

func main() {
	// 1. Connect to the Coordinator
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Worker] Did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewCoordinatorClient(conn)
	ctx := context.Background()

	// 2. Call the streaming RPC
	stream, err := c.RegisterAndStream(ctx)
	if err != nil {
		log.Fatalf("[Worker] Could not open stream: %v", err)
	}
	log.Printf("[Worker] Connected to stream.")

	// 3. Send the first message: Registration
	regReq := &pb.RegistrationRequest{
		WorkerId:   workerID,
		CpuCores:   int32(runtime.NumCPU()), // Here we send the *real* core count
		AvailableRam: 1024 * 8, // We can fake this for now
	}
	firstMsg := &pb.ExecutorMessage{
		Payload: &pb.ExecutorMessage_Register{Register: regReq},
	}

	if err := stream.Send(firstMsg); err != nil {
		log.Fatalf("[Worker] Failed to send registration: %v", err)
	}
	log.Printf("[Worker] Sent registration for worker: %s (Cores: %d)",
		workerID, regReq.CpuCores)

	// --- 4. The Main Loop: Send/Recv ---

	// This goroutine handles sending results *back* to the server
	// We use a channel to pass results from our 'handleTask'
	// function to our stream sender.
	resultsChan := make(chan *pb.TaskResult)
	go func() {
		for result := range resultsChan {
			log.Printf("[Worker] Sending result for Task %s...", result.GetTaskId())
			msg := &pb.ExecutorMessage{
				Payload: &pb.ExecutorMessage_Result{Result: result},
			}
			if err := stream.Send(msg); err != nil {
				log.Printf("[Worker] Error sending result: %v", err)
			}
		}
	}()

	// This loop handles *receiving* tasks from the server
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("[Worker] Server closed the stream.")
			close(resultsChan) // Close the channel when done
			return
		}
		if err != nil {
			log.Fatalf("[Worker] Error receiving task: %v", err)
		}

		// We got a task from the Coordinator
		task := msg.GetTask()

		// When we get a task, run it in a new goroutine
		// so we can handle multiple tasks in parallel
		go func(t *pb.Task) {
			result := handleTask(t)
			resultsChan <- result // Send the result to our sender goroutine
		}(task)
	}
}
