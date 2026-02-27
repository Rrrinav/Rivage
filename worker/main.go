package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os/exec"
	"runtime"
	"time"

	pb "dist-system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address  = "localhost:50051"
	workerID = "go-worker-01"
)

// handleTask executes whatever command the coordinator sends.
// Contract: input_data is piped to STDIN, STDOUT is captured as output.
// The task doesn't have to be Python — any executable works.
func handleTask(task *pb.Task) *pb.TaskResult {
	log.Printf("[Worker] Received Task %s: %s %v", task.GetTaskId(), task.GetCommand(), task.GetArgs())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, task.GetCommand(), task.GetArgs()...)

	// Pipe InputData → STDIN of the subprocess
	cmd.Stdin = bytes.NewReader(task.GetInputData())

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[Worker] Task %s FAILED: %v\nSTDERR: %s", task.GetTaskId(), err, stderr.String())
		return &pb.TaskResult{
			TaskId:   task.GetTaskId(),
			Success:  false,
			ErrorLog: stderr.String(),
		}
	}

	log.Printf("[Worker] Task %s completed successfully.", task.GetTaskId())
	return &pb.TaskResult{
		TaskId:     task.GetTaskId(),
		Success:    true,
		OutputData: stdout.Bytes(),
	}
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Worker] Did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewCoordinatorClient(conn)
	ctx := context.Background()

	stream, err := c.RegisterAndStream(ctx)
	if err != nil {
		log.Fatalf("[Worker] Could not open stream: %v", err)
	}
	log.Printf("[Worker] Connected to coordinator.")

	// Send registration
	regReq := &pb.RegistrationRequest{
		WorkerId:     workerID,
		CpuCores:     int32(runtime.NumCPU()),
		AvailableRam: 1024 * 8,
	}
	if err := stream.Send(&pb.ExecutorMessage{
		Payload: &pb.ExecutorMessage_Register{Register: regReq},
	}); err != nil {
		log.Fatalf("[Worker] Failed to send registration: %v", err)
	}
	log.Printf("[Worker] Registered as %s (Cores: %d)", workerID, regReq.CpuCores)

	// Channel to send results back without blocking the receive loop
	resultsChan := make(chan *pb.TaskResult, 10)

	// Goroutine: sends results back to coordinator
	go func() {
		for result := range resultsChan {
			log.Printf("[Worker] Sending result for Task %s...", result.GetTaskId())
			if err := stream.Send(&pb.ExecutorMessage{
				Payload: &pb.ExecutorMessage_Result{Result: result},
			}); err != nil {
				log.Printf("[Worker] Error sending result: %v", err)
			}
		}
	}()

	// Main loop: receive tasks from coordinator
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Println("[Worker] Server closed the stream.")
			close(resultsChan)
			return
		}
		if err != nil {
			log.Fatalf("[Worker] Error receiving task: %v", err)
		}

		task := msg.GetTask()

		// Run each task in its own goroutine so we can handle multiple in parallel
		go func(t *pb.Task) {
			result := handleTask(t)
			resultsChan <- result
		}(task)
	}
}
