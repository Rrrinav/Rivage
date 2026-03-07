package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"runtime"
	"time"

	pb "dist-system/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address = "localhost:50051"
)

func handleTask(task *pb.Task) *pb.TaskResult {
	log.Printf("[Worker] Received Task %s: %s %v", task.GetTaskId(), task.GetCommand(), task.GetArgs())

	// Write dynamic code to a temporary file if provided
	var tempFilePath string
	if len(task.GetCode()) > 0 {
		tmpFile, err := os.CreateTemp("", "rivage-task-*")
		if err != nil {
			return &pb.TaskResult{
				TaskId:   task.GetTaskId(),
				JobId:    task.GetJobId(),
				TaskType: task.GetTaskType(),
				Success:  false,
				ErrorLog: fmt.Sprintf("Failed to create temp code file: %v", err),
			}
		}
		tmpFile.Write(task.GetCode())
		tmpFile.Close()
		
		// Make the temporary file executable
		os.Chmod(tmpFile.Name(), 0755)
		tempFilePath = tmpFile.Name()
		
		// Crucial: Automatically delete the script when the task is done
		defer os.Remove(tempFilePath)
	}

	// Substitute {CODE} with the actual temp file path
	command := task.GetCommand()
	var args []string
	for _, arg := range task.GetArgs() {
		if arg == "{CODE}" && tempFilePath != "" {
			args = append(args, tempFilePath)
		} else {
			args = append(args, arg)
		}
	}
	
	// Optional: If the command itself is "EXEC_CODE", it means the code IS the binary executable
	if command == "EXEC_CODE" && tempFilePath != "" {
		command = tempFilePath
	}

	// We no longer strictly need a 30s timeout here, but it's good practice for rogue scripts
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdin = bytes.NewReader(task.GetInputData())

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		log.Printf("[Worker] Task %s FAILED: %v\nSTDERR: %s", task.GetTaskId(), err, stderr.String())
		return &pb.TaskResult{
			TaskId:   task.GetTaskId(),
			JobId:    task.GetJobId(),
			TaskType: task.GetTaskType(),
			Success:  false,
			ErrorLog: stderr.String(),
		}
	}

	log.Printf("[Worker] Task %s completed successfully.", task.GetTaskId())
	return &pb.TaskResult{
		TaskId:     task.GetTaskId(),
		JobId:      task.GetJobId(),
		TaskType:   task.GetTaskType(),
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

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	workerID := fmt.Sprintf("%s-%d", hostname, time.Now().UnixMilli()%10000)

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

	// --- NEW: Heartbeat Goroutine ---
	// This runs independently of the tasks, proving the worker process is alive
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			err := stream.Send(&pb.ExecutorMessage{
				Payload: &pb.ExecutorMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{WorkerId: workerID},
				},
			})
			if err != nil {
				log.Printf("[Worker] Heartbeat failed (coordinator might be down): %v", err)
				return
			}
		}
	}()
	// --------------------------------

	resultsChan := make(chan *pb.TaskResult, 10)

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
		go func(t *pb.Task) {
			result := handleTask(t)
			resultsChan <- result
		}(task)
	}
}
