package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rivage/examples/matmul"
	"rivage/pkg/config"
	"rivage/pkg/coordinator"
)

func main() {
	cfgPath := flag.String("config", "configs/coordinator.yaml", "path to coordinator config file")
	flag.Parse()

	cfg, err := config.LoadCoordinatorConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	coord, err := coordinator.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		// Wait for workers to connect
		time.Sleep(3 * time.Second)

		log.Printf("[demo] Starting Distributed Matrix Multiplication...")
		matrixSize := 10000
		tileSize := 2000

		matA := matmul.RandomMatrix(matrixSize)
		matB := matmul.RandomMatrix(matrixSize)

		start := time.Now()
		// Now correctly expects (string, error)
		resultMeta, err := matmul.MatrixJob(ctx, coord, matA, matB, tileSize)
		if err != nil {
			log.Printf("[demo] Matrix job failed: %v", err)
			return
		}

		log.Printf("[demo] Matrix job completed in %v", time.Since(start))
		log.Printf("[demo] Final Matrix Metadata:\n%s", resultMeta)
	}()

	if err := coord.Start(ctx); err != nil {
		log.Fatalf("Coordinator error: %v", err)
	}
}
