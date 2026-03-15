package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rivage/examples/hashcrack"
	"rivage/examples/matmul"
	"rivage/pkg/config"
	"rivage/pkg/coordinator"
)

func main() {
	cfgPath := flag.String("config", "configs/coordinator.yaml", "path to coordinator config file")

	// NEW: This is the flag that was missing!
	exampleFlag := flag.String("example", "matmul", "Which example to run: matmul or hashcrack")
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

		// Dynamically switch between workloads based on the build.py flag
		switch *exampleFlag {
		case "matmul":
			log.Printf("[demo] Starting Distributed Matrix Multiplication...")
			matrixSize := 10000
			tileSize := 2000

			matA := matmul.RandomMatrix(matrixSize)
			matB := matmul.RandomMatrix(matrixSize)

			start := time.Now()
			resultMeta, err := matmul.MatrixJob(ctx, coord, matA, matB, tileSize)
			if err != nil {
				log.Printf("[demo] Matrix job failed: %v", err)
				return
			}

			log.Printf("[demo] Matrix job completed in %v", time.Since(start))
			log.Printf("[demo] Final Matrix Metadata:\n%s", resultMeta)

		case "hashcrack":
			log.Printf("[demo] Starting CPU-Bound Distributed Hash Cracking...")
			// "rivage" is 6 letters. The search space is 26^6 = 308 million hashes.
			// The cluster will divide this into 26 tasks of 11.8 million hashes each.
			targetPassword := "rivage"

			start := time.Now()
			resultMeta, err := hashcrack.CrackJob(ctx, coord, targetPassword)
			if err != nil {
				log.Printf("[demo] HashCrack job failed: %v", err)
				return
			}

			log.Printf("[demo] HashCrack job completed in %v", time.Since(start))
			log.Printf("[demo] Final Result:\n%s", resultMeta)

		default:
			log.Printf("[demo] Unknown example specified: %s", *exampleFlag)
		}
	}()

	if err := coord.Start(ctx); err != nil {
		log.Fatalf("Coordinator error: %v", err)
	}
}
