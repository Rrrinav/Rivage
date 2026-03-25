package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rivage/examples/crmm"
	"rivage/examples/hashcrack"
	"rivage/examples/matmul"
	"rivage/pkg/config"
	"rivage/pkg/coordinator"
)

func main() {
	cfgPath := flag.String("config", "configs/coordinator.yaml", "path to coordinator config file")
	exampleFlag := flag.String("example", "matmul", "Which example to run: matmul, hashcrack, or crmm")
	datastoreFlag := flag.String("datastore", "http://localhost:8081/data", "Base URL for the datastore")
	grpcAddrFlag := flag.String("grpc-addr", "", "Override gRPC listen address (e.g., 10.0.0.50:50051)")
	flag.Parse()

	cfg, err := config.LoadCoordinatorConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Dynamically override the YAML config if the flag is provided by build.py
	if *grpcAddrFlag != "" {
		cfg.Server.GRPCAddr = *grpcAddrFlag
	}

	coord, err := coordinator.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		time.Sleep(3 * time.Second)

		switch *exampleFlag {
		case "crmm":
			log.Printf("[demo] Starting IEEE 2017 CRMM Matrix Multiplication...")
			matrixSize := 10000
			tileSize := 2000

			matA := crmm.RandomMatrix(matrixSize)
			matB := crmm.RandomMatrix(matrixSize)

			start := time.Now()
			resultMeta, err := crmm.CRMMJob(ctx, coord, matA, matB, tileSize, *datastoreFlag)
			if err != nil {
				log.Printf("[demo] CRMM job failed: %v", err)
				return
			}

			log.Printf("[demo] CRMM job completed in %v", time.Since(start))
			log.Printf("[demo] Final CRMM Metadata:\n%s", resultMeta)

		case "matmul":
			log.Printf("[demo] Starting Distributed Matrix Multiplication...")
			matrixSize := 10000
			tileSize := 2000

			matA := matmul.RandomMatrix(matrixSize)
			matB := matmul.RandomMatrix(matrixSize)

			start := time.Now()
			resultMeta, err := matmul.MatrixJob(ctx, coord, matA, matB, tileSize, *datastoreFlag)
			if err != nil {
				log.Printf("[demo] Matrix job failed: %v", err)
				return
			}

			log.Printf("[demo] Matrix job completed in %v", time.Since(start))
			log.Printf("[demo] Final Matrix Metadata:\n%s", resultMeta)

		case "hashcrack":
			log.Printf("[demo] Starting CPU-Bound Distributed Hash Cracking...")
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
