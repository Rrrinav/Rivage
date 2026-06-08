package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"rivage/examples/crmm"
	"rivage/examples/hashcrack"
	"rivage/examples/matmul"
	"rivage/pkg/config"
	"rivage/pkg/coordinator"
)

// getJobMetadata scans the disk to find the most recent Job ID for resuming,
// and calculates the next serial number for new jobs.
func getJobMetadata(prefix string) (latestJobID string, nextSerial int) {
	dataDir := filepath.Join(".", "rivage_data", "jobs")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return "", 1 // Directory doesn't exist yet, start at serial 1
	}

	maxSerial := 0
	var latest string

	for _, e := range entries {
		name := e.Name()
		// Expected format: prefix-0001-1775286237030_stage.wal
		if strings.HasPrefix(name, prefix+"-") && strings.HasSuffix(name, ".wal") {
			parts := strings.Split(name, "_")
			if len(parts) > 0 {
				jobID := parts[0]
				
				// Track the latest string (since serials are zero-padded, standard string compare works perfectly)
				if jobID > latest {
					latest = jobID
				}

				// Extract the serial number to find the max
				idParts := strings.Split(jobID, "-")
				if len(idParts) >= 3 {
					serialStr := idParts[len(idParts)-2] // The second-to-last part is the serial
					var serial int
					if _, err := fmt.Sscanf(serialStr, "%d", &serial); err == nil {
						if serial > maxSerial {
							maxSerial = serial
						}
					}
				}
			}
		}
	}
	return latest, maxSerial + 1
}

func main() {
	cfgPath := flag.String("config", "configs/coordinator.yaml", "path to coordinator config file")
	exampleFlag := flag.String("example", "matmul", "Which example to run: matmul, hashcrack, or crmm")
	datastoreURL := flag.String("datastore", "http://localhost:8081/data", "Base URL for the datastore")
	grpcAddrFlag := flag.String("grpc-addr", "", "Override gRPC listen address (e.g., 10.0.0.50:50051)")
	
	// Flags for our advanced recovery system
	jobIDFlag := flag.String("job-id", "", "Specific job ID (optional)")
	resumeFlag := flag.Bool("resume", false, "Resume from the WAL instead of starting fresh")
	flag.Parse()

	cfg, err := config.LoadCoordinatorConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override the YAML config dynamically if the flag was provided by build.py
	if *grpcAddrFlag != "" {
		cfg.Server.GRPCAddr = *grpcAddrFlag
	}

	if *datastoreURL != "" {
        cfg.Server.DatastoreURL = *datastoreURL
    }

	coord, err := coordinator.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		time.Sleep(3 * time.Second)

		jobID := *jobIDFlag
		latestJobID, nextSerial := getJobMetadata(*exampleFlag)

		// Auto-detect latest job ID if resuming without providing one!
		if *resumeFlag && jobID == "" {
			jobID = latestJobID
			if jobID == "" {
				log.Fatalf("[demo] No previous job found to resume for %s", *exampleFlag)
			}
			log.Printf("[demo] Auto-detected latest job ID to resume: %s", jobID)
		} else if jobID == "" {
			// Generate a uniquely traceable Job ID using the logical prefix, serial number, and timestamp!
			jobID = fmt.Sprintf("%s-%04d-%d", *exampleFlag, nextSerial, time.Now().UnixMilli())
			log.Printf("[demo] Generated new Job ID: %s", jobID)
		}

		switch *exampleFlag {
		case "crmm":
			log.Printf("[demo] Starting IEEE 2017 CRMM Matrix Multiplication...")
			matrixSize := 10000
			tileSize := 2000

			matA := crmm.RandomMatrix(matrixSize)
			matB := crmm.RandomMatrix(matrixSize)

			start := time.Now()
			resultMeta, err := crmm.CRMMJob(ctx, coord, matA, matB, tileSize, *datastoreFlag, jobID, *resumeFlag)
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
			resultMeta, err := matmul.MatrixJob(ctx, coord, matA, matB, tileSize, *datastoreFlag, jobID, *resumeFlag)
			if err != nil {
				log.Printf("[demo] Matrix job failed: %v", err)
				return
			}

			log.Printf("[demo] Matrix job completed in %v", time.Since(start))
			log.Printf("[demo] Final Matrix Metadata:\n%s", resultMeta)

		case "hashcrack":
			log.Printf("[demo] Starting CPU-Bound Distributed Hash Cracking...")
			targetPassword := "zigzag"

			start := time.Now()
			resultMeta, err := hashcrack.CrackJob(ctx, coord, targetPassword, jobID, *resumeFlag)
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
