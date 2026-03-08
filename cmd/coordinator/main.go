package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rivage/examples/matmul"
	"rivage/pkg/config"
	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
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

	// Submit demo jobs after a short delay for workers to connect.
	go func() {
		time.Sleep(3 * time.Second)
		
		// 1. Run Word Count
		if err := submitWordCountJob(ctx, coord); err != nil {
			log.Printf("[demo] Word-count job failed: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// 2. Run Matrix Multiplication (The Grand Finale)
		log.Printf("[demo] Starting Distributed Matrix Multiplication...")
		matrixSize := 400
		tileSize := 100 // Will break the 400x400 matrix into 16 separate 100x100 tiles
		
		matA := matmul.RandomMatrix(matrixSize)
		matB := matmul.RandomMatrix(matrixSize)

		start := time.Now()
		resultMatrix, err := matmul.MatrixJob(ctx, coord, matA, matB, tileSize)
		if err != nil {
			log.Printf("[demo] Matrix job failed: %v", err)
			return
		}

		log.Printf("[demo] Matrix job completed in %v", time.Since(start))
		log.Printf("[demo] Resulting Matrix dimensions: %dx%d", len(resultMatrix), len(resultMatrix[0]))
	}()

	if err := coord.Start(ctx); err != nil {
		log.Fatalf("Coordinator error: %v", err)
	}
}

// submitWordCountJob is the demo job: classic word-count MapReduce.
func submitWordCountJob(ctx context.Context, coord *coordinator.Coordinator) error {
	// Pipeline definition (logical config in Go)
	pipeline, err := dag.New("word-count").
		Stage("map",
			dag.ScriptExecutor("python3", "examples/wordcount/map.py"),
			dag.WithParallelism(8),
		).
		Stage("reduce",
			dag.ScriptExecutor("python3", "examples/wordcount/reduce.py"),
			dag.WithShuffle(dag.JSONKeyGroupShuffle()),
			dag.WithParallelism(4),
		).
		Build()
	if err != nil {
		return err
	}

	// Input data (quantitative)
	lines := []string{
		"the quick brown fox jumps over the lazy dog",
		"the fox ran quickly over the hill and saw another fox",
		"a lazy dog slept all day under the warm sun",
		"the sun rose over the hill as the fox watched",
	}

	chunks := make([][]byte, len(lines))
	for i, line := range lines {
		chunks[i], _ = json.Marshal(map[string]interface{}{
			"chunk": []string{line},
		})
	}

	jobID := "word-count-" + time.Now().Format("20060102-150405")
	result, err := coord.RunJob(ctx, jobID, pipeline, chunks)
	if err != nil {
		return err
	}
	log.Printf("[demo] Word-count result:\n%s", string(result))
	return nil
}
