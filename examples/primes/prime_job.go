package primes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
)

func PrimesJob(ctx context.Context, coord *coordinator.Coordinator, maxVal int, chunkSize int, jobID string, resume bool) (string, error) {
	log.Printf("[primes] Reading C++ source for Just-In-Time heterogeneous compilation...")

	// 1. Read the raw C++ source code from disk
	cppSource, err := os.ReadFile("examples/primes/src/primes_worker.cpp")
	if err != nil {
		return "", fmt.Errorf("failed to read c++ source: %v", err)
	}

	// 2. Create a self-compiling POSIX shell script wrapper
	bashWrapper := fmt.Sprintf("#!/bin/sh\n"+
		"DIR=$(mktemp -d)\n"+
		"cat << 'EOF' > $DIR/worker.cpp\n"+
		"%s\n"+
		"EOF\n"+
		"c++ -O3 -std=c++17 $DIR/worker.cpp -o $DIR/worker_bin\n"+
		"$DIR/worker_bin\n"+
		"rm -rf $DIR\n", string(cppSource))

	wrapperPath := "examples/primes/jit_worker.sh"
	if err := os.WriteFile(wrapperPath, []byte(bashWrapper), 0755); err != nil {
		return "", fmt.Errorf("failed to write jit wrapper: %v", err)
	}

	log.Printf("[primes] Distributing Prime Number calculation up to %d", maxVal)

	// 3. Instruct the engine to ship the shell wrapper instead of a pre-compiled binary
	pipeline, err := dag.New("primes_batch").
		Stage("find_primes",
			dag.ScriptExecutor("sh", wrapperPath),
		).
		Build()

	if err != nil {
		return "", err
	}

	var chunks []dag.TaskInput
	for i := 1; i <= maxVal; i += chunkSize {
		end := i + chunkSize - 1
		if end > maxVal {
			end = maxVal
		}
		payload, _ := json.Marshal(map[string]interface{}{
			"start": i,
			"end":   end,
		})
		chunks = append(chunks, dag.TaskInput{Data: payload})
	}

	if jobID == "" {
		jobID = fmt.Sprintf("primes-%d", time.Now().UnixMilli())
	}

	outputs, err := coord.RunJobRaw(ctx, jobID, pipeline, chunks, resume)
	if err != nil {
		return "", err
	}

	res, err := processResults(outputs, maxVal, jobID)
	if err == nil {
		coord.RegisterJobResult(jobID, res)
	}
	return res, err
}

func processResults(outputs []dag.TaskOutput, maxVal int, jobID string) (string, error) {
	var totalCompute float64
	var totalPrimes int64
	var allPrimes []int64

	for _, out := range outputs {
		var res struct {
			Count          int64   `json:"count"`
			ComputeTimeSec float64 `json:"compute_time_sec"`
			Primes         []int64 `json:"primes"`
		}
		if err := json.Unmarshal(out.Data, &res); err != nil {
			return "", fmt.Errorf("failed to parse output: %v", err)
		}
		totalCompute += res.ComputeTimeSec
		totalPrimes += res.Count
		allPrimes = append(allPrimes, res.Primes...)
	}

	sort.Slice(allPrimes, func(i, j int) bool { return allPrimes[i] < allPrimes[j] })

	outPath := filepath.Join(".", "rivage_data", jobID+"_primes.json")
	primesJSON, _ := json.Marshal(allPrimes)
	os.WriteFile(outPath, primesJSON, 0644)

	summary := map[string]interface{}{
		"status":                      "success",
		"search_space_max":            maxVal,
		"total_primes_found":          totalPrimes,
		"total_aggregate_compute_sec": totalCompute,
		"saved_output_file":           outPath,
	}

	bytes, _ := json.MarshalIndent(summary, "", "  ")
	return string(bytes), nil
}
