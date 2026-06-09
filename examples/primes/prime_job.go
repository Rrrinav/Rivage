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
	log.Printf("[primes] Distributing Prime Number calculation up to %d", maxVal)

	pipeline, err := dag.New("primes_batch").
		Stage("find_primes",
			// We pass "{CODE}" as the command. The worker will execute the shipped binary.
			dag.ScriptExecutor("{CODE}", "examples/primes/primes_worker"),
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
