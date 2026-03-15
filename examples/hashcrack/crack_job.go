package hashcrack

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
)

// CrackJob distributes a brute-force attack across the cluster
func CrackJob(ctx context.Context, coord *coordinator.Coordinator, targetPassword string) (string, error) {
	// Hash the target password so the workers don't actually know what it is
	hashBytes := md5.Sum([]byte(targetPassword))
	targetHash := hex.EncodeToString(hashBytes[:])
	charset := "abcdefghijklmnopqrstuvwxyz"
	passLen := len(targetPassword)

	log.Printf("[hashcrack] Target Hash: %s (Length: %d)", targetHash, passLen)
	log.Printf("[hashcrack] Search Space: %d combinations", intPow(len(charset), passLen))

	// 1. Define a 1-stage CPU pipeline
	pipeline, err := dag.New("hashcrack").
		Stage("brute_force",
			dag.ScriptExecutor("python3", "examples/hashcrack/crack.py"),
		).
		Build()

	if err != nil {
		return "", err
	}

	// 2. Partition the search space (26 tasks, one for each starting letter)
	var chunks []dag.TaskInput
	for i := 0; i < len(charset); i++ {
		prefix := string(charset[i])
		payload, _ := json.Marshal(map[string]interface{}{
			"target_hash": targetHash,
			"charset":     charset,
			"prefix":      prefix,
			"max_length":  passLen,
		})

		chunks = append(chunks, dag.TaskInput{
			Data:         payload,
			AffinityKeys: nil, // CPU bound, no data locality needed!
		})
	}

	// 3. Dispatch the tasks
	jobID := fmt.Sprintf("crack-%d", time.Now().UnixMilli())
	outputs, err := coord.RunJobRaw(ctx, jobID, pipeline, chunks)
	if err != nil {
		return "", err
	}

	// 4. Aggregate results to find the winner
	return processResults(outputs)
}

func processResults(outputs []dag.TaskOutput) (string, error) {
	var totalCompute float64
	var foundPassword string
	var winnerPrefix string

	for _, out := range outputs {
		var res struct {
			Found          bool    `json:"found"`
			Password       string  `json:"password"`
			ComputeTimeSec float64 `json:"compute_time_sec"`
			Prefix         string  `json:"prefix"`
		}
		if err := json.Unmarshal(out.Data, &res); err != nil {
			return "", fmt.Errorf("failed to parse output: %v", err)
		}

		totalCompute += res.ComputeTimeSec
		if res.Found {
			foundPassword = res.Password
			winnerPrefix = res.Prefix
		}
	}

	summary := map[string]interface{}{
		"status":                      "success",
		"password_cracked":            foundPassword != "",
		"recovered_password":          foundPassword,
		"winning_task_prefix":         winnerPrefix,
		"total_aggregate_compute_sec": totalCompute,
		"network_io_bottleneck":       "Eliminated (Pure CPU Workload)",
	}

	bytes, _ := json.MarshalIndent(summary, "", "  ")
	return string(bytes), nil
}

func intPow(base, exp int) int {
	res := 1
	for i := 0; i < exp; i++ {
		res *= base
	}
	return res
}
