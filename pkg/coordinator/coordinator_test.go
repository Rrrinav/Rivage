package coordinator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"rivage/pkg/config"
	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
	"rivage/pkg/worker"
)

// TestWordCountEndToEnd runs a complete word-count MapReduce in-process.
func TestWordCountEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	coordCfg := &config.CoordinatorConfig{
		Server: config.ServerConfig{
			GRPCAddr:          ":0",
			MaxConcurrentJobs: 10,
		},
		Scheduler: config.SchedulerConfig{
			Algorithm:        "least_loaded",
			HeartbeatTimeout: config.Duration{Duration: 5 * time.Second},
			TaskTimeout:      config.Duration{Duration: 30 * time.Second},
			WatchdogInterval: config.Duration{Duration: 200 * time.Millisecond},
			MaxGlobalRetries: 2,
		},
		Telemetry: config.TelemetryConfig{LogLevel: "info"},
	}

	coord, err := coordinator.New(coordCfg)
	if err != nil {
		t.Fatalf("coordinator.New: %v", err)
	}

	coordCtx, coordCancel := context.WithCancel(ctx)
	defer coordCancel()

	addrCh := make(chan string, 1)
	go func() {
		if err := coord.StartOnAddr(coordCtx, ":0", addrCh); err != nil && coordCtx.Err() == nil {
			t.Errorf("coordinator.StartOnAddr: %v", err)
		}
	}()

	addr := <-addrCh
	t.Logf("Coordinator listening on %s", addr)

	for i := 0; i < 2; i++ {
		workerCfg := &config.WorkerConfig{
			Coordinator: config.WorkerCoordinatorConfig{
				Addr:              addr,
				ReconnectInterval: config.Duration{Duration: 500 * time.Millisecond},
			},
			Execution: config.ExecutionConfig{
				MaxConcurrentTasks: 4,
				HeartbeatInterval:  config.Duration{Duration: 500 * time.Millisecond},
				DefaultTaskTimeout: config.Duration{Duration: 30 * time.Second},
			},
		}
		w, err := worker.New(workerCfg)
		if err != nil {
			t.Fatalf("worker.New: %v", err)
		}
		go w.Run(ctx)
	}

	// Give workers time to connect and register
	time.Sleep(800 * time.Millisecond)

	pipeline, err := dag.New("test-word-count").
		Stage("map",
			dag.BinaryExecutor("python3", "-c", mapScript),
		).
		Stage("reduce",
			dag.BinaryExecutor("python3", "-c", reduceScript),
			dag.WithShuffle(dag.JSONKeyGroupShuffle()),
		).
		Build()
	if err != nil {
		t.Fatalf("pipeline.Build: %v", err)
	}

	lines := []string{
		"the quick brown fox",
		"the fox ran away",
		"a quick brown dog",
	}
	chunks := make([][]byte, len(lines))
	for i, l := range lines {
		chunks[i], _ = json.Marshal(map[string]interface{}{"chunk": []string{l}})
	}

	result, err := coord.RunJob(ctx, "test-job-1", pipeline, chunks)
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}

	var counts map[string]int
	if err := json.Unmarshal(result, &counts); err != nil {
		t.Fatalf("parse result: %v (raw: %s)", err, result)
	}

	t.Logf("Word counts: %v", counts)

	expected := map[string]int{
		"the": 2, "quick": 2, "brown": 2, "fox": 2,
		"ran": 1, "away": 1, "a": 1, "dog": 1,
	}
	for word, want := range expected {
		got, ok := counts[word]
		if !ok {
			t.Errorf("missing word %q in result", word)
			continue
		}
		if got != want {
			t.Errorf("word %q: want %d, got %d", word, want, got)
		}
	}
}

// TestPipelineBuildErrors checks the DAG builder rejects invalid configs.
func TestPipelineBuildErrors(t *testing.T) {
	tests := []struct {
		name    string
		build   func() error
		wantErr string
	}{
		{
			name: "empty pipeline",
			build: func() error {
				_, err := dag.New("empty").Build()
				return err
			},
			wantErr: "no stages",
		},
		{
			name: "missing shuffle on non-source stage",
			build: func() error {
				_, err := dag.New("bad").
					Stage("a", dag.BinaryExecutor("echo")).
					Stage("b", dag.BinaryExecutor("echo")).
					Build()
				return err
			},
			wantErr: "no shuffle function",
		},
		{
			name: "duplicate stage ID",
			build: func() error {
				_, err := dag.New("bad").
					Stage("a", dag.BinaryExecutor("echo")).
					Stage("a", dag.BinaryExecutor("echo"),
						dag.WithShuffle(dag.PassThroughShuffle())).
					Build()
				return err
			},
			wantErr: "duplicate stage ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.build()
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if tt.wantErr != "" && !containsStr(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

// TestSchedulerAlgorithms checks all three scheduler algorithms compile and pick.
func TestSchedulerAlgorithms(t *testing.T) {
	for _, algo := range []string{"round_robin", "least_loaded", "tag_affinity"} {
		t.Run(algo, func(t *testing.T) {
			cfg := &config.CoordinatorConfig{
				Server:    config.ServerConfig{GRPCAddr: ":0"},
				Scheduler: config.SchedulerConfig{Algorithm: algo},
				Telemetry: config.TelemetryConfig{LogLevel: "error"},
			}
			_, err := coordinator.New(cfg)
			if err != nil {
				t.Fatalf("coordinator.New with algo=%q: %v", algo, err)
			}
		})
	}
}

// Inline Python scripts
const mapScript = `
import json, sys, re
data = json.load(sys.stdin)
counts = {}
for line in data["chunk"]:
    for word in re.findall(r"[a-z]+", line.lower()):
        counts[word] = counts.get(word, 0) + 1
print(json.dumps(counts))
`

const reduceScript = `
import json, sys
data = json.load(sys.stdin)
key = data["key"]
total = sum(int(v) for v in data["values"])
print(json.dumps({key: total}))
`

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func init() { _ = fmt.Sprintf }
