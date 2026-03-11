// Package dag provides the logical pipeline configuration API.
//
// Users define their MapReduce (or arbitrary multi-stage) pipelines entirely
// in Go using this builder API.  Quantitative settings (timeouts, retries,
// parallelism) come from YAML config; logical wiring is expressed here.
//
// Example: a two-stage word-count pipeline:
//
//	pipeline := dag.New("word-count").
//	    Stage("map",
//	        dag.ScriptExecutor("python3", "tasks/map.py"),
//	        dag.WithParallelism(8),
//	    ).
//	    Stage("reduce",
//	        dag.ScriptExecutor("python3", "tasks/reduce.py"),
//	        dag.WithShuffle(dag.ShuffleByKey),        // wires map → reduce
//	        dag.WithParallelism(4),
//	    ).
//	    Build()
package dag

import (
	"fmt"
	"sort"
)

// Executor — how a stage's tasks are run
// Executor describes how a task should be executed on a worker node.
type Executor struct {
	// Command is the binary to run (e.g. "python3", "node", "/usr/bin/myapp").
	Command string
	// Args are static arguments before any dynamic ones.
	Args []string
	// CodeFile if set causes the coordinator to read this file from disk,
	// embed the bytes in the task, and have the worker write it to a
	// temp file, substituting "{CODE}" in Args with the temp path.
	CodeFile string
	// Env holds extra environment variables for the subprocess.
	Env map[string]string
	// RequiredTags: worker must have ALL of these tags.
	RequiredTags []string
}

// ScriptExecutor is a convenience constructor for running a script file.
// The coordinator reads the script from disk and ships it with each task.
func ScriptExecutor(command, scriptPath string) Executor {
	return Executor{
		Command:  command,
		Args:     []string{"{CODE}"},
		CodeFile: scriptPath,
	}
}

// BinaryExecutor runs a pre-installed command with fixed args.
func BinaryExecutor(command string, args ...string) Executor {
	return Executor{Command: command, Args: args}
}

// GoExecutor is shorthand for a compiled Go binary executor.
func GoExecutor(binaryPath string, args ...string) Executor {
	return Executor{Command: binaryPath, Args: args}
}

// ShuffleFunc — how one stage's outputs become the next stage's inputs
// ShuffleResult is returned by a ShuffleFunc.
// Keys are the task-IDs for the next stage; values are their input payloads.
type ShuffleResult map[string][]byte

// ShuffleFunc transforms a slice of completed task outputs from stage N
// into a map of taskID → inputPayload for stage N+1.
//
// The slice of TaskOutput is guaranteed to be complete (all tasks in the
// upstream stage finished successfully) before the shuffle is called.
type ShuffleFunc func(outputs []TaskOutput) (ShuffleResult, error)

// TaskOutput is one completed task's output passed to a ShuffleFunc.
type TaskOutput struct {
	TaskID  string
	StageID string
	Data    []byte
}

// Built-in shuffle strategies

// JSONKeyGroupShuffle is the classic MapReduce shuffle:
// each map output is a JSON object {"key": ..., "value": ...}.
// The shuffle groups all values by key and emits one reduce task per unique key
// with payload {"key": k, "values": [v1, v2, ...]}.
//
// The keyField and valueField parameters name the JSON fields to group by.
// For classic word-count: keyField="word", valueField="count".
func JSONKeyGroupShuffle() ShuffleFunc {
	return jsonKeyGroupShuffleImpl
}

// PassThroughShuffle concatenates all outputs as a JSON array and sends
// the entire thing to a single downstream task.
func PassThroughShuffle() ShuffleFunc {
	return passThroughShuffleImpl
}

// BroadcastShuffle sends every upstream output to every downstream task.
// Useful for aggregation stages that need the full dataset.
func BroadcastShuffle(downstreamTaskIDs []string) ShuffleFunc {
	return func(outputs []TaskOutput) (ShuffleResult, error) {
		combined := combineOutputsJSON(outputs)
		result := make(ShuffleResult, len(downstreamTaskIDs))
		for _, id := range downstreamTaskIDs {
			result[id] = combined
		}
		return result, nil
	}
}

// Stage definition
// Stage is one node in the execution DAG.
type Stage struct {
	ID          string
	Executor    Executor
	Parallelism int         // max concurrent tasks; 0 = unlimited
	Shuffle     ShuffleFunc // nil = this stage is a source (inputs provided externally)
	MaxRetries  int         // -1 = use global default
	TimeoutSecs int64       // 0 = use global default
	// DependsOn lists stage IDs whose outputs feed into this stage's shuffle.
	// For a linear pipeline this is auto-populated by the builder.
	DependsOn []string
}

// Pipeline
// Pipeline is a validated, immutable DAG of stages.
type Pipeline struct {
	Name   string
	Stages map[string]*Stage
	// Order is a topologically sorted list of stage IDs.
	Order []string
}

// StageByID returns the stage with the given ID or nil.
func (p *Pipeline) StageByID(id string) *Stage {
	return p.Stages[id]
}

// Builder
// Builder constructs a Pipeline using a fluent API.
type Builder struct {
	name   string
	stages []*Stage
	err    error
}

// New creates a new Pipeline builder.
func New(name string) *Builder {
	return &Builder{name: name}
}

// StageOption is a functional option for configuring a Stage.
type StageOption func(*Stage)

// WithParallelism sets the maximum number of concurrent tasks for a stage.
func WithParallelism(n int) StageOption {
	return func(s *Stage) { s.Parallelism = n }
}

// WithShuffle sets the shuffle function that transforms upstream outputs
// into this stage's input tasks. Must be set on all non-source stages.
func WithShuffle(fn ShuffleFunc) StageOption {
	return func(s *Stage) { s.Shuffle = fn }
}

// WithMaxRetries overrides the global retry limit for this stage.
func WithMaxRetries(n int) StageOption {
	return func(s *Stage) { s.MaxRetries = n }
}

// WithTimeout sets a per-task timeout in seconds for this stage.
func WithTimeout(secs int64) StageOption {
	return func(s *Stage) { s.TimeoutSecs = secs }
}

// DependsOn explicitly declares that this stage depends on the listed stages.
// Only needed for non-linear DAGs.
func DependsOn(ids ...string) StageOption {
	return func(s *Stage) { s.DependsOn = ids }
}

// Stage adds a stage to the pipeline.
func (b *Builder) Stage(id string, exec Executor, opts ...StageOption) *Builder {
	if b.err != nil {
		return b
	}
	s := &Stage{
		ID:         id,
		Executor:   exec,
		MaxRetries: -1, // use global default
	}
	for _, o := range opts {
		o(s)
	}
	// Auto-wire dependency: if no DependsOn set, depend on previous stage.
	if len(s.DependsOn) == 0 && len(b.stages) > 0 {
		s.DependsOn = []string{b.stages[len(b.stages)-1].ID}
	}
	b.stages = append(b.stages, s)
	return b
}

// Build validates and returns the compiled Pipeline.
func (b *Builder) Build() (*Pipeline, error) {
	if b.err != nil {
		return nil, b.err
	}
	if b.name == "" {
		return nil, fmt.Errorf("pipeline name is required")
	}
	if len(b.stages) == 0 {
		return nil, fmt.Errorf("pipeline %q has no stages", b.name)
	}

	p := &Pipeline{
		Name:   b.name,
		Stages: make(map[string]*Stage, len(b.stages)),
	}

	// Check for duplicate IDs
	for _, s := range b.stages {
		if _, exists := p.Stages[s.ID]; exists {
			return nil, fmt.Errorf("duplicate stage ID %q", s.ID)
		}
		p.Stages[s.ID] = s
	}

	// Validate dependencies exist
	for _, s := range b.stages {
		for _, dep := range s.DependsOn {
			if _, ok := p.Stages[dep]; !ok {
				return nil, fmt.Errorf("stage %q depends on unknown stage %q", s.ID, dep)
			}
		}
	}

	// Topological sort
	order, err := topoSort(b.stages)
	if err != nil {
		return nil, fmt.Errorf("pipeline %q has a cycle: %w", b.name, err)
	}
	p.Order = order

	// Validate: non-source stages must have a shuffle function
	for i, id := range p.Order {
		s := p.Stages[id]
		if i > 0 && s.Shuffle == nil {
			return nil, fmt.Errorf(
				"stage %q has upstream dependencies but no shuffle function; "+
					"use dag.WithShuffle(...) or dag.JSONKeyGroupShuffle()", id)
		}
	}

	return p, nil
}

// Topological sort (Kahn's algorithm)
func topoSort(stages []*Stage) ([]string, error) {
	inDegree := make(map[string]int, len(stages))
	adj := make(map[string][]string, len(stages))

	for _, s := range stages {
		if _, ok := inDegree[s.ID]; !ok {
			inDegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.ID)
			inDegree[s.ID]++
		}
	}

	// Start with nodes that have no incoming edges
	var queue []string
	for _, s := range stages {
		if inDegree[s.ID] == 0 {
			queue = append(queue, s.ID)
		}
	}
	sort.Strings(queue) // deterministic order

	var order []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)
		neighbors := adj[node]
		sort.Strings(neighbors)
		for _, nb := range neighbors {
			inDegree[nb]--
			if inDegree[nb] == 0 {
				queue = append(queue, nb)
			}
		}
	}

	if len(order) != len(stages) {
		return nil, fmt.Errorf("cycle detected")
	}
	return order, nil
}
