// Package scheduler provides pluggable worker-selection algorithms.
package scheduler

import (
	"fmt"
	"sync"
	"time"
)

// WorkerSnapshot — the scheduler's view of a worker
// WorkerSnapshot is a point-in-time view of a worker, safe to read without locks.
type WorkerSnapshot struct {
	ID            string
	Tags          []string
	ActiveTasks   int
	CPUUsage      float32
	LastHeartbeat time.Time
}

// Scheduler interface
// Scheduler picks the best worker for a given task.
type Scheduler interface {
	// Pick selects a worker from the provided snapshot list.
	// requiredTags: the worker must have ALL of these tags.
	// Returns the chosen worker ID, or an error if no suitable worker is found.
	Pick(workers []WorkerSnapshot, requiredTags []string) (string, error)
}

// Round-robin
type roundRobin struct {
	mu  sync.Mutex
	idx int
}

func NewRoundRobin() Scheduler { return &roundRobin{} }

func (r *roundRobin) Pick(workers []WorkerSnapshot, required []string) (string, error) {
	eligible := filterByTags(workers, required)
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers (required tags: %v)", required)
	}
	r.mu.Lock()
	id := eligible[r.idx%len(eligible)].ID
	r.idx++
	r.mu.Unlock()
	return id, nil
}

// Least-loaded (fewest active tasks, then lowest CPU usage)
type leastLoaded struct{}

func NewLeastLoaded() Scheduler { return &leastLoaded{} }

func (l *leastLoaded) Pick(workers []WorkerSnapshot, required []string) (string, error) {
	eligible := filterByTags(workers, required)
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers (required tags: %v)", required)
	}
	best := eligible[0]
	for _, w := range eligible[1:] {
		if w.ActiveTasks < best.ActiveTasks {
			best = w
		} else if w.ActiveTasks == best.ActiveTasks && w.CPUUsage < best.CPUUsage {
			best = w
		}
	}
	return best.ID, nil
}

// Tag-affinity (best tag overlap, then least loaded)

type tagAffinity struct{}

func NewTagAffinity() Scheduler { return &tagAffinity{} }

func (t *tagAffinity) Pick(workers []WorkerSnapshot, required []string) (string, error) {
	eligible := filterByTags(workers, required)
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers (required tags: %v)", required)
	}
	// Rank by number of matching required tags first, then by active tasks.
	best := eligible[0]
	bestScore := tagScore(best, required)
	for _, w := range eligible[1:] {
		score := tagScore(w, required)
		if score > bestScore ||
			(score == bestScore && w.ActiveTasks < best.ActiveTasks) {
			best = w
			bestScore = score
		}
	}
	return best.ID, nil
}

func tagScore(w WorkerSnapshot, required []string) int {
	tagSet := make(map[string]bool, len(w.Tags))
	for _, t := range w.Tags {
		tagSet[t] = true
	}
	score := 0
	for _, r := range required {
		if tagSet[r] {
			score++
		}
	}
	return score
}

// Factory
// New creates a Scheduler by name.
func New(algorithm string) (Scheduler, error) {
	switch algorithm {
	case "round_robin":
		return NewRoundRobin(), nil
	case "least_loaded", "":
		return NewLeastLoaded(), nil
	case "tag_affinity":
		return NewTagAffinity(), nil
	default:
		return nil, fmt.Errorf("unknown scheduler algorithm %q", algorithm)
	}
}

// Helpers
func filterByTags(workers []WorkerSnapshot, required []string) []WorkerSnapshot {
	if len(required) == 0 {
		return workers
	}
	var out []WorkerSnapshot
	for _, w := range workers {
		if hasAllTags(w.Tags, required) {
			out = append(out, w)
		}
	}
	return out
}

func hasAllTags(workerTags, required []string) bool {
	set := make(map[string]bool, len(workerTags))
	for _, t := range workerTags {
		set[t] = true
	}
	for _, r := range required {
		if !set[r] {
			return false
		}
	}
	return true
}
