// Package scheduler provides pluggable worker-selection algorithms.
package scheduler

import (
	"fmt"
	"sync"
	"time"
)

// WorkerSnapshot is a point-in-time view of a worker, safe to read without locks.
type WorkerSnapshot struct {
	ID            string
	Tags          []string
	ActiveTasks   int
	CPUUsage      float32
	LastHeartbeat time.Time
	CachedKeys    []string // NEW: Data Locality awareness
	Capacity      int      // NEW: Maximum tasks this worker can handle at once
}

// Scheduler picks the best worker for a given task.
type Scheduler interface {
	Pick(workers []WorkerSnapshot, requiredTags []string, affinityKeys []string) (string, error)
}

func affinityScore(cached, requested []string) int {
	if len(requested) == 0 || len(cached) == 0 {
		return 0
	}
	score := 0
	for _, r := range requested {
		for _, c := range cached {
			if r == c {
				score++
				break
			}
		}
	}
	return score
}

// Round-robin
type roundRobin struct {
	mu  sync.Mutex
	idx int
}

func NewRoundRobin() Scheduler { return &roundRobin{} }

func (r *roundRobin) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible) // Apply backpressure
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers with available capacity (required tags: %v)", required)
	}
	r.mu.Lock()
	id := eligible[r.idx%len(eligible)].ID
	r.idx++
	r.mu.Unlock()
	return id, nil
}

// Least-loaded (fewest active tasks, then lowest CPU usage)
// NEW: Upgraded to be Data-Locality aware!
type leastLoaded struct{}

func NewLeastLoaded() Scheduler { return &leastLoaded{} }

func (l *leastLoaded) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible) // Apply backpressure
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers with available capacity (required tags: %v)", required)
	}

	// Find the maximum affinity score among eligible workers
	maxAffinity := -1
	for _, w := range eligible {
		score := affinityScore(w.CachedKeys, affinityKeys)
		if score > maxAffinity {
			maxAffinity = score
		}
	}

	// Filter down to only the workers that have the max affinity score, 
	// then apply the least-loaded tie breaker.
	var best WorkerSnapshot
	first := true
	for _, w := range eligible {
		if affinityScore(w.CachedKeys, affinityKeys) == maxAffinity {
			if first {
				best = w
				first = false
			} else if w.ActiveTasks < best.ActiveTasks {
				best = w
			} else if w.ActiveTasks == best.ActiveTasks && w.CPUUsage < best.CPUUsage {
				best = w
			}
		}
	}
	
	if first {
		return "", fmt.Errorf("no eligible workers")
	}
	return best.ID, nil
}

// Tag-affinity
type tagAffinity struct{}

func NewTagAffinity() Scheduler { return &tagAffinity{} }

func (t *tagAffinity) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible) // Apply backpressure
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers with available capacity (required tags: %v)", required)
	}
	best := eligible[0]
	bestScore := tagScore(best, required)
	for _, w := range eligible[1:] {
		score := tagScore(w, required)
		if score > bestScore || (score == bestScore && w.ActiveTasks < best.ActiveTasks) {
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

// NEW: Filter out workers that are currently at or above their CPU core capacity
func filterByCapacity(workers []WorkerSnapshot) []WorkerSnapshot {
	var out []WorkerSnapshot
	for _, w := range workers {
		// Multiplier of 2 ensures the worker always has a small queue waiting
		// so the CPU never stalls between tasks, while preventing hoarding.
		if w.ActiveTasks < (w.Capacity * 2) {
			out = append(out, w)
		}
	}
	return out
}
