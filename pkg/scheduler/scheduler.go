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
	CachedKeys    []string // data-locality hint: keys this worker has already processed
	Capacity      int      // maximum tasks this worker can handle (usually NumCPU)
}

// Scheduler picks the best worker for a given task.
type Scheduler interface {
	Pick(workers []WorkerSnapshot, requiredTags []string, affinityKeys []string) (string, error)
}

// New creates a Scheduler for the named algorithm.
// prefetchMultiplier controls how many in-flight tasks are allowed per core:
//
//	1.0  → exactly one task per core  (CPU-heavy workloads)
//	2.0+ → over-subscribe to hide latency (fast network-heavy workloads)
func New(algorithm string, prefetchMultiplier float32) (Scheduler, error) {
	if prefetchMultiplier <= 0 {
		prefetchMultiplier = 1.0
	}
	switch algorithm {
	case "round_robin":
		return &roundRobin{prefetch: prefetchMultiplier}, nil
	case "least_loaded", "":
		return &leastLoaded{prefetch: prefetchMultiplier}, nil
	case "tag_affinity":
		return &tagAffinity{prefetch: prefetchMultiplier}, nil
	default:
		return nil, fmt.Errorf("unknown scheduler algorithm %q", algorithm)
	}
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

// filterByCapacity removes workers whose in-flight task count is at or above
// their dynamically calculated limit (Capacity * prefetchMultiplier).
// Every worker is guaranteed at least one slot even if the multiplier rounds down.
func filterByCapacity(workers []WorkerSnapshot, prefetch float32) []WorkerSnapshot {
	var out []WorkerSnapshot
	for _, w := range workers {
		limit := int(float32(w.Capacity) * prefetch)
		if limit < 1 {
			limit = 1
		}
		if w.ActiveTasks < limit {
			out = append(out, w)
		}
	}
	return out
}

type roundRobin struct {
	mu      sync.Mutex
	idx     int
	prefetch float32
}

func (r *roundRobin) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible, r.prefetch)
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers with available capacity (required tags: %v)", required)
	}
	r.mu.Lock()
	id := eligible[r.idx%len(eligible)].ID
	r.idx++
	r.mu.Unlock()
	return id, nil
}

type leastLoaded struct {
	prefetch float32
}

func (l *leastLoaded) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible, l.prefetch)
	if len(eligible) == 0 {
		return "", fmt.Errorf("no eligible workers with available capacity (required tags: %v)", required)
	}

	// Prefer workers with the highest data-affinity score, then break ties by
	// fewest active tasks, then by lowest CPU usage.
	maxAffinity := -1
	for _, w := range eligible {
		if s := affinityScore(w.CachedKeys, affinityKeys); s > maxAffinity {
			maxAffinity = s
		}
	}

	var best WorkerSnapshot
	first := true
	for _, w := range eligible {
		if affinityScore(w.CachedKeys, affinityKeys) != maxAffinity {
			continue
		}
		if first {
			best = w
			first = false
			continue
		}
		if w.ActiveTasks < best.ActiveTasks ||
			(w.ActiveTasks == best.ActiveTasks && w.CPUUsage < best.CPUUsage) {
			best = w
		}
	}

	if first {
		return "", fmt.Errorf("no eligible workers")
	}
	return best.ID, nil
}

type tagAffinity struct {
	prefetch float32
}

func (t *tagAffinity) Pick(workers []WorkerSnapshot, required []string, affinityKeys []string) (string, error) {
	eligible := filterByTags(workers, required)
	eligible = filterByCapacity(eligible, t.prefetch)
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
