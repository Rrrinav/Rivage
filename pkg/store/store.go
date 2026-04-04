// Package store provides a disk-backed key/value store for large task outputs.
// Instead of accumulating gigabytes of []byte in RAM, each task result is
// written to a temp file and read back only when the shuffle or final merge
// actually needs it.  The coordinator and shuffle functions never hold more
// than one task's data in memory at a time.
package store

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ResultStore maps taskID → temp-file path.
// Call New() to create one per job; call Close() when the job is done to
// delete all temp files.
type ResultStore struct {
	mu      sync.RWMutex
	dir     string            // base temp directory
	entries map[string]string // taskID → file path
	durable bool              // NEW: Protects data from being deleted
}

// New creates a ResultStore whose temp files live under dir.
// If dir is empty, os.TempDir() is used.
func New(dir string) (*ResultStore, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	d, err := os.MkdirTemp(dir, "rivage-job-*")
	if err != nil {
		return nil, fmt.Errorf("creating job temp dir: %w", err)
	}
	return &ResultStore{dir: d, entries: make(map[string]string), durable: false}, nil
}

// NewDurable creates a ResultStore in a predictable, permanent path.
// This ensures that massive gigabyte outputs survive a Master reboot.
func NewDurable(baseDir, uniqueID string) (*ResultStore, error) {
	if baseDir == "" {
		baseDir = filepath.Join(".", "rivage_data")
	}
	path := filepath.Join(baseDir, "store_"+sanitize(uniqueID))
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("creating durable job dir: %w", err)
	}
	// NEW: Mark this store as durable so Close() ignores it!
	return &ResultStore{dir: path, entries: make(map[string]string), durable: true}, nil
}

// RecoverEntries scans the disk on reboot and re-links existing task data.
func (s *ResultStore) RecoverEntries() error {
	// FIX: We no longer eagerly populate the map with sanitized filenames,
	// because they don't match the raw TaskIDs with slashes!
	// Instead, the path() function now lazy-loads them directly from disk.
	return nil
}

// Write stores data for taskID, replacing any previous value.
func (s *ResultStore) Write(taskID string, data []byte) error {
	path := filepath.Join(s.dir, sanitize(taskID))
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("writing result for %q: %w", taskID, err)
	}
	s.mu.Lock()
	s.entries[taskID] = path
	s.mu.Unlock()
	return nil
}

// WriteFrom streams from r into a temp file for taskID.
// Useful for large gRPC payloads received as an io.Reader.
func (s *ResultStore) WriteFrom(taskID string, r io.Reader) (int64, error) {
	path := filepath.Join(s.dir, sanitize(taskID))
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("creating result file for %q: %w", taskID, err)
	}
	n, err := io.Copy(f, r)
	f.Close()
	if err != nil {
		os.Remove(path)
		return 0, fmt.Errorf("streaming result for %q: %w", taskID, err)
	}
	s.mu.Lock()
	s.entries[taskID] = path
	s.mu.Unlock()
	return n, nil
}

// Read returns the stored bytes for taskID.
// For very large outputs, prefer Open() to avoid loading everything into RAM.
func (s *ResultStore) Read(taskID string) ([]byte, error) {
	path, err := s.path(taskID)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// Open returns an *os.File positioned at byte 0.  Caller must Close() it.
func (s *ResultStore) Open(taskID string) (*os.File, error) {
	path, err := s.path(taskID)
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// Size returns the byte size of the stored result.
func (s *ResultStore) Size(taskID string) (int64, error) {
	path, err := s.path(taskID)
	if err != nil {
		return 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Keys returns all stored task IDs.
func (s *ResultStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.entries))
	for k := range s.entries {
		keys = append(keys, k)
	}
	return keys
}

// Close deletes all temp files and the temp directory (unless durable).
func (s *ResultStore) Close() error {
	if s.durable {
		return nil // NEW: Preserve the data on disk!
	}
	return os.RemoveAll(s.dir)
}

func (s *ResultStore) path(taskID string) (string, error) {
	s.mu.RLock()
	p, ok := s.entries[taskID]
	s.mu.RUnlock()
	if ok {
		return p, nil
	}

	// Lazy-load for WAL recovery!
	// If the Master asks for a raw taskID that isn't in RAM, check the hard drive directly.
	diskPath := filepath.Join(s.dir, sanitize(taskID))
	if _, err := os.Stat(diskPath); err == nil {
		s.mu.Lock()
		s.entries[taskID] = diskPath
		s.mu.Unlock()
		return diskPath, nil
	}

	return "", fmt.Errorf("no result stored for task %q", taskID)
}

// sanitize replaces path-separator characters so the taskID can be a filename.
func sanitize(id string) string {
	out := make([]byte, len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c == '/' || c == '\\' || c == ':' {
			c = '_'
		}
		out[i] = c
	}
	return string(out)
}
