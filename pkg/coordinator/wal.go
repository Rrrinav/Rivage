package coordinator

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type walEvent string

const (
	walEventDispatched walEvent = "DISPATCHED"
	walEventCompleted  walEvent = "COMPLETED"
	walEventFailed     walEvent = "FAILED"
)

type walRecord struct {
	Event  walEvent `json:"e"`
	TaskID string   `json:"t"`
}

type jobWAL struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// openWAL opens the log file for appending, and replays its history
// to return the last known state of every task.
func openWAL(path string) (*jobWAL, map[string]walEvent, error) {
	state := make(map[string]walEvent)

	// Read existing history to reconstruct state
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec walRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err == nil {
			state[rec.TaskID] = rec.Event
		}
	}

	return &jobWAL{file: f, enc: json.NewEncoder(f)}, state, nil
}

// Log safely writes a state transition to disk instantly.
func (w *jobWAL) Log(event walEvent, taskID string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.enc.Encode(walRecord{Event: event, TaskID: taskID})
}

func (w *jobWAL) Close() {
	if w != nil && w.file != nil {
		w.file.Close()
	}
}
