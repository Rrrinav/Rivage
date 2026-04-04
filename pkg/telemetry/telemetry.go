// Package telemetry provides structured logging and metrics primitives.
package telemetry

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func ParseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

func (l Level) label() string {
	switch l {
	case LevelDebug:
		return "DBG"
	case LevelInfo:
		return "INF"
	case LevelWarn:
		return "WRN"
	case LevelError:
		return "ERR"
	default:
		return "???"
	}
}

// Logger is a structured, levelled logger with optional rate-limiting on
// noisy log sites via LogEvery / LogOnce.
type Logger struct {
	component string
	level     atomic.Int32
	out       *log.Logger

	suppressMu sync.Mutex
	suppress   map[string]suppressState
}

type suppressState struct {
	lastEmitted time.Time
	suppressed  int // messages dropped since last emission
}

func New(component string, level Level) *Logger {
	return newLogger(component, level, os.Stderr)
}

func NewWithWriter(component string, level Level, w io.Writer) *Logger {
	return newLogger(component, level, w)
}

func newLogger(component string, level Level, w io.Writer) *Logger {
	l := &Logger{
		component: component,
		out:       log.New(w, "", 0),
		suppress:  make(map[string]suppressState),
	}
	l.level.Store(int32(level))
	return l
}

func (l *Logger) SetLevel(level Level) { l.level.Store(int32(level)) }

func (l *Logger) Debug(msg string, fields ...interface{}) { l.emit(LevelDebug, msg, fields) }
func (l *Logger) Info(msg string, fields ...interface{})  { l.emit(LevelInfo, msg, fields) }
func (l *Logger) Warn(msg string, fields ...interface{})  { l.emit(LevelWarn, msg, fields) }
func (l *Logger) Error(msg string, fields ...interface{}) { l.emit(LevelError, msg, fields) }

// LogEvery emits msg at most once per interval for the given dedup key.
// Suppressed calls are counted; when the window expires the next emission
// includes a "suppressed=N" field so you know it was repeating.
func (l *Logger) LogEvery(key string, interval time.Duration, level Level, msg string, fields ...interface{}) {
	if int32(level) < l.level.Load() {
		return
	}
	now := time.Now()
	l.suppressMu.Lock()
	st := l.suppress[key]
	if !st.lastEmitted.IsZero() && now.Sub(st.lastEmitted) < interval {
		st.suppressed++
		l.suppress[key] = st
		l.suppressMu.Unlock()
		return
	}
	suppressed := st.suppressed
	l.suppress[key] = suppressState{lastEmitted: now}
	l.suppressMu.Unlock()

	extra := fields
	if suppressed > 0 {
		extra = append(append([]interface{}{}, fields...), "suppressed", suppressed)
	}
	l.emit(level, msg, extra)
}

// LogOnce emits msg exactly once per key for the lifetime of the logger.
func (l *Logger) LogOnce(key string, level Level, msg string, fields ...interface{}) {
	if int32(level) < l.level.Load() {
		return
	}
	l.suppressMu.Lock()
	if _, seen := l.suppress[key]; seen {
		l.suppressMu.Unlock()
		return
	}
	l.suppress[key] = suppressState{lastEmitted: time.Now()}
	l.suppressMu.Unlock()
	l.emit(level, msg, fields)
}

func (l *Logger) emit(level Level, msg string, fields []interface{}) {
	if int32(level) < l.level.Load() {
		return
	}
	ts := time.Now().Format("15:04:05.000")
	comp := l.component
	// Truncate long component names from the left so the important suffix shows.
	if len(comp) > 20 {
		comp = "…" + comp[len(comp)-19:]
	}
	kv := formatFields(fields)
	if kv != "" {
		l.out.Printf("%s %s [%s] %s  %s", ts, level.label(), comp, msg, kv)
	} else {
		l.out.Printf("%s %s [%s] %s", ts, level.label(), comp, msg)
	}
}

func formatFields(fields []interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	var sb strings.Builder
	for i := 0; i+1 < len(fields); i += 2 {
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%v=%v", fields[i], fields[i+1])
	}
	return sb.String()
}

// Counter is a monotonically increasing counter.
type Counter struct{ v atomic.Int64 }

func (c *Counter) Inc()        { c.v.Add(1) }
func (c *Counter) Add(n int64) { c.v.Add(n) }
func (c *Counter) Load() int64 { return c.v.Load() }

// Gauge is a value that can go up or down.
type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(n int64) { g.v.Store(n) }
func (g *Gauge) Inc()        { g.v.Add(1) }
func (g *Gauge) Dec()        { g.v.Add(-1) }
func (g *Gauge) Load() int64 { return g.v.Load() }

// Metrics holds cluster-wide counters.
type Metrics struct {
	TasksDispatched Counter
	TasksCompleted  Counter
	TasksFailed     Counter
	TasksRetried    Counter
	ActiveWorkers   Gauge
	ActiveTasks     Gauge
	TotalComputeMs  Counter
	BytesProcessed  Counter
}

var Global = &Metrics{}
