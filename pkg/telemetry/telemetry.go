// Package telemetry provides structured logging and metrics primitives.
package telemetry

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Logger
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

// Logger is a simple levelled logger.
type Logger struct {
	component string
	level     atomic.Int32
	out       *log.Logger
}

func New(component string, level Level) *Logger {
	l := &Logger{
		component: component,
		out:       log.New(os.Stderr, "", 0),
	}
	l.level.Store(int32(level))
	return l
}

func NewWithWriter(component string, level Level, w io.Writer) *Logger {
	l := &Logger{
		component: component,
		out:       log.New(w, "", 0),
	}
	l.level.Store(int32(level))
	return l
}

func (l *Logger) SetLevel(level Level) { l.level.Store(int32(level)) }

func (l *Logger) Debug(msg string, fields ...interface{}) { l.log(LevelDebug, "DEBUG", msg, fields) }
func (l *Logger) Info(msg string, fields ...interface{})  { l.log(LevelInfo, "INFO ", msg, fields) }
func (l *Logger) Warn(msg string, fields ...interface{})  { l.log(LevelWarn, "WARN ", msg, fields) }
func (l *Logger) Error(msg string, fields ...interface{}) { l.log(LevelError, "ERROR", msg, fields) }

func (l *Logger) log(level Level, label, msg string, fields []interface{}) {
	if int32(level) < l.level.Load() {
		return
	}
	ts := time.Now().Format("2006-01-02T15:04:05.000Z07:00")
	kv := formatFields(fields)
	if kv != "" {
		l.out.Printf("%s [%s] [%s] %s | %s", ts, label, l.component, msg, kv)
	} else {
		l.out.Printf("%s [%s] [%s] %s", ts, label, l.component, msg)
	}
}

func formatFields(fields []interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	var sb strings.Builder
	for i := 0; i+1 < len(fields); i += 2 {
		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(fmt.Sprintf("%v=%v", fields[i], fields[i+1]))
	}
	return sb.String()
}

// Metrics (in-process counters — swap for Prometheus in production)
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
}

var Global = &Metrics{}
