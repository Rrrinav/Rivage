// Package config handles loading and validating quantitative configuration
// from YAML/JSON files. Logical/structural config is done in Go via the DAG API.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ─────────────────────────────────────────────────────────────────────────────
// Coordinator config
// ─────────────────────────────────────────────────────────────────────────────

type CoordinatorConfig struct {
	Server    ServerConfig    `yaml:"server"`
	Security  SecurityConfig  `yaml:"security"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
}

type ServerConfig struct {
	GRPCAddr          string `yaml:"grpc_addr"`
	HTTPAddr          string `yaml:"http_addr"`
	MaxConcurrentJobs int    `yaml:"max_concurrent_jobs"`
}

type SecurityConfig struct {
	Enabled      bool   `yaml:"enabled"`
	SharedSecret string `yaml:"shared_secret"`
	TLSCertFile  string `yaml:"tls_cert_file"`
	TLSKeyFile   string `yaml:"tls_key_file"`
	TLSCAFile    string `yaml:"tls_ca_file"`
}

type SchedulerConfig struct {
	// Algorithm: "round_robin" | "least_loaded" | "tag_affinity"
	Algorithm        string   `yaml:"algorithm"`
	HeartbeatTimeout Duration `yaml:"heartbeat_timeout"`
	TaskTimeout      Duration `yaml:"task_timeout"`
	WatchdogInterval Duration `yaml:"watchdog_interval"`
	MaxGlobalRetries int      `yaml:"max_global_retries"`
}

type TelemetryConfig struct {
	MetricsAddr string `yaml:"metrics_addr"`
	LogLevel    string `yaml:"log_level"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker config
// ─────────────────────────────────────────────────────────────────────────────

type WorkerConfig struct {
	Coordinator WorkerCoordinatorConfig `yaml:"coordinator"`
	Execution   ExecutionConfig         `yaml:"execution"`
	Security    WorkerSecurityConfig    `yaml:"security"`
	Tags        []string                `yaml:"tags"`
}

type WorkerCoordinatorConfig struct {
	Addr                 string   `yaml:"addr"`
	ReconnectInterval    Duration `yaml:"reconnect_interval"`
	MaxReconnectAttempts int      `yaml:"max_reconnect_attempts"`
}

type ExecutionConfig struct {
	// MaxConcurrentTasks: 0 means use runtime.NumCPU()
	MaxConcurrentTasks int      `yaml:"max_concurrent_tasks"`
	HeartbeatInterval  Duration `yaml:"heartbeat_interval"`
	TempDir            string   `yaml:"temp_dir"`
	DefaultTaskTimeout Duration `yaml:"default_task_timeout"`
}

type WorkerSecurityConfig struct {
	SharedSecret string `yaml:"shared_secret"`
	TLSCertFile  string `yaml:"tls_cert_file"`
	TLSKeyFile   string `yaml:"tls_key_file"`
	TLSCAFile    string `yaml:"tls_ca_file"`
}

// ─────────────────────────────────────────────────────────────────────────────
// Duration: yaml/json-serialisable time.Duration
// ─────────────────────────────────────────────────────────────────────────────

type Duration struct{ time.Duration }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}
func (d Duration) MarshalYAML() (interface{}, error) { return d.String(), nil }
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	dur, err := time.ParseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = dur
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Loaders
// ─────────────────────────────────────────────────────────────────────────────

func LoadCoordinatorConfig(path string) (*CoordinatorConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}
	cfg := defaultCoordinatorConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config %q: %w", path, err)
	}
	return cfg, cfg.validate()
}

func LoadWorkerConfig(path string) (*WorkerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}
	cfg := defaultWorkerConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config %q: %w", path, err)
	}
	return cfg, cfg.validate()
}

func defaultCoordinatorConfig() *CoordinatorConfig {
	return &CoordinatorConfig{
		Server: ServerConfig{
			GRPCAddr:          ":50051",
			HTTPAddr:          ":8080",
			MaxConcurrentJobs: 100,
		},
		Scheduler: SchedulerConfig{
			Algorithm:        "least_loaded",
			HeartbeatTimeout: Duration{Duration: 10 * time.Second},
			TaskTimeout:      Duration{Duration: 5 * time.Minute},
			WatchdogInterval: Duration{Duration: 1 * time.Second},
			MaxGlobalRetries: 3,
		},
		Telemetry: TelemetryConfig{LogLevel: "info"},
	}
}

func defaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		Coordinator: WorkerCoordinatorConfig{
			Addr:                 "localhost:50051",
			ReconnectInterval:    Duration{Duration: 5 * time.Second},
			MaxReconnectAttempts: 0,
		},
		Execution: ExecutionConfig{
			MaxConcurrentTasks: 0,
			HeartbeatInterval:  Duration{Duration: 3 * time.Second},
			DefaultTaskTimeout: Duration{Duration: 5 * time.Minute},
		},
	}
}

func (c *CoordinatorConfig) validate() error {
	if c.Server.GRPCAddr == "" {
		return fmt.Errorf("server.grpc_addr is required")
	}
	switch c.Scheduler.Algorithm {
	case "round_robin", "least_loaded", "tag_affinity", "":
	default:
		return fmt.Errorf("scheduler.algorithm %q is unknown", c.Scheduler.Algorithm)
	}
	return nil
}

func (c *WorkerConfig) validate() error {
	if c.Coordinator.Addr == "" {
		return fmt.Errorf("coordinator.addr is required")
	}
	return nil
}

