package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"rivage/pkg/config"
	"rivage/pkg/worker"
)

func main() {
	cfgPath := flag.String("config", "configs/worker.yaml", "path to worker config file")
	// NEW: Define the coord-addr flag
	coordAddrFlag := flag.String("coord-addr", "", "Override coordinator gRPC address from config")
	flag.Parse()

	cfg, err := config.LoadWorkerConfig(*cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Override the YAML config dynamically if the flag was provided by build.py
	if *coordAddrFlag != "" {
		cfg.Coordinator.Addr = *coordAddrFlag
	}

	w, err := worker.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("Starting worker node (ID: %s) connecting to %s", w.ID(), cfg.Coordinator.Addr)

	if err := w.Run(ctx); err != nil {
		log.Fatalf("Worker exited with error: %v", err)
	}

	log.Println("Worker shutdown gracefully.")
}
