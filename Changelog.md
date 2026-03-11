# Change-log to track progress for report making

## 7th commit

> Decoupled Storage Architecture: Storage/Compute Separation, I/O Pipelining, and Cloud-Native Output Aggregation.

**Architectural Shift:**
Transitioned the system from a "Heavy Coordinator" model (where all data flowed through the central node via gRPC) to a "Decoupled Data Plane" model. This shift eliminates the gRPC Head-of-Line blocking bottleneck and allows the cluster to process datasets that significantly exceed the physical RAM capacity of any single node.

**Changes:**
1. **Dedicated Data Store Node (cmd/datastore):**
    - Engineered a high-performance, standalone HTTP service that acts as a centralized object store (mimicking AWS S3 or HDFS).
    - The service handles atomic binary uploads (PUT) and downloads (GET) directly to/from a local project directory (./rivage_data).
    - Benefit: The Coordinator is no longer a bottleneck for large binary transfers, reducing its memory footprint to nearly zero during massive jobs.
2. **URL-Based Task Dispatching & Control Plane Separation:**
    - Modified the matmul pipeline to utilize "Data Pointers" (URLs) instead of raw byte payloads.
    - The Coordinator now dispatches tiny JSON messages containing URLs for Matrix tiles. This ensures that the gRPC control stream remains ultra-responsive, even while gigabytes of data are moving across the network.
3. **Asynchronous I/O Profiling & Instrumentation:**
    - Injected granular instrumentation into the Python compute workers (tile_multiply.py and assemble.py) using high-resolution timers.
    - Metric Isolation: Successfully isolated io_time (network fetch + disk write) from compute_time (NumPy arithmetic).
    - Workers now return these metrics as part of their metadata, allowing for cluster-wide performance analysis.
4. **Local Mmap Hybrid (Zero-Copy Persistence):**
    - Optimized workers to stream HTTP data directly to the local disk (./rivage_worker_data) and subsequently treat those files as virtual memory via numpy.memmap.
    - This allows the system to compute 10,000x10,000+ matrices (which require ~1GB+ per band) on machines with limited physical RAM, relying on the Linux kernel's page-swapping mechanisms for high-efficiency I/O.
5. **Cloud-Native Output Aggregation:**
    - Redesigned the job completion logic. The final matrix result is no longer downloaded and merged into the Coordinator's RAM.
    - The system now returns a "Cloud-Native Summary"—a JSON object containing aggregate cluster metrics and URLs to the final binary result bands stored on the Data Store.
6. **Advanced Build Orchestration (build.py):**
    - Upgraded the Python build tool to manage three distinct binary classes: Coordinator, Worker, and DataStore.
    - Implemented automated environment cleanup to ensure local data directories are purged upon cluster shutdown.

## 6th commit

> Complete V2 Architectural Refactor: Enterprise-grade DAG pipelines, Pluggable Schedulers, Security, and Distributed Matrix Multiplication.

**Changes:**

1. **Standard Go Project Layout**: Completely restructured the repository to follow industry-standard Go directory patterns.
    - **`cmd/`**: Thin entry points for compiling coordinator and worker binaries.
    - **`pkg/`**: Reusable, imported core library packages.
    - **`configs/`**: YAML-based quantitative configuration separation.
    - **`examples/`**: Implementation payloads (WordCount, MatMul).
2. **Directed Acyclic Graph (DAG) Pipeline Engine (pkg/dag):**
    - Replaced the hardcoded, inflexible MapReduce logic with a fluent Domain Specific Language (DSL) for building arbitrary execution pipelines.
    - Implemented Topological Sorting (Kahn's Algorithm) to automatically calculate execution order and guarantee deadlock-free resolution of stage dependencies.
    - Introduced programmable ShuffleFunc interfaces (e.g., JSONKeyGroupShuffle, PassThroughShuffle) to dictate how data flows between execution stages.
3. **Pluggable Scheduling & Backpressure (pkg/scheduler):**
    - **Abstracted worker selection into a decoupled Scheduler interface.**
    - **Implemented three distinct routing algorithms**: RoundRobin, LeastLoaded, and TagAffinity.
    - **Introduced Capacity-Aware Backpressure**: The scheduler dynamically tracks the ActiveTasks and CPUUsage of each worker node. It prevents "firehose" bottlenecks by holding tasks in a Pending state if a worker's concurrency limits (based on runtime CPU cores) are maxed out.
4. **Security & Telemetry Layers (pkg/security, pkg/telemetry):**
    - Engineered an HMAC-SHA256 token issuance and verification system to prevent unauthorized nodes from joining the cluster.
    - Added foundational support for mutual TLS (mTLS) certificate verification on the gRPC streams.
    - Built a concurrency-safe, leveled logger.
    - Implemented Prometheus-compatible atomic counters and gauges (e.g., TasksDispatched, ActiveWorkers) exposed via a dedicated HTTP admin API (:8080/metrics).
5. **Distributed Matrix Multiplication (examples/matmul):**
    - Successfully proved the dynamic code-injection architecture by distributing heavy linear algebra.
    - The coordinator mathematically chunks a dense 400x400 Matrix into 16 independent 100x100 tiles.
    - It ships raw python3 calculation scripts over the gRPC wire to ephemeral worker nodes.
    - Workers compute the sub-matrices via local OS sub-processes and return the JSON results to a final Reduce stage, which assembles the complete matrix in ~3.5 seconds.
6. Build Automation (build.py):
    - Developed a comprehensive Python orchestrator to manage go mod tidy, Protobuf compilation (protoc), binary compilation, and parallel multi-node local cluster simulation (./build.py run 4).

## 5th commit

> Implemented dynamic source code distribution over gRPC and telemetry logging.

**Changes:**
- **Protobuf Payload Upgrade**: Added a bytes code = 7; field to the Task message in system.proto to allow raw executable scripts or binaries to be transmitted directly over the network.
- **Dynamic Code Injection (Coordinator)**: The Coordinator now reads the target execution scripts (e.g., map.py, reduce.py) from its local disk into memory and embeds the raw bytes into the outgoing gRPC tasks. It passes a "{CODE}" placeholder in the arguments list instead of relying on hardcoded paths.
- **Ephemeral Execution (Worker)**: Workers no longer require any pre-installed task scripts on their local file systems. When a worker receives a task with an embedded code payload, it creates an ephemeral, executable temporary file (e.g., /tmp/rivage-task-*), substitutes the "{CODE}" placeholder with the generated file path, executes the process, and automatically deletes the temporary file upon completion to prevent disk bloat.
- **Telemetry Logging**: Added explicit `[Telemetry]` tags to heartbeat logs in the Coordinator to improve visibility into cluster node health.


## 4th commit

> Implemented gRPC Heartbeat Telemetry for robust Worker node health tracking.

**Changes:**
- **Protobuf Telemetry**: Upgraded the system.proto contract by adding a Heartbeat message to the ExecutorMessage oneof payload, allowing workers to send real-time health pings without disrupting the task result streams.
- **Worker Pulse Goroutine**: Workers now spawn a dedicated, lightweight background goroutine upon registration. This routine transmits a heartbeat to the Coordinator every 3 seconds, proving the process is alive independently of what tasks are currently executing.
- **Coordinator Radar**: The Coordinator now maintains a thread-safe workerHeartbeats map to track the lastSeen timestamp of every active worker.
- **Watchdog Refactor**: Completely decoupled Task Duration from Worker Health. The Watchdog no longer times out tasks after 15 seconds of execution. Instead, it checks if the assigned worker has missed its 10-second heartbeat window. This critical upgrade allows workers to process long-running, CPU-intensive tasks (like dense matrix multiplication) for hours without triggering false-positive failure recoveries.
- **Verified**: Ran tasks with simulated 5-minute execution times. The Watchdog correctly kept the tasks in the Running state because the worker's heartbeat loop continued to ping. Killing the worker process immediately halted the heartbeats, triggering the Watchdog to re-queue the tasks exactly 10 seconds later.

## 3rd commit

> Refactored Protobuf contracts, added dynamic worker IDs, and implemented Round-Robin task scheduling.

**Changes:**
- **Protobuf Contract Upgrade**: Removed the fragile string-parsing hack (extractJobID) by explicitly adding job_id and an enum TaskType (MAP, REDUCE) to both Task and TaskResult messages. Recompiled Protobuf files using the new build_proto.py script.
- **Dynamic Worker Registration**: Workers now dynamically generate unique IDs using their hostname and a timestamp (e.g., fedora-9540-1234). This allows multiple worker instances to run concurrently on the exact same machine without ID collisions in the Coordinator's sync.Map.
- **Round-Robin Task Scheduling**: Upgraded the coordinator's pickWorker logic. Instead of blindly sending 100% of tasks to the first available worker, it now maintains a thread-safe workerList (protected by a sync.Mutex) and distributes tasks evenly across all connected workers using a modulo-based Round-Robin algorithm (rrIndex % len).
- **Directory Restructuring**: Moved example-tasks/ inside the worker/ directory for better logical separation, updating the coordinator's script execution paths to worker/example-tasks/map.py and worker/example-tasks/reduce.py.
- **Build Automation**: Added build_proto.py to automate environment pathing and compilation of system.proto.
- **Verified**: Spun up multiple workers in separate terminals. The coordinator successfully alternated task dispatching between all workers, routing results seamlessly back to the Job struct using the new, strictly typed protobuf fields

## 2nd commit

> Implemented polyglot MapReduce pipeline using Python scripts as task executors.

**Changes**:
- Worker now does real task execution using os/exec instead of fake sleep.
  Pipes input_data to subprocess STDIN, captures STDOUT as result output.
  Worker is now language agnostic - it just runs whatever command it receives.

- Coordinator now has a full MapReduce orchestration function with 3 phases:
    - Map phase: splits input into chunks, dispatches each as a python3 task
    - Shuffle phase: runs in-memory on coordinator, merges map outputs into { key: [v1, v2, ...] }
    - Reduce phase: sends one task per unique key, waits, assembles final output

- Added Job struct with task tracking and a allDone channel so the coordinator
  blocks on each phase before moving to the next.

- Added two Python scripts under tasks/:
    - tasks/map.py   - reads { "chunk": [...] } from STDIN, emits { word: count }
    - tasks/reduce.py - reads { "key": "word", "values": [...] } from STDIN, emits { word: total }

- Proto unchanged for now. job_id and task_type fields are planned for next commit
  to replace the current string-parsing workaround in extractJobID.

Verified: word count job across 3 input chunks produces correct results with
reduce tasks completing out of order, confirming genuine concurrent execution.

```proto
message TaskResult {
  string   task_id     = 1;
  string   job_id      = 2;
  TaskType task_type   = 3;
  bool     success     = 4;
  bytes    output_data = 5;
  string   error_log   = 6;
}

message Task {
  string          task_id    = 1;
  string          job_id     = 2;   // NEW: ties this task back to a job
  TaskType        task_type  = 3;   // NEW: MAP, REDUCE, etc.
  string          command    = 4;   // e.g. "python3"
  repeated string args       = 5;   // e.g. ["tasks/map.py"]
  bytes           input_data = 6;   // JSON piped to STDIN
}
```

## 1st commit

> Currently we only have a coordinator that and a worker script.

- **In coordinator**: We have a bi-directional stream which starts accepting client connections which can be a registeration request or a Task `result`
- **In worker**: We connect to the worker and recieve messages which is just a `task`

```proto
message TaskResult {
  string task_id = 1;   // The ID of the task this is a result for
  bool success = 2;     // True if exit code was 0
  bytes output_data = 3; // Data captured from the sub-process's STDOUT
  string error_log = 4;   // Data captured from STDERR
}

message Task {
  string task_id = 1; // A unique ID so we can track this task

  // The command to run.
  // e.g., "./my_cpp_task", "python3", or "echo"
  string command = 2;

  // The arguments for the command.
  // e.g., "my_script.py" (if command was "python3")
  repeated string args = 3;

  // The raw data (e.g., JSON, text, etc.) that will be piped
  // to the sub-process's STDIN.
  bytes input_data = 4;
}
```
