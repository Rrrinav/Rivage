# Change-log to track progress for report making

## 2nd commit

Implemented polyglot MapReduce pipeline using Python scripts as task executors.

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

Currently we only have a coordinator that and a worker script.

- In coordinator: We have a bi-directional stream which starts accepting client connections which can be a registeration request or a Task `result`
- In worker: We connect to the worker and recieve messages which is just a `task`

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
