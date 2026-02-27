# 1st commit

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
