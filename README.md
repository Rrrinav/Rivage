# Rivage

## Usage

```sh
$ go mod tidy

# run coordinator/server
go run coordinator/main.go

# run worker/client
go run coordinator/main.go
```

## Bugs

- [ ] Negative number of active tasks.

```sh
$ curl http://localhost:8080/metrics

rivage_tasks_dispatched_total 45
rivage_tasks_completed_total 45
rivage_tasks_failed_total 0
rivage_tasks_retried_total 0
rivage_active_workers 2
rivage_active_tasks -45
```

**rivage_active_tasks -45**
Negative number must not be possible
