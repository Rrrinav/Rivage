#!/usr/bin/env python3
"""
build.py — Rivage build, proto-generation, test, and run script.

Usage:
    python build.py              # tidy deps + build binaries
    python build.py proto        # generate protobuf Go files only
    python build.py test         # tidy + build + run tests
    python build.py run          # tidy + build + run coordinator + 2 workers
    python build.py run 4 -e matmul     # run with Matrix Multiplication
    python build.py run 4 -e hashcrack  # run with Hash Cracking
    python build.py clean        # remove bin/ directory
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# ANSI colours (disabled on Windows)
USE_COLOR = sys.platform != "win32"


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def info(msg: str) -> None: print(_c("1;34", f">>> {msg}"))
def ok(msg: str) -> None: print(_c("1;32", f"    ✓ {msg}"))
def warn(msg: str) -> None: print(_c("1;33", f"    ! {msg}"))
def fatal(msg: str) -> None: print(_c("1;31",
                                      f"    ✗ {msg}"), file=sys.stderr); sys.exit(1)


def step(msg: str) -> None: print(_c("0;36", f"  → {msg}"))


def run(cmd: list[str], env: dict | None = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a command, streaming output live, with a merged environment."""
    merged_env = {**os.environ, **(env or {})}
    step(" ".join(cmd))
    result = subprocess.run(cmd, env=merged_env)
    if check and result.returncode != 0:
        fatal(f"Command failed with exit code {
              result.returncode}: {' '.join(cmd)}")
    return result


def require(binary: str) -> str:
    """Return full path to binary or abort with a helpful message."""
    path = shutil.which(binary)
    if not path:
        fatal(
            f"'{binary}' not found in PATH.\n"
            f"  Install it and make sure it is on your PATH, then re-run."
        )
    return path


def go_env(key: str) -> str:
    """Query a single Go environment variable (e.g. GOPATH, GOROOT)."""
    result = subprocess.run(
        ["go", "env", key],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        fatal(f"'go env {key}' failed — is Go installed?")
    return result.stdout.strip()


def go_bin_path() -> str:
    """Return the Go bin directory where `go install`ed tools land."""
    gopath = go_env("GOPATH")
    if not gopath:
        # Fall back to the universal default
        gopath = str(Path.home() / "go")
    return str(Path(gopath) / "bin")


def patched_env() -> dict:
    """Return an env dict with the Go bin directory prepended to PATH."""
    go_bin = go_bin_path()
    current_path = os.environ.get("PATH", "")
    if go_bin not in current_path.split(os.pathsep):
        return {"PATH": go_bin + os.pathsep + current_path}
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent


def cmd_proto() -> None:
    """Generate Go code from proto/rivage.proto using protoc."""
    info("Generating protobuf Go files")

    proto_file = ROOT / "proto" / "rivage.proto"
    if not proto_file.exists():
        fatal(f"Proto file not found: {proto_file}")

    # 1. Check protoc
    require("protoc")
    step(f"protoc version: {subprocess.check_output(
        ['protoc', '--version'], text=True).strip()}")

    # 2. Ensure Go protoc plugins are installed
    env = patched_env()
    go_bin = go_bin_path()

    for plugin, import_path in [
        ("protoc-gen-go",      "google.golang.org/protobuf/cmd/protoc-gen-go@latest"),
        ("protoc-gen-go-grpc", "google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"),
    ]:
        plugin_path = Path(go_bin) / plugin
        if not plugin_path.exists():
            warn(f"{plugin} not found — installing...")
            run(["go", "install", import_path])
        else:
            step(f"{plugin} found at {plugin_path}")

    # 3. Run protoc
    run([
        "protoc",
        "--go_out=.",
        "--go_opt=paths=source_relative",
        "--go-grpc_out=.",
        "--go-grpc_opt=paths=source_relative",
        str(proto_file.relative_to(ROOT)),
    ], env=env)

    # 4. Remove hand-written stubs if they still exist (generated files replace them)
    for stub in ["proto/messages.go", "proto/service.go"]:
        stub_path = ROOT / stub
        if stub_path.exists():
            stub_path.unlink()
            step(f"Removed hand-written stub: {stub}")

    ok("Proto generation complete → proto/rivage.pb.go + proto/rivage_grpc.pb.go")


def cmd_tidy() -> None:
    """Run go mod tidy."""
    info("Tidying Go module dependencies")
    os.chdir(ROOT)
    run(["go", "mod", "tidy"])
    ok("go mod tidy complete")


def cmd_build() -> None:
    """Build coordinator and worker binaries into bin/."""
    info("Building binaries")
    os.chdir(ROOT)

    bin_dir = ROOT / "bin"
    bin_dir.mkdir(exist_ok=True)

    coordinator_out = bin_dir / \
        ("coordinator.exe" if sys.platform == "win32" else "coordinator")
    worker_out = bin_dir / \
        ("worker.exe" if sys.platform == "win32" else "worker")
    datastore_out = bin_dir / \
        ("datastore.exe" if sys.platform == "win32" else "datastore")

    run(["go", "build", "-o", str(coordinator_out), "./cmd/coordinator/"])
    ok(f"coordinator → {coordinator_out}")

    run(["go", "build", "-o", str(worker_out), "./cmd/worker/"])
    ok(f"worker      → {worker_out}")

    run(["go", "build", "-o", str(datastore_out), "./cmd/datastore/"])
    ok(f"datastore   → {datastore_out}")

    ok("Build complete")


def cmd_test() -> None:
    """Run the full test suite."""
    info("Running tests")
    os.chdir(ROOT)
    run(["go", "test", "./...", "-v", "-timeout", "60s"])
    ok("All tests passed")


def cmd_run(num_workers: int = 2, example: str = "matmul") -> None:
    """Start coordinator + N workers, Ctrl+C to stop all."""
    info(f"Starting Data Store, Coordinator + {num_workers} worker(s) [Example: {example}]")
    os.chdir(ROOT)

    bin_dir = ROOT / "bin"
    coordinator = bin_dir / \
        ("coordinator.exe" if sys.platform == "win32" else "coordinator")
    worker_bin = bin_dir / \
        ("worker.exe" if sys.platform == "win32" else "worker")
    datastore_bin = bin_dir / \
        ("datastore.exe" if sys.platform == "win32" else "datastore")
    coord_cfg = ROOT / "configs" / "coordinator.yaml"
    worker_cfg = ROOT / "configs" / "worker.yaml"

    for f in [coordinator, worker_bin, datastore_bin, coord_cfg, worker_cfg]:
        if not f.exists():
            fatal(f"Required file not found: {
                  f}\nRun 'python build.py' first.")

    processes: list[subprocess.Popen] = []

    def cleanup(sig=None, frame=None):
        print()
        info("Shutting down all processes...")
        for p in processes:
            try:
                p.terminate()
            except Exception:
                pass
        for p in processes:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

        ok("All processes stopped and data directories cleaned")
        sys.exit(0)

    signal.signal(signal.SIGINT,  cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # Start Data Store
    step("Starting datastore...")
    ds_proc = subprocess.Popen([str(datastore_bin)])
    processes.append(ds_proc)
    time.sleep(0.5)

    # Start coordinator, passing the chosen example
    step(f"Starting coordinator with example '{example}'...")
    coord_proc = subprocess.Popen(
        [str(coordinator), "-config", str(coord_cfg), "-example", example])
    processes.append(coord_proc)
    time.sleep(1)  # give the coordinator a moment to bind the port

    # Start workers
    for i in range(1, num_workers + 1):
        step(f"Starting worker {i}...")
        wp = subprocess.Popen([str(worker_bin), "-config", str(worker_cfg)])
        processes.append(wp)
        time.sleep(0.2)

    print()
    ok(f"Data Store PID  : {ds_proc.pid}")
    ok(f"Coordinator PID : {coord_proc.pid}")
    ok(f"Worker PIDs     : {[p.pid for p in processes[2:]]}")
    print()
    print(_c("1;37", "  Status API : http://localhost:8080/status"))
    print(_c("1;37", "  Metrics    : http://localhost:8080/metrics"))
    print(_c("1;37", "  Health     : http://localhost:8080/healthz"))
    print()
    warn("Press Ctrl+C to stop all processes")
    print()

    # Wait for coordinator to exit (it usually runs forever)
    coord_proc.wait()
    cleanup()


def cmd_clean() -> None:
    """Remove the bin/ directory."""
    info("Cleaning build artifacts")
    bin_dir = ROOT / "bin"
    if bin_dir.exists():
        shutil.rmtree(bin_dir)
        ok(f"Removed {bin_dir}")
    else:
        step("Nothing to clean")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rivage build tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="build",
        choices=["build", "proto", "test", "run", "clean"],
        help="Command to run (default: build)",
    )
    parser.add_argument(
        "workers",
        nargs="?",
        type=int,
        default=2,
        help="Number of workers to start (only used with 'run', default: 2)",
    )
    # NEW: The Example Flag
    parser.add_argument(
        "-e", "--example",
        type=str,
        default="matmul",
        choices=["matmul", "hashcrack"],
        help="Which example job to run (default: matmul)",
    )
    args = parser.parse_args()

    os.chdir(ROOT)

    if args.command == "proto":
        cmd_proto()

    elif args.command == "build":
        cmd_tidy()
        cmd_build()

    elif args.command == "test":
        cmd_tidy()
        cmd_build()
        cmd_test()

    elif args.command == "run":
        cmd_tidy()
        cmd_build()
        cmd_run(num_workers=args.workers, example=args.example)

    elif args.command == "clean":
        cmd_clean()


if __name__ == "__main__":
    main()
