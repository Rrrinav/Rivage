#!/usr/bin/env python3
"""
build.py — Rivage build, proto-generation, test, and run script.

Usage:
    python build.py              # tidy deps + build binaries
    python build.py proto        # generate protobuf Go files only
    python build.py test         # tidy + build + run tests
    python build.py run 4        # (Local mode) run datastore, coordinator + 4 workers
    python build.py run --role master --master-ip 0.0.0.0 -e matmul
    python build.py run 4 --role worker --master-ip 192.168.1.100
    python build.py clean        # remove bin/ directory
"""

import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

USE_COLOR = sys.platform != "win32"


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if USE_COLOR else text


def info(msg: str) -> None: print(_c("1;34", f">>> {msg}"))
def ok(msg: str) -> None: print(_c("1;32", f"    [OK] {msg}"))
def warn(msg: str) -> None: print(_c("1;33", f"    [WARN] {msg}"))
def fatal(msg: str) -> None: print(_c("1;31",
                                      f"    [ERR] {msg}"), file=sys.stderr); sys.exit(1)


def step(msg: str) -> None: print(_c("0;36", f"  -> {msg}"))


def get_local_ip() -> str:
    """Attempts to find the machine's physical LAN IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Doesn't even have to be reachable, just forces OS to route a packet
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


def run(cmd: list[str], env: dict | None = None, check: bool = True) -> subprocess.CompletedProcess:
    merged_env = {**os.environ, **(env or {})}
    step(" ".join(cmd))
    result = subprocess.run(cmd, env=merged_env)
    if check and result.returncode != 0:
        fatal(f"Command failed with exit code {
              result.returncode}: {' '.join(cmd)}")
    return result


def require(binary: str) -> str:
    path = shutil.which(binary)
    if not path:
        fatal(f"'{
              binary}' not found in PATH.\n  Install it and make sure it is on your PATH, then re-run.")
    return path


def go_env(key: str) -> str:
    result = subprocess.run(["go", "env", key], capture_output=True, text=True)
    if result.returncode != 0:
        fatal(f"'go env {key}' failed — is Go installed?")
    return result.stdout.strip()


def go_bin_path() -> str:
    gopath = go_env("GOPATH")
    if not gopath:
        gopath = str(Path.home() / "go")
    return str(Path(gopath) / "bin")


def patched_env() -> dict:
    go_bin = go_bin_path()
    current_path = os.environ.get("PATH", "")
    if go_bin not in current_path.split(os.pathsep):
        return {"PATH": go_bin + os.pathsep + current_path}
    return {}


ROOT = Path(__file__).resolve().parent

# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────


def cmd_proto() -> None:
    info("Generating protobuf Go files")
    proto_file = ROOT / "proto" / "rivage.proto"
    if not proto_file.exists():
        fatal(f"Proto file not found: {proto_file}")

    require("protoc")
    step(f"protoc version: {subprocess.check_output(
        ['protoc', '--version'], text=True).strip()}")

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

    run([
        "protoc", "--go_out=.", "--go_opt=paths=source_relative",
        "--go-grpc_out=.", "--go-grpc_opt=paths=source_relative",
        str(proto_file.relative_to(ROOT)),
    ], env=env)

    for stub in ["proto/messages.go", "proto/service.go"]:
        stub_path = ROOT / stub
        if stub_path.exists():
            stub_path.unlink()
            step(f"Removed hand-written stub: {stub}")

    ok("Proto generation complete -> proto/rivage.pb.go + proto/rivage_grpc.pb.go")


def cmd_tidy() -> None:
    info("Tidying Go module dependencies")
    os.chdir(ROOT)
    run(["go", "mod", "tidy"])
    ok("go mod tidy complete")


def cmd_build() -> None:
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
    ok(f"coordinator -> {coordinator_out}")
    run(["go", "build", "-o", str(worker_out), "./cmd/worker/"])
    ok(f"worker      -> {worker_out}")
    run(["go", "build", "-o", str(datastore_out), "./cmd/datastore/"])
    ok(f"datastore   -> {datastore_out}")
    ok("Build complete")


def cmd_test() -> None:
    info("Running tests")
    os.chdir(ROOT)
    run(["go", "test", "./...", "-v", "-timeout", "60s"])
    ok("All tests passed")


def cmd_run(num_workers: int = 2, example: str = "matmul", role: str = "local", master_ip: str = "localhost") -> None:
    info(f"Starting deployment role: {role.upper()} [Workers: {
         num_workers}, Example: {example}, Master IP: {master_ip}]")
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

    # Setup connection URLs based on provided master_ip
    bind_ip = master_ip if master_ip not in ["localhost", ""] else "127.0.0.1"

    # If the master is binding to 0.0.0.0, it needs to tell its own workers to connect to 127.0.0.1
    coord_addr = f"{'127.0.0.1' if bind_ip == '0.0.0.0' else bind_ip}:50051"
    ds_url = f"http://{'127.0.0.1' if bind_ip ==
                       '0.0.0.0' else bind_ip}:8081/data"

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

        ok("All processes stopped safely")
        sys.exit(0)

    signal.signal(signal.SIGINT,  cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # MASTER ROLE (Datastore + Coordinator)
    if role in ["local", "master"]:
        step(f"Starting datastore on {bind_ip}:8081...")
        ds_proc = subprocess.Popen(
            [str(datastore_bin), "-addr", f"{bind_ip}:8081"])
        processes.append(ds_proc)
        time.sleep(0.5)

        step(f"Starting coordinator on {bind_ip}:50051 with example '{
             example}' and datastore '{ds_url}'...")
        coord_proc = subprocess.Popen([
            str(coordinator),
            "-config", str(coord_cfg),
            "-example", example,
            "-datastore", ds_url,
            "-grpc-addr", f"{bind_ip}:50051"
        ])
        processes.append(coord_proc)
        time.sleep(1)

    # WORKER ROLE (Worker nodes)
    if role in ["local", "worker"]:
        # If starting workers only, we strictly connect to the exact master_ip provided
        target_addr = coord_addr if role == "local" else f"{master_ip}:50051"
        for i in range(1, num_workers + 1):
            step(f"Starting worker {i} connecting to '{target_addr}'...")
            wp = subprocess.Popen(
                [str(worker_bin), "-config", str(worker_cfg), "-coord-addr", target_addr])
            processes.append(wp)
            time.sleep(0.2)

    print()
    if role in ["local", "master"]:
        ok(f"Data Store PID  : {ds_proc.pid}")
        ok(f"Coordinator PID : {coord_proc.pid}")

        # Display helpful connection string for external workers
        if role == "master":
            display_ip = master_ip if master_ip not in [
                "0.0.0.0", "localhost", "127.0.0.1", ""] else get_local_ip()
            print()
            info("To connect workers from other machines, run this command on them:")
            print(
                _c("1;32", f"    python build.py run 4 --role worker --master-ip {display_ip}"))
            print()

    if role in ["local", "worker"]:
        worker_pids = [p.pid for p in processes if "worker" in str(p.args)]
        ok(f"Worker PIDs     : {worker_pids}")

    print()
    warn("Press Ctrl+C to stop all processes")
    print()

    try:
        if role in ["local", "master"]:
            coord_proc.wait()
        elif len(processes) > 0:
            processes[0].wait()
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()


def cmd_clean() -> None:
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
        description="Rivage distributed build and run tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("command", nargs="?", default="build", choices=[
                        "build", "proto", "test", "run", "clean"])
    parser.add_argument("workers", nargs="?", type=int, default=2,
                        help="Number of workers (if role is local or worker)")

    # Flags for dynamic testing and distributed deployment
    parser.add_argument("-e", "--example", type=str, default="matmul",
                        choices=["matmul", "hashcrack", "crmm"], help="Example to run")
    parser.add_argument("--role", type=str, default="local",
                        choices=["local", "master", "worker"], help="Deployment role for the node")
    parser.add_argument("--master-ip", type=str, default="localhost",
                        help="IP address of the master node for network distributed runs")

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
        cmd_run(num_workers=args.workers, example=args.example,
                role=args.role, master_ip=args.master_ip)
    elif args.command == "clean":
        cmd_clean()


if __name__ == "__main__":
    main()
