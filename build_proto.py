#!/usr/bin/env python3

import subprocess
import sys
import os
import shutil

def check_command(cmd):
    """Check if a command exists on the system."""
    return shutil.which(cmd) is not None

def get_go_bin_path():
    """Dynamically find the Go bin directory."""
    try:
        # Ask Go where its GOPATH is
        result = subprocess.run(["go", "env", "GOPATH"], check=True, capture_output=True, text=True)
        gopath = result.stdout.strip()
        return os.path.join(gopath, "bin")
    except Exception:
        # Fallback to the standard default
        return os.path.expanduser("~/go/bin")

def main():
    # Ensure protoc is installed
    if not check_command("protoc"):
        print("Error: 'protoc' is not installed or not in PATH.", file=sys.stderr)
        print("Please install the Protocol Buffers compiler.", file=sys.stderr)
        sys.exit(1)

    # Ensure we are running from the root directory where the proto folder exists
    proto_file = os.path.join("proto", "system.proto")
    if not os.path.exists(proto_file):
        print(f"Error: Could not find '{proto_file}'.", file=sys.stderr)
        print("Make sure you are running this script from the project root.", file=sys.stderr)
        sys.exit(1)

    # Inject the Go bin directory into the PATH so protoc can find the plugins
    env = os.environ.copy()
    go_bin = get_go_bin_path()
    if go_bin not in env.get("PATH", ""):
        env["PATH"] = f"{go_bin}{os.pathsep}{env.get('PATH', '')}"

    # The protoc command to generate Go code
    # Using '.' as the out dir with 'paths=source_relative' means it will
    # place the generated files exactly where the source file is (in proto/)
    cmd = [
        "protoc",
        "--go_out=.",
        "--go_opt=paths=source_relative",
        "--go-grpc_out=.",
        "--go-grpc_opt=paths=source_relative",
        proto_file
    ]

    print(f"Running: {' '.join(cmd)}")
    
    try:
        # Execute the command with our modified environment
        result = subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print("Successfully generated protobuf Go files in the proto/ directory!")
        
        # Print any non-error output if it exists
        if result.stdout:
            print(result.stdout)
            
    except subprocess.CalledProcessError as e:
        print("Failed to generate protobuf files.", file=sys.stderr)
        if e.stderr:
            print("Error Details:\n" + e.stderr, file=sys.stderr)
            
        if "program not found or is not executable" in e.stderr:
            print("\nHint: You are missing the Go protoc plugins. Run these commands:")
            print("  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest")
            print("  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest")
            
        sys.exit(1)

if __name__ == "__main__":
    main()
