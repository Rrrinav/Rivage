#!/usr/bin/env python3
import sys
import os
import json
import urllib.request
import time
import numpy as np

DATASTORE_URL = os.environ.get("RIVAGE_DATASTORE_URL")
if not DATASTORE_URL:
    print("FATAL: RIVAGE_DATASTORE_URL environment variable is missing!", file=sys.stderr)
    sys.exit(1)

def download_range(url, start_byte, end_byte):
    headers = {'Range': f'bytes={start_byte}-{end_byte}'}
    req = urllib.request.Request(url, headers=headers)
    
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read()

def upload_data(url, data_bytes):
    req = urllib.request.Request(url, data=data_bytes, method="PUT")
    urllib.request.urlopen(req, timeout=60)

def main():
    config = json.load(sys.stdin)
    tr, tc, n = config["tile_row"], config["tile_col"], config["total_n"]
    r_start, r_end = config["r_start"], config["r_end"]
    c_start, c_end = config["c_start"], config["c_end"]

    a_url = f"{DATASTORE_URL}/{config['a_file']}"
    bt_url = f"{DATASTORE_URL}/{config['b_t_file']}"
    upload_url = f"{DATASTORE_URL}/{config['upload_file']}"

    # Calculate precise byte boundaries (8 bytes per float64)
    a_start = r_start * n * 8
    a_end = r_end * n * 8 - 1
    bt_start = c_start * n * 8
    bt_end = c_end * n * 8 - 1

    t0 = time.time()
    
    # Pull exactly the byte-bands needed directly into RAM
    a_bytes = download_range(a_url, a_start, a_end)
    bt_bytes = download_range(bt_url, bt_start, bt_end)
    io_time = time.time() - t0

    # Parse raw bytes into NumPy arrays instantly
    A_band = np.frombuffer(a_bytes, dtype=np.float64).reshape((r_end - r_start, n))
    BT_band = np.frombuffer(bt_bytes, dtype=np.float64).reshape((c_end - c_start, n))

    t0 = time.time()
    # BT_band.T reconstructs the original vertical column structure
    c_tile = np.dot(A_band, BT_band.T)
    compute_time = time.time() - t0

    t0 = time.time()
    # Push the generated matrix slice straight to the Datastore
    upload_data(upload_url, c_tile.tobytes())
    io_time += time.time() - t0

    print(
        json.dumps(
            {
                "tile_row": tr,
                "tile_col": tc,
                "file": config["upload_file"],
                "rows": c_tile.shape[0],
                "cols": c_tile.shape[1],
                "io_time": io_time,
                "compute_time": compute_time,
            }
        )
    )

if __name__ == "__main__":
    main()
