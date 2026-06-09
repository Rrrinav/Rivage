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

def download_to_ram(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=60) as response:
        return response.read()

def upload_data(url, data_bytes):
    req = urllib.request.Request(url, data=data_bytes, method="PUT")
    urllib.request.urlopen(req, timeout=60)

def main():
    config = json.load(sys.stdin)
    row_idx = config["row"]
    tiles = sorted(config["tiles"], key=lambda t: t["tile_col"])

    upload_url = f"{DATASTORE_URL}/{config['upload_file']}"

    t0 = time.time()
    loaded_tiles = []
    
    # Download tiles directly into memory
    for t in tiles:
        tile_url = f"{DATASTORE_URL}/{t['file']}"
        raw_bytes = download_to_ram(tile_url)
        arr = np.frombuffer(raw_bytes, dtype=np.float64).reshape((t["rows"], t["cols"]))
        loaded_tiles.append(arr)
        
    io_time = time.time() - t0

    t0 = time.time()
    row_band = np.hstack(loaded_tiles)
    compute_time = time.time() - t0

    t0 = time.time()
    # Stream the final assembled row directly to the Datastore
    upload_data(upload_url, row_band.tobytes())
    io_time += time.time() - t0

    print(
        json.dumps(
            {
                "row": row_idx,
                "file": config["upload_file"],
                "rows": row_band.shape[0],
                "cols": row_band.shape[1],
                "io_time": io_time,
                "compute_time": compute_time,
            }
        )
    )

if __name__ == "__main__":
    main()
