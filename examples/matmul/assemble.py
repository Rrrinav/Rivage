#!/usr/bin/env python3
import sys
import os
import json
import urllib.request
import time
import numpy as np

WORKER_DATA_DIR = "./rivage_worker_data"

def download_file(url, local_path):
    if os.path.exists(local_path):
        return

    temp_path = local_path + ".download"
    
    if os.path.exists(temp_path):
        while not os.path.exists(local_path):
            time.sleep(0.5)
        return

    try:
        with open(temp_path, 'w') as f:
            f.write("")
        urllib.request.urlretrieve(url, temp_path)
        os.replace(temp_path, local_path)
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise e

def upload_file(url, local_path):
    with open(local_path, 'rb') as f:
        req = urllib.request.Request(url, data=f, method='PUT')
        urllib.request.urlopen(req)

def main():
    os.makedirs(WORKER_DATA_DIR, exist_ok=True)
    config = json.load(sys.stdin)
    row_idx = config["row"]
    tiles = sorted(config["tiles"], key=lambda t: t["tile_col"])

    t0 = time.time()
    loaded_tiles = []
    for t in tiles:
        lp = os.path.join(WORKER_DATA_DIR, f"tile_{t['tile_row']}_{t['tile_col']}.bin")
        download_file(t["url"], lp)
        loaded_tiles.append(np.memmap(lp, dtype=np.float64,
                            mode='r', shape=(t["rows"], t["cols"])))
    io_time = time.time() - t0

    t0 = time.time()
    row_band = np.hstack(loaded_tiles)
    compute_time = time.time() - t0

    t0 = time.time()
    out_lp = os.path.join(WORKER_DATA_DIR, f"row_{row_idx}.bin")
    out_fp = np.memmap(out_lp, dtype=np.float64,
                       mode='w+', shape=row_band.shape)
    out_fp[:] = row_band[:]
    out_fp.flush()
    upload_file(config["upload_url"], out_lp)
    io_time += (time.time() - t0)

    print(json.dumps({
        "row": row_idx, "url": config["upload_url"],
        "rows": row_band.shape[0], "cols": row_band.shape[1],
        "io_time": io_time, "compute_time": compute_time
    }))

if __name__ == "__main__":
    main()
