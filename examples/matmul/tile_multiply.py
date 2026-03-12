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
    
    # Simple check: If another task is already downloading it, just wait!
    if os.path.exists(temp_path):
        while not os.path.exists(local_path):
            time.sleep(0.5)
        return

    # Otherwise, claim the download by creating the .download file
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
    tr, tc, n = config["tile_row"], config["tile_col"], config["total_n"]

    a_local = os.path.join(WORKER_DATA_DIR, "A.bin")
    b_local = os.path.join(WORKER_DATA_DIR, "B.bin")
    out_local = os.path.join(WORKER_DATA_DIR, f"out_{tr}_{tc}.bin")

    t0 = time.time()
    download_file(config["a_url"], a_local)
    download_file(config["b_url"], b_local)
    io_time = time.time() - t0

    A = np.memmap(a_local, dtype=np.float64, mode='r', shape=(n, n))
    B = np.memmap(b_local, dtype=np.float64, mode='r', shape=(n, n))

    t0 = time.time()
    c_tile = np.dot(A[config["r_start"]:config["r_end"], :],
                    B[:, config["c_start"]:config["c_end"]])
    compute_time = time.time() - t0

    t0 = time.time()
    out_fp = np.memmap(out_local, dtype=np.float64,
                       mode='w+', shape=c_tile.shape)
    out_fp[:] = c_tile[:]
    out_fp.flush()
    upload_file(config["upload_url"], out_local)
    io_time += (time.time() - t0)

    print(json.dumps({
        "tile_row": tr, "tile_col": tc, "url": config["upload_url"],
        "rows": c_tile.shape[0], "cols": c_tile.shape[1],
        "io_time": io_time, "compute_time": compute_time
    }))

if __name__ == "__main__":
    main()
