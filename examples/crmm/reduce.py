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
    i, j = config["i"], config["j"]
    ts = config["tile_size"]
    tiles = config["tiles"]

    t0 = time.time()
    # Initialize a 2000x2000 matrix of zeros (approx 32MB in RAM)
    C_block = np.zeros((ts, ts), dtype=np.float64)

    io_time = 0.0
    compute_time = 0.0

    for t in tiles:
        k = t["k"]
        lp = os.path.join(WORKER_DATA_DIR, f"p_{i}_{j}_{k}.bin")

        t1 = time.time()
        download_file(t["url"], lp)
        io_time += (time.time() - t1)

        t1 = time.time()
        # Read the partial 32MB matrix into RAM and add it to our sum
        P = np.memmap(lp, dtype=np.float64, mode='r', shape=(ts, ts))
        C_block += P
        compute_time += (time.time() - t1)

    t1 = time.time()
    out_local = os.path.join(WORKER_DATA_DIR, f"c_{i}_{j}.bin")
    out_fp = np.memmap(out_local, dtype=np.float64, mode='w+', shape=(ts, ts))
    out_fp[:] = C_block[:]
    out_fp.flush()
    upload_file(config["upload_url"], out_local)
    io_time += (time.time() - t1)

    print(json.dumps({
        "i": i, "j": j, "url": config["upload_url"],
        "io_time": io_time, "compute_time": compute_time
    }))


if __name__ == "__main__":
    main()
