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
    i, j, k = config["i"], config["j"], config["k"]
    n, ts = config["total_n"], config["tile_size"]

    a_local = os.path.join(WORKER_DATA_DIR, "A.bin")
    b_local = os.path.join(WORKER_DATA_DIR, "B.bin")
    out_local = os.path.join(WORKER_DATA_DIR, f"p_{i}_{j}_{k}.bin")

    t0 = time.time()
    download_file(config["a_url"], a_local)
    download_file(config["b_url"], b_local)
    io_time = time.time() - t0

    A = np.memmap(a_local, dtype=np.float64, mode='r', shape=(n, n))
    B = np.memmap(b_local, dtype=np.float64, mode='r', shape=(n, n))

    # CRMM 3D Blocking: We extract the exact sub-block for (i, k) and (k, j)
    A_block = A[i*ts: (i+1)*ts, k*ts: (k+1)*ts]
    B_block = B[k*ts: (k+1)*ts, j*ts: (j+1)*ts]

    t0 = time.time()
    P_ijk = np.dot(A_block, B_block)
    compute_time = time.time() - t0

    t0 = time.time()
    out_fp = np.memmap(out_local, dtype=np.float64,
                       mode='w+', shape=P_ijk.shape)
    out_fp[:] = P_ijk[:]
    out_fp.flush()
    upload_file(config["upload_url"], out_local)
    io_time += (time.time() - t0)

    print(json.dumps({
        "i": i, "j": j, "k": k,
        "url": config["upload_url"],
        "io_time": io_time, "compute_time": compute_time
    }))


if __name__ == "__main__":
    main()
