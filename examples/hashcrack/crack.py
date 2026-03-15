#!/usr/bin/env python3
import sys
import json
import hashlib
import itertools
import time


def brute_force(target_hash, charset, prefix, max_len):
    # Calculate how many characters we need to guess
    rem_len = max_len - len(prefix)
    if rem_len < 0:
        return None
    elif rem_len == 0:
        if hashlib.md5(prefix.encode('utf-8')).hexdigest() == target_hash:
            return prefix
        return None

    # Generate all possible suffixes and test them
    for suffix_tuple in itertools.product(charset, repeat=rem_len):
        guess = prefix + "".join(suffix_tuple)
        if hashlib.md5(guess.encode('utf-8')).hexdigest() == target_hash:
            return guess
    return None


def main():
    # 1. Read the tiny JSON input
    config = json.load(sys.stdin)
    target = config["target_hash"]
    charset = config["charset"]
    prefix = config["prefix"]
    max_len = config["max_length"]

    # 2. Maximize CPU usage
    t0 = time.process_time()
    result = brute_force(target, charset, prefix, max_len)
    compute_time = time.process_time() - t0

    # 3. Return the tiny JSON result
    if result:
        print(json.dumps({
            "found": True,
            "password": result,
            "compute_time_sec": compute_time,
            "prefix": prefix
        }))
    else:
        print(json.dumps({
            "found": False,
            "password": "",
            "compute_time_sec": compute_time,
            "prefix": prefix
        }))


if __name__ == "__main__":
    main()
