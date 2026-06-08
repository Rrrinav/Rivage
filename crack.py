import hashlib
import itertools
import string
import time

# Target hash for 'zigzag'
target_hash = hashlib.sha256(b"zigzag").hexdigest()
alphabet = string.ascii_lowercase


def crack_sequential():
    start_t = time.time()

    # Generate all 676 two-character prefixes
    prefixes = ["".join(p) for p in itertools.product(alphabet, repeat=2)]

    for prefix in prefixes:
        # Search suffix lengths 1 through 4 (total max length 6)
        for length in range(1, 5):
            for suffix_tuple in itertools.product(alphabet, repeat=length):
                suffix = "".join(suffix_tuple)
                attempt = prefix + suffix

                # Check if hash matches
                if hashlib.sha256(attempt.encode()).hexdigest() == target_hash:
                    end_t = time.time()
                    return attempt, end_t - start_t

    return None, time.time() - start_t


if __name__ == "__main__":
    print("Starting single-process sequential cracking...")
    password, duration = crack_sequential()

    if password:
        print(f"Success. Password: {password}")
    else:
        print("Failed to crack.")

    print(f"Wall-clock time: {duration:.4f} seconds")
