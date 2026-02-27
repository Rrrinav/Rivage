#!/usr/bin/env python3
"""
Map script for word count.

Input  (STDIN): JSON  { "chunk": ["line1", "line2", ...] }
Output (STDOUT): JSON { "word": count, ... }
"""

import json
import sys


def main():
    data = json.load(sys.stdin)
    counts = {}

    for line in data["chunk"]:
        for word in line.lower().split():
            # Strip basic punctuation
            word = word.strip(".,!?;:\"'")
            if word:
                counts[word] = counts.get(word, 0) + 1

    print(json.dumps(counts))


if __name__ == "__main__":
    main()
