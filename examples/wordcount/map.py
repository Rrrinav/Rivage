#!/usr/bin/env python3
"""
Rivage map task: word count.

Input  (STDIN): JSON {"chunk": ["line1", "line2", ...]}
Output (STDOUT): JSON {"word1": count1, "word2": count2, ...}
"""
import json, sys, re

def main():
    data = json.load(sys.stdin)
    counts = {}
    for line in data["chunk"]:
        for word in re.findall(r"[a-z]+", line.lower()):
            counts[word] = counts.get(word, 0) + 1
    print(json.dumps(counts))

if __name__ == "__main__":
    main()

