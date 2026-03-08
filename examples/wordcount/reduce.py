#!/usr/bin/env python3
"""
Rivage reduce task: word count.

Input  (STDIN): JSON {"key": "word", "values": [count1, count2, ...]}
Output (STDOUT): JSON {"word": total_count}
"""
import json, sys

def main():
    data = json.load(sys.stdin)
    key = data["key"]
    total = sum(int(v) for v in data["values"])
    print(json.dumps({key: total}))

if __name__ == "__main__":
    main()

