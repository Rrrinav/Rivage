#!/usr/bin/env python3
"""
Rivage reduce task: matrix tile assembler.

Receives an array of tile results and assembles the final matrix.

Input (STDIN): JSON array of tile results:
    [{"tile_row": r, "tile_col": c, "result_tile": [[...]]}, ...]

Output (STDOUT): JSON {"matrix": [[...full result matrix...]]}
"""
import json, sys

def main():
    tiles = json.load(sys.stdin)  # list of tile result objects

    if not tiles:
        print(json.dumps({"matrix": []}))
        return

    # Determine total dimensions from tiles
    max_row = max(t["tile_row"] for t in tiles)
    max_col = max(t["tile_col"] for t in tiles)
    tile_h  = len(tiles[0]["result_tile"])
    tile_w  = len(tiles[0]["result_tile"][0])

    total_rows = (max_row + 1) * tile_h
    total_cols = (max_col + 1) * tile_w

    result = [[0.0] * total_cols for _ in range(total_rows)]

    for t in tiles:
        r_off = t["tile_row"] * tile_h
        c_off = t["tile_col"] * tile_w
        for i, row in enumerate(t["result_tile"]):
            for j, val in enumerate(row):
                result[r_off + i][c_off + j] = val

    print(json.dumps({"matrix": result}))

if __name__ == "__main__":
    main()

