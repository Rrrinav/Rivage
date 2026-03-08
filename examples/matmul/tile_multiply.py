#!/usr/bin/env python3
"""
Rivage map task: matrix multiplication tiling.

This script does the actual tile-level dot product computation.

Input (STDIN): JSON {
    "a_tile": [[...], ...],   # submatrix of A (rows_per_tile × K)
    "b_tile": [[...], ...],   # submatrix of B (K × cols_per_tile)
    "tile_row": <int>,         # which output tile row
    "tile_col": <int>          # which output tile col
}
Output (STDOUT): JSON {
    "tile_row": <int>,
    "tile_col": <int>,
    "result_tile": [[...], ...]   # partial result for this tile
}
"""
import json, sys

def matmul(A, B):
    rows_a = len(A)
    cols_a = len(A[0])
    cols_b = len(B[0])
    C = [[0.0] * cols_b for _ in range(rows_a)]
    for i in range(rows_a):
        for k in range(cols_a):
            if A[i][k] == 0:
                continue
            for j in range(cols_b):
                C[i][j] += A[i][k] * B[k][j]
    return C

def main():
    data = json.load(sys.stdin)
    a_tile  = data["a_tile"]
    b_tile  = data["b_tile"]
    tile_row = data["tile_row"]
    tile_col = data["tile_col"]

    result_tile = matmul(a_tile, b_tile)

    print(json.dumps({
        "tile_row":    tile_row,
        "tile_col":    tile_col,
        "result_tile": result_tile,
    }))

if __name__ == "__main__":
    main()

