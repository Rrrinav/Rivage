// matmul_job.go demonstrates how to use Rivage for distributed matrix
// multiplication — faster than single-node because each tile is computed in
// parallel across workers.
package matmul

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
)

// MatrixJob runs a distributed matrix multiplication C = A x B.
// Both matrices must be square with dimension n.
// tileSize controls how large each sub-problem is (typically 64-256).
func MatrixJob(ctx context.Context, coord *coordinator.Coordinator, A, B [][]float64, tileSize int) ([][]float64, error) {
	n := len(A)
	if n == 0 {
		return nil, fmt.Errorf("empty matrix")
	}

	// ── Pipeline definition ──────────────────────────────────────────────────
	//
	// Stage 1 (map):    tile_multiply — each task computes one output tile
	// Stage 2 (reduce): assemble    — one task assembles the final matrix
	//
	pipeline, err := dag.New("matmul").
		Stage("tile_multiply",
			dag.ScriptExecutor("python3", "examples/matmul/tile_multiply.py"),
			dag.WithParallelism(0), // unlimited — one goroutine per tile
		).
		Stage("assemble",
			dag.ScriptExecutor("python3", "examples/matmul/assemble.py"),
			dag.WithShuffle(dag.PassThroughShuffle()), // all tiles -> one assembler
			dag.WithParallelism(1),
		).
		Build()
	if err != nil {
		return nil, err
	}

	// ── Create input chunks: one chunk per output tile ───────────────────────
	chunks := makeTileChunks(A, B, n, tileSize)
	log.Printf("[matmul] %dx%d matrix, tile_size=%d -> %d tiles", n, n, tileSize, len(chunks))

	jobID := fmt.Sprintf("matmul-%d", time.Now().UnixMilli())
	result, err := coord.RunJob(ctx, jobID, pipeline, chunks)
	if err != nil {
		return nil, err
	}

	// ── Parse assembled result ───────────────────────────────────────────────
	var resp struct {
		Matrix [][]float64 `json:"matrix"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing result: %w (raw: %s)", err, string(result))
	}
	return resp.Matrix, nil
}

// makeTileChunks partitions A and B into tiles and creates one input chunk
// per output tile position (tile_row, tile_col).
func makeTileChunks(A, B [][]float64, n, tileSize int) [][]byte {
	var chunks [][]byte
	for tileRow := 0; tileRow*tileSize < n; tileRow++ {
		for tileCol := 0; tileCol*tileSize < n; tileCol++ {
			rStart := tileRow * tileSize
			rEnd := min(rStart+tileSize, n)

			cStart := tileCol * tileSize
			cEnd := min(cStart+tileSize, n)

			// A tile: rows [rStart:rEnd], all K columns
			aTile := make([][]float64, rEnd-rStart)
			for i := rStart; i < rEnd; i++ {
				aTile[i-rStart] = A[i]
			}

			// B tile: all K rows, cols [cStart:cEnd]
			bTile := make([][]float64, n)
			for k := 0; k < n; k++ {
				row := make([]float64, cEnd-cStart)
				for j := cStart; j < cEnd; j++ {
					row[j-cStart] = B[k][j]
				}
				bTile[k] = row
			}

			payload, _ := json.Marshal(map[string]interface{}{
				"a_tile":   aTile,
				"b_tile":   bTile,
				"tile_row": tileRow,
				"tile_col": tileCol,
			})
			chunks = append(chunks, payload)
		}
	}
	return chunks
}

// RandomMatrix generates an nxn matrix of random float64 values.
func RandomMatrix(n int) [][]float64 {
	m := make([][]float64, n)
	for i := range m {
		m[i] = make([]float64, n)
		for j := range m[i] {
			m[i][j] = rand.Float64()*2 - 1 // [-1, 1)
		}
	}
	return m
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
