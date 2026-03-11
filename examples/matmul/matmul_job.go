package matmul

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"time"

	"rivage/pkg/coordinator"
	"rivage/pkg/dag"
)

const dataStoreURL = "http://localhost:8081/data"

// MatrixJob runs a distributed matrix multiplication C = A x B.
func MatrixJob(ctx context.Context, coord *coordinator.Coordinator, A, B [][]float64, tileSize int) (string, error) {
	n := len(A)
	if n == 0 {
		return "", fmt.Errorf("empty matrix")
	}

	log.Printf("[matmul] Uploading dense binary matrices to Data Store...")
	if err := uploadMatrix(A, dataStoreURL+"/A.bin"); err != nil {
		return "", fmt.Errorf("failed to upload A: %w", err)
	}
	if err := uploadMatrix(B, dataStoreURL+"/B.bin"); err != nil {
		return "", fmt.Errorf("failed to upload B: %w", err)
	}

	pipeline, err := dag.New("matmul").
		Stage("tile_multiply",
			dag.ScriptExecutor("python3", "examples/matmul/tile_multiply.py"),
		).
		Stage("assemble_row",
			dag.ScriptExecutor("python3", "examples/matmul/assemble.py"),
			dag.WithShuffle(rowBandShuffle),
		).
		Build()
	if err != nil {
		return "", err
	}

	chunks := makeTileMetadata(dataStoreURL+"/A.bin", dataStoreURL+"/B.bin", n, tileSize)
	jobID := fmt.Sprintf("matmul-%d", time.Now().UnixMilli())
	outputs, err := coord.RunJobRaw(ctx, jobID, pipeline, chunks)
	if err != nil {
		return "", err
	}

	return aggregateFinalMetadata(outputs, n)
}

func rowBandShuffle(outputs []dag.TaskOutput) (dag.ShuffleResult, error) {
	rowGroups := make(map[int][]json.RawMessage)
	var totalIO, totalCompute float64

	for _, out := range outputs {
		var meta struct {
			TileRow     int     `json:"tile_row"`
			IOTime      float64 `json:"io_time"`
			ComputeTime float64 `json:"compute_time"`
		}
		if err := json.Unmarshal(out.Data, &meta); err != nil {
			return nil, err
		}
		totalIO += meta.IOTime
		totalCompute += meta.ComputeTime
		rowGroups[meta.TileRow] = append(rowGroups[meta.TileRow], out.Data)
	}

	log.Printf("[matmul] MAP Phase -> Aggregate I/O: %.2fs | Compute: %.2fs", totalIO, totalCompute)

	res := make(dag.ShuffleResult)
	for row, tiles := range rowGroups {
		payload, _ := json.Marshal(map[string]interface{}{
			"row":        row,
			"tiles":      tiles,
			"upload_url": fmt.Sprintf("%s/row_%d.bin", dataStoreURL, row),
		})
		res[fmt.Sprintf("assemble-row-%d", row)] = payload
	}
	return res, nil
}

func aggregateFinalMetadata(outputs []dag.TaskOutput, n int) (string, error) {
	type band struct {
		Row         int     `json:"row"`
		Rows        int     `json:"rows"`
		Cols        int     `json:"cols"`
		URL         string  `json:"url"`
		IOTime      float64 `json:"io_time"`
		ComputeTime float64 `json:"compute_time"`
	}
	var bands []band
	var totalIO, totalCompute float64

	for _, out := range outputs {
		var b band
		if err := json.Unmarshal(out.Data, &b); err != nil {
			return "", err
		}
		bands = append(bands, b)
		totalIO += b.IOTime
		totalCompute += b.ComputeTime
	}
	
	log.Printf("[matmul] REDUCE Phase -> Aggregate I/O: %.2fs | Compute: %.2fs", totalIO, totalCompute)
	sort.Slice(bands, func(i, j int) bool { return bands[i].Row < bands[j].Row })

	summary, _ := json.MarshalIndent(map[string]interface{}{
		"status": "success",
		"dimensions": fmt.Sprintf("%dx%d", n, n),
		"aggregate_io_sec": totalIO,
		"aggregate_compute_sec": totalCompute,
		"final_output_urls": bands,
	}, "", "  ")
	return string(summary), nil
}

func makeTileMetadata(aUrl, bUrl string, n, tileSize int) [][]byte {
	var chunks [][]byte
	for tr := 0; tr*tileSize < n; tr++ {
		for tc := 0; tc*tileSize < n; tc++ {
			rs, re := tr*tileSize, min((tr+1)*tileSize, n)
			cs, ce := tc*tileSize, min((tc+1)*tileSize, n)
			payload, _ := json.Marshal(map[string]interface{}{
				"a_url": aUrl, "b_url": bUrl, "total_n": n,
				"tile_row": tr, "tile_col": tc,
				"r_start": rs, "r_end": re, "c_start": cs, "c_end": ce,
				"upload_url": fmt.Sprintf("%s/tile_%d_%d.bin", dataStoreURL, tr, tc),
			})
			chunks = append(chunks, payload)
		}
	}
	return chunks
}

func uploadMatrix(m [][]float64, url string) error {
	buf := new(bytes.Buffer)
	for _, row := range m {
		for _, val := range row {
			binary.Write(buf, binary.LittleEndian, val)
		}
	}
	req, _ := http.NewRequest(http.MethodPut, url, buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	resp.Body.Close()
	return nil
}

func RandomMatrix(n int) [][]float64 {
	m := make([][]float64, n)
	for i := range m {
		m[i] = make([]float64, n)
		for j := range m[i] { m[i][j] = rand.Float64()*2 - 1 }
	}
	return m
}

func min(a, b int) int { if a < b { return a }; return b }
