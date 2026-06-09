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

type PipelineStats struct {
	MapIO         float64 `json:"map_io_sec"`
	MapCompute    float64 `json:"map_compute_sec"`
	ReduceIO      float64 `json:"reduce_io_sec"`
	ReduceCompute float64 `json:"reduce_compute_sec"`
}

func transposeMatrix(m [][]float64) [][]float64 {
	n := len(m)
	out := make([][]float64, n)
	for i := range out {
		out[i] = make([]float64, n)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			out[i][j] = m[j][i]
		}
	}
	return out
}

func MatrixJob(ctx context.Context, coord *coordinator.Coordinator, A, B [][]float64, tileSize int, dataStoreURL string, jobID string, resume bool) (string, error) {
	n := len(A)
	if n == 0 {
		return "", fmt.Errorf("empty matrix")
	}

	log.Printf("[matmul] Transposing matrix B to enable contiguous HTTP Range requests...")
	tTranspose := time.Now()
	bTransposed := transposeMatrix(B)
	log.Printf("[matmul] Transpose completed in %v", time.Since(tTranspose))

	log.Printf("[matmul] Uploading dense binary matrices to Data Store at %s...", dataStoreURL)
	
	tUploadA := time.Now()
	if err := uploadMatrix(A, dataStoreURL+"/A.bin"); err != nil {
		return "", fmt.Errorf("failed to upload A: %w", err)
	}
	log.Printf("[matmul] Uploaded A.bin successfully in %v", time.Since(tUploadA))

	tUploadB := time.Now()
	if err := uploadMatrix(bTransposed, dataStoreURL+"/B_T.bin"); err != nil {
		return "", fmt.Errorf("failed to upload B_T: %w", err)
	}
	log.Printf("[matmul] Uploaded B_T.bin successfully in %v", time.Since(tUploadB))

	stats := &PipelineStats{}

	pipeline, err := dag.New("matmul").
		Stage("tile_multiply",
			dag.ScriptExecutor("python3", "examples/matmul/tile_multiply.py"),
		).
		Stage("assemble_row",
			dag.ScriptExecutor("python3", "examples/matmul/assemble.py"),
			dag.WithShuffle(func(out []dag.TaskOutput) (dag.ShuffleResult, error) {
				return rowBandShuffle(out, stats)
			}),
		).
		Build()
	if err != nil {
		return "", err
	}

	chunks := makeTileMetadata("A.bin", "B_T.bin", n, tileSize)

	if jobID == "" {
		jobID = fmt.Sprintf("matmul-%d", time.Now().UnixMilli())
	}

	outputs, err := coord.RunJobRaw(ctx, jobID, pipeline, chunks, resume)
	if err != nil {
		return "", err
	}

	res, err := aggregateFinalMetadata(outputs, n, stats)
	if err == nil {
		coord.RegisterJobResult(jobID, res)
	}
	return res, err
}

func rowBandShuffle(outputs []dag.TaskOutput, stats *PipelineStats) (dag.ShuffleResult, error) {
	rowGroups := make(map[int][]json.RawMessage)

	for _, out := range outputs {
		var meta struct {
			TileRow     int     `json:"tile_row"`
			IOTime      float64 `json:"io_time"`
			ComputeTime float64 `json:"compute_time"`
		}
		if err := json.Unmarshal(out.Data, &meta); err != nil {
			return nil, err
		}
		stats.MapIO += meta.IOTime
		stats.MapCompute += meta.ComputeTime
		rowGroups[meta.TileRow] = append(rowGroups[meta.TileRow], out.Data)
	}

	res := make(dag.ShuffleResult)
	for row, tiles := range rowGroups {
		payload, _ := json.Marshal(map[string]interface{}{
			"row":         row,
			"tiles":       tiles,
			"upload_file": fmt.Sprintf("row_%d.bin", row),
		})

		res[fmt.Sprintf("assemble-row-%d", row)] = dag.TaskInput{Data: payload}
	}
	return res, nil
}

func aggregateFinalMetadata(outputs []dag.TaskOutput, n int, stats *PipelineStats) (string, error) {
	type band struct {
		Row         int     `json:"row"`
		Rows        int     `json:"rows"`
		Cols        int     `json:"cols"`
		File        string  `json:"file"`
		IOTime      float64 `json:"io_time"`
		ComputeTime float64 `json:"compute_time"`
	}
	var bands []band

	for _, out := range outputs {
		var b band
		if err := json.Unmarshal(out.Data, &b); err != nil {
			return "", err
		}
		bands = append(bands, b)
		stats.ReduceIO += b.IOTime
		stats.ReduceCompute += b.ComputeTime
	}

	sort.Slice(bands, func(i, j int) bool { return bands[i].Row < bands[j].Row })

	summary, _ := json.MarshalIndent(map[string]interface{}{
		"status":                      "success",
		"dimensions":                  fmt.Sprintf("%dx%d", n, n),
		"pipeline_stats":              stats,
		"total_aggregate_compute_sec": stats.MapCompute + stats.ReduceCompute,
		"total_aggregate_io_sec":      stats.MapIO + stats.ReduceIO,
		"final_output_urls":           bands,
	}, "", "  ")
	return string(summary), nil
}

func makeTileMetadata(aFile, btFile string, n, tileSize int) []dag.TaskInput {
	var chunks []dag.TaskInput
	for tr := 0; tr*tileSize < n; tr++ {
		for tc := 0; tc*tileSize < n; tc++ {
			rs, re := tr*tileSize, min((tr+1)*tileSize, n)
			cs, ce := tc*tileSize, min((tc+1)*tileSize, n)
			payload, _ := json.Marshal(map[string]interface{}{
				"a_file":      aFile,
				"b_t_file":    btFile,
				"total_n":     n,
				"tile_row":    tr,
				"tile_col":    tc,
				"r_start":     rs,
				"r_end":       re,
				"c_start":     cs,
				"c_end":       ce,
				"upload_file": fmt.Sprintf("tile_%d_%d.bin", tr, tc),
			})

			chunks = append(chunks, dag.TaskInput{
				Data:         payload,
				AffinityKeys: nil, 
			})
		}
	}
	return chunks
}

func uploadMatrix(m [][]float64, url string) error {
	log.Printf("[matmul] -> Starting buffer generation for %s", url)
	tBuf := time.Now()
	
	buf := new(bytes.Buffer)
	
	n := len(m)
	if n > 0 {
		buf.Grow(n * len(m[0]) * 8)
	}

	for _, row := range m {
		for _, val := range row {
			binary.Write(buf, binary.LittleEndian, val)
		}
	}
	log.Printf("[matmul] -> Buffer generation complete in %v (Size: %d bytes)", time.Since(tBuf), buf.Len())

	log.Printf("[matmul] -> Starting HTTP PUT to %s", url)
	tHttp := time.Now()
	req, _ := http.NewRequest(http.MethodPut, url, buf)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	
	log.Printf("[matmul] -> HTTP PUT complete in %v (Status: %s)", time.Since(tHttp), resp.Status)
	return nil
}

func RandomMatrix(n int) [][]float64 {
	m := make([][]float64, n)
	for i := range m {
		m[i] = make([]float64, n)
		for j := range m[i] {
			m[i][j] = rand.Float64()*2 - 1
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
