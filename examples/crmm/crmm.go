package crmm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
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

func CRMMJob(ctx context.Context, coord *coordinator.Coordinator, A, B [][]float64, tileSize int, dataStoreURL string) (string, error) {
	n := len(A)
	if n == 0 {
		return "", fmt.Errorf("empty matrix")
	}

	log.Printf("[crmm] Uploading dense binary matrices to Data Store at %s...", dataStoreURL)
	if err := uploadMatrix(A, dataStoreURL+"/A.bin"); err != nil {
		return "", err
	}
	if err := uploadMatrix(B, dataStoreURL+"/B.bin"); err != nil {
		return "", err
	}

	stats := &PipelineStats{}

	pipeline, err := dag.New("crmm").
		Stage("multiply_3d",
			dag.ScriptExecutor("python3", "examples/crmm/crmm_multiply.py"),
		).
		Stage("sum_reduce",
			dag.ScriptExecutor("python3", "examples/crmm/crmm_reduce.py"),
			dag.WithShuffle(makeCrmmShuffle(stats, tileSize, dataStoreURL)),
		).
		Build()
	if err != nil {
		return "", err
	}

	chunks := makeCRMMMetadata(dataStoreURL+"/A.bin", dataStoreURL+"/B.bin", n, tileSize, dataStoreURL)
	jobID := fmt.Sprintf("crmm-%d", time.Now().UnixMilli())

	outputs, err := coord.RunJobRaw(ctx, jobID, pipeline, chunks)
	if err != nil {
		return "", err
	}

	return aggregateFinalMetadata(outputs, n, stats)
}

func makeCrmmShuffle(stats *PipelineStats, tileSize int, dataStoreURL string) dag.ShuffleFunc {
	return func(outputs []dag.TaskOutput) (dag.ShuffleResult, error) {
		groups := make(map[string][]json.RawMessage)

		for _, out := range outputs {
			var meta struct {
				I           int     `json:"i"`
				J           int     `json:"j"`
				K           int     `json:"k"`
				IOTime      float64 `json:"io_time"`
				ComputeTime float64 `json:"compute_time"`
			}
			json.Unmarshal(out.Data, &meta)
			stats.MapIO += meta.IOTime
			stats.MapCompute += meta.ComputeTime

			key := fmt.Sprintf("%d_%d", meta.I, meta.J)
			groups[key] = append(groups[key], out.Data)
		}

		res := make(dag.ShuffleResult)
		for key, tiles := range groups {
			var i, j int
			fmt.Sscanf(key, "%d_%d", &i, &j)

			payload, _ := json.Marshal(map[string]interface{}{
				"i": i, "j": j, "tile_size": tileSize,
				"tiles":      tiles,
				"upload_url": fmt.Sprintf("%s/c_%d_%d.bin", dataStoreURL, i, j),
			})
			res[fmt.Sprintf("reduce-%s", key)] = dag.TaskInput{Data: payload}
		}
		return res, nil
	}
}

func aggregateFinalMetadata(outputs []dag.TaskOutput, n int, stats *PipelineStats) (string, error) {
	var finalTiles []map[string]interface{}

	for _, out := range outputs {
		var meta map[string]interface{}
		json.Unmarshal(out.Data, &meta)

		stats.ReduceIO += meta["io_time"].(float64)
		stats.ReduceCompute += meta["compute_time"].(float64)
		finalTiles = append(finalTiles, meta)
	}

	summary, _ := json.MarshalIndent(map[string]interface{}{
		"status":                      "success_crmm",
		"dimensions":                  fmt.Sprintf("%dx%d", n, n),
		"pipeline_stats":              stats,
		"total_aggregate_compute_sec": stats.MapCompute + stats.ReduceCompute,
		"total_aggregate_io_sec":      stats.MapIO + stats.ReduceIO,
		"final_output_urls":           finalTiles,
	}, "", "  ")
	return string(summary), nil
}

func makeCRMMMetadata(aUrl, bUrl string, n, tileSize int, dataStoreURL string) []dag.TaskInput {
	var chunks []dag.TaskInput
	blocks := n / tileSize

	for i := 0; i < blocks; i++ {
		for j := 0; j < blocks; j++ {
			for k := 0; k < blocks; k++ {
				payload, _ := json.Marshal(map[string]interface{}{
					"a_url": aUrl, "b_url": bUrl, "total_n": n, "tile_size": tileSize,
					"i": i, "j": j, "k": k,
					"upload_url": fmt.Sprintf("%s/p_%d_%d_%d.bin", dataStoreURL, i, j, k),
				})

				chunks = append(chunks, dag.TaskInput{
					Data:         payload,
					AffinityKeys: nil,
				})
			}
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
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
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
