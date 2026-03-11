package dag

import (
	"encoding/json"
	"fmt"
)

// jsonKeyGroupShuffleImpl is the classic MapReduce key-grouping shuffle.
// Each upstream task must emit a JSON object: {"key": <string>, "value": <any>}
// OR a JSON object of arbitrary key→value pairs (word-count style).
// Both formats are supported.
func jsonKeyGroupShuffleImpl(outputs []TaskOutput) (ShuffleResult, error) {
	merged := map[string][]interface{}{}

	for _, out := range outputs {
		// Try parsing as {"key": "...", "value": ...}
		var kv struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(out.Data, &kv); err == nil && kv.Key != "" {
			merged[kv.Key] = append(merged[kv.Key], kv.Value)
			continue
		}

		// Fall back to treating it as a flat map[string]any (word-count style)
		var flat map[string]interface{}
		if err := json.Unmarshal(out.Data, &flat); err != nil {
			return nil, fmt.Errorf("task %q output is not valid JSON: %w", out.TaskID, err)
		}
		for k, v := range flat {
			merged[k] = append(merged[k], v)
		}
	}

	result := make(ShuffleResult, len(merged))
	i := 0
	for k, vals := range merged {
		taskID := fmt.Sprintf("reduce-%d", i)
		payload, err := json.Marshal(map[string]interface{}{
			"key":    k,
			"values": vals,
		})
		if err != nil {
			return nil, err
		}
		result[taskID] = payload
		i++
	}
	return result, nil
}

// passThroughShuffleImpl collects all upstream outputs into a JSON array
// and passes the array to a single downstream task.
func passThroughShuffleImpl(outputs []TaskOutput) (ShuffleResult, error) {
	combined := combineOutputsJSON(outputs)
	return ShuffleResult{"task-0": combined}, nil
}

// combineOutputsJSON marshals all task data fields into a JSON array.
func combineOutputsJSON(outputs []TaskOutput) []byte {
	items := make([]json.RawMessage, 0, len(outputs))
	for _, o := range outputs {
		if len(o.Data) > 0 {
			items = append(items, json.RawMessage(o.Data))
		}
	}
	b, _ := json.Marshal(items)
	return b
}

// MatMulShuffle partitions a matrix-multiplication job across workers.
//
// The upstream stage emits one tile per task:
//
//	{"tile_row": <int>, "tile_col": <int>, "a_block": [[...]], "b_block": [[...]]}
//
// The shuffle simply passes each tile directly to one reduce task that does the
// partial dot-product for that tile position and emits {"row": r, "col": c, "value": v}.
// A final "assemble" stage collects all tiles.
func MatMulTileShuffle() ShuffleFunc {
	return func(outputs []TaskOutput) (ShuffleResult, error) {
		result := make(ShuffleResult, len(outputs))
		for i, out := range outputs {
			taskID := fmt.Sprintf("matmul-reduce-%d", i)
			result[taskID] = out.Data
		}
		return result, nil
	}
}
