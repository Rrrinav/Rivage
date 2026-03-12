package dag

import (
	"encoding/json"
	"fmt"
)

// jsonKeyGroupShuffleImpl is the classic MapReduce key-grouping shuffle.
func jsonKeyGroupShuffleImpl(outputs []TaskOutput) (ShuffleResult, error) {
	merged := map[string][]interface{}{}

	for _, out := range outputs {
		var kv struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(out.Data, &kv); err == nil && kv.Key != "" {
			merged[kv.Key] = append(merged[kv.Key], kv.Value)
			continue
		}

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
		result[taskID] = TaskInput{Data: payload}
		i++
	}
	return result, nil
}

// passThroughShuffleImpl collects all upstream outputs into a JSON array
func passThroughShuffleImpl(outputs []TaskOutput) (ShuffleResult, error) {
	combined := combineOutputsJSON(outputs)
	return ShuffleResult{"task-0": TaskInput{Data: combined}}, nil
}

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

func MatMulTileShuffle() ShuffleFunc {
	return func(outputs []TaskOutput) (ShuffleResult, error) {
		result := make(ShuffleResult, len(outputs))
		for i, out := range outputs {
			taskID := fmt.Sprintf("matmul-reduce-%d", i)
			result[taskID] = TaskInput{Data: out.Data}
		}
		return result, nil
	}
}
