package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// ControlEvaluation is the deterministic result shared by the production
// executor and Studio preview. Snapshot is an opaque loop checkpoint.
type ControlEvaluation struct {
	Output       json.RawMessage
	SelectedPort string
	Snapshot     json.RawMessage
}

// EvaluateControlNode applies a side-effect-free built-in control node.
// attempt starts at one and is only a durable fallback when no loop snapshot
// is available.
func EvaluateControlNode(node PlanNode, input, snapshot json.RawMessage, attempt int) (ControlEvaluation, error) {
	var value any
	if !json.Valid(input) || json.Unmarshal(input, &value) != nil {
		return ControlEvaluation{}, errors.New("input must be valid JSON")
	}
	result := ControlEvaluation{Output: append(json.RawMessage(nil), input...)}
	switch node.Type {
	case NodeTypeCondition:
		var cfg struct {
			Path     string `json:"path"`
			Operator string `json:"operator"`
			Value    any    `json:"value"`
		}
		if json.Unmarshal(node.Config, &cfg) != nil {
			return ControlEvaluation{}, errors.New("condition config is invalid")
		}
		result.SelectedPort = strconv.FormatBool(compareControlJSON(resolveControlPath(value, cfg.Path), cfg.Operator, cfg.Value))
	case NodeTypeSwitch:
		var cfg struct {
			Path  string            `json:"path"`
			Cases map[string]string `json:"cases"`
		}
		if json.Unmarshal(node.Config, &cfg) != nil {
			return ControlEvaluation{}, errors.New("switch config is invalid")
		}
		result.SelectedPort = cfg.Cases[fmt.Sprint(resolveControlPath(value, cfg.Path))]
		if result.SelectedPort == "" {
			result.SelectedPort = "default"
		}
	case NodeTypeTransform:
		var cfg struct {
			Select map[string]string `json:"select"`
		}
		if json.Unmarshal(node.Config, &cfg) != nil || cfg.Select == nil {
			return ControlEvaluation{}, errors.New("transform config is invalid")
		}
		mapped := make(map[string]any, len(cfg.Select))
		for key, path := range cfg.Select {
			mapped[key] = resolveControlPath(value, path)
		}
		result.Output, _ = json.Marshal(mapped)
	case NodeTypeLoop:
		var cfg struct {
			MaxIterations int `json:"maxIterations"`
		}
		var state struct {
			Iteration int `json:"iteration"`
		}
		if json.Unmarshal(node.Config, &cfg) != nil || cfg.MaxIterations < 1 {
			return ControlEvaluation{}, errors.New("loop config is invalid")
		}
		if len(snapshot) > 0 {
			if json.Unmarshal(snapshot, &state) != nil || state.Iteration < 0 {
				return ControlEvaluation{}, errors.New("loop snapshot is invalid")
			}
		} else if attempt > 1 {
			state.Iteration = attempt - 1
		}
		if state.Iteration >= cfg.MaxIterations {
			result.SelectedPort = "done"
		} else {
			state.Iteration++
			result.SelectedPort = "body"
		}
		result.Snapshot, _ = json.Marshal(state)
	default:
		return ControlEvaluation{}, fmt.Errorf("node type %q has no control evaluation", node.Type)
	}
	return result, nil
}

func resolveControlPath(value any, path string) any {
	if path == "" || path == "$" {
		return value
	}
	current := value
	for _, part := range strings.Split(strings.TrimPrefix(path, "$."), ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = object[part]
	}
	return current
}

func compareControlJSON(left any, operator string, right any) bool {
	switch operator {
	case "exists":
		return left != nil
	case "eq":
		return reflect.DeepEqual(left, right)
	case "ne":
		return !reflect.DeepEqual(left, right)
	}
	l, lok := left.(float64)
	r, rok := right.(float64)
	if !lok || !rok {
		return false
	}
	switch operator {
	case "gt":
		return l > r
	case "gte":
		return l >= r
	case "lt":
		return l < r
	case "lte":
		return l <= r
	default:
		return false
	}
}
