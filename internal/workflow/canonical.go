package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

func canonicalizeJSON(input json.RawMessage) (json.RawMessage, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("empty JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func canonicalizePlanJSONValues(semantics *planSemantics) error {
	for nodeIndex := range semantics.Nodes {
		config, err := canonicalizeJSON(semantics.Nodes[nodeIndex].Config)
		if err != nil {
			return fmt.Errorf("node %q config: %w", semantics.Nodes[nodeIndex].ID, err)
		}
		semantics.Nodes[nodeIndex].Config = config
		for name, binding := range semantics.Nodes[nodeIndex].Inputs {
			if len(binding.Literal) == 0 {
				continue
			}
			literal, err := canonicalizeJSON(binding.Literal)
			if err != nil {
				return fmt.Errorf("node %q input %q literal: %w", semantics.Nodes[nodeIndex].ID, name, err)
			}
			binding.Literal = literal
			semantics.Nodes[nodeIndex].Inputs[name] = binding
		}
	}
	return nil
}

func decodeStrict(input json.RawMessage, target any) error {
	if len(input) == 0 {
		return fmt.Errorf("config is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func cloneRaw(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	return append(json.RawMessage(nil), value...)
}
