// Command generate-node-contract writes the canonical built-in node registry artifact.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/pazhou/supaflow/internal/workflow"
)

func main() {
	output := flag.String("output", "contracts/schemas/builtin-node-types-v1.json", "generated contract path")
	flag.Parse()
	artifact := struct {
		SchemaVersion string                     `json:"schemaVersion"`
		Nodes         []workflow.NodeTypeVersion `json:"nodes"`
	}{SchemaVersion: "v1", Nodes: workflow.BuiltinRegistry().Entries()}
	encoded, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		fail(err)
	}
	encoded = append(encoded, '\n')
	if err = os.WriteFile(*output, encoded, 0o644); err != nil {
		fail(err)
	}
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
