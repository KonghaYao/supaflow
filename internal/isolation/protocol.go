// Package isolation defines the out-of-process Shell/Python runner protocol.
package isolation

import (
	"encoding/json"
	"time"
)

// Request is one isolated invocation received over stdin.
type Request struct {
	OrganizationID string            `json:"organizationId"`
	RunID          string            `json:"runId"`
	AttemptID      string            `json:"attemptId"`
	FencingToken   uint64            `json:"fencingToken"`
	Language       string            `json:"language"`
	Source         string            `json:"source"`
	Args           []string          `json:"args,omitempty"`
	Environment    map[string]string `json:"environment,omitempty"`
	Network        bool              `json:"network"`
	Input          json.RawMessage   `json:"input"`
	Timeout        time.Duration     `json:"timeout"`
	OutputLimit    int64             `json:"outputLimit"`
	Limits         ResourceLimits    `json:"limits"`
}

// ResourceLimits optionally tightens the runner's finite production defaults.
// Zero values select the defaults; values above them are rejected.
type ResourceLimits struct {
	CPUTime           time.Duration `json:"cpuTime"`
	AddressSpaceBytes uint64        `json:"addressSpaceBytes"`
	Processes         uint64        `json:"processes"`
	OpenFiles         uint64        `json:"openFiles"`
	FileSizeBytes     uint64        `json:"fileSizeBytes"`
	WorkspaceBytes    uint64        `json:"workspaceBytes"`
	WorkspaceInodes   uint64        `json:"workspaceInodes"`
}

// Response is the bounded runner response written to stdout.
type Response struct {
	Output json.RawMessage `json:"output,omitempty"`
	Code   string          `json:"code,omitempty"`
}
