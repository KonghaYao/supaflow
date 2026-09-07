package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pazhou/supaflow/internal/executor"
	"github.com/pazhou/supaflow/internal/platformops"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
)

const (
	defaultManagedOutputLimit = int64(64 << 20)
	maximumManagedOutputLimit = int64(1 << 30)
)

type managedOutputs struct {
	manager managedOutputBackend
	maximum int64
}

type managedOutputBackend interface {
	Store(context.Context, string, string, string, *time.Time, io.Reader) (platformops.ManagedObject, error)
	OpenRunOutput(context.Context, string, string) (io.ReadCloser, platformops.ManagedObject, error)
	Discard(context.Context, string, string) error
}

func configureWorkerOutputs(pool *pgxpool.Pool) (*managedOutputs, error) {
	root := strings.TrimSpace(envValue("SUPAFLOW_OBJECT_STORE_DIR"))
	if root == "" {
		root = "./data/objects"
	}
	maximum := defaultManagedOutputLimit
	if raw := strings.TrimSpace(envValue("SUPAFLOW_OUTPUT_MAX_BYTES")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 || parsed > maximumManagedOutputLimit {
			return nil, errors.New("SUPAFLOW_OUTPUT_MAX_BYTES must be between 1 and 1073741824")
		}
		maximum = parsed
	}
	objects, err := platformops.NewFileObjectStore(root)
	if err != nil {
		return nil, err
	}
	manager, err := platformops.NewOutputManager(pool, objects, maximum)
	if err != nil {
		return nil, err
	}
	return &managedOutputs{manager: manager, maximum: maximum}, nil
}

func (m *managedOutputs) StoreOutput(ctx context.Context, organizationID workflowruntime.OrganizationID, runID, contentType string, value []byte) (executor.ExternalOutput, error) {
	managed, err := m.manager.Store(ctx, string(organizationID), runID, contentType, nil, bytes.NewReader(value))
	if err != nil {
		return executor.ExternalOutput{}, err
	}
	return executor.ExternalOutput{ObjectRef: managed.ID, Size: managed.Size, SHA256: managed.SHA256}, nil
}

func (m *managedOutputs) DiscardOutput(ctx context.Context, organizationID workflowruntime.OrganizationID, objectRef string) error {
	return m.manager.Discard(ctx, string(organizationID), objectRef)
}

// ReadRunOutput resolves an external value only through the organization/run
// relation and checks both projections before exposing bounded JSON bytes.
func (m *managedOutputs) ReadRunOutput(ctx context.Context, organizationID workflowruntime.OrganizationID, runID string, expected workflowruntime.RunOutput) (json.RawMessage, error) {
	if m == nil || m.manager == nil || organizationID == "" || runID == "" || !expected.External || expected.ContentType != "application/json" || expected.Size < 0 || expected.Size > m.maximum {
		return nil, errors.New("invalid external run output request")
	}
	reader, metadata, err := m.manager.OpenRunOutput(ctx, string(organizationID), runID)
	if err != nil {
		return nil, fmt.Errorf("open managed run output: %w", err)
	}
	defer reader.Close()
	if metadata.OrganizationID != string(organizationID) || metadata.RunID != runID || metadata.Kind != "output" || metadata.Status != "active" ||
		metadata.ContentType != expected.ContentType || metadata.Size != expected.Size || metadata.SHA256 != expected.SHA256 {
		return nil, errors.New("managed run output metadata mismatch")
	}
	value, err := io.ReadAll(io.LimitReader(reader, expected.Size+1))
	if err != nil {
		return nil, fmt.Errorf("read managed run output: %w", err)
	}
	if int64(len(value)) != expected.Size || !json.Valid(value) {
		return nil, errors.New("managed run output is not bounded valid JSON")
	}
	return json.RawMessage(value), nil
}

var _ executor.OutputManager = (*managedOutputs)(nil)
