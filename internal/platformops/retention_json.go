package platformops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
)

type boundedJSONWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedJSONWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > w.remaining {
		return 0, ErrRateLimited
	}
	written, err := w.writer.Write(value)
	w.remaining -= int64(written)
	return written, err
}

type tenantExportSection struct {
	name  string
	query string
}

func tenantExportSections() []tenantExportSection {
	return []tenantExportSection{
		{name: "workflows", query: `SELECT id,slug,name,description,status,created_by,created_at,updated_at FROM workflows WHERE tenant_id=$1 ORDER BY id`},
		{name: "versions", query: `SELECT id,workflow_id,version,definition,encode(definition_hash,'hex'),published_by,published_at FROM workflow_versions WHERE tenant_id=$1 ORDER BY workflow_id,version`},
		{name: "runs", query: `SELECT id,workflow_id,workflow_version_id,execution_plan_id,status,created_at,updated_at,finished_at FROM runs WHERE tenant_id=$1 ORDER BY created_at,id`},
		{name: "events", query: `SELECT id,run_id,sequence,event_type,payload,created_at FROM run_events WHERE tenant_id=$1 ORDER BY run_id,sequence`},
		{name: "credentials", query: `SELECT id,name,provider,purpose,status,current_version,created_by,created_at,updated_at,revoked_at FROM credential_refs WHERE tenant_id=$1 ORDER BY id`},
		{name: "audit", query: `SELECT id,actor_id,action,resource_type,resource_id,outcome,metadata,created_at FROM audit_records WHERE tenant_id=$1 ORDER BY created_at,id`},
	}
}

func streamTenantExport(ctx context.Context, tx pgx.Tx, writer io.Writer, maxBytes int64, tenantID string, exportedAt time.Time) error {
	bounded := &boundedJSONWriter{writer: writer, remaining: maxBytes}
	header, err := json.Marshal(map[string]any{"schemaVersion": 1, "tenantId": tenantID, "exportedAt": exportedAt})
	if err != nil {
		return fmt.Errorf("encode tenant export header: %w", err)
	}
	header = header[:len(header)-1]
	if _, err = bounded.Write(header); err != nil {
		return err
	}
	for _, section := range tenantExportSections() {
		prefix, _ := json.Marshal(section.name)
		if _, err = bounded.Write(append(append([]byte{','}, prefix...), ':', '[')); err != nil {
			return err
		}
		if err = streamExportRows(ctx, tx, bounded, section.query, tenantID); err != nil {
			return err
		}
		if _, err = bounded.Write([]byte{']'}); err != nil {
			return err
		}
	}
	_, err = bounded.Write([]byte{'}'})
	return err
}

func streamExportRows(ctx context.Context, tx pgx.Tx, writer io.Writer, query, tenantID string) error {
	rows, err := tx.Query(ctx, query, tenantID)
	if err != nil {
		return fmt.Errorf("query tenant export: %w", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	first := true
	for rows.Next() {
		values, valuesErr := rows.Values()
		if valuesErr != nil {
			return fmt.Errorf("read tenant export row: %w", valuesErr)
		}
		record := make(map[string]any, len(values))
		for index, value := range values {
			record[fields[index].Name] = value
		}
		encoded, encodeErr := json.Marshal(record)
		if encodeErr != nil {
			return fmt.Errorf("encode tenant export row: %w", encodeErr)
		}
		encoded, encodeErr = redactJSON(encoded)
		if encodeErr != nil {
			return fmt.Errorf("redact tenant export row: %w", encodeErr)
		}
		if !first {
			if _, encodeErr = writer.Write([]byte{','}); encodeErr != nil {
				return encodeErr
			}
		}
		if _, encodeErr = writer.Write(encoded); encodeErr != nil {
			return encodeErr
		}
		first = false
	}
	if err = rows.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("iterate tenant export: %w", err)
	}
	return err
}
