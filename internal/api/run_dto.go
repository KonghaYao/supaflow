package api

import (
	"bytes"
	"encoding/json"
	"net/url"
	"time"

	"github.com/pazhou/supaflow/internal/runtime"
)

type runResponse struct {
	ID                string                `json:"id"`
	PlanID            string                `json:"planId"`
	PlanHash          string                `json:"planHash"`
	Status            runtime.RunStatus     `json:"status"`
	Priority          int                   `json:"priority"`
	DeadlineAt        *time.Time            `json:"deadlineAt,omitempty"`
	Output            json.RawMessage       `json:"output,omitempty"`
	OutputMetadata    *outputMetadata       `json:"outputMetadata,omitempty"`
	Rerun             *rerunLineageResponse `json:"rerun,omitempty"`
	CancelRequestedAt *time.Time            `json:"cancelRequestedAt,omitempty"`
	CreatedAt         time.Time             `json:"createdAt"`
	UpdatedAt         time.Time             `json:"updatedAt"`
}

type rerunLineageResponse struct {
	SourceRunID string    `json:"sourceRunId"`
	FromNodeID  string    `json:"fromNodeId,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
}

type outputMetadata struct {
	ContentType     string `json:"contentType"`
	SizeBytes       int64  `json:"sizeBytes"`
	SHA256          string `json:"sha256"`
	Sensitive       bool   `json:"sensitive"`
	Storage         string `json:"storage"`
	DownloadURL     string `json:"downloadUrl,omitempty"`
	RetentionStatus string `json:"retentionStatus,omitempty"`
}

func runDTO(run runtime.Run) runResponse {
	response := runResponse{ID: run.ID, PlanID: run.Plan.ID, PlanHash: run.Plan.Hash, Status: run.Status, Priority: run.Priority,
		DeadlineAt: run.DeadlineAt, CancelRequestedAt: run.CancelRequestedAt, CreatedAt: run.CreatedAt, UpdatedAt: run.UpdatedAt}
	if run.Rerun != nil {
		response.Rerun = &rerunLineageResponse{SourceRunID: run.Rerun.SourceRunID, FromNodeID: run.Rerun.FromNodeID, CreatedAt: run.Rerun.CreatedAt}
	}
	if run.Output == nil {
		return response
	}
	storage := "inline"
	if run.Output.External {
		storage = "object"
	} else if !run.Output.Sensitive {
		response.Output = bytes.Clone(run.Output.Inline)
	}
	response.OutputMetadata = &outputMetadata{
		ContentType: run.Output.ContentType, SizeBytes: run.Output.Size,
		SHA256: run.Output.SHA256, Sensitive: run.Output.Sensitive, Storage: storage, RetentionStatus: run.Output.RetentionStatus,
	}
	if run.Output.External && run.Output.RetentionStatus == "active" {
		response.OutputMetadata.DownloadURL = "/v1/runs/" + url.PathEscape(run.ID) + "/output"
	}
	return response
}
