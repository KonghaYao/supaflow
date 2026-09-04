package load

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"testing"
	"time"
)

var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

type latencyPercentiles struct {
	P50Milliseconds float64 `json:"p50Milliseconds"`
	P95Milliseconds float64 `json:"p95Milliseconds"`
	P99Milliseconds float64 `json:"p99Milliseconds"`
}

type startPressureEvidence struct {
	Requests       int                `json:"requests"`
	Concurrency    int                `json:"concurrency"`
	ElapsedSeconds float64            `json:"elapsedSeconds"`
	Throughput     float64            `json:"throughputRequestsPerSecond"`
	Latency        latencyPercentiles `json:"latency"`
}

type runtimePressureEvidence struct {
	Workers                      int                `json:"workers"`
	Advances                     int64              `json:"advances"`
	AdvanceCallLimit             int64              `json:"advanceCallLimit"`
	AdvanceCalls                 int64              `json:"advanceCalls"`
	EmptyAdvanceCalls            int64              `json:"emptyAdvanceCalls"`
	ElapsedSeconds               float64            `json:"elapsedSeconds"`
	AdvanceThroughput            float64            `json:"advanceThroughputPerSecond"`
	AdvanceLatency               latencyPercentiles `json:"advanceLatency"`
	CommittedEvents              int64              `json:"committedEvents"`
	EventCommitThroughput        float64            `json:"eventCommitThroughputPerSecond"`
	CommittedOutboxMessages      int64              `json:"committedOutboxMessages"`
	OutboxEnqueueThroughput      float64            `json:"outboxEnqueueThroughputPerSecond"`
	QueuedRunsBeforeDrain        int                `json:"queuedRunsBeforeDrain"`
	OldestQueueAgeBeforeDrainMS  float64            `json:"oldestQueueAgeBeforeDrainMilliseconds"`
	MaximumQueueAdmissionDelayMS float64            `json:"maximumQueueAdmissionDelayMilliseconds"`
	LeaseTakeovers               int64              `json:"leaseTakeovers"`
}

type pressureEvidence struct {
	SchemaVersion   int                     `json:"schemaVersion"`
	Profile         string                  `json:"profile"`
	Revision        string                  `json:"revision"`
	GeneratedAt     string                  `json:"generatedAt"`
	GoVersion       string                  `json:"goVersion"`
	OperatingSystem string                  `json:"operatingSystem"`
	Architecture    string                  `json:"architecture"`
	LogicalCPUs     int                     `json:"logicalCpus"`
	PostgreSQL      string                  `json:"postgresqlVersion"`
	PoolMaxConns    int32                   `json:"poolMaxConnections"`
	PoolMinConns    int32                   `json:"poolMinConnections"`
	Start           startPressureEvidence   `json:"start"`
	Runtime         runtimePressureEvidence `json:"runtime"`
}

func durationPercentiles(samples []time.Duration) latencyPercentiles {
	if len(samples) == 0 {
		return latencyPercentiles{}
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left] < ordered[right] })
	value := func(percentile float64) float64 {
		rank := int(math.Ceil(percentile * float64(len(ordered))))
		if rank < 1 {
			rank = 1
		}
		return float64(ordered[rank-1]) / float64(time.Millisecond)
	}
	return latencyPercentiles{
		P50Milliseconds: value(0.50),
		P95Milliseconds: value(0.95),
		P99Milliseconds: value(0.99),
	}
}

func newPressureEvidence(revision string) pressureEvidence {
	return pressureEvidence{
		SchemaVersion:   1,
		Profile:         "postgres-control-plane",
		Revision:        revision,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		GoVersion:       runtime.Version(),
		OperatingSystem: runtime.GOOS,
		Architecture:    runtime.GOARCH,
		LogicalCPUs:     runtime.NumCPU(),
	}
}

func validateEvidenceRevision() error {
	revision := os.Getenv("SUPAFLOW_RELEASE_COMMIT")
	if !commitPattern.MatchString(revision) {
		return errors.New("SUPAFLOW_RELEASE_COMMIT must be the exact lowercase 40-character commit SHA")
	}
	return nil
}

func writePressureEvidence(tb testing.TB, evidence pressureEvidence) string {
	tb.Helper()
	compact, err := json.Marshal(evidence)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Logf("release_evidence_json=%s", compact)
	directory := os.Getenv("SUPAFLOW_RELEASE_EVIDENCE_DIR")
	if directory == "" {
		return ""
	}
	if err = validateEvidenceRevision(); err != nil {
		tb.Fatal(err)
	}
	if evidence.Revision != os.Getenv("SUPAFLOW_RELEASE_COMMIT") {
		tb.Fatal("release evidence revision does not match SUPAFLOW_RELEASE_COMMIT")
	}
	path, err := persistPressureEvidence(directory, evidence)
	if err != nil {
		tb.Fatal(err)
	}
	return path
}

func persistPressureEvidence(directory string, evidence pressureEvidence) (string, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create release evidence directory: %w", err)
	}
	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal release evidence: %w", err)
	}
	path := filepath.Join(directory, evidence.Profile+"-"+evidence.Revision+".json")
	temporary, err := os.CreateTemp(directory, ".pressure-evidence-*")
	if err != nil {
		return "", fmt.Errorf("create release evidence temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err = temporary.Chmod(0o600); err == nil {
		var written int
		written, err = temporary.Write(payload)
		if err == nil && written != len(payload) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = temporary.Sync()
	}
	closeErr := temporary.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Link(temporaryPath, path)
	}
	if err == nil {
		err = os.Remove(temporaryPath)
	}
	if err == nil {
		var directoryHandle *os.File
		directoryHandle, err = os.Open(directory)
		if err == nil {
			err = directoryHandle.Sync()
			closeDirectoryErr := directoryHandle.Close()
			if err == nil {
				err = closeDirectoryErr
			}
		}
	}
	if err != nil {
		return "", fmt.Errorf("publish immutable release evidence: %w", err)
	}
	return path, nil
}
