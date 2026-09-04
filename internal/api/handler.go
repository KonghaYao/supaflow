package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/definition"
	"github.com/pazhou/supaflow/internal/platform"
	"github.com/pazhou/supaflow/internal/platformops"
	"github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const maxRequestBody = 1 << 20

// Handler serves the stable Supaflow HTTP contract.
type Handler struct {
	runtime         *runtime.Service
	catalog         platform.PlanCatalog
	auth            *Authenticator
	resumeTokens    *runtime.ResumeTokenIssuer
	readinessProbe  func(context.Context) error
	definitions     *definition.Service
	nodeTypeCatalog platform.NodeTypeCatalog
	studioAssets    fs.FS
	audit           AuditRecorder
	outputs         OutputReader
	eventStreams    *eventStreamLimiter
}

// NewHandler wires authenticated transport adapters to application services.
func NewHandler(runtimeService *runtime.Service, catalog platform.PlanCatalog, auth *Authenticator, resumeTokens *runtime.ResumeTokenIssuer) (*Handler, error) {
	if runtimeService == nil || catalog == nil || auth == nil || resumeTokens == nil {
		return nil, errors.New("runtime service, plan catalog, authenticator, and resume token issuer are required")
	}
	runtimeService.SetResumeTokenIssuer(resumeTokens)
	streams, _ := newEventStreamLimiter(defaultEventStreamConfig())
	return &Handler{runtime: runtimeService, catalog: catalog, auth: auth, resumeTokens: resumeTokens, eventStreams: streams}, nil
}

// SetReadinessProbe configures the dependency check used by /health/ready.
// Configure it before serving requests; a nil probe reports process readiness.
func (h *Handler) SetReadinessProbe(probe func(context.Context) error) {
	h.readinessProbe = probe
}

// SetStudio configures draft, version-history, and editor discovery routes.
// Configure it before Routes; handlers do not mutate this dependency afterward.
func (h *Handler) SetStudio(definitions *definition.Service, nodeTypes platform.NodeTypeCatalog) {
	h.definitions = definitions
	h.nodeTypeCatalog = nodeTypes
}

// SetStudioAssets configures the prebuilt Studio filesystem served at /studio/.
func (h *Handler) SetStudioAssets(assets fs.FS) { h.studioAssets = assets }

// SetAudit enables immutable evidence for authenticated high-risk API mutations.
func (h *Handler) SetAudit(audit AuditRecorder) { h.audit = audit }

// OutputReader opens opaque tenant-scoped output references.
type OutputReader interface {
	OpenRunOutput(context.Context, string, string) (io.ReadCloser, platformops.ManagedObject, error)
}

// SetOutputReader enables authenticated external run-output downloads.
func (h *Handler) SetOutputReader(outputs OutputReader) { h.outputs = outputs }

// Routes returns an authenticated Go 1.22 ServeMux with bounded request bodies.
func (h *Handler) Routes() http.Handler {
	root := http.NewServeMux()
	root.HandleFunc("GET /health/live", h.live)
	root.HandleFunc("GET /health/ready", h.ready)
	root.HandleFunc("POST /hooks/resume/{interruptID}", h.resumeWebhook)
	h.registerStudioAssets(root)
	protected := http.NewServeMux()
	h.registerStudioRoutes(protected)
	protected.Handle("POST /v1/workflows/compile", h.require(RoleEditor, http.HandlerFunc(h.compile)))
	protected.Handle("POST /v1/workflows/{workflowID}/publish", h.require(RolePublisher, http.HandlerFunc(h.publish)))
	protected.Handle("POST /v1/runs", h.require(RoleOperator, http.HandlerFunc(h.startRun)))
	h.registerRunMonitorRoutes(protected)
	protected.Handle("GET /v1/runs/{runID}", h.require(RoleViewer, http.HandlerFunc(h.getRun)))
	protected.Handle("GET /v1/runs/{runID}/output", h.require(RoleViewer, http.HandlerFunc(h.getRunOutput)))
	protected.Handle("POST /v1/runs/{runID}/rerun", h.require(RoleOperator, http.HandlerFunc(h.rerunRun)))
	protected.Handle("POST /v1/runs/{runID}/cancel", h.require(RoleOperator, http.HandlerFunc(h.cancelRun)))
	protected.Handle("GET /v1/runs/{runID}/events", h.require(RoleViewer, http.HandlerFunc(h.events)))
	protected.Handle("GET /v1/runs/{runID}/interrupts", h.require(RoleViewer, http.HandlerFunc(h.interrupts)))
	protected.Handle("POST /v1/interrupts/{interruptID}/resolve", h.require(RoleApprover, http.HandlerFunc(h.resolveInterrupt)))
	root.Handle("/v1/", privateNoStore(h.auth.Middleware(h.auditMutations(protected))))
	return root
}

func privateNoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if h.readinessProbe != nil {
		if err := h.readinessProbe(r.Context()); err != nil {
			writeProblem(w, r, http.StatusServiceUnavailable, "not_ready", "service dependencies are unavailable")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) compile(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	var definition workflow.WorkflowDefinition
	if err := decodeJSON(w, r, &definition); err != nil {
		return
	}
	plan, err := h.catalog.Compile(r.Context(), principal.TenantID, definition)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var definition workflow.WorkflowDefinition
	if err := decodeJSON(w, r, &definition); err != nil {
		return
	}
	var published platform.PublishedPlan
	var created bool
	var err error
	if h.definitions != nil {
		published, created, err = h.definitions.Publish(r.Context(), principal.TenantID, r.PathValue("workflowID"), definition, idempotencyKey)
	} else {
		published, created, err = h.catalog.Publish(r.Context(), principal.TenantID, r.PathValue("workflowID"), definition, idempotencyKey)
	}
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, published)
}

func (h *Handler) startRun(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		PlanID    string          `json:"planId"`
		PlanHash  string          `json:"planHash"`
		Input     json.RawMessage `json:"input"`
		Priority  int             `json:"priority"`
		TimeoutMS int64           `json:"timeoutMs"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	published, err := h.catalog.Resolve(r.Context(), principal.TenantID, body.PlanID, body.PlanHash)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	order := published.Plan.TopologicalOrder()
	if len(order) == 0 {
		writeProblem(w, r, http.StatusConflict, "invalid_plan", "published plan has no entry node")
		return
	}
	entry, found := published.Plan.Node(order[0])
	if !found {
		writeProblem(w, r, http.StatusConflict, "invalid_plan", "published plan entry is unavailable")
		return
	}
	result, err := h.runtime.StartRun(r.Context(), runtime.TenantID(principal.TenantID), runtime.StartRunRequest{
		Plan:  runtime.PlanRef{ID: published.ID, Hash: published.Hash, WorkflowID: published.WorkflowID, EntryNodeID: entry.ID, Executor: entry.Executor.Kind},
		Input: body.Input, IdempotencyKey: idempotencyKey, Priority: body.Priority,
		Timeout: time.Duration(body.TimeoutMS) * time.Millisecond,
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, runDTO(result.Run))
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	run, err := h.runtime.GetRun(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runDTO(run))
}

func (h *Handler) cancelRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireIdempotencyKey(w, r); !ok {
		return
	}
	principal, _ := PrincipalFromContext(r.Context())
	result, err := h.runtime.CancelRun(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, runDTO(result.Run))
}

func (h *Handler) interrupts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Vary", "X-API-Key")
	principal, _ := PrincipalFromContext(r.Context())
	items, err := h.runtime.ListInterrupts(r.Context(), runtime.TenantID(principal.TenantID), r.PathValue("runID"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	response := make([]interruptResponse, len(items))
	for index, item := range items {
		response[index] = interruptDTO(item)
		if principal.Role == RoleApprover || principal.Role == RoleAdmin {
			token, issueErr := h.resumeTokens.Issue(item)
			if issueErr != nil {
				h.writeError(w, r, issueErr)
				return
			}
			response[index].ResumeToken = token
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) resolveInterrupt(w http.ResponseWriter, r *http.Request) {
	principal, _ := PrincipalFromContext(r.Context())
	idempotencyKey, ok := requireIdempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		ResumeToken string          `json:"resumeToken"`
		Decision    string          `json:"decision"`
		Comment     string          `json:"comment"`
		Data        json.RawMessage `json:"data"`
	}
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	if body.Decision != "approved" && body.Decision != "rejected" {
		writeProblem(w, r, http.StatusBadRequest, "invalid_argument", "decision must be approved or rejected")
		return
	}
	result, err := h.runtime.ResolveApproval(r.Context(), runtime.TenantID(principal.TenantID), runtime.ResolveApprovalCommand{
		ActorID: principal.ActorID, InterruptID: r.PathValue("interruptID"), ResumeToken: body.ResumeToken,
		IdempotencyKey: idempotencyKey, Decision: runtime.ApprovalDecision{
			Approved: body.Decision == "approved", Comment: body.Comment, Data: body.Data,
		},
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, runDTO(result.Run))
}

func (h *Handler) require(required Role, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok || !roleAllows(principal.Role, required) {
			writeProblem(w, r, http.StatusForbidden, "forbidden", "permission denied")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		writeProblem(w, r, http.StatusBadRequest, "invalid_json", "request body is invalid")
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, r, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return errors.New("request contains trailing JSON")
	}
	return nil
}

func requireIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) < 8 || len(key) > 128 {
		writeProblem(w, r, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain 8 to 128 characters")
		return "", false
	}
	return key, true
}

func eventCursor(r *http.Request) (uint64, error) {
	value := r.URL.Query().Get("afterCursor")
	if value == "" {
		value = r.Header.Get("Last-Event-ID")
	}
	if value == "" {
		return 0, nil
	}
	return strconv.ParseUint(value, 10, 64)
}

func roleAllows(actual, required Role) bool {
	if actual == RoleAdmin || actual == required {
		return true
	}
	viewerRoles := map[Role]bool{RoleViewer: true, RoleEditor: true, RolePublisher: true, RoleOperator: true, RoleApprover: true}
	return required == RoleViewer && viewerRoles[actual]
}

func isTerminalEvent(kind string) bool {
	return kind == "run.succeeded" || kind == "run.failed" || kind == "run.cancelled" || kind == "run.error"
}

func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var validation *workflow.ValidationError
	var runtimeError *runtime.Error
	switch {
	case errors.Is(err, definition.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, definition.ErrRevisionConflict):
		writeProblem(w, r, http.StatusConflict, "revision_conflict", "draft revision does not match")
	case errors.Is(err, definition.ErrIdempotencyConflict):
		writeProblem(w, r, http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with an earlier request")
	case errors.As(err, &validation):
		writeProblem(w, r, http.StatusBadRequest, "invalid_workflow", validation.Error())
	case errors.Is(err, platform.ErrPlanNotFound):
		writeProblem(w, r, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, platform.ErrPublicationConflict):
		writeProblem(w, r, http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with an earlier request")
	case errors.As(err, &runtimeError):
		status := http.StatusConflict
		if runtimeError.Code == runtime.CodeInvalidArgument {
			status = http.StatusBadRequest
		} else if runtimeError.Code == runtime.CodeNotFound {
			status = http.StatusNotFound
		} else if runtimeError.Code == runtime.CodeQuotaExceeded {
			status = http.StatusTooManyRequests
			if runtimeError.RetryAfter > 0 {
				w.Header().Set("Retry-After", strconv.FormatInt(max(1, int64(runtimeError.RetryAfter/time.Second)), 10))
			}
		}
		writeProblem(w, r, status, string(runtimeError.Code), runtimeError.Message)
	default:
		writeProblem(w, r, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}

type interruptResponse struct {
	ID          string     `json:"id"`
	RunID       string     `json:"runId"`
	NodeID      string     `json:"nodeId"`
	Kind        string     `json:"kind"`
	ExpiresAt   time.Time  `json:"expiresAt"`
	CreatedAt   time.Time  `json:"createdAt"`
	ResolvedAt  *time.Time `json:"resolvedAt,omitempty"`
	ResumeToken string     `json:"resumeToken,omitempty"`
}

func interruptDTO(interrupt runtime.Interrupt) interruptResponse {
	return interruptResponse{ID: interrupt.ID, RunID: interrupt.RunID, NodeID: interrupt.NodeID,
		Kind: interrupt.Kind, ExpiresAt: interrupt.ExpiresAt, CreatedAt: interrupt.CreatedAt, ResolvedAt: interrupt.ResolvedAt}
}

type eventResponse struct {
	Cursor uint64          `json:"cursor"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data,omitempty"`
	At     time.Time       `json:"at"`
}

func eventDTO(event runtime.Event) eventResponse {
	return eventResponse{Cursor: event.Cursor, Type: event.Type, Data: bytes.Clone(event.Data), At: event.At}
}
