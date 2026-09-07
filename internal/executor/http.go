package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pazhou/supaflow/internal/netpolicy"
	workflowruntime "github.com/pazhou/supaflow/internal/runtime"
	"github.com/pazhou/supaflow/internal/workflow"
)

const maxRedirects = 10

var errCredentialRedirect = errors.New("credentialed HTTP redirects are disabled")

type credentialedRequestContextKey struct{}

type httpExecutor struct {
	credentials   CredentialResolver
	requestLimit  int64
	responseLimit int64
	policy        *egressPolicy
	client        *http.Client
}

type httpConfig struct {
	Method        string            `json:"method"`
	URL           string            `json:"url"`
	CredentialRef string            `json:"credentialRef,omitempty"`
	TimeoutMS     int64             `json:"timeoutMs,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	RetrySafe     bool              `json:"retrySafe,omitempty"`
}

type httpOutput struct {
	StatusCode  int                 `json:"statusCode"`
	ContentType string              `json:"contentType,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	Body        json.RawMessage     `json:"body,omitempty"`
	BodyBase64  string              `json:"bodyBase64,omitempty"`
}

type egressPolicy struct {
	resolver     DNSResolver
	allowedHosts map[string]struct{}
	dialNetwork  func(context.Context, string, string) (net.Conn, error)
}

type policyTransport struct {
	policy *egressPolicy
	base   *http.Transport
}

func newHTTPExecutor(cfg Config) (*httpExecutor, error) {
	policy, err := newEgressPolicy(cfg)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            policy.dialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    10,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
	}
	client := &http.Client{Transport: &policyTransport{policy: policy, base: transport}}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errors.New("redirect limit exceeded")
		}
		if len(via) > 0 && requestCarriesCredential(via[0]) {
			return errCredentialRedirect
		}
		return policy.authorizeURL(req.Context(), req.URL)
	}
	return &httpExecutor{
		credentials: cfg.CredentialResolver, requestLimit: cfg.RequestBodyLimit,
		responseLimit: cfg.ResponseBodyLimit, policy: policy, client: client,
	}, nil
}

func (e *httpExecutor) execute(ctx context.Context, req workflowruntime.ExecutionRequest, node workflow.PlanNode) (json.RawMessage, error) {
	var cfg httpConfig
	if err := decodeStrict(node.Config, &cfg); err != nil || !allowedHTTPMethod(cfg.Method) {
		return nil, executionError("invalid_node_config", false, errors.New("HTTP configuration is invalid"))
	}
	target, err := url.Parse(cfg.URL)
	if err != nil || target.User != nil || target.Fragment != "" || target.Hostname() == "" {
		return nil, executionError("invalid_http_url", false, errors.New("HTTP target is invalid"))
	}
	for key := range target.Query() {
		if isSensitiveName(key) {
			return nil, executionError("plaintext_credential", false, errors.New("HTTP target contains credential-like query data"))
		}
	}
	if cfg.CredentialRef != "" && target.Scheme != "https" {
		return nil, executionError("plaintext_credential", false, errors.New("HTTP credentials require HTTPS"))
	}
	if err := e.policy.authorizeURL(ctx, target); err != nil {
		return nil, executionError("http_egress_denied", false, errors.New("HTTP target is denied by egress policy"))
	}
	if int64(len(req.Input)) > e.requestLimit {
		return nil, executionError("request_limit_exceeded", false, errors.New("HTTP request body exceeded its configured limit"))
	}

	requestCtx := ctx
	var cancel context.CancelFunc
	timeout := time.Duration(node.TimeoutMS) * time.Millisecond
	if cfg.TimeoutMS > 0 {
		timeout = time.Duration(cfg.TimeoutMS) * time.Millisecond
	}
	requestCtx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	if cfg.CredentialRef != "" {
		requestCtx = context.WithValue(requestCtx, credentialedRequestContextKey{}, true)
	}

	var body io.Reader
	if cfg.Method != http.MethodGet && cfg.Method != http.MethodHead && len(req.Input) > 0 && string(req.Input) != "null" {
		body = bytes.NewReader(req.Input)
	}
	httpRequest, err := http.NewRequestWithContext(requestCtx, cfg.Method, target.String(), body)
	if err != nil {
		return nil, executionError("invalid_http_request", false, errors.New("HTTP request could not be created"))
	}
	if body != nil {
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	for name, value := range cfg.Headers {
		if isSensitiveHeader(name) || isForbiddenRequestHeader(name) {
			return nil, executionError("plaintext_credential", false, errors.New("HTTP configuration contains a forbidden header"))
		}
		httpRequest.Header.Set(name, value)
	}
	httpRequest.Header.Set("Idempotency-Key", attemptIdempotencyKey(req))
	if cfg.CredentialRef != "" {
		if e.credentials == nil {
			return nil, executionError("credential_unavailable", false, errors.New("HTTP credential resolver is not configured"))
		}
		headers, resolveErr := e.credentials.ResolveHTTPHeaders(requestCtx, attemptScope(req), cfg.CredentialRef)
		if resolveErr != nil {
			return nil, executionError("credential_unavailable", false, errors.New("HTTP credential could not be resolved"))
		}
		for name, values := range headers {
			if isForbiddenRequestHeader(name) {
				return nil, executionError("credential_invalid", false, errors.New("HTTP credential contains a forbidden header"))
			}
			for _, value := range values {
				httpRequest.Header.Add(name, value)
			}
		}
	}
	if requestSize(httpRequest, req.Input) > e.requestLimit {
		return nil, executionError("request_limit_exceeded", false, errors.New("HTTP request exceeded its configured limit"))
	}

	response, err := e.client.Do(httpRequest)
	if err != nil {
		if errors.Is(requestCtx.Err(), context.Canceled) {
			return nil, executionError("cancelled", false, errors.New("HTTP execution was cancelled"))
		}
		if errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return nil, executionError("http_timeout", true, errors.New("HTTP execution timed out"))
		}
		if errors.Is(err, errCredentialRedirect) {
			return nil, executionError("credential_redirect_denied", false, errCredentialRedirect)
		}
		var policyErr *egressDeniedError
		if errors.As(err, &policyErr) {
			return nil, executionError("http_egress_denied", false, errors.New("HTTP target is denied by egress policy"))
		}
		return nil, executionError("http_transport_failed", true, errors.New("HTTP transport failed"))
	}
	defer response.Body.Close()

	readLimit := min(e.responseLimit, node.OutputLimit)
	payload, err := io.ReadAll(io.LimitReader(response.Body, readLimit+1))
	if err != nil {
		return nil, executionError("http_response_failed", true, errors.New("HTTP response could not be read"))
	}
	if int64(len(payload)) > readLimit {
		return nil, executionError("output_limit_exceeded", false, errors.New("HTTP response exceeded its configured limit"))
	}
	encoded := httpOutput{
		StatusCode: response.StatusCode, ContentType: response.Header.Get("Content-Type"),
		Headers: safeResponseHeaders(response.Header),
	}
	if json.Valid(payload) {
		encoded.Body = cloneJSON(payload)
	} else if len(payload) > 0 {
		encoded.BodyBase64 = base64.StdEncoding.EncodeToString(payload)
	}
	output, err := json.Marshal(encoded)
	if err != nil || int64(len(output)) > node.OutputLimit {
		return nil, executionError("output_limit_exceeded", false, errors.New("HTTP output envelope exceeded the immutable plan limit"))
	}
	return output, nil
}

func requestCarriesCredential(request *http.Request) bool {
	if request == nil {
		return false
	}
	if marked, _ := request.Context().Value(credentialedRequestContextKey{}).(bool); marked {
		return true
	}
	for name := range request.Header {
		if isSensitiveHeader(name) {
			return true
		}
	}
	return false
}

func newEgressPolicy(cfg Config) (*egressPolicy, error) {
	resolver := cfg.DNSResolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedHosts))
	for _, raw := range cfg.AllowedHosts {
		host := normalizeHost(raw)
		if host == "" || strings.Contains(host, "/") {
			return nil, errors.New("executor: allowed HTTP hosts must be exact hostnames or IP addresses")
		}
		allowed[host] = struct{}{}
	}
	dialContext := cfg.DialContext
	if dialContext == nil {
		dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
		dialContext = dialer.DialContext
	}
	return &egressPolicy{resolver: resolver, allowedHosts: allowed, dialNetwork: dialContext}, nil
}

func (t *policyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.policy.authorizeURL(req.Context(), req.URL); err != nil {
		return nil, err
	}
	return t.base.RoundTrip(req)
}

func (p *egressPolicy) authorizeURL(ctx context.Context, target *url.URL) error {
	if target == nil || (target.Scheme != "http" && target.Scheme != "https") || target.User != nil || target.Hostname() == "" {
		return &egressDeniedError{}
	}
	for key := range target.Query() {
		if isSensitiveName(key) {
			return &egressDeniedError{}
		}
	}
	host := normalizeHost(target.Hostname())
	if len(p.allowedHosts) > 0 {
		if _, ok := p.allowedHosts[host]; !ok {
			return &egressDeniedError{}
		}
	}
	addresses, err := p.resolve(ctx, host)
	if err != nil || len(addresses) == 0 {
		return &egressDeniedError{}
	}
	literal := net.ParseIP(host)
	_, literalExplicitlyAllowed := p.allowedHosts[host]
	for _, address := range addresses {
		if !netpolicy.IsPublicIP(address) && !(literal != nil && literalExplicitlyAllowed && literal.Equal(address)) {
			return &egressDeniedError{}
		}
	}
	return nil
}

func (p *egressPolicy) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, &egressDeniedError{}
	}
	normalized := normalizeHost(host)
	if len(p.allowedHosts) > 0 {
		if _, ok := p.allowedHosts[normalized]; !ok {
			return nil, &egressDeniedError{}
		}
	}
	addresses, err := p.resolve(ctx, normalized)
	if err != nil || len(addresses) == 0 {
		return nil, &egressDeniedError{}
	}
	literal := net.ParseIP(normalized)
	_, literalExplicitlyAllowed := p.allowedHosts[normalized]
	for _, ip := range addresses {
		if !netpolicy.IsPublicIP(ip) && !(literal != nil && literalExplicitlyAllowed && literal.Equal(ip)) {
			return nil, &egressDeniedError{}
		}
	}
	var lastErr error
	for _, ip := range addresses {
		connection, dialErr := p.dialNetwork(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		lastErr = dialErr
	}
	return nil, lastErr
}

func (p *egressPolicy) resolve(ctx context.Context, host string) ([]net.IP, error) {
	if literal := net.ParseIP(host); literal != nil {
		return []net.IP{literal}, nil
	}
	resolved, err := p.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	addresses := make([]net.IP, 0, len(resolved))
	for _, value := range resolved {
		if value.IP == nil {
			return nil, errors.New("empty resolved address")
		}
		addresses = append(addresses, value.IP)
	}
	return addresses, nil
}

type egressDeniedError struct{}

func (*egressDeniedError) Error() string { return "HTTP egress denied" }

func normalizeHost(host string) string {
	host = strings.TrimSpace(strings.ToLower(host))
	host = strings.TrimSuffix(host, ".")
	if parsed := net.ParseIP(strings.Trim(host, "[]")); parsed != nil {
		return parsed.String()
	}
	return strings.Trim(host, "[]")
}

func allowedHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead:
		return true
	default:
		return false
	}
}

func isSensitiveName(name string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(name))
	return strings.Contains(normalized, "authorization") || strings.Contains(normalized, "password") ||
		strings.Contains(normalized, "secret") || strings.Contains(normalized, "token") ||
		strings.Contains(normalized, "apikey") || strings.Contains(normalized, "privatekey") ||
		strings.Contains(normalized, "accesskey") || strings.Contains(normalized, "signature")
}

func isSensitiveHeader(name string) bool {
	canonical := http.CanonicalHeaderKey(name)
	return canonical == "Cookie" || canonical == "Set-Cookie" || canonical == "Proxy-Authorization" ||
		canonical == "Www-Authenticate" || isSensitiveName(canonical)
}

func isForbiddenRequestHeader(name string) bool {
	switch http.CanonicalHeaderKey(name) {
	case "Host", "Content-Length", "Transfer-Encoding", "Connection", "Proxy-Connection", "Upgrade", "Trailer", "Idempotency-Key":
		return true
	default:
		return false
	}
}

func attemptIdempotencyKey(req workflowruntime.ExecutionRequest) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(req.OrganizationID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(req.RunID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(req.NodeID))
	_, _ = digest.Write([]byte{0})
	executionID := req.ExecutionID
	if executionID == "" {
		executionID = req.AttemptID
	}
	_, _ = digest.Write([]byte(executionID))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(req.Plan.Hash))
	_, _ = digest.Write([]byte{0})
	var input any
	decoder := json.NewDecoder(bytes.NewReader(req.Input))
	decoder.UseNumber()
	if decoder.Decode(&input) == nil {
		if canonical, err := json.Marshal(input); err == nil {
			_, _ = digest.Write(canonical)
		} else {
			_, _ = digest.Write(req.Input)
		}
	} else {
		_, _ = digest.Write(req.Input)
	}
	return "sf_" + hex.EncodeToString(digest.Sum(nil))
}

func safeResponseHeaders(headers http.Header) map[string][]string {
	safe := make(map[string][]string)
	for name, values := range headers {
		if isSensitiveHeader(name) || isForbiddenRequestHeader(name) {
			continue
		}
		safe[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
	}
	if len(safe) == 0 {
		return nil
	}
	return safe
}

func requestSize(request *http.Request, input json.RawMessage) int64 {
	size := int64(len(input))
	for name, values := range request.Header {
		size += int64(len(name))
		for _, value := range values {
			size += int64(len(value))
		}
	}
	return size
}

func min(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
