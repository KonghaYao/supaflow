package platformops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxOutboundWebhookResponse = 64 << 10

// SafeOutboundWebhookClient blocks private-address egress and redirect escapes.
type SafeOutboundWebhookClient struct {
	client    *http.Client
	allowlist []string
}

// NewSafeOutboundWebhookClient creates an SSRF-resistant webhook transport.
func NewSafeOutboundWebhookClient(allowlist []string) *SafeOutboundWebhookClient {
	copyList := append([]string(nil), allowlist...)
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil, ForceAttemptHTTP2: true, MaxIdleConns: 64, MaxIdleConnsPerHost: 4,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second,
	}
	transport.DialContext = safeWebhookDialer(dialer, copyList)
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("outbound webhook redirects are disabled")
	}}
	return &SafeOutboundWebhookClient{client: client, allowlist: copyList}
}

// Send performs one signed POST with bounded headers, timeout, and response bytes.
func (c *SafeOutboundWebhookClient) Send(ctx context.Context, request OutboundWebhookRequest) (int, error) {
	parsed, err := url.Parse(request.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" ||
		!hostAllowed(parsed.Hostname(), c.allowlist) || len(request.Body) > maxOutboundWebhookBody {
		return 0, ErrInvalidArgument
	}
	timeout := request.Timeout
	if timeout < 100*time.Millisecond || timeout > 10*time.Second {
		timeout = 5 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, request.URL, bytes.NewReader(request.Body))
	if err != nil {
		return 0, fmt.Errorf("create outbound webhook request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("User-Agent", "Supaflow-Webhooks/1.0")
	httpRequest.Header.Set("X-Supaflow-Event", request.EventType)
	httpRequest.Header.Set("X-Supaflow-Request-ID", request.RequestID)
	httpRequest.Header.Set("X-Supaflow-Timestamp", strconv.FormatInt(request.Timestamp.Unix(), 10))
	httpRequest.Header.Set("X-Supaflow-Signature", request.Signature)
	for name, value := range request.Headers {
		if strings.EqualFold(name, "traceparent") || strings.EqualFold(name, "tracestate") || strings.EqualFold(name, "baggage") {
			httpRequest.Header.Set(name, value)
		}
	}
	response, err := c.client.Do(httpRequest)
	if err != nil {
		return 0, fmt.Errorf("send outbound webhook: %w", err)
	}
	defer response.Body.Close()
	read, err := io.Copy(io.Discard, io.LimitReader(response.Body, maxOutboundWebhookResponse+1))
	if err != nil {
		return response.StatusCode, fmt.Errorf("drain outbound webhook response: %w", err)
	}
	if read > maxOutboundWebhookResponse {
		return response.StatusCode, errors.New("outbound webhook response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, fmt.Errorf("outbound webhook returned HTTP %d", response.StatusCode)
	}
	return response.StatusCode, nil
}

func safeWebhookDialer(dialer *net.Dialer, allowlist []string) func(context.Context, string, string) (net.Conn, error) {
	resolver := net.DefaultResolver
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || !hostAllowed(host, allowlist) {
			return nil, ErrPermissionDenied
		}
		addresses, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve outbound webhook host: %w", err)
		}
		if len(addresses) == 0 {
			return nil, errors.New("outbound webhook host has no addresses")
		}
		for _, resolved := range addresses {
			if !isPublicIP(resolved.IP) {
				return nil, ErrPermissionDenied
			}
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
	}
}
