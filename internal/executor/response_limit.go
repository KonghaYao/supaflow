package executor

import (
	"errors"
	"io"
	"net/http"
)

const maximumProviderResponseBytes = int64(16 << 20)

var errProviderResponseTooLarge = errors.New("credential provider response exceeds limit")

type boundedResponseTransport struct {
	base     http.RoundTripper
	maxBytes int64
}

func newBoundedResponseTransport(base http.RoundTripper, maxBytes int64) http.RoundTripper {
	return &boundedResponseTransport{base: base, maxBytes: maxBytes}
}

func (t *boundedResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response.ContentLength > t.maxBytes {
		_ = response.Body.Close()
		return nil, errProviderResponseTooLarge
	}
	response.Body = &boundedReadCloser{reader: response.Body, remaining: t.maxBytes}
	return response, nil
}

type boundedReadCloser struct {
	reader    io.ReadCloser
	remaining int64
	exceeded  bool
}

func (r *boundedReadCloser) Read(buffer []byte) (int, error) {
	if r.exceeded {
		return 0, errProviderResponseTooLarge
	}
	if r.remaining < int64(len(buffer)) {
		buffer = buffer[:r.remaining+1]
	}
	count, err := r.reader.Read(buffer)
	r.remaining -= int64(count)
	if r.remaining < 0 {
		r.exceeded = true
		return count, errProviderResponseTooLarge
	}
	return count, err
}

func (r *boundedReadCloser) Close() error { return r.reader.Close() }
