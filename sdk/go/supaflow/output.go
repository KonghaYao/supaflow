package supaflow

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// OutputDownload describes a verified, bounded external output stream.
type OutputDownload struct {
	Body        io.ReadCloser
	ContentType string
	SizeBytes   int64
	SHA256      string
}

// DownloadRunOutput opens a organization-authorized output stream. maxBytes is a
// caller-owned memory/processing budget and is checked before any bytes return.
func (c *Client) DownloadRunOutput(ctx context.Context, runID string, maxBytes int64) (OutputDownload, error) {
	if strings.TrimSpace(runID) == "" || maxBytes <= 0 || maxBytes > 1<<30 {
		return OutputDownload{}, errors.New("supaflow: valid run ID and output size limit are required")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL.String()+"/v1/runs/"+url.PathEscape(runID)+"/output", nil)
	if err != nil {
		return OutputDownload{}, fmt.Errorf("supaflow: create output request: %w", err)
	}
	request.Header.Set("Accept", "application/json, application/octet-stream")
	c.setTrustedHeaders(request)
	response, err := c.http.Do(request)
	if err != nil {
		return OutputDownload{}, fmt.Errorf("supaflow: send output request: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		payload, _ := io.ReadAll(io.LimitReader(response.Body, maxResponseBody+1))
		problem := &APIError{Status: response.StatusCode, Code: "request_failed", Title: response.Status}
		_ = json.Unmarshal(payload, problem)
		return OutputDownload{}, problem
	}
	size, parseErr := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	digest, digestErr := hex.DecodeString(response.Header.Get("X-Content-SHA256"))
	contentType := response.Header.Get("Content-Type")
	if parseErr != nil || digestErr != nil || size < 0 || size > maxBytes || len(digest) != sha256.Size || contentType == "" {
		_ = response.Body.Close()
		if size > maxBytes {
			return OutputDownload{}, errors.New("supaflow: output exceeds caller size limit")
		}
		return OutputDownload{}, errors.New("supaflow: invalid output metadata")
	}
	body := &verifiedOutputBody{reader: response.Body, remaining: size, expected: digest, hash: sha256.New()}
	return OutputDownload{Body: body, ContentType: contentType, SizeBytes: size, SHA256: hex.EncodeToString(digest)}, nil
}

type verifiedOutputBody struct {
	reader    io.ReadCloser
	remaining int64
	expected  []byte
	hash      hash.Hash
	verified  bool
}

func (r *verifiedOutputBody) Read(buffer []byte) (int, error) {
	if r.remaining == 0 {
		var extra [1]byte
		count, err := r.reader.Read(extra[:])
		if count != 0 || err != io.EOF {
			return 0, errors.New("supaflow: output size verification failed")
		}
		if !r.verified {
			r.verified = true
			if subtle.ConstantTimeCompare(r.hash.Sum(nil), r.expected) != 1 {
				return 0, errors.New("supaflow: output hash verification failed")
			}
		}
		return 0, io.EOF
	}
	if int64(len(buffer)) > r.remaining {
		buffer = buffer[:r.remaining]
	}
	count, err := r.reader.Read(buffer)
	if count > 0 {
		r.remaining -= int64(count)
		_, _ = r.hash.Write(buffer[:count])
	}
	if err == io.EOF && r.remaining != 0 {
		return count, errors.New("supaflow: output ended before declared size")
	}
	return count, err
}

func (r *verifiedOutputBody) Close() error { return r.reader.Close() }
