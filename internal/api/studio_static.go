package api

import (
	"bytes"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
	"time"
)

var zeroTime time.Time

func (h *Handler) registerStudioAssets(root *http.ServeMux) {
	if h.studioAssets == nil {
		return
	}
	root.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/studio/", http.StatusTemporaryRedirect)
	})
	root.HandleFunc("GET /studio/", h.serveStudio)
}

func (h *Handler) serveStudio(w http.ResponseWriter, r *http.Request) {
	requested := strings.TrimPrefix(r.URL.Path, "/studio/")
	requested = strings.TrimPrefix(path.Clean("/"+requested), "/")
	if requested == "." || requested == "" {
		requested = "index.html"
	}
	content, err := fs.ReadFile(h.studioAssets, requested)
	if err != nil {
		content, err = fs.ReadFile(h.studioAssets, "index.html")
		requested = "index.html"
	}
	if err != nil {
		writeProblem(w, r, http.StatusServiceUnavailable, "studio_unavailable", "Studio assets are unavailable")
		return
	}
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if requested == "index.html" {
		w.Header().Set("Cache-Control", "private, no-cache, no-store")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	if contentType := mime.TypeByExtension(path.Ext(requested)); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	http.ServeContent(w, r, requested, zeroTime, bytes.NewReader(content))
}
