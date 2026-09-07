package platformops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

type cleanupObjectStore struct {
	staged  []string
	deleted []string
}

func (*cleanupObjectStore) Put(_ context.Context, _, key string, source io.Reader, _ int64) (StoredObject, error) {
	value, err := io.ReadAll(source)
	if err != nil {
		return StoredObject{}, err
	}
	digest := sha256.Sum256(value)
	return StoredObject{Key: key, Size: int64(len(value)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (*cleanupObjectStore) Open(context.Context, string, string) (io.ReadCloser, error) {
	return nil, ErrNotFound
}

func (s *cleanupObjectStore) Delete(_ context.Context, _, key string) error {
	s.deleted = append(s.deleted, key)
	return nil
}

func (s *cleanupObjectStore) StageUpload(_ context.Context, _, key, _ string, source io.Reader, _ int64) (StoredObject, error) {
	s.staged = append(s.staged, key)
	return s.Put(context.Background(), "", key, source, 0)
}

func (*cleanupObjectStore) PublishUpload(context.Context, string, string, string) error { return nil }

func (s *cleanupObjectStore) AbortUpload(_ context.Context, _, key, _ string, removeFinal bool) error {
	if removeFinal {
		s.deleted = append(s.deleted, key)
	}
	return nil
}

// 验证 PostgreSQL intent 持久化失败时不会先写入任何对象字节。
func TestOutputManagerDoesNotWriteBeforeIntentFailure(t *testing.T) {
	config, err := pgxpool.ParseConfig("postgres://supaflow:secret@127.0.0.1:1/supaflow?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	objects := &cleanupObjectStore{}
	manager, err := NewOutputManager(pool, objects, 1024)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Store(context.Background(), "organization", "run", "application/json", nil, strings.NewReader(`{"value":true}`))
	if err == nil || len(objects.staged) != 0 || len(objects.deleted) != 0 {
		t.Fatalf("error=%v staged=%v deleted=%v", err, objects.staged, objects.deleted)
	}
}

var _ ObjectStore = (*cleanupObjectStore)(nil)
var _ uploadAborter = (*cleanupObjectStore)(nil)
