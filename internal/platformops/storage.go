package platformops

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var objectPathPart = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)

// StoredObject describes bytes persisted by an ObjectStore.
type StoredObject struct {
	Key    string
	Size   int64
	SHA256 string
}

// ObjectStore is the durable large-payload boundary.
type ObjectStore interface {
	Put(context.Context, string, string, io.Reader, int64) (StoredObject, error)
	Open(context.Context, string, string) (io.ReadCloser, error)
	Delete(context.Context, string, string) error
}

// FileObjectStore is a local/self-hosted adapter rooted below an injected directory.
type FileObjectStore struct{ root string }

// NewFileObjectStore creates a traversal-safe local object adapter.
func NewFileObjectStore(root string) (*FileObjectStore, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil || root == "" {
		return nil, ErrInvalidArgument
	}
	if err = os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create object store root: %w", err)
	}
	return &FileObjectStore{root: absolute}, nil
}

// Put provides the basic ObjectStore API. Production managers use the owned
// staging methods below so database fencing surrounds publication.
func (s *FileObjectStore) Put(ctx context.Context, organizationID, key string, source io.Reader, maxBytes int64) (StoredObject, error) {
	ownerToken, err := newID("direct")
	if err != nil {
		return StoredObject{}, err
	}
	stored, err := s.StageUpload(ctx, organizationID, key, ownerToken, source, maxBytes)
	if err != nil {
		return StoredObject{}, err
	}
	if err = s.PublishUpload(ctx, organizationID, key, ownerToken); err != nil {
		_ = s.AbortUpload(context.WithoutCancel(ctx), organizationID, key, ownerToken, false)
		return StoredObject{}, err
	}
	return stored, nil
}

// StageUpload writes one owner-scoped temporary payload without making it
// visible as the final object.
func (s *FileObjectStore) StageUpload(ctx context.Context, organizationID, key, ownerToken string, source io.Reader, maxBytes int64) (StoredObject, error) {
	path, err := s.path(organizationID, key)
	if err != nil || !objectPathPart.MatchString(ownerToken) || source == nil || maxBytes <= 0 {
		return StoredObject{}, ErrInvalidArgument
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return StoredObject{}, fmt.Errorf("create organization object directory: %w", err)
	}
	temporaryPath := path + ".upload." + ownerToken
	temporary, err := os.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return StoredObject{}, fmt.Errorf("create object temporary file: %w", err)
	}
	keepStage := false
	defer func() {
		if !keepStage {
			_ = os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	reader := &contextReader{ctx: ctx, reader: source}
	written, copyErr := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(reader, maxBytes+1))
	syncErr := temporary.Sync()
	closeErr := temporary.Close()
	if copyErr != nil {
		return StoredObject{}, fmt.Errorf("write object: %w", copyErr)
	}
	if syncErr != nil {
		return StoredObject{}, fmt.Errorf("sync object: %w", syncErr)
	}
	if closeErr != nil {
		return StoredObject{}, fmt.Errorf("close object: %w", closeErr)
	}
	if written > maxBytes {
		return StoredObject{}, ErrInvalidArgument
	}
	if err = ctx.Err(); err != nil {
		return StoredObject{}, err
	}
	keepStage = true
	return StoredObject{Key: key, Size: written, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

// PublishUpload atomically renames only the named owner's stage. Callers must
// hold the matching database intent row lock while invoking it.
func (s *FileObjectStore) PublishUpload(_ context.Context, organizationID, key, ownerToken string) error {
	path, err := s.path(organizationID, key)
	if err != nil || !objectPathPart.MatchString(ownerToken) {
		return ErrInvalidArgument
	}
	if err = os.Rename(path+".upload."+ownerToken, path); err != nil {
		return fmt.Errorf("commit object: %w", err)
	}
	if err = syncDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync object directory: %w", err)
	}
	return nil
}

// Open returns a organization-scoped object stream.
func (s *FileObjectStore) Open(_ context.Context, organizationID, key string) (io.ReadCloser, error) {
	path, err := s.path(organizationID, key)
	if err != nil {
		return nil, ErrInvalidArgument
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("open object: %w", err)
	}
	return file, nil
}

// Delete removes exactly one validated organization object.
func (s *FileObjectStore) Delete(_ context.Context, organizationID, key string) error {
	path, err := s.path(organizationID, key)
	if err != nil {
		return ErrInvalidArgument
	}
	err = os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

// AbortUpload always removes only the named owner's stage. The final object is
// removed only after the database coordinator proves this owner may do so.
func (s *FileObjectStore) AbortUpload(_ context.Context, organizationID, key, ownerToken string, removeFinal bool) error {
	path, err := s.path(organizationID, key)
	if err != nil || !objectPathPart.MatchString(ownerToken) {
		return ErrInvalidArgument
	}
	removed := false
	candidates := []string{path + ".upload." + ownerToken}
	if removeFinal {
		candidates = append(candidates, path)
	}
	for _, candidate := range candidates {
		removeErr := os.Remove(candidate)
		if removeErr == nil {
			removed = true
			continue
		}
		if !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("abort object upload: %w", removeErr)
		}
	}
	if removed {
		return syncDirectory(filepath.Dir(path))
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *FileObjectStore) path(organizationID, key string) (string, error) {
	if !objectPathPart.MatchString(organizationID) || !objectPathPart.MatchString(key) {
		return "", ErrInvalidArgument
	}
	path := filepath.Join(s.root, organizationID, key)
	prefix := filepath.Join(s.root, organizationID) + string(filepath.Separator)
	if !strings.HasPrefix(path, prefix) {
		return "", ErrInvalidArgument
	}
	return path, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(buffer)
	}
}

// ManagedObject is safe database metadata for one large object.
type ManagedObject struct {
	OrganizationID string     `json:"-"`
	ID             string     `json:"id"`
	RunID          string     `json:"runId,omitempty"`
	Kind           string     `json:"kind"`
	ObjectKey      string     `json:"objectKey"`
	ContentType    string     `json:"contentType"`
	Size           int64      `json:"size"`
	SHA256         string     `json:"sha256"`
	RetentionUntil *time.Time `json:"retentionUntil,omitempty"`
	Status         string     `json:"status"`
	CreatedAt      time.Time  `json:"createdAt"`
}

// OutputManager persists large output bytes before atomically registering metadata.
type OutputManager struct {
	pool     *pgxpool.Pool
	objects  ObjectStore
	maxBytes int64
}

// NewOutputManager creates a large-output manager with a hard size bound.
func NewOutputManager(pool *pgxpool.Pool, objects ObjectStore, maxBytes int64) (*OutputManager, error) {
	if pool == nil || objects == nil || maxBytes <= 0 {
		return nil, ErrInvalidArgument
	}
	return &OutputManager{pool: pool, objects: objects, maxBytes: maxBytes}, nil
}

// Store persists bytes and metadata; metadata failure compensates by deleting the object.
func (m *OutputManager) Store(ctx context.Context, organizationID, runID, contentType string, retentionUntil *time.Time, source io.Reader) (ManagedObject, error) {
	if organizationID == "" || runID == "" || contentType == "" {
		return ManagedObject{}, ErrInvalidArgument
	}
	id, err := newID("obj")
	if err != nil {
		return ManagedObject{}, err
	}
	upload, err := beginObjectUpload(ctx, m.pool, m.objects, objectUploadSpec{
		organizationID: organizationID, id: id, runID: runID, kind: "output", key: id,
		contentType: contentType, retentionUntil: retentionUntil,
	})
	if err != nil {
		return ManagedObject{}, err
	}
	committed := false
	defer func() {
		if committed {
			_ = upload.closeHeartbeat()
		} else {
			_ = upload.abort()
		}
	}()
	stored, err := upload.put(ctx, source, m.maxBytes)
	if err != nil {
		return ManagedObject{}, err
	}
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ManagedObject{}, fmt.Errorf("begin managed output registration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = lockOrganizationRetention(ctx, tx, organizationID); err != nil {
		return ManagedObject{}, err
	}
	if err = upload.publishTx(ctx, tx); err != nil {
		return ManagedObject{}, err
	}
	metadata := ManagedObject{OrganizationID: organizationID, ID: id, RunID: runID, Kind: "output", ObjectKey: stored.Key, ContentType: contentType, Size: stored.Size, SHA256: stored.SHA256, RetentionUntil: retentionUntil, Status: "active"}
	digest, _ := hex.DecodeString(stored.SHA256)
	err = tx.QueryRow(ctx, `
		INSERT INTO managed_objects (organization_id,id,run_id,kind,object_key,content_type,size_bytes,content_hash,retention_until)
		VALUES ($1,$2,$3,'output',$4,$5,$6,$7,
		        LEAST(
		            COALESCE($8, now() + COALESCE(
		                (SELECT output_days FROM retention_policies WHERE organization_id=$1), 30
		            ) * interval '1 day'),
		            now() + COALESCE(
		                (SELECT output_days FROM retention_policies WHERE organization_id=$1), 30
		            ) * interval '1 day'
		        ))
		RETURNING created_at,retention_until`, organizationID, id, runID,
		stored.Key, contentType, stored.Size, digest, retentionUntil).Scan(&metadata.CreatedAt, &metadata.RetentionUntil)
	if err != nil {
		return ManagedObject{}, fmt.Errorf("register managed output: %w", err)
	}
	if err = upload.completeTx(ctx, tx); err != nil {
		return ManagedObject{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ManagedObject{}, fmt.Errorf("commit managed output registration: %w", err)
	}
	committed = true
	return metadata, nil
}

// Open verifies stored bytes against immutable size and hash metadata.
func (m *OutputManager) Open(ctx context.Context, organizationID, objectID string) (io.ReadCloser, ManagedObject, error) {
	var metadata ManagedObject
	var digest []byte
	err := m.pool.QueryRow(ctx, `
		SELECT organization_id,id,COALESCE(run_id,''),kind,object_key,content_type,size_bytes,content_hash,
		       retention_until,status,created_at
		FROM managed_objects WHERE organization_id=$1 AND id=$2 AND status IN ('active','archived')`, organizationID, objectID).Scan(
		&metadata.OrganizationID, &metadata.ID, &metadata.RunID, &metadata.Kind, &metadata.ObjectKey,
		&metadata.ContentType, &metadata.Size, &digest, &metadata.RetentionUntil, &metadata.Status, &metadata.CreatedAt)
	if err != nil {
		return nil, ManagedObject{}, ErrNotFound
	}
	metadata.SHA256 = hex.EncodeToString(digest)
	reader, err := m.objects.Open(ctx, organizationID, metadata.ObjectKey)
	if err != nil {
		return nil, ManagedObject{}, err
	}
	expected, _ := hex.DecodeString(metadata.SHA256)
	return &verifyingReadCloser{reader: reader, hash: sha256.New(), expected: expected, expectedSize: metadata.Size}, metadata, nil
}

// OpenRunOutput resolves an opaque object only through a organization-owned run's
// final output relation; callers never receive or supply an object-store key.
func (m *OutputManager) OpenRunOutput(ctx context.Context, organizationID, runID string) (io.ReadCloser, ManagedObject, error) {
	var objectID string
	err := m.pool.QueryRow(ctx, `
		SELECT managed.id
		FROM runs run
		JOIN node_outputs output ON output.organization_id=run.organization_id AND output.id=run.output_id
		JOIN managed_objects managed ON managed.organization_id=output.organization_id AND managed.id=output.object_ref
		WHERE run.organization_id=$1 AND run.id=$2 AND output.retention_status='active'
		  AND managed.status='active'
		  AND (output.retention_until IS NULL OR output.retention_until > now())
		  AND (managed.retention_until IS NULL OR managed.retention_until > now())`, organizationID, runID).Scan(&objectID)
	if err != nil {
		return nil, ManagedObject{}, ErrNotFound
	}
	return m.Open(ctx, organizationID, objectID)
}

// Discard durably hides one organization-scoped output before deleting its bytes.
// Interrupted physical deletion is retried by object recovery.
func (m *OutputManager) Discard(ctx context.Context, organizationID, objectID string) error {
	if organizationID == "" || objectID == "" {
		return ErrInvalidArgument
	}
	_, err := deleteManagedObject(ctx, m.pool, m.objects, organizationID, objectID, "output", nil)
	return err
}

type verifyingReadCloser struct {
	reader       io.ReadCloser
	hash         hash.Hash
	expected     []byte
	expectedSize int64
	read         int64
	verified     bool
}

func (r *verifyingReadCloser) Read(buffer []byte) (int, error) {
	count, err := r.reader.Read(buffer)
	if count > 0 {
		r.read += int64(count)
		_, _ = r.hash.Write(buffer[:count])
	}
	if errors.Is(err, io.EOF) && !r.verified {
		r.verified = true
		if r.read != r.expectedSize || subtle.ConstantTimeCompare(r.hash.Sum(nil), r.expected) != 1 {
			return count, errors.New("object integrity verification failed")
		}
	}
	return count, err
}

func (r *verifyingReadCloser) Close() error { return r.reader.Close() }
