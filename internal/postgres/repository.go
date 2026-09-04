package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Stable repository errors let application services map failures without
// exposing SQL or database topology to callers.
var (
	ErrNotFound            = errors.New("postgres record not found")
	ErrIdempotencyConflict = errors.New("idempotency key was used for another request")
	ErrStaleLease          = errors.New("worker lease is stale")
)

// Repository owns shared PostgreSQL query primitives such as event reads and
// outbox delivery. Durable runtime state transitions live only on RuntimeStore.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a PostgreSQL repository for event and outbox adapters.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) requirePool() error {
	if r == nil || r.pool == nil {
		return errors.New("postgres repository requires a pool")
	}
	return nil
}

// NewID returns a cryptographically unpredictable identifier with prefix.
func NewID(prefix string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate identifier: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(random[:]), nil
}

type rowScanner interface {
	Scan(dest ...any) error
}
