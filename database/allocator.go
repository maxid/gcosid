package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/maxid/gcosid"
)

// ErrSegmentMissing means the counter row does not exist. The caller must
// create it (CosID's initializer inserts `{namespace}.{name}, 0, now`).
var ErrSegmentMissing = errors.New("gcosid/database: segment row is missing")

// Allocator implements gcosid.SegmentAllocator against the CosID counter table.
type Allocator struct {
	db      *sql.DB
	dialect Dialect
	// namespace prefixes every name as `{namespace}.{name}`, matching CosID's
	// IdSegmentDistributorDefinition.
	namespace string
}

// Option configures an Allocator.
type Option func(*Allocator)

// WithNamespace prefixes segment names with the namespace.
func WithNamespace(namespace string) Option {
	return func(a *Allocator) { a.namespace = namespace }
}

// NewAllocator creates an allocator for the given dialect.
func NewAllocator(db *sql.DB, dialect Dialect, opts ...Option) (*Allocator, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db is required", gcosid.ErrInvalidConfig)
	}
	if dialect == nil {
		return nil, fmt.Errorf("%w: dialect is required", gcosid.ErrInvalidConfig)
	}
	a := &Allocator{db: db, dialect: dialect}
	for _, o := range opts {
		o(a)
	}
	return a, nil
}

// NextMaxId implements gcosid.SegmentAllocator: one UPDATE reserves the range,
// one SELECT reads the upper bound back.
func (a *Allocator) NextMaxId(ctx context.Context, name string, step int64) (int64, error) {
	if step <= 0 {
		return 0, fmt.Errorf("%w: step must be positive", gcosid.ErrInvalidConfig)
	}
	key := a.key(name)

	res, err := a.db.ExecContext(ctx, a.dialect.Increment(), step, key)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", gcosid.ErrAllocatorUnavailable, err)
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return 0, ErrSegmentMissing
	}

	var maxID int64
	if err := a.db.QueryRowContext(ctx, a.dialect.Fetch(), key).Scan(&maxID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrSegmentMissing
		}
		return 0, fmt.Errorf("%w: %w", gcosid.ErrAllocatorUnavailable, err)
	}
	return maxID, nil
}

func (a *Allocator) key(name string) string {
	if a.namespace == "" {
		return name
	}
	return a.namespace + "." + name
}
