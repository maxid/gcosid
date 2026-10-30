package database_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/maxid/gcosid"
	"github.com/maxid/gcosid/database"
)

func newAllocator(t *testing.T, opts ...database.Option) (*database.Allocator, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	alloc, err := database.NewAllocator(db, database.MySQL{}, opts...)
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}
	return alloc, mock
}

func TestNextMaxIdReservesThenReadsBack(t *testing.T) {
	alloc, mock := newAllocator(t, database.WithNamespace("xdmp"))
	ctx := context.Background()

	mock.ExpectExec("update cosid set last_max_id").
		WithArgs(int64(100), "xdmp.order").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("select last_max_id from cosid").
		WithArgs("xdmp.order").
		WillReturnRows(sqlmock.NewRows([]string{"last_max_id"}).AddRow(int64(100)))

	maxID, err := alloc.NextMaxId(ctx, "order", 100)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if maxID != 100 {
		t.Fatalf("maxID = %d, want 100", maxID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// A missing counter row must be a distinct error: it means the initializer was
// never run, which is a configuration problem, not a transient outage.
func TestNextMaxIdReportsMissingSegment(t *testing.T) {
	alloc, mock := newAllocator(t)

	mock.ExpectExec("update cosid set last_max_id").
		WithArgs(int64(10), "order").
		WillReturnResult(sqlmock.NewResult(0, 0))

	if _, err := alloc.NextMaxId(context.Background(), "order", 10); !errors.Is(err, database.ErrSegmentMissing) {
		t.Fatalf("err = %v, want ErrSegmentMissing", err)
	}
}

func TestNextMaxIdMapsDatabaseErrorsToUnavailable(t *testing.T) {
	alloc, mock := newAllocator(t)

	mock.ExpectExec("update cosid set last_max_id").
		WithArgs(int64(10), "order").
		WillReturnError(errors.New("connection refused"))

	if _, err := alloc.NextMaxId(context.Background(), "order", 10); !errors.Is(err, gcosid.ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}
}

func TestPostgresDialectUsesNumberedPlaceholders(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	alloc, err := database.NewAllocator(db, database.Postgres{})
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}

	mock.ExpectExec("UPDATE cosid SET last_max_id").
		WithArgs(int64(5), "order").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT last_max_id FROM cosid").
		WithArgs("order").
		WillReturnRows(sqlmock.NewRows([]string{"last_max_id"}).AddRow(int64(5)))

	if _, err := alloc.NextMaxId(context.Background(), "order", 5); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestNewAllocatorRejectsBadConfig(t *testing.T) {
	if _, err := database.NewAllocator(nil, database.MySQL{}); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("nil db err = %v", err)
	}
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := database.NewAllocator(db, nil); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("nil dialect err = %v", err)
	}
}
