package database_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/maxid/gcosid"
	"github.com/maxid/gcosid/database"
)

var machineCols = []string{"machine_id", "last_timestamp"}

func newMachineStorage(t *testing.T, opts ...database.MachineStorageOption) (*database.MachineStorage, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	storage, err := database.NewMachineStorage(db, "order", append([]database.MachineStorageOption{
		database.WithMachineClock(func() int64 { return 1_000_000 }),
	}, opts...)...)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	return storage, mock
}

// An instance that already holds a slot keeps it: a restart must not race for a
// new machine id.
func TestAllocateReusesTheExistingSlot(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-a").
		WillReturnRows(sqlmock.NewRows(machineCols).AddRow(int64(3), int64(999)))

	state, err := storage.Allocate(context.Background(), "order", "node-a", 10)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if state.MachineID != 3 {
		t.Fatalf("machine id = %d, want 3", state.MachineID)
	}
}

// A released slot (empty owner or a stale heartbeat) is claimed before a new id
// is appended.
func TestAllocateClaimsAReleasedSlot(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-b").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine").
		WithArgs("order", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows(machineCols).AddRow(int64(2), int64(5)))
	mock.ExpectExec("UPDATE cosid_machine SET instance_id").
		WithArgs("node-b", int64(1_000_000), int64(1_000_000), "order.2", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	state, err := storage.Allocate(context.Background(), "order", "node-b", 10)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if state.MachineID != 2 {
		t.Fatalf("machine id = %d, want the released slot 2", state.MachineID)
	}
}

func TestAllocateAppendsWhenNothingIsReleased(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-c").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT MAX\\(machine_id\\) \\+ 1").
		WithArgs("order").
		WillReturnRows(sqlmock.NewRows([]string{"next"}).AddRow(int64(4)))
	mock.ExpectExec("INSERT INTO cosid_machine").
		WithArgs("order.4", "order", int64(4), int64(1_000_000), "node-c", int64(1_000_000)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	state, err := storage.Allocate(context.Background(), "order", "node-c", 10)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if state.MachineID != 4 {
		t.Fatalf("machine id = %d, want 4", state.MachineID)
	}
}

func TestAllocateRejectsExhaustedNamespace(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-d").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT MAX\\(machine_id\\) \\+ 1").
		WithArgs("order").
		WillReturnRows(sqlmock.NewRows([]string{"next"}).AddRow(int64(11)))

	if _, err := storage.Allocate(context.Background(), "order", "node-d", 10); !errors.Is(err, gcosid.ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
}

func TestUpdateGuardsByInstanceAndMachine(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectExec("UPDATE cosid_machine SET last_timestamp").
		WithArgs(int64(42), "order", "node-a", int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := storage.Update(context.Background(), "order", gcosid.MachineState{MachineID: 3, LastTimestamp: 42, InstanceID: "node-a"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}

func TestGetMissingSlot(t *testing.T) {
	storage, mock := newMachineStorage(t)

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-z").
		WillReturnRows(sqlmock.NewRows(machineCols))

	if _, ok, err := storage.Get(context.Background(), "order", "node-z"); ok || err != nil {
		t.Fatalf("ok = %v, err = %v", ok, err)
	}
}

func TestNewMachineStorageValidates(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := database.NewMachineStorage(nil, "order"); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("nil db err = %v", err)
	}
	if _, err := database.NewMachineStorage(db, ""); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
}

// The reclaim window is what lets a dead instance's slot be reused; it must be
// derived from the safe guard, not a hard-coded zero.
func TestSafeGuardWindow(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	storage, err := database.NewMachineStorage(db, "order",
		database.WithMachineSafeGuard(time.Minute),
		database.WithMachineClock(func() int64 { return 1_000_000 }))
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = \\? AND instance_id = \\?").
		WithArgs("order", "node-e").
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT machine_id, last_timestamp FROM cosid_machine").
		WithArgs("order", int64(1_000_000-60_000)).
		WillReturnRows(sqlmock.NewRows(machineCols))
	mock.ExpectQuery("SELECT MAX\\(machine_id\\) \\+ 1").
		WillReturnRows(sqlmock.NewRows([]string{"next"}).AddRow(int64(1)))
	mock.ExpectExec("INSERT INTO cosid_machine").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if _, err := storage.Allocate(context.Background(), "order", "node-e", 10); err != nil {
		t.Fatalf("allocate: %v", err)
	}
}
