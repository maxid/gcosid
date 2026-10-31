package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/maxid/gcosid"
)

// The statements below are CosID 2.15.1's JdbcMachineIdDistributor verbatim.
// They are not an approximation: a Go process and a Java process share the
// `cosid_machine` table, so a different predicate, a different timestamp or a
// different name would let both take the same machine id.
const (
	getMachineStateSQL = `select machine_id, last_timestamp from cosid_machine
		where namespace=? and instance_id=? and last_timestamp>?`

	getRevertMachineStateSQL = `select machine_id, last_timestamp from cosid_machine
		where namespace=? and (instance_id='' or last_timestamp<=?)`

	distributeRevertMachineStateSQL = `update cosid_machine
		set instance_id=?,last_timestamp=?,distribute_time=?
		where name=? and (instance_id='' or last_timestamp<=?)`

	nextMachineIDSQL = `select max(machine_id)+1 from cosid_machine where namespace=?`

	distributeMachineSQL = `insert into cosid_machine
		(name, namespace, machine_id, last_timestamp, instance_id, distribute_time, revert_time)
		values (?,?,?,?,?,?,0)`

	revertMachineStateSQL = `update cosid_machine
		set instance_id=?,last_timestamp=?,revert_time=?
		where namespace=? and instance_id=?`

	guardMachineStateSQL = `update cosid_machine
		set last_timestamp=?
		where namespace=? and instance_id=? and machine_id=?`
)

// MachineStorage implements gcosid.MachineStateStorage on `cosid_machine`.
type MachineStorage struct {
	db        *sql.DB
	namespace string
	now       func() int64
	// maxDistributeRetries bounds the duplicate-key retry CosID performs by
	// recursing in distributeMachine.
	maxDistributeRetries int
}

// MachineStorageOption configures MachineStorage.
type MachineStorageOption func(*MachineStorage)

// WithMachineClock overrides the timestamp source (tests).
func WithMachineClock(now func() int64) MachineStorageOption {
	return func(m *MachineStorage) {
		if now != nil {
			m.now = now
		}
	}
}

// NewMachineStorage creates a storage bound to a namespace.
func NewMachineStorage(db *sql.DB, namespace string, opts ...MachineStorageOption) (*MachineStorage, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db is required", gcosid.ErrInvalidConfig)
	}
	if namespace == "" {
		return nil, fmt.Errorf("%w: namespace is required", gcosid.ErrInvalidConfig)
	}
	m := &MachineStorage{
		db:                   db,
		namespace:            namespace,
		now:                  func() int64 { return time.Now().UnixMilli() },
		maxDistributeRetries: 32,
	}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Distribute implements gcosid.MachineStateStorage with CosID's exact order:
// distributeBySelf, then distributeByRevert, then distributeMachine.
func (m *MachineStorage) Distribute(ctx context.Context, namespace, instanceID string, maxMachineID int64, safeGuard time.Duration, stable bool) (gcosid.MachineState, error) {
	if namespace == "" {
		namespace = m.namespace
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return gcosid.MachineState{}, fmt.Errorf("gcosid/database: machine begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if state, ok, err := m.distributeBySelf(ctx, tx, namespace, instanceID, safeGuard, stable); err != nil {
		return gcosid.MachineState{}, err
	} else if ok {
		if err := tx.Commit(); err != nil {
			return gcosid.MachineState{}, fmt.Errorf("gcosid/database: machine commit: %w", err)
		}
		return state, nil
	}

	if state, ok, err := m.distributeByRevert(ctx, tx, namespace, instanceID, safeGuard, stable); err != nil {
		return gcosid.MachineState{}, err
	} else if ok {
		if err := tx.Commit(); err != nil {
			return gcosid.MachineState{}, fmt.Errorf("gcosid/database: machine commit: %w", err)
		}
		return state, nil
	}

	state, err := m.distributeMachine(ctx, tx, namespace, instanceID, maxMachineID, 0)
	if err != nil {
		return gcosid.MachineState{}, err
	}
	if err := tx.Commit(); err != nil {
		return gcosid.MachineState{}, fmt.Errorf("gcosid/database: machine commit: %w", err)
	}
	return state, nil
}

func (m *MachineStorage) distributeBySelf(ctx context.Context, tx *sql.Tx, namespace, instanceID string, safeGuard time.Duration, stable bool) (gcosid.MachineState, bool, error) {
	var machineID, lastTimestamp int64
	err := tx.QueryRowContext(ctx, getMachineStateSQL, namespace, instanceID,
		gcosid.SafeGuardAt(m.now(), safeGuard, stable)).Scan(&machineID, &lastTimestamp)
	if errors.Is(err, sql.ErrNoRows) {
		return gcosid.MachineState{}, false, nil
	}
	if err != nil {
		return gcosid.MachineState{}, false, fmt.Errorf("gcosid/database: machine self: %w", err)
	}

	// CosID stamps the slot with now and returns that state.
	state := gcosid.MachineState{MachineID: machineID, LastTimestamp: m.now(), InstanceID: instanceID}
	if err := m.guardTx(ctx, tx, namespace, state); err != nil {
		return gcosid.MachineState{}, false, err
	}
	return state, true, nil
}

func (m *MachineStorage) distributeByRevert(ctx context.Context, tx *sql.Tx, namespace, instanceID string, safeGuard time.Duration, stable bool) (gcosid.MachineState, bool, error) {
	var machineID, lastTimestamp int64
	err := tx.QueryRowContext(ctx, getRevertMachineStateSQL, namespace,
		gcosid.SafeGuardAt(m.now(), safeGuard, stable)).Scan(&machineID, &lastTimestamp)
	if errors.Is(err, sql.ErrNoRows) {
		return gcosid.MachineState{}, false, nil
	}
	if err != nil {
		return gcosid.MachineState{}, false, fmt.Errorf("gcosid/database: machine revert select: %w", err)
	}

	now := m.now()
	res, err := tx.ExecContext(ctx, distributeRevertMachineStateSQL, instanceID, now, now,
		m.key(namespace, machineID), gcosid.SafeGuardAt(now, safeGuard, stable))
	if err != nil {
		return gcosid.MachineState{}, false, fmt.Errorf("gcosid/database: machine revert claim: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil || affected == 0 {
		return gcosid.MachineState{}, false, nil
	}
	// CosID returns the *previous* timestamp it read, not the new one.
	return gcosid.MachineState{MachineID: machineID, LastTimestamp: lastTimestamp, InstanceID: instanceID}, true, nil
}

func (m *MachineStorage) distributeMachine(ctx context.Context, tx *sql.Tx, namespace, instanceID string, maxMachineID int64, attempt int) (gcosid.MachineState, error) {
	var next sql.NullInt64
	if err := tx.QueryRowContext(ctx, nextMachineIDSQL, namespace).Scan(&next); err != nil {
		return gcosid.MachineState{}, fmt.Errorf("gcosid/database: machine next id: %w", err)
	}
	// `max(machine_id)+1` over an empty table is NULL, and CosID's getInt turns
	// NULL into 0 — the first machine id is 0, not 1.
	machineID := next.Int64

	if machineID > maxMachineID {
		return gcosid.MachineState{}, fmt.Errorf("%w: namespace %s exhausted at %d", gcosid.ErrExhausted, namespace, machineID)
	}

	now := m.now()
	_, err := tx.ExecContext(ctx, distributeMachineSQL,
		m.key(namespace, machineID), namespace, machineID, now, instanceID, now)
	if err == nil {
		return gcosid.MachineState{MachineID: machineID, LastTimestamp: now, InstanceID: instanceID}, nil
	}

	// Another instance took the id between the SELECT and the INSERT: CosID
	// retries the whole distribution.
	if attempt >= m.maxDistributeRetries {
		return gcosid.MachineState{}, fmt.Errorf("%w: machine insert retries exhausted: %v", gcosid.ErrAllocatorUnavailable, err)
	}
	return m.distributeMachine(ctx, tx, namespace, instanceID, maxMachineID, attempt+1)
}

// Guard implements gcosid.MachineStateStorage: one UPDATE, and zero affected rows
// means the lease is gone (CosID's MachineIdLostException).
func (m *MachineStorage) Guard(ctx context.Context, namespace string, state gcosid.MachineState, _ time.Duration, _ bool) error {
	if namespace == "" {
		namespace = m.namespace
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gcosid/database: machine guard begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := m.guardTx(ctx, tx, namespace, state); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gcosid/database: machine guard commit: %w", err)
	}
	return nil
}

func (m *MachineStorage) guardTx(ctx context.Context, tx *sql.Tx, namespace string, state gcosid.MachineState) error {
	res, err := tx.ExecContext(ctx, guardMachineStateSQL, state.LastTimestamp, namespace, state.InstanceID, state.MachineID)
	if err != nil {
		return fmt.Errorf("gcosid/database: machine guard: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("gcosid/database: machine guard rows: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: machine slot %d of %s is no longer owned by %s",
			gcosid.ErrInvalidMachineID, state.MachineID, namespace, state.InstanceID)
	}
	return nil
}

// Revert implements gcosid.MachineStateStorage. Unstable instances clear the
// owner so the slot is immediately reusable; stable instances keep it.
func (m *MachineStorage) Revert(ctx context.Context, namespace, instanceID string, state gcosid.MachineState, stable bool) error {
	if namespace == "" {
		namespace = m.namespace
	}
	owner := ""
	if stable {
		owner = instanceID
	}
	_, err := m.db.ExecContext(ctx, revertMachineStateSQL, owner, state.LastTimestamp, m.now(), namespace, instanceID)
	if err != nil {
		return fmt.Errorf("gcosid/database: machine revert: %w", err)
	}
	return nil
}

// key is CosID's namespacedMachineId: the machine id zero-padded to 8 digits. The
// padded form is the primary key of cosid_machine, so an unpadded name addresses
// a different row.
func (m *MachineStorage) key(namespace string, machineID int64) string {
	raw := strconv.FormatInt(machineID, 10)
	if len(raw) >= 8 {
		return namespace + "." + raw
	}
	return namespace + "." + strings.Repeat("0", 8-len(raw)) + raw
}
