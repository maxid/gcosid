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

// MachineStorage implements gcosid.MachineStateStorage on the CosID
// `cosid_machine` table:
//
//	cosid_machine(name PK, namespace, machine_id, last_timestamp, instance_id,
//	              distribute_time, revert_time)
//
// name is `{namespace}.{machine_id}`. A slot with an empty instance_id, or one
// whose last_timestamp is older than the safe-guard window, is considered
// released and may be handed to another instance.
type MachineStorage struct {
	db        *sql.DB
	namespace string
	// safeGuard is how long after an instance's last heartbeat its slot is
	// considered reclaimable.
	safeGuard time.Duration
	// stable disables reclaiming entirely, like an InstanceId with stable=true.
	stable bool
	now    func() int64
}

// MachineStorageOption configures MachineStorage.
type MachineStorageOption func(*MachineStorage)

// WithMachineSafeGuard sets the reclaim window (default: 10 minutes).
func WithMachineSafeGuard(d time.Duration) MachineStorageOption {
	return func(m *MachineStorage) {
		if d > 0 {
			m.safeGuard = d
		}
	}
}

// WithMachineStable marks this instance as stable (for example a StatefulSet
// pod). CosID then never reclaims slots by heartbeat age: safeGuardAt is 0, so
// only an explicitly released slot can be reused.
func WithMachineStable(stable bool) MachineStorageOption {
	return func(m *MachineStorage) { m.stable = stable }
}

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
		db:        db,
		namespace: namespace,
		safeGuard: 10 * time.Minute,
		now:       func() int64 { return time.Now().UnixMilli() },
	}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Allocate implements gcosid.MachineStateStorage. It keeps an existing slot for
// the instance, otherwise claims a released one, otherwise appends a new id.
func (m *MachineStorage) Allocate(ctx context.Context, namespace, instanceID string, maxMachineID int64) (gcosid.MachineState, error) {
	if state, ok, err := m.Get(ctx, namespace, instanceID); err != nil {
		return gcosid.MachineState{}, err
	} else if ok {
		return state, nil
	}

	now := m.now()
	safeGuardAt := m.safeGuardAt(now)

	// Claim a released slot: empty owner or stale heartbeat.
	machineID, lastTimestamp, ok, err := m.claimReleased(ctx, namespace, instanceID, now, safeGuardAt)
	if err != nil {
		return gcosid.MachineState{}, err
	}
	if ok {
		return gcosid.MachineState{MachineID: machineID, LastTimestamp: lastTimestamp, InstanceID: instanceID}, nil
	}

	// No released slot: append a new machine id.
	newID, err := m.nextMachineID(ctx, namespace)
	if err != nil {
		return gcosid.MachineState{}, err
	}
	if newID > maxMachineID {
		return gcosid.MachineState{}, fmt.Errorf("%w: namespace %s exhausted at %d", gcosid.ErrExhausted, namespace, newID)
	}
	_, err = m.db.ExecContext(ctx,
		`INSERT INTO cosid_machine (name, namespace, machine_id, last_timestamp, instance_id, distribute_time, revert_time)
		 VALUES (?, ?, ?, ?, ?, ?, 0)`,
		m.key(namespace, newID), namespace, newID, now, instanceID, now)
	if err != nil {
		// Another instance may have taken the id between SELECT and INSERT. The
		// adapter cannot inspect driver-specific duplicate-key codes without
		// depending on the driver, so it reports the insert as retryable: the
		// next Allocate sees the row and reuses or skips it.
		return gcosid.MachineState{}, fmt.Errorf("%w: machine insert: %v", gcosid.ErrAllocatorUnavailable, err)
	}
	return gcosid.MachineState{MachineID: newID, LastTimestamp: now, InstanceID: instanceID}, nil
}

func (m *MachineStorage) claimReleased(ctx context.Context, namespace, instanceID string, now, safeGuardAt int64) (int64, int64, bool, error) {
	row := m.db.QueryRowContext(ctx,
		`SELECT machine_id, last_timestamp FROM cosid_machine
		 WHERE namespace = ? AND (instance_id = '' OR last_timestamp <= ?)
		 ORDER BY machine_id LIMIT 1`, namespace, safeGuardAt)

	var machineID, lastTimestamp int64
	if err := row.Scan(&machineID, &lastTimestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("gcosid/database: machine select: %w", err)
	}

	res, err := m.db.ExecContext(ctx,
		`UPDATE cosid_machine SET instance_id = ?, last_timestamp = ?, distribute_time = ?
		 WHERE name = ? AND (instance_id = '' OR last_timestamp <= ?)`,
		instanceID, now, now, m.key(namespace, machineID), safeGuardAt)
	if err != nil {
		return 0, 0, false, fmt.Errorf("gcosid/database: machine claim: %w", err)
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		// Lost the race: the caller retries on the next Distribute call.
		return 0, 0, false, nil
	}
	return machineID, now, true, nil
}

// safeGuardAt mirrors MachineIdDistributor.getSafeGuardAt: a stable instance,
// or a "forever" guard, yields 0 so no slot is ever considered stale.
func (m *MachineStorage) safeGuardAt(now int64) int64 {
	if m.stable || m.safeGuard <= 0 {
		return 0
	}
	at := now - m.safeGuard.Milliseconds()
	if at < 0 {
		return 0
	}
	return at
}

func (m *MachineStorage) nextMachineID(ctx context.Context, namespace string) (int64, error) {
	var next sql.NullInt64
	if err := m.db.QueryRowContext(ctx,
		`SELECT MAX(machine_id) + 1 FROM cosid_machine WHERE namespace = ?`, namespace).Scan(&next); err != nil {
		return 0, fmt.Errorf("gcosid/database: next machine id: %w", err)
	}
	if !next.Valid {
		return 1, nil
	}
	return next.Int64, nil
}

// Update implements gcosid.MachineStateStorage (CosID's GUARD_MACHINE_STATE).
func (m *MachineStorage) Update(ctx context.Context, namespace string, state gcosid.MachineState) error {
	_, err := m.db.ExecContext(ctx,
		`UPDATE cosid_machine SET last_timestamp = ? WHERE namespace = ? AND instance_id = ? AND machine_id = ?`,
		state.LastTimestamp, namespace, state.InstanceID, state.MachineID)
	if err != nil {
		return fmt.Errorf("gcosid/database: machine update: %w", err)
	}
	return nil
}

// Get implements gcosid.MachineStateStorage.
func (m *MachineStorage) Get(ctx context.Context, namespace, instanceID string) (gcosid.MachineState, bool, error) {
	var machineID, lastTimestamp int64
	err := m.db.QueryRowContext(ctx,
		`SELECT machine_id, last_timestamp FROM cosid_machine WHERE namespace = ? AND instance_id = ?`,
		namespace, instanceID).Scan(&machineID, &lastTimestamp)
	if errors.Is(err, sql.ErrNoRows) {
		return gcosid.MachineState{}, false, nil
	}
	if err != nil {
		return gcosid.MachineState{}, false, fmt.Errorf("gcosid/database: machine get: %w", err)
	}
	return gcosid.MachineState{MachineID: machineID, LastTimestamp: lastTimestamp, InstanceID: instanceID}, true, nil
}

// key is CosID's namespacedMachineId: the machine id is zero-padded to 8 digits.
// The padded form is the primary key of cosid_machine, so an unpadded name would
// address a different row — two runtimes sharing the database would then hand
// out the same machine id.
func (m *MachineStorage) key(namespace string, machineID int64) string {
	return namespace + "." + strings.Repeat("0", max(0, 8-len(strconv.FormatInt(machineID, 10)))) + strconv.FormatInt(machineID, 10)
}
