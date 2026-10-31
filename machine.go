package gcosid

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// MachineState is the leased machine slot CosID keeps in `cosid_machine`
// (columns name/namespace/machine_id/last_timestamp/instance_id).
type MachineState struct {
	MachineID     int64
	LastTimestamp int64
	InstanceID    string
}

// SafeGuardAt mirrors MachineIdDistributor.getSafeGuardAt: a stable instance or a
// non-positive guard yields 0, which makes the reclaim predicate
// `last_timestamp <= 0` — i.e. nothing is ever reclaimed by age.
func SafeGuardAt(now int64, safeGuard time.Duration, stable bool) int64 {
	if stable || safeGuard <= 0 {
		return 0
	}
	at := now - safeGuard.Milliseconds()
	if at < 0 {
		return 0
	}
	return at
}

// ManualDistributor uses a configured machine id, which is what single-host and
// explicit-container deployments use — CosID's ManualMachineIdDistributor.
type ManualDistributor struct {
	MachineID int64
	Instance  string
	Validate  func(machineID int64) error
}

// Distribute implements MachineIdDistributor.
func (m *ManualDistributor) Distribute(_ context.Context, _ string) (int64, error) {
	if m.Validate != nil {
		if err := m.Validate(m.MachineID); err != nil {
			return 0, err
		}
	}
	if m.MachineID < 0 {
		return 0, fmt.Errorf("%w: %d", ErrInvalidMachineID, m.MachineID)
	}
	return m.MachineID, nil
}

// Revert implements MachineIdDistributor: a manual id has nothing to release.
func (m *ManualDistributor) Revert(context.Context) error { return nil }

// Guard implements MachineIdDistributor: a manual id is configured, not leased,
// so there is nothing to renew.
func (m *ManualDistributor) Guard(context.Context, string) error { return nil }

// InstanceID implements MachineIdDistributor.
func (m *ManualDistributor) InstanceID() string { return m.Instance }

// MachineStateStorage is the persistent side of machine-id allocation: the three
// operations CosID's JDBC and Redis distributors perform. Each implementation
// must run the statements of its CosID counterpart verbatim, because a Go process
// and a Java process share the same table (or the same Redis keys).
type MachineStateStorage interface {
	// Distribute returns this instance's machine id for the namespace. The
	// implementation follows CosID's order: reuse an own live slot, take a
	// released slot, otherwise append a new id.
	Distribute(ctx context.Context, namespace, instanceID string, maxMachineID int64, safeGuard time.Duration, stable bool) (MachineState, error)
	// Guard refreshes the slot's last_timestamp. A slot that no longer belongs
	// to the instance is an error, never a silent success.
	Guard(ctx context.Context, namespace string, state MachineState, safeGuard time.Duration, stable bool) error
	// Revert releases the slot: CosID clears instance_id for unstable instances
	// and keeps it for stable ones.
	Revert(ctx context.Context, namespace, instanceID string, state MachineState, stable bool) error
}

// MachineIdDistributor hands a snowflake generator its machine id.
//
// CosID never derives the machine id from a hostname or a random value: the id
// must come from explicit configuration or a leased slot, because a duplicate
// machine id silently produces duplicate snowflakes.
type MachineIdDistributor interface {
	// Distribute returns this instance's machine id for the namespace.
	Distribute(ctx context.Context, namespace string) (int64, error)
	// Revert releases the slot on shutdown.
	Revert(ctx context.Context) error
	// Guard renews the lease on the slot. Returning an error means the slot may
	// already belong to another instance.
	Guard(ctx context.Context, namespace string) error
	// InstanceID identifies this process in the state store.
	InstanceID() string
}

// MemoryMachineStorage is an in-process MachineStateStorage for tests and
// single-instance deployments. It mimics the `cosid_machine` table: slots are
// keyed by padded name, and a released slot (empty instance id or a stale
// heartbeat) is reusable. It is NOT safe across processes.
type MemoryMachineStorage struct {
	mu    sync.Mutex
	slots map[string]MachineState // key: namespace|machineID
	next  map[string]int64        // namespace -> last allocated machine id (-1 = none)
}

// NewMemoryMachineStorage creates an empty store.
func NewMemoryMachineStorage() *MemoryMachineStorage {
	return &MemoryMachineStorage{slots: map[string]MachineState{}, next: map[string]int64{}}
}

// Distribute implements MachineStateStorage with the same order as CosID.
func (m *MemoryMachineStorage) Distribute(_ context.Context, namespace, instanceID string, maxMachineID int64, safeGuard time.Duration, stable bool) (MachineState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UnixMilli()
	safeGuardAt := SafeGuardAt(now, safeGuard, stable)

	// 1. Own live slot.
	for _, state := range m.slots {
		if state.InstanceID == instanceID && state.LastTimestamp > safeGuardAt {
			return state, nil
		}
	}
	// 2. Released slot (empty owner or stale heartbeat), lowest id first.
	var released bool
	var best MachineState
	for _, state := range m.slots {
		if state.InstanceID == "" || state.LastTimestamp <= safeGuardAt {
			if !released || state.MachineID < best.MachineID {
				best, released = state, true
			}
		}
	}
	if released {
		// CosID returns the previous timestamp; the row itself gets `now`.
		claimed := MachineState{MachineID: best.MachineID, LastTimestamp: best.LastTimestamp, InstanceID: instanceID}
		m.put(namespace, MachineState{MachineID: best.MachineID, LastTimestamp: now, InstanceID: instanceID})
		return claimed, nil
	}
	// 3. Append a new id; CosID's `select max(machine_id)+1` over an empty table
	// yields NULL, and `getInt` turns that into 0.
	next := m.next[namespace]
	if _, ok := m.next[namespace]; !ok {
		next = -1
	}
	if next < 0 {
		// Distinguish "no rows yet" from "rows starting at 0": use the highest
		// known id instead of a separate counter when slots exist.
		highest := int64(-1)
		for _, state := range m.slots {
			if state.MachineID > highest {
				highest = state.MachineID
			}
		}
		next = highest
	}
	candidate := next + 1
	if candidate > maxMachineID {
		return MachineState{}, fmt.Errorf("%w: namespace %s exhausted", ErrExhausted, namespace)
	}
	state := MachineState{MachineID: candidate, LastTimestamp: now, InstanceID: instanceID}
	m.put(namespace, state)
	m.next[namespace] = candidate
	return state, nil
}

// Guard implements MachineStateStorage.
func (m *MemoryMachineStorage) Guard(_ context.Context, namespace string, state MachineState, _ time.Duration, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.slots[namespace+"|"+strconv.FormatInt(state.MachineID, 10)]
	if !ok || current.InstanceID != state.InstanceID {
		return fmt.Errorf("%w: slot %d of %s is no longer owned by %s", ErrInvalidMachineID, state.MachineID, namespace, state.InstanceID)
	}
	current.LastTimestamp = time.Now().UnixMilli()
	m.put(namespace, current)
	return nil
}

// Revert implements MachineStateStorage: unstable instances clear the owner so the
// slot is immediately reusable; stable instances keep it.
func (m *MemoryMachineStorage) Revert(_ context.Context, namespace, instanceID string, state MachineState, stable bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := namespace + "|" + strconv.FormatInt(state.MachineID, 10)
	current, ok := m.slots[key]
	if !ok || current.InstanceID != instanceID {
		return nil
	}
	owner := ""
	if stable {
		owner = instanceID
	}
	current.InstanceID = owner
	m.slots[key] = current
	return nil
}

func (m *MemoryMachineStorage) put(namespace string, state MachineState) {
	m.slots[namespace+"|"+strconv.FormatInt(state.MachineID, 10)] = state
}

// LeaseDistributor obtains the machine id from a state store and caches it
// locally — CosID's AbstractMachineIdDistributor plus its MachineStateStorage
// cache. The cache is why a restart reuses the same machine id without a remote
// round trip.
type LeaseDistributor struct {
	storage    MachineStateStorage
	instanceID string
	maxID      int64
	stable     bool
	safeGuard  time.Duration

	mu    sync.Mutex
	local map[string]MachineState
}

// LeaseOption configures a LeaseDistributor.
type LeaseOption func(*LeaseDistributor)

// WithLeaseStable marks the instance stable: its slot is never reclaimed by age
// and revert keeps the instance id, matching InstanceId(stable=true).
func WithLeaseStable(stable bool) LeaseOption {
	return func(l *LeaseDistributor) { l.stable = stable }
}

// WithLeaseSafeGuard sets the reclaim window used by the store.
func WithLeaseSafeGuard(d time.Duration) LeaseOption {
	return func(l *LeaseDistributor) {
		if d > 0 {
			l.safeGuard = d
		}
	}
}

// NewLeaseDistributor creates a distributor backed by a state store. maxID is
// the layout's machine-id ceiling.
func NewLeaseDistributor(storage MachineStateStorage, instanceID string, maxMachineID int64, opts ...LeaseOption) (*LeaseDistributor, error) {
	if storage == nil {
		return nil, fmt.Errorf("%w: machine state storage is required", ErrInvalidConfig)
	}
	if instanceID == "" {
		return nil, fmt.Errorf("%w: instance id is required", ErrInvalidConfig)
	}
	if maxMachineID <= 0 {
		return nil, fmt.Errorf("%w: max machine id must be positive", ErrInvalidConfig)
	}
	l := &LeaseDistributor{
		storage:    storage,
		instanceID: instanceID,
		maxID:      maxMachineID,
		safeGuard:  10 * time.Minute,
		local:      map[string]MachineState{},
	}
	for _, o := range opts {
		o(l)
	}
	return l, nil
}

// Distribute implements MachineIdDistributor. A locally known slot wins, exactly
// like the local MachineStateStorage cache in CosID.
func (l *LeaseDistributor) Distribute(ctx context.Context, namespace string) (int64, error) {
	l.mu.Lock()
	if state, ok := l.local[namespace]; ok {
		l.mu.Unlock()
		return state.MachineID, nil
	}
	l.mu.Unlock()

	state, err := l.storage.Distribute(ctx, namespace, l.instanceID, l.maxID, l.safeGuard, l.stable)
	if err != nil {
		return 0, err
	}
	state.InstanceID = l.instanceID

	l.mu.Lock()
	l.local[namespace] = state
	l.mu.Unlock()
	return state.MachineID, nil
}

// Revert implements MachineIdDistributor: release the remote slot, keeping the
// local cache so a restart reuses the same id (CosID does not clear it either).
func (l *LeaseDistributor) Revert(ctx context.Context) error {
	l.mu.Lock()
	states := make(map[string]MachineState, len(l.local))
	for ns, state := range l.local {
		states[ns] = state
	}
	l.mu.Unlock()

	for namespace, state := range states {
		if err := l.storage.Revert(ctx, namespace, l.instanceID, state, l.stable); err != nil {
			return err
		}
	}
	return nil
}

// Guard implements MachineIdDistributor: refresh every known slot. A slot the
// store no longer recognises is reported, because another instance may own it.
func (l *LeaseDistributor) Guard(ctx context.Context, namespace string) error {
	l.mu.Lock()
	state, ok := l.local[namespace]
	if !ok {
		l.mu.Unlock()
		return fmt.Errorf("%w: no machine slot for instance %s in %s", ErrInvalidMachineID, l.instanceID, namespace)
	}
	l.mu.Unlock()

	return l.storage.Guard(ctx, namespace, state, l.safeGuard, l.stable)
}

// InstanceID implements MachineIdDistributor.
func (l *LeaseDistributor) InstanceID() string { return l.instanceID }
