package gcosid

import (
	"context"
	"fmt"
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

// MachineStateStorage persists machine slots. Implementations must make
// Allocate atomic: two instances asking at the same time must never receive the
// same machine id.
type MachineStateStorage interface {
	// Allocate returns the slot for instanceID, creating or reusing it.
	Allocate(ctx context.Context, namespace, instanceID string, maxMachineID int64) (MachineState, error)
	// Update persists the last timestamp handed out for the slot.
	Update(ctx context.Context, namespace string, state MachineState) error
	// Get reads the current slot of instanceID.
	Get(ctx context.Context, namespace, instanceID string) (MachineState, bool, error)
}

// ManualDistributor uses a configured machine id, which is what single-host and
// explicit-container deployments use.
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

// MemoryMachineStorage is an in-process MachineStateStorage for tests and
// single-instance deployments. It is NOT safe across processes: production uses
// the JDBC or Redis adapter, where the assignment is atomic.
type MemoryMachineStorage struct {
	mu    sync.Mutex
	next  int64
	slots map[string]MachineState // key: namespace|instance
}

// NewMemoryMachineStorage creates an empty store.
func NewMemoryMachineStorage() *MemoryMachineStorage {
	return &MemoryMachineStorage{slots: map[string]MachineState{}}
}

// Allocate implements MachineStateStorage.
func (m *MemoryMachineStorage) Allocate(_ context.Context, namespace, instanceID string, maxMachineID int64) (MachineState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := namespace + "|" + instanceID
	if state, ok := m.slots[key]; ok {
		return state, nil
	}
	if m.next >= maxMachineID {
		return MachineState{}, fmt.Errorf("%w: namespace %s exhausted", ErrExhausted, namespace)
	}
	m.next++
	state := MachineState{MachineID: m.next, InstanceID: instanceID}
	m.slots[key] = state
	return state, nil
}

// Update implements MachineStateStorage.
func (m *MemoryMachineStorage) Update(_ context.Context, namespace string, state MachineState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := namespace + "|" + state.InstanceID
	if _, ok := m.slots[key]; !ok {
		return fmt.Errorf("%w: no slot for instance %s", ErrInvalidConfig, state.InstanceID)
	}
	m.slots[key] = state
	return nil
}

// Get implements MachineStateStorage.
func (m *MemoryMachineStorage) Get(_ context.Context, namespace, instanceID string) (MachineState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.slots[namespace+"|"+instanceID]
	return state, ok, nil
}

// LeaseDistributor obtains the machine id from a state store.
type LeaseDistributor struct {
	storage    MachineStateStorage
	instanceID string
	maxID      int64
	namespace  string
}

// NewLeaseDistributor creates a distributor backed by a state store. maxID is
// the layout's machine-id ceiling.
func NewLeaseDistributor(storage MachineStateStorage, instanceID string, maxMachineID int64) (*LeaseDistributor, error) {
	if storage == nil {
		return nil, fmt.Errorf("%w: machine state storage is required", ErrInvalidConfig)
	}
	if instanceID == "" {
		return nil, fmt.Errorf("%w: instance id is required", ErrInvalidConfig)
	}
	if maxMachineID <= 0 {
		return nil, fmt.Errorf("%w: max machine id must be positive", ErrInvalidConfig)
	}
	return &LeaseDistributor{storage: storage, instanceID: instanceID, maxID: maxMachineID}, nil
}

// Distribute implements MachineIdDistributor.
func (l *LeaseDistributor) Distribute(ctx context.Context, namespace string) (int64, error) {
	state, err := l.storage.Allocate(ctx, namespace, l.instanceID, l.maxID)
	if err != nil {
		return 0, err
	}
	l.namespace = namespace
	return state.MachineID, nil
}

// Revert releases nothing implicitly: the slot stays reserved for this instance
// so a restart reuses the same machine id instead of racing for a new one.
func (l *LeaseDistributor) Revert(context.Context) error { return nil }

// Guard implements MachineIdDistributor: refresh the slot's last_timestamp so the
// safe-guard window does not expire. A missing slot means the lease is gone.
func (l *LeaseDistributor) Guard(ctx context.Context, namespace string) error {
	if l.namespace != "" && namespace == "" {
		namespace = l.namespace
	}
	state, ok, err := l.storage.Get(ctx, namespace, l.instanceID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: no machine slot for instance %s", ErrInvalidMachineID, l.instanceID)
	}
	state.LastTimestamp = time.Now().UnixMilli()
	if err := l.storage.Update(ctx, namespace, state); err != nil {
		return err
	}
	return nil
}

// InstanceID implements MachineIdDistributor.
func (l *LeaseDistributor) InstanceID() string { return l.instanceID }
