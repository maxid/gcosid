package gcosid

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// trackingDistributor records guard calls and can fail on demand. The counter is
// atomic because the heartbeat loop guards from its own goroutine.
type trackingDistributor struct {
	calls atomic.Int64
	err   error
}

func (d *trackingDistributor) Distribute(context.Context, string) (int64, error) { return 1, nil }
func (d *trackingDistributor) Revert(context.Context) error                      { return nil }
func (d *trackingDistributor) InstanceID() string                                { return "node-a" }
func (d *trackingDistributor) Guard(context.Context, string) error {
	d.calls.Add(1)
	return d.err
}

func (d *trackingDistributor) callCount() int64 { return d.calls.Load() }

func TestGuarderRenewsRegisteredSlots(t *testing.T) {
	dist := &trackingDistributor{}
	guarder, err := NewGuarder(dist, time.Minute, WithGuardTiming(time.Hour, time.Hour))
	if err != nil {
		t.Fatalf("guarder: %v", err)
	}

	instance := InstanceID{ID: "node-a"}
	if err := guarder.Register("order", instance); err != nil {
		t.Fatalf("register: %v", err)
	}
	guarder.SafeGuard(context.Background())

	if dist.callCount() != 1 {
		t.Fatalf("guard calls = %d, want 1", dist.callCount())
	}
	states := guarder.States()
	state, ok := states[NamespacedInstanceID{Namespace: "order", Instance: instance}]
	if !ok || state.Failed() {
		t.Fatalf("state = %+v, ok=%v", state, ok)
	}
	if guarder.HasFailure() {
		t.Fatal("HasFailure after a successful guard")
	}
}

// A lost slot must surface: the machine id may already belong to another
// instance, so silently ignoring the error would risk duplicate ids.
func TestGuarderReportsFailure(t *testing.T) {
	dist := &trackingDistributor{err: errors.New("slot lost")}
	guarder, _ := NewGuarder(dist, time.Minute, WithGuardTiming(time.Hour, time.Hour))
	instance := InstanceID{ID: "node-a"}
	_ = guarder.Register("order", instance)

	guarder.SafeGuard(context.Background())

	if !guarder.HasFailure() {
		t.Fatal("guard failure was not recorded")
	}
	state := guarder.States()[NamespacedInstanceID{Namespace: "order", Instance: instance}]
	if !state.Failed() || state.Err == nil {
		t.Fatalf("state = %+v", state)
	}
}

func TestGuarderUnregisterStopsGuarding(t *testing.T) {
	dist := &trackingDistributor{}
	guarder, _ := NewGuarder(dist, time.Minute, WithGuardTiming(time.Hour, time.Hour))
	instance := InstanceID{ID: "node-a"}

	_ = guarder.Register("order", instance)
	guarder.Unregister("order", instance)
	guarder.SafeGuard(context.Background())

	if dist.callCount() != 0 {
		t.Fatalf("guard calls = %d after unregister, want 0", dist.callCount())
	}
}

func TestGuarderStartStop(t *testing.T) {
	dist := &trackingDistributor{}
	guarder, _ := NewGuarder(dist, time.Minute, WithGuardTiming(0, 5*time.Millisecond))
	_ = guarder.Register("order", InstanceID{ID: "node-a"})

	guarder.Start()
	if !guarder.Running() {
		t.Fatal("guarder did not start")
	}
	guarder.Start() // idempotent

	deadline := time.Now().Add(time.Second)
	for dist.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	guarder.Stop()
	if guarder.Running() {
		t.Fatal("guarder still running after stop")
	}
	if dist.callCount() == 0 {
		t.Fatal("the heartbeat loop never guarded")
	}
}

func TestGuarderValidatesConfig(t *testing.T) {
	if _, err := NewGuarder(nil, time.Minute); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil distributor err = %v", err)
	}
	guarder, _ := NewGuarder(&trackingDistributor{}, time.Minute)
	if err := guarder.Register("", InstanceID{ID: "x"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
}

func TestNoopGuarder(t *testing.T) {
	g := NoopGuarder{}
	g.Start()
	g.Stop()
	if g.Running() || g.HasFailure() {
		t.Fatal("noop guarder must be inert")
	}
	if len(g.States()) != 0 {
		t.Fatal("noop guarder must have no state")
	}
}

func TestLeaseDistributorGuardRefreshesTheSlot(t *testing.T) {
	storage := NewMemoryMachineStorage()
	lease, err := NewLeaseDistributor(storage, "node-a", 10)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if _, err := lease.Distribute(context.Background(), "order"); err != nil {
		t.Fatalf("distribute: %v", err)
	}

	if err := lease.Guard(context.Background(), "order"); err != nil {
		t.Fatalf("guard: %v", err)
	}
	state, ok, err := storage.Get(context.Background(), "order", "node-a")
	if err != nil || !ok {
		t.Fatalf("state = %+v, %v, %v", state, ok, err)
	}
	if state.LastTimestamp == 0 {
		t.Fatal("guard did not refresh the timestamp")
	}

	// An instance with no slot cannot guard.
	orphan, _ := NewLeaseDistributor(storage, "node-z", 10)
	if err := orphan.Guard(context.Background(), "order"); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("orphan guard err = %v, want ErrInvalidMachineID", err)
	}
}
