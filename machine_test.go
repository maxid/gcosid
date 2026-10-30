package gcosid

import (
	"context"
	"errors"
	"testing"
)

func TestManualDistributorReturnsTheConfiguredID(t *testing.T) {
	d := &ManualDistributor{MachineID: 7, Instance: "node-a"}
	id, err := d.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if id != 7 {
		t.Fatalf("machine id = %d, want 7", id)
	}
	if d.InstanceID() != "node-a" {
		t.Fatalf("instance = %q", d.InstanceID())
	}
	if err := d.Revert(context.Background()); err != nil {
		t.Fatalf("revert: %v", err)
	}
}

func TestManualDistributorValidates(t *testing.T) {
	d := &ManualDistributor{MachineID: 99, Validate: func(int64) error { return ErrInvalidMachineID }}
	if _, err := d.Distribute(context.Background(), "order"); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("err = %v, want ErrInvalidMachineID", err)
	}

	if _, err := (&ManualDistributor{MachineID: -1}).Distribute(context.Background(), "order"); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("negative err = %v", err)
	}
}

// Two instances must never share a slot: a duplicate machine id silently
// produces duplicate snowflakes.
func TestLeaseDistributorGivesDistinctSlots(t *testing.T) {
	storage := NewMemoryMachineStorage()
	maxID := int64(4)

	a, err := NewLeaseDistributor(storage, "node-a", maxID)
	if err != nil {
		t.Fatalf("distributor: %v", err)
	}
	b, _ := NewLeaseDistributor(storage, "node-b", maxID)

	aID, err := a.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("a distribute: %v", err)
	}
	bID, err := b.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("b distribute: %v", err)
	}
	if aID == bID {
		t.Fatalf("both instances got machine id %d", aID)
	}

	// Re-distribution for the same instance is stable: a restart keeps its slot.
	again, _ := a.Distribute(context.Background(), "order")
	if again != aID {
		t.Fatalf("restart changed machine id %d -> %d", aID, again)
	}
}

func TestLeaseDistributorExhausts(t *testing.T) {
	storage := NewMemoryMachineStorage()
	for i := 0; i < 2; i++ {
		d, _ := NewLeaseDistributor(storage, string(rune('a'+i)), 2)
		if _, err := d.Distribute(context.Background(), "order"); err != nil {
			t.Fatalf("distribute %d: %v", i, err)
		}
	}

	d, _ := NewLeaseDistributor(storage, "c", 2)
	if _, err := d.Distribute(context.Background(), "order"); !errors.Is(err, ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
}

func TestMachineStateUpdateRequiresAllocatedSlot(t *testing.T) {
	storage := NewMemoryMachineStorage()
	ctx := context.Background()

	if _, err := storage.Allocate(ctx, "order", "node-a", 10); err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if err := storage.Update(ctx, "order", MachineState{MachineID: 1, InstanceID: "node-a", LastTimestamp: 42}); err != nil {
		t.Fatalf("update: %v", err)
	}
	state, ok, err := storage.Get(ctx, "order", "node-a")
	if err != nil || !ok {
		t.Fatalf("get = %+v, %v, %v", state, ok, err)
	}
	if state.LastTimestamp != 42 {
		t.Fatalf("state = %+v", state)
	}

	if err := storage.Update(ctx, "order", MachineState{MachineID: 9, InstanceID: "unknown"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown instance err = %v, want ErrInvalidConfig", err)
	}
}

func TestNewLeaseDistributorRejectsBadConfig(t *testing.T) {
	if _, err := NewLeaseDistributor(nil, "node", 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil storage err = %v", err)
	}
	if _, err := NewLeaseDistributor(NewMemoryMachineStorage(), "", 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty instance err = %v", err)
	}
	if _, err := NewLeaseDistributor(NewMemoryMachineStorage(), "node", 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero max err = %v", err)
	}
}
