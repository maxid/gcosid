package gcosid

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
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
// produces duplicate snowflakes. CosID's first machine id is 0 (max()+1 over an
// empty table is NULL, and getInt turns NULL into 0).
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
	if aID != 0 {
		t.Fatalf("first machine id = %d, want 0", aID)
	}
	bID, err := b.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("b distribute: %v", err)
	}
	if bID != 1 {
		t.Fatalf("second machine id = %d, want 1", bID)
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
	for i := int64(0); i < 2; i++ {
		d, _ := NewLeaseDistributor(storage, fmt.Sprintf("node-%d", i), 1)
		if _, err := d.Distribute(context.Background(), "order"); err != nil {
			t.Fatalf("distribute %d: %v", i, err)
		}
	}

	d, _ := NewLeaseDistributor(storage, "node-c", 1)
	if _, err := d.Distribute(context.Background(), "order"); !errors.Is(err, ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
}

// Revert releases the slot for an unstable instance so the next process can take
// it; a stable instance keeps its id (CosID's revertRemote).
func TestLeaseDistributorRevertReleasesUnstableSlots(t *testing.T) {
	storage := NewMemoryMachineStorage()

	first, _ := NewLeaseDistributor(storage, "node-a", 4)
	if _, err := first.Distribute(context.Background(), "order"); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if err := first.Revert(context.Background()); err != nil {
		t.Fatalf("revert: %v", err)
	}

	// The released slot is reusable by a different instance.
	second, _ := NewLeaseDistributor(storage, "node-b", 4)
	id, err := second.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("distribute after revert: %v", err)
	}
	if id != 0 {
		t.Fatalf("machine id = %d, want the released slot 0", id)
	}
}

func TestLeaseDistributorStableKeepsItsSlot(t *testing.T) {
	storage := NewMemoryMachineStorage()

	stable, _ := NewLeaseDistributor(storage, "pod-0", 4, WithLeaseStable(true))
	if _, err := stable.Distribute(context.Background(), "order"); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if err := stable.Revert(context.Background()); err != nil {
		t.Fatalf("revert: %v", err)
	}

	// A stable revert keeps the owner, so another instance must take a new id.
	other, _ := NewLeaseDistributor(storage, "pod-1", 4)
	id, err := other.Distribute(context.Background(), "order")
	if err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if id == 0 {
		t.Fatalf("stable slot 0 was handed to another instance")
	}
}

func TestLeaseDistributorGuard(t *testing.T) {
	storage := NewMemoryMachineStorage()
	lease, _ := NewLeaseDistributor(storage, "node-a", 4)
	if _, err := lease.Distribute(context.Background(), "order"); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if err := lease.Guard(context.Background(), "order"); err != nil {
		t.Fatalf("guard: %v", err)
	}

	// An instance with no slot cannot guard one.
	orphan, _ := NewLeaseDistributor(storage, "node-z", 4)
	if err := orphan.Guard(context.Background(), "order"); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("orphan guard err = %v, want ErrInvalidMachineID", err)
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

func TestSafeGuardAt(t *testing.T) {
	now := int64(1_000_000)
	if got := SafeGuardAt(now, time.Minute, false); got != now-60_000 {
		t.Fatalf("safeGuardAt = %d, want %d", got, now-60_000)
	}
	if got := SafeGuardAt(now, time.Minute, true); got != 0 {
		t.Fatalf("stable safeGuardAt = %d, want 0", got)
	}
	if got := SafeGuardAt(now, 0, false); got != 0 {
		t.Fatalf("forever safeGuardAt = %d, want 0", got)
	}
}
