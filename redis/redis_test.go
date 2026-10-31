package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maxid/gcosid"
)

type fakeClient struct {
	incrKey  string
	incrStep int64
	incrVal  int64
	incrErr  error

	lastScript string
	lastKeys   []string
	lastArgs   []any
	reply      any
	evalErr    error
}

func (f *fakeClient) IncrBy(_ context.Context, key string, step int64) (int64, error) {
	f.incrKey, f.incrStep = key, step
	return f.incrVal, f.incrErr
}

func (f *fakeClient) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	f.lastScript, f.lastKeys, f.lastArgs = script, keys, args
	return f.reply, f.evalErr
}

func TestSegmentUsesTheCosIDKey(t *testing.T) {
	fake := &fakeClient{incrVal: 100}
	alloc, err := NewSegmentAllocator(fake, "app")
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}

	maxID, err := alloc.NextMaxId(context.Background(), "order", 100)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if maxID != 100 {
		t.Fatalf("maxID = %d, want 100", maxID)
	}
	// CosID 2.15.1: cosid:{namespace.name}:adder — the braces are a Redis hash
	// tag, so they are part of the key, not decoration.
	if fake.incrKey != "cosid:{app.order}:adder" {
		t.Fatalf("key = %q", fake.incrKey)
	}
	if fake.incrStep != 100 {
		t.Fatalf("step = %d", fake.incrStep)
	}
}

func TestSegmentErrorsAreRetryable(t *testing.T) {
	fake := &fakeClient{incrErr: errors.New("connection refused")}
	alloc, _ := NewSegmentAllocator(fake, "app")

	if _, err := alloc.NextMaxId(context.Background(), "order", 10); !errors.Is(err, gcosid.ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}
}

func TestMachineDistributeParsesTheScriptReply(t *testing.T) {
	fake := &fakeClient{reply: []any{int64(3), int64(1000)}}
	// A realistic millisecond clock: CosID clamps a negative safe-guard to 0.
	storage, err := NewMachineStorage(fake, "xdmp", WithClock(func() int64 { return 1_000_000 }))
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	state, err := storage.Distribute(context.Background(), "xdmp", "node-a", 10, time.Minute, false)
	if err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if state.MachineID != 3 || state.LastTimestamp != 1000 || state.InstanceID != "node-a" {
		t.Fatalf("state = %+v", state)
	}
	if fake.lastKeys[0] != "xdmp" {
		t.Fatalf("keys = %v", fake.lastKeys)
	}
	args := fake.lastArgs
	if args[0] != "node-a" || args[1] != int64(10) {
		t.Fatalf("args = %v", args)
	}
	// safeGuardAt is now - 1 minute, the value CosID passes as ARGV[4].
	if args[3] != int64(1_000_000-60_000) {
		t.Fatalf("safeGuardAt = %v", args[3])
	}
	if !strings.Contains(fake.lastScript, "itc_idx") {
		t.Fatal("script is not the CosID distribute script")
	}
}

// A stable instance passes safeGuardAt=0, so the script never recycles a slot by
// age — CosID's getSafeGuardAt(duration, stable=true).
func TestMachineDistributeStablePassesZeroSafeGuard(t *testing.T) {
	fake := &fakeClient{reply: []any{int64(1), int64(5)}}
	storage, _ := NewMachineStorage(fake, "xdmp", WithClock(func() int64 { return 2000 }))

	if _, err := storage.Distribute(context.Background(), "xdmp", "node-a", 10, time.Minute, true); err != nil {
		t.Fatalf("distribute: %v", err)
	}
	if fake.lastArgs[3] != int64(0) {
		t.Fatalf("safeGuardAt = %v, want 0 for a stable instance", fake.lastArgs[3])
	}
}

func TestMachineDistributeExhausted(t *testing.T) {
	fake := &fakeClient{reply: []any{int64(-1), int64(-1)}}
	storage, _ := NewMachineStorage(fake, "xdmp")

	if _, err := storage.Distribute(context.Background(), "xdmp", "node-a", 10, time.Minute, false); !errors.Is(err, gcosid.ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
}

func TestMachineGuardRequiresASlot(t *testing.T) {
	fake := &fakeClient{reply: int64(0)}
	storage, _ := NewMachineStorage(fake, "xdmp")

	err := storage.Guard(context.Background(), "xdmp", gcosid.MachineState{MachineID: 1, InstanceID: "node-a"}, time.Minute, false)
	if !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}

	fake.reply = int64(1)
	if err := storage.Guard(context.Background(), "xdmp", gcosid.MachineState{MachineID: 1, InstanceID: "node-a"}, time.Minute, false); err != nil {
		t.Fatalf("guard: %v", err)
	}
}

func TestMachineRevertUsesTheScript(t *testing.T) {
	fake := &fakeClient{reply: int64(1)}
	storage, _ := NewMachineStorage(fake, "xdmp", WithClock(func() int64 { return 42 }))

	err := storage.Revert(context.Background(), "xdmp", "node-a", gcosid.MachineState{MachineID: 1}, false)
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	if !strings.Contains(fake.lastScript, "instanceRevertKey") {
		t.Fatal("revert did not run the revert script")
	}
}

func TestValidatesConfig(t *testing.T) {
	if _, err := NewSegmentAllocator(nil, "x"); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("nil client err = %v", err)
	}
	if _, err := NewSegmentAllocator(&fakeClient{}, ""); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewMachineStorage(nil, "x"); !errors.Is(err, gcosid.ErrInvalidConfig) {
		t.Fatalf("nil client err = %v", err)
	}
}
