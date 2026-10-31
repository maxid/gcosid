package gcosid

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The bit layout is the contract: a wrong shift silently corrupts every id ever
// written, so it is pinned with an explicit golden vector.
func TestLayoutGoldenVector(t *testing.T) {
	l := MillisecondLayout
	if err := l.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	id, err := l.Compose(CosIdEpoch+1, 1, 1)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	// (1 << 22) | (1 << 12) | 1
	if want := int64(4198401); id != want {
		t.Fatalf("id = %d, want %d", id, want)
	}

	state := l.Parse(id)
	if state.Timestamp != CosIdEpoch+1 || state.MachineID != 1 || state.Sequence != 1 {
		t.Fatalf("parsed = %+v", state)
	}
}

func TestLayoutBounds(t *testing.T) {
	if err := (Layout{Epoch: 0, TimestampBits: 41, MachineBits: 15, SequenceBits: 12}).Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("68 bits err = %v, want ErrInvalidConfig", err)
	}

	l := SecondLayout
	if _, err := l.Compose(CosIdEpochSecond+1, l.MaxMachineID()+1, 0); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("machine overflow err = %v", err)
	}
	if _, err := l.Compose(CosIdEpochSecond-1, 0, 0); !errors.Is(err, ErrExhausted) {
		t.Fatalf("pre-epoch err = %v", err)
	}
}

func TestSafeJSLayoutsStaySmall(t *testing.T) {
	for name, l := range map[string]Layout{
		"millisecond": SafeJSMillisecondLayout,
		"second":      SafeJSSecondLayout,
	} {
		if !l.IsSafeJS() {
			t.Errorf("%s layout is not JS-safe", name)
		}
		epoch := l.Epoch
		if l.TimestampBits == 31 {
			epoch = CosIdEpochSecond
		}
		id, err := l.Compose(epoch+1, l.MaxMachineID(), l.MaxSequence())
		if err != nil {
			t.Fatalf("%s compose: %v", name, err)
		}
		if id > JSMaxSafeNumber {
			t.Errorf("%s id %d exceeds the JS safe number", name, id)
		}
	}
}

func TestSnowflakeIsMonotonicAndUnique(t *testing.T) {
	now := CosIdEpoch + 1000
	g, err := NewSnowflake(MillisecondLayout, 7, WithSnowflakeClock(func() int64 { return now }))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	seen := map[int64]bool{}
	prev := int64(0)
	for i := 0; i < 500; i++ {
		if i%5 == 0 {
			now++ // advance a millisecond now and then
		}
		id, err := g.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if id <= prev {
			t.Fatalf("id %d did not advance past %d", id, prev)
		}
		if seen[id] {
			t.Fatalf("id %d duplicated", id)
		}
		seen[id] = true
		prev = id
		if got := g.Layout().Parse(id).MachineID; got != 7 {
			t.Fatalf("machine id = %d, want 7", got)
		}
	}
}

// Exhausting the sequence inside one tick must wait for the next tick instead of
// wrapping into an id that already exists.
func TestSnowflakeWaitsForNextTickOnSequenceOverflow(t *testing.T) {
	now := CosIdEpoch + 1
	advance := 0
	g, err := NewSnowflake(MillisecondLayout, 0, WithSnowflakeClock(func() int64 {
		advance++
		if advance > 2 {
			now++ // the generator spins, so the clock eventually moves
		}
		return now
	}))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	seen := map[int64]bool{}
	for i := 0; i <= int(MillisecondLayout.MaxSequence()); i++ {
		id, err := g.Next(context.Background())
		if err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("id %d duplicated across the sequence wrap", id)
		}
		seen[id] = true
	}
}

func TestSnowflakeRejectsBackwardsClock(t *testing.T) {
	now := CosIdEpoch + 10
	g, err := NewSnowflake(MillisecondLayout, 0, WithSnowflakeClock(func() int64 { return now }))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	now -= 5
	if _, err := g.Next(context.Background()); !errors.Is(err, ErrClockMovedBackwards) {
		t.Fatalf("err = %v, want ErrClockMovedBackwards", err)
	}
}

func TestSnowflakeRecoversWithClockSync(t *testing.T) {
	now := CosIdEpoch + 10
	g, err := NewSnowflake(MillisecondLayout, 0,
		WithSnowflakeClock(func() int64 { return now }),
		WithClockSync(&DefaultClockSync{
			SpinThreshold:   1,
			BrokenThreshold: 500,
			Now:             func() int64 { return now },
			// The synchroniser polls until the clock catches up; advance it so
			// the loop terminates instead of spinning forever.
			Sleep: func(time.Duration) { now++ },
		}),
	)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	now -= 2 // within the broken threshold: the synchronizer waits it out
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("after backwards: %v", err)
	}
	if id <= 0 {
		t.Fatalf("id = %d", id)
	}
}

func TestSnowflakeRejectsBadMachineID(t *testing.T) {
	if _, err := NewSnowflake(MillisecondLayout, -1); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("negative err = %v", err)
	}
	if _, err := NewSnowflake(MillisecondLayout, MillisecondLayout.MaxMachineID()+1); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("overflow err = %v", err)
	}
}
