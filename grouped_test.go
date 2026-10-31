package gcosid

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDateGroupKeysAndTTL(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		period  Period
		wantKey string
		wantTTL time.Time // exclusive end
	}{
		{PeriodYear, "2026", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
		{PeriodYearMonth, "2026-10", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)},
		{PeriodYearDay, "2026-10-31", time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tc := range cases {
		g, err := NewDateGroup(tc.period)
		if err != nil {
			t.Fatalf("%s: %v", tc.period, err)
		}
		g.Location = time.UTC
		key := g.Group(now)

		if key.Key != tc.wantKey {
			t.Errorf("%s key = %q, want %q", tc.period, key.Key, tc.wantKey)
		}
		// CosID uses LocalTime.MAX, so TTLAt is the last second of the period.
		wantTTLAt := tc.wantTTL.Unix() - 1
		if key.TTLAt != wantTTLAt {
			t.Errorf("%s ttlAt = %d, want %d", tc.period, key.TTLAt, wantTTLAt)
		}
		if key.TTL(now) <= 0 {
			t.Errorf("%s ttl = %d, want positive", tc.period, key.TTL(now))
		}
	}
}

func TestNewDateGroupRejectsUnknownPeriod(t *testing.T) {
	if _, err := NewDateGroup("hour"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
}

// The whole point of grouping: a new period starts a new counter, so ids restart
// instead of continuing forever.
func TestGroupedAllocatorRestartsPerPeriod(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	group, _ := NewDateGroup(PeriodYearMonth)
	group.Location = time.UTC

	alloc := &GroupedAllocator{
		Source: NewMemoryAllocator(100),
		By:     group,
		Now:    func() time.Time { return now },
	}

	first, err := alloc.NextMaxId(context.Background(), "order", 100)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != 100 {
		t.Fatalf("first maxId = %d, want 100", first)
	}
	if name := alloc.NameFor("order"); name != "order@2026-10" {
		t.Fatalf("storage name = %q, want order@2026-10", name)
	}

	// Same period: the counter continues.
	second, _ := alloc.NextMaxId(context.Background(), "order", 100)
	if second != 200 {
		t.Fatalf("second maxId = %d, want 200", second)
	}

	// New period: a different storage key, so the counter starts again.
	now = time.Date(2026, 11, 1, 0, 0, 1, 0, time.UTC)
	if name := alloc.NameFor("order"); name != "order@2026-11" {
		t.Fatalf("storage name = %q, want order@2026-11", name)
	}
	third, _ := alloc.NextMaxId(context.Background(), "order", 100)
	if third != 100 {
		t.Fatalf("new period maxId = %d, want 100 (restarted)", third)
	}
}

func TestGroupedGeneratorRendersPrefixedIDs(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	group, _ := NewDateGroup(PeriodYear)
	group.Location = time.UTC

	alloc := &GroupedAllocator{Source: NewMemoryAllocator(10), By: group, Now: func() time.Time { return now }}
	gen, err := NewGroupedGenerator(alloc, 10)
	if err != nil {
		t.Fatalf("generator: %v", err)
	}
	defer gen.Close(context.Background())

	decimal := ToString{}
	id, key, err := gen.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if key.Key != "2026" {
		t.Fatalf("key = %q, want 2026", key.Key)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}

	rendered, err := gen.NextString(context.Background(), "-", decimal)
	if err != nil {
		t.Fatalf("next string: %v", err)
	}
	if rendered != "2026-2" {
		t.Fatalf("rendered = %q, want 2026-2", rendered)
	}
}

func TestGroupedGeneratorValidates(t *testing.T) {
	if _, err := NewGroupedGenerator(nil, 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil allocator err = %v", err)
	}
	if _, err := NewGroupedGenerator(&GroupedAllocator{Source: NewMemoryAllocator(10)}, 10); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing GroupBy err = %v", err)
	}
}
