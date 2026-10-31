package gcosid

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func chainGenerator(t *testing.T, step int64, alloc SegmentAllocator, opts ...ChainOption) *ChainGenerator {
	t.Helper()
	g, err := NewChainGenerator("order", step, alloc, opts...)
	if err != nil {
		t.Fatalf("new chain: %v", err)
	}
	t.Cleanup(func() { _ = g.Close(context.Background()) })
	return g
}

func TestChainHandsOutUniqueIncreasingIDs(t *testing.T) {
	g := chainGenerator(t, 10, NewMemoryAllocator(10))

	prev := int64(0)
	seen := map[int64]bool{}
	for i := 0; i < 200; i++ {
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
	}
}

func TestChainIsConcurrencySafe(t *testing.T) {
	g := chainGenerator(t, 10, NewMemoryAllocator(10))

	const workers, per = 8, 300
	ids := make(chan int64, workers*per)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				id, err := g.Next(context.Background())
				if err != nil {
					t.Errorf("next: %v", err)
					return
				}
				ids <- id
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := map[int64]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("id %d duplicated", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*per {
		t.Fatalf("unique = %d, want %d", len(seen), workers*per)
	}
}

// A chain reserves safeDistance segments per extension, so it must not touch the
// allocator once per segment.
func TestChainReservesSeveralSegmentsAtOnce(t *testing.T) {
	alloc := &countingAllocator{inner: NewMemoryAllocator(10)}
	g := chainGenerator(t, 10, alloc, WithSafeDistance(2))

	for i := 0; i < 100; i++ {
		if _, err := g.Next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	// 100 ids at step 10 need 10 segments; merged reservations cover 2 each, so
	// far fewer calls than 10 are expected.
	if calls := alloc.Calls(); calls > 8 {
		t.Fatalf("allocator calls = %d, want <= 8 for 100 ids with safe distance 2", calls)
	}
}

func TestChainWalksPastExpiredSegments(t *testing.T) {
	now := int64(1000)
	g := chainGenerator(t, 10, NewMemoryAllocator(10),
		WithChainTTL(time.Second),
		WithChainClock(func() int64 { return now }),
	)

	first, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != 1 {
		t.Fatalf("first id = %d, want 1", first)
	}

	now += 5 // every buffered segment is now expired
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next after expiry: %v", err)
	}
	if id <= 10 {
		t.Fatalf("id %d came from an expired segment", id)
	}
}

func TestChainSurfacesAllocatorFailure(t *testing.T) {
	alloc := &countingAllocator{inner: NewMemoryAllocator(10), err: errors.New("db down")}
	if _, err := NewChainGenerator("order", 10, alloc); !errors.Is(err, ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable on the seed reservation", err)
	}
}

func TestChainClose(t *testing.T) {
	g, err := NewChainGenerator("order", 10, NewMemoryAllocator(10))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := g.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := g.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestNewChainGeneratorValidates(t *testing.T) {
	if _, err := NewChainGenerator("", 10, NewMemoryAllocator(10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewChainGenerator("order", 10, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil allocator err = %v", err)
	}
	if _, err := NewChainGenerator("order", 0, NewMemoryAllocator(10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero step err = %v", err)
	}
}

// A sub-second TTL must not round down to zero: that would expire every range the
// moment it is reserved and livelock the generator.
func TestSubSecondTTLRoundsUp(t *testing.T) {
	if got := ttlSeconds(time.Nanosecond); got != 1 {
		t.Fatalf("ttlSeconds(1ns) = %d, want 1", got)
	}
	if got := ttlSeconds(1500 * time.Millisecond); got != 2 {
		t.Fatalf("ttlSeconds(1.5s) = %d, want 2", got)
	}

	now := int64(1000)
	g := chainGenerator(t, 10, NewMemoryAllocator(10),
		WithChainTTL(time.Nanosecond),
		WithChainClock(func() int64 { return now }),
	)
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
}
