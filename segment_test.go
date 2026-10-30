package gcosid

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// countingAllocator observes how often a range is reserved.
type countingAllocator struct {
	inner *MemoryAllocator
	mu    sync.Mutex
	calls int
	err   error
}

func (c *countingAllocator) NextMaxId(ctx context.Context, namespace string, step int64) (int64, error) {
	c.mu.Lock()
	c.calls++
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return c.inner.NextMaxId(ctx, namespace, step)
}

func (c *countingAllocator) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newGenerator(t *testing.T, step int64, opts ...SegmentOption) (*SegmentGenerator, *countingAllocator) {
	t.Helper()
	alloc := &countingAllocator{inner: NewMemoryAllocator(step)}
	g, err := NewSegmentGenerator("order", step, alloc, opts...)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return g, alloc
}

// The first id of a reserved range is offset+1, and offset is maxId-step — the
// CosID 2.15.1 default segment arithmetic.
func TestSegmentArithmeticMatchesCosID(t *testing.T) {
	g, _ := newGenerator(t, 100)
	defer g.Close(context.Background())

	first, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != 1 {
		t.Fatalf("first id = %d, want 1 (maxId 100 - step 100 + 1)", first)
	}
	if !g.current.Available(g.now()) {
		t.Fatal("a fresh segment must be available")
	}
	if g.current.Offset != 0 || g.current.MaxID != 100 {
		t.Fatalf("segment = %+v, want offset 0 maxId 100", g.current)
	}
}

func TestSegmentGeneratorHandsOutEveryIDOnce(t *testing.T) {
	g, _ := newGenerator(t, 100)
	defer g.Close(context.Background())

	seen := map[int64]bool{}
	prev := int64(0)
	for i := 0; i < 250; i++ {
		id, err := g.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if id <= prev {
			t.Fatalf("id %d did not advance past %d", id, prev)
		}
		if seen[id] {
			t.Fatalf("id %d handed out twice", id)
		}
		seen[id] = true
		prev = id
	}
}

func TestSegmentGeneratorIsConcurrencySafe(t *testing.T) {
	g, _ := newGenerator(t, 50)
	defer g.Close(context.Background())

	const goroutines, perGoroutine = 8, 200
	ids := make(chan int64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
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
			t.Fatalf("id %d handed out twice", id)
		}
		seen[id] = true
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("unique ids = %d, want %d", len(seen), goroutines*perGoroutine)
	}
}

func TestSegmentGeneratorPrefetches(t *testing.T) {
	g, alloc := newGenerator(t, 100)
	defer g.Close(context.Background())

	for i := 0; i < 600; i++ {
		if _, err := g.Next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	if calls := alloc.Calls(); calls > 9 {
		t.Fatalf("allocator calls = %d, want <= 9 for 600 ids with step 100", calls)
	}
}

// An expired range must be abandoned, never partially reused: the ids below the
// cursor may already have been handed to another instance.
func TestSegmentGeneratorAbandonsExpiredRange(t *testing.T) {
	now := int64(1000)
	g, _ := newGenerator(t, 10, WithSegmentTTL(10*time.Second), WithSegmentClock(func() int64 { return now }))
	defer g.Close(context.Background())

	first, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if first != 1 {
		t.Fatalf("first id = %d, want 1", first)
	}

	now += 11 // past the ttl
	afterExpiry, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("next after expiry: %v", err)
	}
	if afterExpiry <= 10 {
		t.Fatalf("id %d came from the expired range", afterExpiry)
	}
	if afterExpiry != 11 {
		t.Fatalf("id = %d, want the first id of the next range (11)", afterExpiry)
	}
}

func TestSegmentGeneratorSurfacesAllocatorFailure(t *testing.T) {
	alloc := &countingAllocator{inner: NewMemoryAllocator(10), err: errors.New("db down")}
	g, err := NewSegmentGenerator("order", 10, alloc, WithPrefetchRatio(0))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer g.Close(context.Background())

	if _, err := g.Next(context.Background()); !errors.Is(err, ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}

	alloc.mu.Lock()
	alloc.err = nil
	alloc.mu.Unlock()

	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestSegmentGeneratorClose(t *testing.T) {
	g, _ := newGenerator(t, 10)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := g.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if err := g.Close(ctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestNewSegmentGeneratorRejectsBadConfig(t *testing.T) {
	if _, err := NewSegmentGenerator("", 10, NewMemoryAllocator(10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewSegmentGenerator("order", 10, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil allocator err = %v", err)
	}
	if _, err := NewSegmentGenerator("order", 0, NewMemoryAllocator(10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("zero step err = %v", err)
	}
}
