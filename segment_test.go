package gcosid

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// countingAllocator wraps a MemoryAllocator to observe how often a range is
// fetched, which is what double buffering is supposed to reduce.
type countingAllocator struct {
	inner *MemoryAllocator
	mu    sync.Mutex
	calls int
	err   error
}

func (c *countingAllocator) NextSegment(ctx context.Context, namespace string) (Segment, error) {
	c.mu.Lock()
	c.calls++
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return Segment{}, err
	}
	return c.inner.NextSegment(ctx, namespace)
}

func (c *countingAllocator) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestSegmentGeneratorHandsOutEveryIDOnce(t *testing.T) {
	g, err := NewSegmentGenerator("order", NewMemoryAllocator(100))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
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
	g, err := NewSegmentGenerator("order", NewMemoryAllocator(50))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
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
	alloc := &countingAllocator{inner: NewMemoryAllocator(100)}
	g, err := NewSegmentGenerator("order", alloc)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer g.Close(context.Background())

	for i := 0; i < 600; i++ {
		if _, err := g.Next(context.Background()); err != nil {
			t.Fatalf("next: %v", err)
		}
	}

	// 600 ids need 6 ranges; prefetch may fetch one ahead, but never one per
	// refill (that would be the no-prefetch behaviour with 6+ synchronous
	// fetches plus waiting).
	if calls := alloc.Calls(); calls > 9 {
		t.Fatalf("allocator calls = %d, want <= 9 for 600 ids with step 100", calls)
	}
}

func TestSegmentGeneratorSurfacesAllocatorFailure(t *testing.T) {
	alloc := &countingAllocator{inner: NewMemoryAllocator(10), err: errors.New("db down")}
	g, err := NewSegmentGeneratorWithPrefetch("order", alloc, 0)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer g.Close(context.Background())

	if _, err := g.Next(context.Background()); !errors.Is(err, ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want ErrAllocatorUnavailable", err)
	}

	// Recovery: once the allocator works again, the generator keeps going.
	alloc.mu.Lock()
	alloc.err = nil
	alloc.mu.Unlock()

	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("after recovery: %v", err)
	}
}

func TestSegmentGeneratorClose(t *testing.T) {
	g, err := NewSegmentGenerator("order", NewMemoryAllocator(10))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
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
	if _, err := NewSegmentGenerator("", NewMemoryAllocator(10)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty namespace err = %v", err)
	}
	if _, err := NewSegmentGenerator("order", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil allocator err = %v", err)
	}
	if _, err := NewSegmentGeneratorWithPrefetch("order", NewMemoryAllocator(10), 2); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ratio err = %v", err)
	}
}
