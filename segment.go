package gcosid

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// prefetchTimeout bounds a background reservation so a hung store cannot leave
// the generator waiting forever.
const prefetchTimeout = 5 * time.Second

// SegmentGenerator issues the values of ranges reserved from a
// SegmentAllocator. It keeps one range in reserve so a refill does not stall
// every caller.
//
// Concurrency: Next is safe for concurrent use. The synchronous refill path
// holds the generator lock across the allocator call; that path is rare because
// the fetcher keeps a range ready.
type SegmentGenerator struct {
	namespace     string
	alloc         SegmentAllocator
	step          int64
	ttl           int64
	prefetchRatio float64
	now           func() int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	current  Segment
	next     int64
	buffer   *Segment
	fetching bool
	lastErr  error
}

// SegmentOption configures a SegmentGenerator.
type SegmentOption func(*SegmentGenerator)

// WithSegmentTTL gives reserved ranges a lifetime. An expired range is
// abandoned and a new one is reserved, matching CosID's IdSegment.isExpired.
func WithSegmentTTL(ttl time.Duration) SegmentOption {
	return func(g *SegmentGenerator) {
		if ttl > 0 {
			g.ttl = int64(ttl.Seconds())
		}
	}
}

// WithPrefetchRatio sets the fraction of a range that must remain for a
// prefetch to start. 0 disables prefetching.
func WithPrefetchRatio(ratio float64) SegmentOption {
	return func(g *SegmentGenerator) {
		if ratio < 0 || ratio > 1 {
			g.prefetchRatio = 0
			return
		}
		g.prefetchRatio = ratio
	}
}

// WithSegmentClock overrides the clock used for TTL checks (tests).
func WithSegmentClock(now func() int64) SegmentOption {
	return func(g *SegmentGenerator) {
		if now != nil {
			g.now = now
		}
	}
}

// NewSegmentGenerator creates a generator for one namespace. Ranges are
// prefetched once half of the current one is consumed.
func NewSegmentGenerator(namespace string, step int64, alloc SegmentAllocator, opts ...SegmentOption) (*SegmentGenerator, error) {
	if namespace == "" {
		return nil, fmt.Errorf("%w: namespace is required", ErrInvalidConfig)
	}
	if alloc == nil {
		return nil, fmt.Errorf("%w: allocator is required", ErrInvalidConfig)
	}
	if step <= 0 {
		return nil, fmt.Errorf("%w: step must be positive", ErrInvalidConfig)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &SegmentGenerator{
		namespace:     namespace,
		alloc:         alloc,
		step:          step,
		ttl:           TTLForever,
		prefetchRatio: 0.5,
		now:           clockSeconds,
		ctx:           ctx,
		cancel:        cancel,
	}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

// Namespace implements NamedGenerator.
func (g *SegmentGenerator) Namespace() string { return g.namespace }

// Next implements Generator. The first id of a reserved range (offset+1) is
// handed out first, exactly like CosID's DefaultIdSegment.
func (g *SegmentGenerator) Next(ctx context.Context) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return 0, ErrClosed
	}

	now := g.now()
	if g.next == 0 || g.next > g.current.MaxID || !g.current.Available(now) {
		if err := g.refillLocked(ctx, now); err != nil {
			return 0, err
		}
	}

	id := g.next
	g.next++
	if g.shouldPrefetchLocked() {
		g.startPrefetchLocked()
	}
	return id, nil
}

func (g *SegmentGenerator) refillLocked(ctx context.Context, now int64) error {
	if g.buffer != nil {
		seg := *g.buffer
		g.buffer = nil
		if seg.Available(now) {
			g.current = seg
			g.next = seg.Offset + 1
			g.lastErr = nil
			return nil
		}
		// An expired prefetched range is wasted: gaps are allowed, reuse is
		// not, because the ids may already belong to someone else.
	}

	maxID, err := g.alloc.NextMaxId(ctx, g.namespace, g.step)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAllocatorUnavailable, err)
	}
	seg, err := g.newSegment(maxID, now)
	if err != nil {
		return err
	}
	g.current = seg
	g.next = seg.Offset + 1
	g.lastErr = nil
	return nil
}

func (g *SegmentGenerator) newSegment(maxID, now int64) (Segment, error) {
	if maxID <= 0 {
		return Segment{}, fmt.Errorf("%w: allocator returned max id %d", ErrInvalidConfig, maxID)
	}
	seg := Segment{
		MaxID:     maxID,
		Step:      g.step,
		Offset:    maxID - g.step,
		FetchTime: now,
		TTL:       g.ttl,
	}
	if seg.Offset < 0 {
		seg.Offset = 0
	}
	return seg, nil
}

func (g *SegmentGenerator) shouldPrefetchLocked() bool {
	if g.prefetchRatio <= 0 || g.fetching || g.buffer != nil || g.closed {
		return false
	}
	remaining := g.current.MaxID - g.next + 1
	threshold := int64(g.prefetchRatio * float64(g.current.Step))
	if threshold < 1 {
		threshold = 1
	}
	return remaining <= threshold
}

func (g *SegmentGenerator) startPrefetchLocked() {
	g.fetching = true
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()

		ctx, cancel := context.WithTimeout(g.ctx, prefetchTimeout)
		defer cancel()

		maxID, err := g.alloc.NextMaxId(ctx, g.namespace, g.step)

		g.mu.Lock()
		defer g.mu.Unlock()
		g.fetching = false
		if g.closed {
			return
		}
		if err != nil {
			// Keep the error: the next refill must surface it instead of
			// pretending the generator is healthy.
			g.lastErr = err
			return
		}
		seg, err := g.newSegment(maxID, g.now())
		if err != nil {
			g.lastErr = err
			return
		}
		g.buffer = &seg
		g.lastErr = nil
	}()
}

// Close stops prefetching and waits for the in-flight reservation to finish.
// Ranges already buffered are dropped: a closed generator never hands out
// another id.
func (g *SegmentGenerator) Close(ctx context.Context) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	g.mu.Unlock()

	g.cancel()

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
