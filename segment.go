package gcosid

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// prefetchTimeout bounds a background allocation so a hung database cannot
// leave the generator waiting forever.
const prefetchTimeout = 5 * time.Second

// SegmentGenerator hands out ids from DB-allocated ranges and keeps one range
// in reserve, so a refill does not stall every request.
//
// Concurrency: Next is safe for concurrent use. The synchronous refill path
// holds the generator lock across the allocator call; the common path (an
// id is available in the current range) never waits on I/O, and the prefetch
// path keeps the lock only long enough to install the fetched range.
type SegmentGenerator struct {
	namespace string
	alloc     SegmentAllocator
	// prefetchRatio is the fraction of a range that must remain for a prefetch
	// to start. Zero disables prefetching.
	prefetchRatio float64

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

// NewSegmentGenerator creates a generator for one namespace. Ranges are
// prefetched once the current one is half consumed.
func NewSegmentGenerator(namespace string, alloc SegmentAllocator) (*SegmentGenerator, error) {
	return NewSegmentGeneratorWithPrefetch(namespace, alloc, 0.5)
}

// NewSegmentGeneratorWithPrefetch is NewSegmentGenerator with an explicit
// prefetch threshold: the fraction of a range that must remain before the next
// one is fetched. 0 disables prefetching; values outside (0,1] are rejected.
func NewSegmentGeneratorWithPrefetch(namespace string, alloc SegmentAllocator, ratio float64) (*SegmentGenerator, error) {
	if namespace == "" {
		return nil, fmt.Errorf("%w: namespace is required", ErrInvalidConfig)
	}
	if alloc == nil {
		return nil, fmt.Errorf("%w: allocator is required", ErrInvalidConfig)
	}
	if ratio < 0 || ratio > 1 {
		return nil, fmt.Errorf("%w: prefetch ratio %v outside [0,1]", ErrInvalidConfig, ratio)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &SegmentGenerator{
		namespace: namespace,
		alloc:     alloc,
		ctx:       ctx,
		cancel:    cancel,
	}
	g.prefetchRatio = ratio
	return g, nil
}

func (g *SegmentGenerator) prefetchThreshold(seg Segment) int64 {
	if g.prefetchRatio <= 0 {
		return 0
	}
	size := seg.End - seg.Start + 1
	threshold := int64(g.prefetchRatio * float64(size))
	if threshold < 1 {
		threshold = 1
	}
	return threshold
}

// Namespace implements NamedGenerator.
func (g *SegmentGenerator) Namespace() string { return g.namespace }

// Next implements Generator.
func (g *SegmentGenerator) Next(ctx context.Context) (int64, error) {
	for {
		g.mu.Lock()
		if g.closed {
			g.mu.Unlock()
			return 0, ErrClosed
		}

		if g.next == 0 || g.next > g.current.End {
			seg, err := g.refillLocked(ctx)
			if err != nil {
				g.mu.Unlock()
				return 0, err
			}
			g.current = seg
			g.next = seg.Start
		}

		id := g.next
		g.next++

		if g.shouldPrefetchLocked() {
			g.startPrefetchLocked()
		}
		g.mu.Unlock()
		return id, nil
	}
}

// refillLocked installs the next range, taking the buffered one when present.
// It runs with g.mu held because it is the rare path; the prefetcher keeps it
// rare.
func (g *SegmentGenerator) refillLocked(ctx context.Context) (Segment, error) {
	if g.buffer != nil {
		seg := *g.buffer
		g.buffer = nil
		g.lastErr = nil
		return seg, nil
	}

	seg, err := g.alloc.NextSegment(ctx, g.namespace)
	if err != nil {
		return Segment{}, fmt.Errorf("%w: %w", ErrAllocatorUnavailable, err)
	}
	if err := validateSegment(seg); err != nil {
		return Segment{}, err
	}
	g.lastErr = nil
	return seg, nil
}

func (g *SegmentGenerator) shouldPrefetchLocked() bool {
	if g.prefetchRatio <= 0 || g.fetching || g.buffer != nil || g.closed {
		return false
	}
	remaining := g.current.End - g.next + 1
	return remaining <= g.prefetchThreshold(g.current)
}

func (g *SegmentGenerator) startPrefetchLocked() {
	g.fetching = true
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()

		ctx, cancel := context.WithTimeout(g.ctx, prefetchTimeout)
		defer cancel()

		seg, err := g.alloc.NextSegment(ctx, g.namespace)

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
		if err := validateSegment(seg); err != nil {
			g.lastErr = err
			return
		}
		g.buffer = &seg
		g.lastErr = nil
	}()
}

// Close stops prefetching and waits for the in-flight fetch to finish. Ids
// already buffered are dropped: a closed generator never hands out another id.
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

func validateSegment(seg Segment) error {
	if seg.Start <= 0 || seg.End < seg.Start {
		return fmt.Errorf("%w: allocator returned [%d,%d]", ErrInvalidConfig, seg.Start, seg.End)
	}
	return nil
}
