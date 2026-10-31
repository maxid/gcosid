package gcosid

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultSafeDistance is CosID's SegmentChainId.DEFAULT_SAFE_DISTANCE: the chain
// keeps two segments ready ahead of the cursor.
const DefaultSafeDistance = 2

// maxPrefetchDistance is CosID's PrefetchJob.MAX_PREFETCH_DISTANCE.
const maxPrefetchDistance = 100_000_000

// hungerThresholdSeconds is PrefetchJob.hungerThreshold: a synchronous extension
// within this window marks the generator as hungry and doubles the prefetch
// distance.
const hungerThresholdSeconds = 5

// maxExtendAttempts bounds how many reservations one Next call may perform before
// it reports a configuration problem instead of looping.
const maxExtendAttempts = 3

// ttlSeconds converts a configured lifetime to whole seconds, rounding up.
// A sub-second or zero TTL would otherwise become 0, which makes every reserved
// range expire the instant it is created (CosID rejects ttl<=0 outright).
func ttlSeconds(ttl time.Duration) int64 {
	secs := int64((ttl + time.Second - 1) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return secs
}

// chainNode is one IdSegmentChain link: a reserved range plus a lazy successor.
type chainNode struct {
	version int64
	segment Segment
	seq     atomic.Int64
	next    atomic.Pointer[chainNode]
}

func newNode(version int64, seg Segment) *chainNode {
	n := &chainNode{version: version, segment: seg}
	n.seq.Store(seg.Offset)
	return n
}

// incrementAndGet mirrors DefaultIdSegment.incrementAndGet: it bumps the shared
// cursor by one and refuses to hand out a value past MaxID.
func (n *chainNode) incrementAndGet(now int64) (int64, bool) {
	if !n.segment.Available(now) {
		return 0, false
	}
	next := n.seq.Add(1)
	if next > n.segment.MaxID {
		return 0, false
	}
	return next, true
}

// ChainGenerator is CosID's SegmentChainId: a chain of reserved ranges, walked
// forward as they expire or overflow, with a background prefetcher that keeps
// `safeDistance` ranges ready.
type ChainGenerator struct {
	namespace    string
	step         int64
	ttl          int64
	safeDistance int
	alloc        SegmentAllocator
	now          func() int64

	head atomic.Pointer[chainNode]

	prefetchDistance atomic.Int64
	lastHunger       atomic.Int64

	extendMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// ChainOption configures a ChainGenerator.
type ChainOption func(*ChainGenerator)

// WithChainTTL gives every reserved range a lifetime. Durations shorter than a
// second round up: a zero TTL would expire each range the moment it is created.
func WithChainTTL(ttl time.Duration) ChainOption {
	return func(g *ChainGenerator) {
		if ttl > 0 {
			g.ttl = ttlSeconds(ttl)
		}
	}
}

// WithSafeDistance keeps `n` ranges ready ahead of the cursor.
func WithSafeDistance(n int) ChainOption {
	return func(g *ChainGenerator) {
		if n > 0 {
			g.safeDistance = n
		}
	}
}

// WithChainClock overrides the clock (tests).
func WithChainClock(now func() int64) ChainOption {
	return func(g *ChainGenerator) {
		if now != nil {
			g.now = now
		}
	}
}

// NewChainGenerator creates a chain generator and starts the prefetcher.
func NewChainGenerator(namespace string, step int64, alloc SegmentAllocator, opts ...ChainOption) (*ChainGenerator, error) {
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
	g := &ChainGenerator{
		namespace:    namespace,
		step:         step,
		ttl:          TTLForever,
		safeDistance: DefaultSafeDistance,
		alloc:        alloc,
		now:          clockSeconds,
		ctx:          ctx,
		cancel:       cancel,
	}
	for _, o := range opts {
		o(g)
	}
	g.prefetchDistance.Store(int64(g.safeDistance))

	// Seed the chain so the first call does not have to reserve synchronously.
	if err := g.extend(ctx); err != nil {
		cancel()
		return nil, err
	}
	g.wg.Add(1)
	go g.prefetchLoop()
	return g, nil
}

// Namespace implements NamedGenerator.
func (g *ChainGenerator) Namespace() string { return g.namespace }

// Next implements Generator, mirroring SegmentChainId.generate: walk the chain
// for an available range, and extend it when nothing is left.
func (g *ChainGenerator) Next(ctx context.Context) (int64, error) {
	attempts := 0
	for {
		now := g.now()
		for node := g.head.Load(); node != nil; node = node.next.Load() {
			if id, ok := node.incrementAndGet(now); ok {
				g.forward(node)
				return id, nil
			}
		}

		g.hungry()
		if err := g.extend(ctx); err != nil {
			return 0, err
		}
		// A freshly reserved range is only unusable if the configuration makes
		// it so (for example a TTL that already elapsed). Fail instead of
		// spinning the allocator forever.
		if attempts++; attempts > maxExtendAttempts {
			return 0, fmt.Errorf("%w: no usable segment after %d reservations", ErrInvalidConfig, attempts)
		}
	}
}

// extend reserves a merged range of `safeDistance` segments and links it at the
// tail. Reserving several at once is what keeps the prefetcher ahead of demand.
func (g *ChainGenerator) extend(ctx context.Context) error {
	g.extendMu.Lock()
	defer g.extendMu.Unlock()

	tail := g.tail()
	segments := int(g.prefetchDistance.Load())
	if segments < g.safeDistance {
		segments = g.safeDistance
	}

	totalStep := g.step * int64(segments)
	maxID, err := g.alloc.NextMaxId(ctx, g.namespace, totalStep)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrAllocatorUnavailable, err)
	}
	if maxID <= 0 {
		return fmt.Errorf("%w: allocator returned max id %d", ErrInvalidConfig, maxID)
	}

	seg := Segment{
		MaxID:     maxID,
		Step:      totalStep,
		Offset:    maxID - totalStep,
		FetchTime: g.now(),
		TTL:       g.ttl,
	}
	if seg.Offset < 0 {
		seg.Offset = 0
	}

	node := newNode(tail.version+1, seg)
	tail.next.Store(node)
	if g.head.Load() == nil {
		g.head.Store(node)
	}
	return nil
}

// tail walks the chain to its last link.
func (g *ChainGenerator) tail() *chainNode {
	tail := g.head.Load()
	if tail == nil {
		return &chainNode{version: -1} // root, never handed out
	}
	for next := tail.next.Load(); next != nil; next = tail.next.Load() {
		tail = next
	}
	return tail
}

// forward advances the head to a resolved node, like SegmentChainId.forward.
func (g *ChainGenerator) forward(node *chainNode) {
	for {
		head := g.head.Load()
		if head == nil || head.version >= node.version {
			return
		}
		if g.head.CompareAndSwap(head, node) {
			return
		}
	}
}

// hungry records that generation had to reserve synchronously.
func (g *ChainGenerator) hungry() {
	g.lastHunger.Store(g.now())
}

func (g *ChainGenerator) prefetchLoop() {
	defer g.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.prefetch()
		}
	}
}

// prefetch keeps the chain ahead of the cursor, expanding the distance while the
// generator is hungry and shrinking it when it is not — CosID's PrefetchJob.
func (g *ChainGenerator) prefetch() {
	hungry := g.now()-g.lastHunger.Load() < hungerThresholdSeconds

	current := g.prefetchDistance.Load()
	switch {
	case hungry:
		if next := current * 2; next <= maxPrefetchDistance {
			g.prefetchDistance.Store(next)
		}
	case current > int64(g.safeDistance):
		next := current / 2
		if next < int64(g.safeDistance) {
			next = int64(g.safeDistance)
		}
		g.prefetchDistance.Store(next)
	}

	if g.gapAhead() >= g.safeDistance {
		return
	}
	ctx, cancel := context.WithTimeout(g.ctx, prefetchTimeout)
	defer cancel()
	_ = g.extend(ctx)
}

// gapAhead counts how many usable ranges sit between the cursor and the chain
// tail, which is what safeDistance is measured in.
func (g *ChainGenerator) gapAhead() int {
	now := g.now()
	count := 0
	for node := g.head.Load(); node != nil; node = node.next.Load() {
		if node.segment.Available(now) && node.seq.Load() < node.segment.MaxID {
			count++
		}
	}
	return count
}

// Close stops the prefetcher. Buffered ranges are dropped: a closed generator
// never hands out another id.
func (g *ChainGenerator) Close(ctx context.Context) error {
	g.once.Do(func() {
		g.cancel()
	})

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
