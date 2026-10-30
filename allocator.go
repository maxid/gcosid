package gcosid

import (
	"context"
	"sync"
)

// Segment is one allocated range of ids, both ends inclusive.
type Segment struct {
	Start int64
	End   int64
}

// SegmentAllocator hands out disjoint ranges. The database implementation
// performs the atomic max_id bump; the port keeps the generator testable.
//
// An allocator must never return the same range twice for the same namespace,
// and must return ErrAllocatorUnavailable for transient failures so the
// generator can retry without changing its layout.
type SegmentAllocator interface {
	NextSegment(ctx context.Context, namespace string) (Segment, error)
}

// MemoryAllocator is an in-process allocator for tests and single-process
// deployments without a database.
type MemoryAllocator struct {
	mu     sync.Mutex
	step   int64
	cursor map[string]int64
}

// NewMemoryAllocator creates an allocator handing out ranges of `step` ids
// starting at 1.
func NewMemoryAllocator(step int64) *MemoryAllocator {
	if step <= 0 {
		step = 100
	}
	return &MemoryAllocator{step: step, cursor: map[string]int64{}}
}

// NextSegment implements SegmentAllocator.
func (m *MemoryAllocator) NextSegment(_ context.Context, namespace string) (Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	start := m.cursor[namespace] + 1
	end := start + m.step - 1
	m.cursor[namespace] = end
	return Segment{Start: start, End: end}, nil
}
