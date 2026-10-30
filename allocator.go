package gcosid

import (
	"context"
	"math"
	"sync"
	"time"
)

// TTLForever marks a segment that never expires (CosID's
// IdSegment.TIME_TO_LIVE_FOREVER).
const TTLForever = math.MaxInt64

// Segment is one reserved range, modelled exactly like CosID 2.15.1's
// IdSegment: the range is (Offset, MaxID], the first id handed out is
// Offset+1, and TTL participates in availability.
type Segment struct {
	Offset    int64 // MaxID - Step
	MaxID     int64 // inclusive upper bound
	Step      int64
	FetchTime int64 // unix seconds
	TTL       int64 // seconds; TTLForever means never
}

// Available reports whether the segment may still hand out ids at `now`
// (unix seconds).
func (s Segment) Available(now int64) bool {
	if s.TTL == TTLForever {
		return true
	}
	return now < s.FetchTime+s.TTL
}

// SegmentAllocator reserves a range and returns its upper bound, mirroring
// CosID's IdSegmentDistributor.nextMaxId(step). The range is
// (maxId-step, maxId].
type SegmentAllocator interface {
	NextMaxId(ctx context.Context, namespace string, step int64) (int64, error)
}

// MemoryAllocator is an in-process allocator for tests and single-process
// deployments without a database.
type MemoryAllocator struct {
	mu     sync.Mutex
	step   int64
	cursor map[string]int64
}

// NewMemoryAllocator creates an allocator using `step` when the caller does not
// request one.
func NewMemoryAllocator(step int64) *MemoryAllocator {
	if step <= 0 {
		step = 100
	}
	return &MemoryAllocator{step: step, cursor: map[string]int64{}}
}

// NextMaxId implements SegmentAllocator.
func (m *MemoryAllocator) NextMaxId(_ context.Context, namespace string, step int64) (int64, error) {
	if step <= 0 {
		step = m.step
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.cursor[namespace] + step
	m.cursor[namespace] = next
	return next, nil
}

// clockSeconds is the default time source for TTL checks.
func clockSeconds() int64 { return time.Now().Unix() }
