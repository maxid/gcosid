package gcosid

import (
	"context"
	"sync"
	"time"
)

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
