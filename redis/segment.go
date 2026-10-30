package redis

import (
	"context"
	"fmt"

	"github.com/maxid/gcosid"
)

// SegmentAllocator reserves number ranges with INCRBY on
// `cosid:{namespace.name}:adder`, exactly like SpringRedisIdSegmentDistributor.
type SegmentAllocator struct {
	client    Client
	namespace string
}

// NewSegmentAllocator builds an allocator for a namespace.
func NewSegmentAllocator(client Client, namespace string) (*SegmentAllocator, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: redis client is required", gcosid.ErrInvalidConfig)
	}
	if namespace == "" {
		return nil, fmt.Errorf("%w: namespace is required", gcosid.ErrInvalidConfig)
	}
	return &SegmentAllocator{client: client, namespace: namespace}, nil
}

// NextMaxId implements gcosid.SegmentAllocator.
func (s *SegmentAllocator) NextMaxId(ctx context.Context, name string, step int64) (int64, error) {
	if step <= 0 {
		return 0, fmt.Errorf("%w: step must be positive", gcosid.ErrInvalidConfig)
	}
	key := "cosid:{" + s.namespace + "." + name + "}:adder"
	maxID, err := s.client.IncrBy(ctx, key, step)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", gcosid.ErrAllocatorUnavailable, err)
	}
	return maxID, nil
}
