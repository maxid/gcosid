package gcosid

import (
	"context"
	"fmt"
	"math/rand/v2"
)

// UncertaintyGenerator appends random low bits to every id — CosID's
// UncertaintyIdGenerator. The ordering is preserved at the granularity of the
// original id, while the trailing digits no longer expose the sequence, which
// keeps ids from advertising how many were issued or which one comes next.
type UncertaintyGenerator struct {
	Actual          Generator
	UncertaintyBits int

	originalMax int64
	bound       int64
}

// NewUncertaintyGenerator wraps a generator, reserving `bits` low bits for
// randomness. `bits` must leave room for the original id inside 63 bits.
func NewUncertaintyGenerator(actual Generator, bits int) (*UncertaintyGenerator, error) {
	if actual == nil {
		return nil, fmt.Errorf("%w: generator is required", ErrInvalidConfig)
	}
	if bits <= 0 || bits >= TotalBit {
		return nil, fmt.Errorf("%w: uncertainty bits %d outside 1..%d", ErrInvalidConfig, bits, TotalBit-1)
	}
	originalBits := TotalBit - bits
	return &UncertaintyGenerator{
		Actual:          actual,
		UncertaintyBits: bits,
		originalMax:     int64(1)<<uint(originalBits) - 1,
		bound:           int64(1) << uint(bits),
	}, nil
}

// OriginalMaxID is the largest id the wrapped generator may produce.
func (g *UncertaintyGenerator) OriginalMaxID() int64 { return g.originalMax }

// Bound is the exclusive upper bound of the random suffix.
func (g *UncertaintyGenerator) Bound() int64 { return g.bound }

// Next implements Generator.
func (g *UncertaintyGenerator) Next(ctx context.Context) (int64, error) {
	original, err := g.Actual.Next(ctx)
	if err != nil {
		return 0, err
	}
	if original < 0 || original > g.originalMax {
		return 0, fmt.Errorf("%w: original id %d exceeds %d with %d uncertainty bits",
			ErrExhausted, original, g.originalMax, g.UncertaintyBits)
	}
	return original<<uint(g.UncertaintyBits) | rand.Int64N(g.bound), nil
}

// Stat implements Statistical.
func (g *UncertaintyGenerator) Stat() Stat {
	return NewStat("uncertainty").
		Set("uncertaintyBits", g.UncertaintyBits).
		Set("originalMaxId", g.originalMax).
		Set("bound", g.bound)
}
