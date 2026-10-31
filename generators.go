package gcosid

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"sync/atomic"
)

// StringGenerator renders ids as strings. Some generators (UUID, CosID's
// base36/base62 family) only make sense as strings, so they implement this
// instead of Generator.
type StringGenerator interface {
	GenerateAsString(ctx context.Context) (string, error)
}

// IntegerGenerator exposes a Generator as a 32-bit id source, refusing values
// that do not fit — CosID's IntegerIdGenerator.
type IntegerGenerator struct {
	Actual Generator
}

// NextAsInt implements the integer view with an overflow check.
func (g IntegerGenerator) NextAsInt(ctx context.Context) (int32, error) {
	id, err := g.Actual.Next(ctx)
	if err != nil {
		return 0, err
	}
	if id < math.MinInt32 || id > math.MaxInt32 {
		return 0, fmt.Errorf("%w: id %d does not fit in int32", ErrExhausted, id)
	}
	return int32(id), nil
}

// GenerateAsString implements StringGenerator: the decimal rendering of the id.
func (g IntegerGenerator) GenerateAsString(ctx context.Context) (string, error) {
	id, err := g.NextAsInt(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", id), nil
}

// AtomicGenerator is CosID's jvm.AtomicLongGenerator: a per-process counter. It
// is only unique inside one process, so it is for caches and tests, never for a
// persisted primary key.
type AtomicGenerator struct{ counter atomic.Int64 }

// NewAtomicGenerator creates a counter starting at zero.
func NewAtomicGenerator() *AtomicGenerator { return &AtomicGenerator{} }

// Next implements Generator.
func (g *AtomicGenerator) Next(context.Context) (int64, error) { return g.counter.Add(1), nil }

// UUIDGenerator is CosID's jvm.UuidGenerator: random UUIDv4 as a string.
type UUIDGenerator struct{}

// GenerateAsString implements StringGenerator.
func (UUIDGenerator) GenerateAsString(context.Context) (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	out := make([]byte, 0, 36)
	for i, b := range buf {
		switch i {
		case 4, 6, 8, 10:
			out = append(out, '-')
		}
		out = append(out, hexDigits[b>>4], hexDigits[b&0x0f])
	}
	return string(out), nil
}

const hexDigits = "0123456789abcdef"

// Next refuses to produce a long id, exactly like CosID: a UUID does not fit in
// an int64 and pretending otherwise silently loses half of it.
func (UUIDGenerator) Next(context.Context) (int64, error) {
	return 0, fmt.Errorf("%w: UUID has no int64 representation", ErrInvalidConfig)
}

// StringOf adapts a Generator to StringGenerator using a converter — the Go
// equivalent of CosID's StringIdGenerator decorators (Radix36/62, friendly).
func StringOf(gen Generator, conv StringConverter) StringGenerator {
	return stringOf{gen: gen, conv: conv}
}

type stringOf struct {
	gen  Generator
	conv StringConverter
}

func (s stringOf) GenerateAsString(ctx context.Context) (string, error) {
	id, err := s.gen.Next(ctx)
	if err != nil {
		return "", err
	}
	return s.conv.Format(id), nil
}
