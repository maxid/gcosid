package gcosid

import (
	"context"
	"fmt"
)

// ClockSyncGenerator wraps any generator with a clock-backwards handler — CosID's
// ClockSyncSnowflakeId / ClockSyncCosIdGenerator. The wrapped generator still owns
// the layout; this decorator only intervenes when the clock moved backwards.
//
// SnowflakeGenerator already accepts WithClockSync directly, but the string family
// and user-supplied generators do not, which is what this decorator is for.
type ClockSyncGenerator struct {
	Actual Generator
	Sync   ClockSync
}

// NewClockSyncGenerator wraps a generator. A nil sync means CosID's default
// thresholds.
func NewClockSyncGenerator(actual Generator, sync ClockSync) (*ClockSyncGenerator, error) {
	if actual == nil {
		return nil, fmt.Errorf("%w: generator is required", ErrInvalidConfig)
	}
	if sync == nil {
		sync = NewDefaultClockSync()
	}
	return &ClockSyncGenerator{Actual: actual, Sync: sync}, nil
}

// Next implements Generator.
func (g *ClockSyncGenerator) Next(ctx context.Context) (int64, error) {
	id, err := g.Actual.Next(ctx)
	if err == nil {
		return id, nil
	}
	// The only error this decorator can act on is a backwards clock: wait it out
	// and retry once. Everything else is passed through unchanged.
	if !isClockBackwards(err) {
		return 0, err
	}
	if syncErr := g.Sync.Synchronize(lastTimestampOf(err)); syncErr != nil {
		return 0, syncErr
	}
	return g.Actual.Next(ctx)
}

// Stat implements Statistical.
func (g *ClockSyncGenerator) Stat() Stat {
	return NewStat("clockSync")
}

// StringClockSyncGenerator is ClockSyncGenerator for string generators.
type StringClockSyncGenerator struct {
	Actual StringGenerator
	Sync   ClockSync
}

// NewStringClockSyncGenerator wraps a string generator.
func NewStringClockSyncGenerator(actual StringGenerator, sync ClockSync) (*StringClockSyncGenerator, error) {
	if actual == nil {
		return nil, fmt.Errorf("%w: generator is required", ErrInvalidConfig)
	}
	if sync == nil {
		sync = NewDefaultClockSync()
	}
	return &StringClockSyncGenerator{Actual: actual, Sync: sync}, nil
}

// GenerateAsString implements StringGenerator.
func (g *StringClockSyncGenerator) GenerateAsString(ctx context.Context) (string, error) {
	id, err := g.Actual.GenerateAsString(ctx)
	if err == nil {
		return id, nil
	}
	if !isClockBackwards(err) {
		return "", err
	}
	if syncErr := g.Sync.Synchronize(lastTimestampOf(err)); syncErr != nil {
		return "", syncErr
	}
	return g.Actual.GenerateAsString(ctx)
}

// isClockBackwards reports whether a generator reported a backwards clock.
func isClockBackwards(err error) bool {
	return errorIs(err, ErrClockMovedBackwards)
}

// lastTimestampOf extracts the last timestamp from a backwards-clock error. The
// generators embed it in the message, but a decorated error may not; the
// synchroniser only needs a lower bound, so it reads the clock directly when the
// value is unavailable.
func lastTimestampOf(err error) int64 {
	type withLastTimestamp interface{ LastTimestamp() int64 }
	if e, ok := err.(withLastTimestamp); ok {
		return e.LastTimestamp()
	}
	return nowMillis()
}
