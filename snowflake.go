package gcosid

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CosID 2.15.1 constants (the CosId class, SnowflakeId implementations).
type ClockSync interface {
	// Synchronize waits until the clock catches up with lastTimestamp.
	Synchronize(lastTimestamp int64) error
}

// DefaultClockSync mirrors DefaultClockBackwardsSynchronizer.
type DefaultClockSync struct {
	SpinThreshold   int64 // milliseconds
	BrokenThreshold int64 // milliseconds
	Now             func() int64
	Sleep           func(time.Duration)
}

// NewDefaultClockSync returns CosID's default thresholds (1ms / 500ms).
func NewDefaultClockSync() *DefaultClockSync {
	return &DefaultClockSync{SpinThreshold: 1, BrokenThreshold: 500}
}

// Synchronize implements ClockSync.
func (d *DefaultClockSync) Synchronize(lastTimestamp int64) error {
	now := d.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	sleep := d.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}

	backwards := lastTimestamp - now()
	if backwards > d.BrokenThreshold {
		return fmt.Errorf("%w: backwards %dms exceeds %dms", ErrClockMovedBackwards, backwards, d.BrokenThreshold)
	}
	if backwards > d.SpinThreshold {
		sleep(time.Duration(backwards) * time.Millisecond)
	}
	for now() <= lastTimestamp {
		sleep(time.Millisecond)
	}
	return nil
}

// SnowflakeGenerator implements Generator on a fixed layout.
type SnowflakeGenerator struct {
	layout         Layout
	machineID      int64
	resetThreshold int64
	clock          func() int64
	sync           ClockSync

	mu            sync.Mutex
	lastTimestamp int64
	sequence      int64
}

// SnowflakeOption configures a SnowflakeGenerator.
type SnowflakeOption func(*SnowflakeGenerator)

// WithSnowflakeClock overrides the time source (tests). It must return the same
// unit as the layout: milliseconds for millisecond layouts, seconds for second
// layouts.
func WithSnowflakeClock(now func() int64) SnowflakeOption {
	return func(g *SnowflakeGenerator) {
		if now != nil {
			g.clock = now
		}
	}
}

// WithClockSync installs a clock-backwards handler. Without one the generator
// fails on any backwards movement.
func WithClockSync(sync ClockSync) SnowflakeOption {
	return func(g *SnowflakeGenerator) { g.sync = sync }
}

// WithSequenceResetThreshold overrides CosID's default (half the sequence
// space), which evens out id distribution across shards.
func WithSequenceResetThreshold(threshold int64) SnowflakeOption {
	return func(g *SnowflakeGenerator) {
		if threshold >= 0 {
			g.resetThreshold = threshold
		}
	}
}

// NewSnowflake creates a generator for one machine id.
func NewSnowflake(layout Layout, machineID int64, opts ...SnowflakeOption) (*SnowflakeGenerator, error) {
	if err := layout.Validate(); err != nil {
		return nil, err
	}
	if machineID < 0 || machineID > layout.MaxMachineID() {
		return nil, fmt.Errorf("%w: machine id %d outside 0..%d", ErrInvalidMachineID, machineID, layout.MaxMachineID())
	}

	g := &SnowflakeGenerator{
		layout:         layout,
		machineID:      machineID,
		resetThreshold: layout.MaxSequence() >> 1,
		clock:          defaultClockFor(layout),
	}
	for _, o := range opts {
		o(g)
	}
	return g, nil
}

func defaultClockFor(layout Layout) func() int64 {
	if layout.Epoch == CosIdEpochSecond || layout.TimestampBits == SecondLayout.TimestampBits && layout.Epoch != CosIdEpoch {
		return func() int64 { return time.Now().Unix() }
	}
	return func() int64 { return time.Now().UnixMilli() }
}

// Next implements Generator, following AbstractSnowflakeId.generate.
func (g *SnowflakeGenerator) Next(context.Context) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock()
	if now < g.lastTimestamp {
		if g.sync == nil {
			return 0, fmt.Errorf("%w: last %d, now %d", ErrClockMovedBackwards, g.lastTimestamp, now)
		}
		if err := g.sync.Synchronize(g.lastTimestamp); err != nil {
			return 0, err
		}
		now = g.clock()
		if now < g.lastTimestamp {
			return 0, fmt.Errorf("%w: last %d, now %d", ErrClockMovedBackwards, g.lastTimestamp, now)
		}
	}

	// Reset the sequence early on a new tick so ids spread across shards.
	if now > g.lastTimestamp && g.sequence >= g.resetThreshold {
		g.sequence = 0
	}
	g.sequence = (g.sequence + 1) & g.layout.MaxSequence()
	if g.sequence == 0 {
		now = g.waitNextTick(now)
	}

	g.lastTimestamp = now
	return g.layout.Compose(now, g.machineID, g.sequence)
}

// waitNextTick spins until the clock moves past `now`, which is what CosID does
// when the sequence space is exhausted inside one tick.
func (g *SnowflakeGenerator) waitNextTick(now int64) int64 {
	for {
		next := g.clock()
		if next > now {
			return next
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// Layout returns the configured layout.
func (g *SnowflakeGenerator) Layout() Layout { return g.layout }
