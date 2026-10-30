package gcosid

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CosID 2.15.1 constants (CosId.java, SnowflakeId implementations).
const (
	// CosIdEpoch is CosId.COSID_EPOCH in milliseconds (2019-12-24T16:00Z).
	CosIdEpoch = int64(1577203200000)
	// CosIdEpochSecond is CosId.COSID_EPOCH_SECOND in seconds.
	CosIdEpochSecond = int64(1577203200)
	// TotalBit is the usable bit count: the sign bit stays zero.
	TotalBit = 63
	// JavaScriptMaxSafeNumberBit is SafeJavaScriptSnowflakeId.JAVA_SCRIPT_MAX_SAFE_NUMBER_BIT.
	JavaScriptMaxSafeNumberBit = 53
	// JavaScriptMaxSafeNumber is SafeJavaScriptSnowflakeId.JAVA_SCRIPT_MAX_SAFE_NUMBER.
	JavaScriptMaxSafeNumber = int64(9007199254740991)
)

// Layout is one snowflake bit layout: timestamp | machine | sequence.
type Layout struct {
	Epoch         int64
	TimestampBits int
	MachineBits   int
	SequenceBits  int
}

// Default millisecond layout: 41 | 10 | 12.
var MillisecondLayout = Layout{
	Epoch:         CosIdEpoch,
	TimestampBits: 41,
	MachineBits:   10,
	SequenceBits:  12,
}

// Default second layout: 31 | 10 | 22.
var SecondLayout = Layout{
	Epoch:         CosIdEpochSecond,
	TimestampBits: 31,
	MachineBits:   10,
	SequenceBits:  22,
}

// SafeJavaScript variants drop machine and sequence bits so the id stays a
// JavaScript safe integer: milliseconds 41 | 3 | 9, seconds 31 | 3 | 19.
var (
	SafeJavaScriptMillisecondLayout = Layout{
		Epoch:         CosIdEpoch,
		TimestampBits: 41,
		MachineBits:   3,
		SequenceBits:  9,
	}
	SafeJavaScriptSecondLayout = Layout{
		Epoch:         CosIdEpochSecond,
		TimestampBits: 31,
		MachineBits:   3,
		SequenceBits:  19,
	}
)

// Validate reports whether the layout can produce positive int64 values.
func (l Layout) Validate() error {
	if l.TimestampBits <= 0 || l.MachineBits < 0 || l.SequenceBits <= 0 {
		return fmt.Errorf("%w: layout bits must be positive", ErrInvalidConfig)
	}
	if l.TimestampBits+l.MachineBits+l.SequenceBits > TotalBit {
		return fmt.Errorf("%w: %d bits exceed %d", ErrInvalidConfig,
			l.TimestampBits+l.MachineBits+l.SequenceBits, TotalBit)
	}
	return nil
}

// MaxTimestamp is the largest value the timestamp field can hold.
func (l Layout) MaxTimestamp() int64 { return mask(l.TimestampBits) }

// MaxMachineID is the largest machine id the layout can hold.
func (l Layout) MaxMachineID() int64 { return mask(l.MachineBits) }

// MaxSequence is the largest sequence the layout can hold.
func (l Layout) MaxSequence() int64 { return mask(l.SequenceBits) }

// Compose packs the three fields exactly like AbstractSnowflakeId.generate:
// (timestamp - epoch) << (machine+sequence) | machine << sequence | sequence.
func (l Layout) Compose(timestamp, machineID, sequence int64) (int64, error) {
	if err := l.Validate(); err != nil {
		return 0, err
	}
	diff := timestamp - l.Epoch
	if diff < 0 || diff > l.MaxTimestamp() {
		return 0, fmt.Errorf("%w: timestamp %d outside the layout", ErrExhausted, timestamp)
	}
	if machineID < 0 || machineID > l.MaxMachineID() {
		return 0, fmt.Errorf("%w: machine id %d", ErrInvalidMachineID, machineID)
	}
	if sequence < 0 || sequence > l.MaxSequence() {
		return 0, fmt.Errorf("%w: sequence %d", ErrInvalidConfig, sequence)
	}
	return diff<<(l.MachineBits+l.SequenceBits) | machineID<<l.SequenceBits | sequence, nil
}

// State is an id split back into its fields.
type State struct {
	Timestamp int64
	MachineID int64
	Sequence  int64
}

// Parse splits an id back into timestamp, machine id and sequence.
func (l Layout) Parse(id int64) State {
	sequence := id & l.MaxSequence()
	machine := (id >> l.SequenceBits) & l.MaxMachineID()
	diff := id >> (l.MachineBits + l.SequenceBits)
	return State{Timestamp: diff + l.Epoch, MachineID: machine, Sequence: sequence}
}

// IsSafeJavaScript reports whether ids produced by the layout stay below the
// JavaScript safe integer limit.
func (l Layout) IsSafeJavaScript() bool {
	return l.TimestampBits+l.MachineBits+l.SequenceBits <= JavaScriptMaxSafeNumberBit
}

func mask(bits int) int64 {
	if bits <= 0 {
		return 0
	}
	if bits >= 63 {
		return int64(^uint64(0) >> 1)
	}
	return int64(1)<<bits - 1
}

// ClockSync handles a clock that moved backwards. CosID's default spins for up
// to 1ms, sleeps for up to 500ms, and refuses anything larger.
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
