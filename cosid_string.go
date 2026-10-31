package gcosid

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// CosID's string generator family (RadixCosIdGenerator) uses a different layout
// from the int64 snowflake: 44 timestamp bits + 20 machine bits + 16 sequence
// bits. The sum exceeds 63 on purpose — the output is a fixed-width string, not
// a packed int64, and the timestamp is the *raw* epoch millisecond (no epoch
// offset).
const (
	CosIDStringTimestampBits = 44
	CosIDStringMachineBits   = 20
	CosIDStringSequenceBits  = 16
)

// CosIDState is one generated value split into its three parts
// (CosID's CosIdState).
type CosIDState struct {
	Timestamp int64
	MachineID int32
	Sequence  int32
}

// Compare orders states by timestamp, then machine, then sequence.
func (s CosIDState) Compare(other CosIDState) int {
	switch {
	case s.Timestamp != other.Timestamp:
		if s.Timestamp < other.Timestamp {
			return -1
		}
		return 1
	case s.MachineID != other.MachineID:
		if s.MachineID < other.MachineID {
			return -1
		}
		return 1
	case s.Sequence != other.Sequence:
		if s.Sequence < other.Sequence {
			return -1
		}
		return 1
	}
	return 0
}

// RadixStateParser renders a state as fixed-width padded radix fields — CosID's
// RadixCosIdStateParser. Each field is padded to the digit count its bit width
// needs, so the total length is constant.
type RadixStateParser struct {
	Base      int
	Timestamp *Radix
	Machine   *Radix
	Sequence  *Radix
}

// NewRadixStateParser builds a parser for the given bits and radix.
func NewRadixStateParser(base, timestampBits, machineBits, sequenceBits int) (*RadixStateParser, error) {
	makeField := func(bits int) (*Radix, error) {
		size := maxRadixCharSize(base, bits)
		return NewRadix(base, size, true)
	}
	ts, err := makeField(timestampBits)
	if err != nil {
		return nil, err
	}
	machine, err := makeField(machineBits)
	if err != nil {
		return nil, err
	}
	sequence, err := makeField(sequenceBits)
	if err != nil {
		return nil, err
	}
	return &RadixStateParser{Base: base, Timestamp: ts, Machine: machine, Sequence: sequence}, nil
}

// Format implements the string rendering.
func (p RadixStateParser) Format(state CosIDState) string {
	return p.Timestamp.Format(state.Timestamp) +
		p.Machine.Format(int64(state.MachineID)) +
		p.Sequence.Format(int64(state.Sequence))
}

// Parse is the inverse of Format.
func (p RadixStateParser) Parse(id string) (CosIDState, error) {
	total := p.Timestamp.CharSize + p.Machine.CharSize + p.Sequence.CharSize
	if len(id) != total {
		return CosIDState{}, fmt.Errorf("%w: id length %d, want %d", ErrInvalidConfig, len(id), total)
	}
	ts, err := p.Timestamp.Parse(id[:p.Timestamp.CharSize])
	if err != nil {
		return CosIDState{}, err
	}
	machine, err := p.Machine.Parse(id[p.Timestamp.CharSize : p.Timestamp.CharSize+p.Machine.CharSize])
	if err != nil {
		return CosIDState{}, err
	}
	sequence, err := p.Sequence.Parse(id[p.Timestamp.CharSize+p.Machine.CharSize:])
	if err != nil {
		return CosIDState{}, err
	}
	return CosIDState{Timestamp: ts, MachineID: int32(machine), Sequence: int32(sequence)}, nil
}

// maxRadixCharSize is RadixIdConverter.maxCharSize: how many digits the largest
// value of `bits` needs in `base`.
func maxRadixCharSize(base, bits int) int {
	if bits <= 0 {
		return 1
	}
	max := int64(1)<<uint(bits) - 1
	if base <= 2 {
		return bits
	}
	digits := 0
	for max > 0 {
		digits++
		max /= int64(base)
	}
	if digits == 0 {
		digits = 1
	}
	return digits
}

// StringCosIDGenerator is CosID's Radix36/Radix62 generator: a string id made of
// timestamp, machine and sequence, rendered in a radix.
type StringCosIDGenerator struct {
	base           int
	timestampBits  int
	machineBits    int
	sequenceBits   int
	machineID      int32
	resetThreshold int32
	parser         RadixStateParser
	clock          func() int64

	mu            sync.Mutex
	lastTimestamp int64
	sequence      int32
}

// StringCosIDOption configures a StringCosIDGenerator.
type StringCosIDOption func(*StringCosIDGenerator)

// WithStringCosIDLayout overrides the bit layout (defaults 44/20/16).
func WithStringCosIDLayout(timestampBits, machineBits, sequenceBits int) StringCosIDOption {
	return func(g *StringCosIDGenerator) {
		if timestampBits > 0 && machineBits > 0 && sequenceBits > 0 {
			g.timestampBits, g.machineBits, g.sequenceBits = timestampBits, machineBits, sequenceBits
		}
	}
}

// WithStringCosIDClock overrides the millisecond clock (tests).
func WithStringCosIDClock(now func() int64) StringCosIDOption {
	return func(g *StringCosIDGenerator) {
		if now != nil {
			g.clock = now
		}
	}
}

// NewStringCosIDGenerator creates a base36 or base62 string generator.
func NewStringCosIDGenerator(base int, machineID int32, opts ...StringCosIDOption) (*StringCosIDGenerator, error) {
	if base != 36 && base != 62 {
		return nil, fmt.Errorf("%w: base %d, want 36 or 62", ErrInvalidConfig, base)
	}

	g := &StringCosIDGenerator{
		base:          base,
		timestampBits: CosIDStringTimestampBits,
		machineBits:   CosIDStringMachineBits,
		sequenceBits:  CosIDStringSequenceBits,
		machineID:     machineID,
		clock:         func() int64 { return time.Now().UnixMilli() },
	}
	for _, o := range opts {
		o(g)
	}

	maxMachine := int32(int64(1)<<uint(g.machineBits) - 1)
	if machineID < 0 || machineID > maxMachine {
		return nil, fmt.Errorf("%w: machine id %d outside 0..%d", ErrInvalidMachineID, machineID, maxMachine)
	}
	g.resetThreshold = (int32(int64(1)<<uint(g.sequenceBits)) - 1) >> 1

	parser, err := NewRadixStateParser(base, g.timestampBits, g.machineBits, g.sequenceBits)
	if err != nil {
		return nil, err
	}
	g.parser = *parser
	return g, nil
}

// NextState implements CosID's RadixCosIdGenerator.generateAsState: the same
// sequence reset / overflow handling as the snowflake, but without an epoch
// offset and without bit packing.
func (g *StringCosIDGenerator) NextState() (CosIDState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock()
	if now < g.lastTimestamp {
		return CosIDState{}, fmt.Errorf("%w: last %d, now %d", ErrClockMovedBackwards, g.lastTimestamp, now)
	}

	maxSequence := int32(int64(1)<<uint(g.sequenceBits)) - 1
	if now > g.lastTimestamp && g.sequence >= g.resetThreshold {
		g.sequence = 0
	}
	g.sequence = (g.sequence + 1) & maxSequence
	if g.sequence == 0 {
		for {
			next := g.clock()
			if next > g.lastTimestamp {
				now = next
				break
			}
			time.Sleep(50 * time.Microsecond)
		}
	}

	maxTimestamp := int64(1)<<uint(g.timestampBits) - 1
	if now > maxTimestamp {
		return CosIDState{}, fmt.Errorf("%w: timestamp %d exceeds %d bits", ErrExhausted, now, g.timestampBits)
	}
	g.lastTimestamp = now
	return CosIDState{Timestamp: now, MachineID: g.machineID, Sequence: g.sequence}, nil
}

// GenerateAsString implements StringGenerator.
func (g *StringCosIDGenerator) GenerateAsString(ctx context.Context) (string, error) {
	state, err := g.NextState()
	if err != nil {
		return "", err
	}
	return g.parser.Format(state), nil
}

// Next is not supported: this generator produces strings, not int64 ids.
func (g *StringCosIDGenerator) Next(context.Context) (int64, error) {
	return 0, fmt.Errorf("%w: string generator has no int64 representation", ErrInvalidConfig)
}

// Parser exposes the state parser (useful for tests and tooling).
func (g *StringCosIDGenerator) Parser() RadixStateParser { return g.parser }

// StateOfString parses an id produced by a generator with the same layout.
func (g *StringCosIDGenerator) StateOfString(id string) (CosIDState, error) {
	return g.parser.Parse(strings.TrimSpace(id))
}
