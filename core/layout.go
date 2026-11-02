package core

import (
	"fmt"
)

type Layout struct {
	Epoch         int64
	TimestampBits int
	MachineBits   int
	SequenceBits  int
}

// Default millisecond layout: 41 | 10 | 12.
const (
	// CosIdEpoch is CosId.COSID_EPOCH in milliseconds (2019-12-24T16:00Z).
	CosIdEpoch = int64(1577203200000)
	// CosIdEpochSecond is CosId.COSID_EPOCH_SECOND in seconds.
	CosIdEpochSecond = int64(1577203200)
	// TotalBit is the usable bit count: the sign bit stays zero.
	TotalBit = 63
	// JSMaxSafeNumberBit is SafeJSSnowflakeId.JS_MAX_SAFE_NUMBER_BIT.
	JSMaxSafeNumberBit = 53
	// JSMaxSafeNumber is SafeJSSnowflakeId.JS_MAX_SAFE_NUMBER.
	JSMaxSafeNumber = int64(9007199254740991)
)

// Layout is one snowflake bit layout: timestamp | machine | sequence.
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

// IsSafeJS reports whether ids produced by the layout stay below the JS safe
// integer limit.
func (l Layout) IsSafeJS() bool {
	return l.TimestampBits+l.MachineBits+l.SequenceBits <= JSMaxSafeNumberBit
}

// ClockSync handles a clock that moved backwards. CosID's default spins for up
// to 1ms, sleeps for up to 500ms, and refuses anything larger.

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

// SafeJS variants drop machine and sequence bits so the id stays a JS safe
// integer: milliseconds 41 | 3 | 9, seconds 31 | 3 | 19.
var (
	SafeJSMillisecondLayout = Layout{
		Epoch:         CosIdEpoch,
		TimestampBits: 41,
		MachineBits:   3,
		SequenceBits:  9,
	}
	SafeJSSecondLayout = Layout{
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

func mask(bits int) int64 {
	if bits <= 0 {
		return 0
	}
	if bits >= 63 {
		return int64(^uint64(0) >> 1)
	}
	return int64(1)<<bits - 1
}
