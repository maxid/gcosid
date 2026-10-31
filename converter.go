package gcosid

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CosID's radix alphabet (RadixIdConverter): digits, uppercase, lowercase.
const radixDigits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// converterError marks malformed input for any converter.
type converterError struct{ msg string }

func (e *converterError) Error() string { return "gcosid: " + e.msg }

// Radix renders an id in a base between 2 and 62 using CosID's alphabet order.
type Radix struct {
	Base     int
	CharSize int  // 0 means "no fixed width"
	PadStart bool // pad on the left with '0' to CharSize
}

// NewRadix validates the base and width.
func NewRadix(base, charSize int, padStart bool) (*Radix, error) {
	if base < 2 || base > len(radixDigits) {
		return nil, fmt.Errorf("%w: radix %d outside 2..%d", ErrInvalidConfig, base, len(radixDigits))
	}
	if charSize < 0 {
		return nil, fmt.Errorf("%w: char size must not be negative", ErrInvalidConfig)
	}
	return &Radix{Base: base, CharSize: charSize, PadStart: padStart}, nil
}

// Format implements StringConverter.
func (r *Radix) Format(id int64) string {
	if id < 0 {
		// CosID ids are positive; render the two's complement for diagnostics.
		return strconv.FormatInt(id, r.Base)
	}
	var buf [maxRadixLen]byte
	pos := len(buf)
	for {
		pos--
		buf[pos] = radixDigits[id%int64(r.Base)]
		id /= int64(r.Base)
		if id == 0 {
			break
		}
	}
	out := string(buf[pos:])
	if r.CharSize > 0 && len(out) < r.CharSize {
		if r.PadStart {
			out = strings.Repeat(string(radixDigits[0]), r.CharSize-len(out)) + out
		}
	}
	return out
}

const maxRadixLen = 64

// Parse is the inverse of Format.
func (r *Radix) Parse(s string) (int64, error) {
	if s == "" {
		return 0, &converterError{"empty id"}
	}
	var id int64
	for _, c := range s {
		digit := strings.IndexRune(radixDigits[:r.Base], c)
		if digit < 0 {
			return 0, &converterError{fmt.Sprintf("invalid digit %q for radix %d", c, r.Base)}
		}
		id = id*int64(r.Base) + int64(digit)
	}
	return id, nil
}

// Prefix renders `<prefix><delimiter><id>`, like PrefixIdConverter.
type Prefix struct {
	Prefix    string
	Delimiter string
	Actual    StringConverter
}

// Format implements StringConverter.
func (p Prefix) Format(id int64) string {
	return p.Prefix + p.Delimiter + p.Actual.Format(id)
}

// Suffix renders `<id><delimiter><suffix>`, like SuffixIdConverter.
type Suffix struct {
	Suffix    string
	Delimiter string
	Actual    StringConverter
}

// Format implements StringConverter.
func (s Suffix) Format(id int64) string {
	return s.Actual.Format(id) + s.Delimiter + s.Suffix
}

// DatePrefix renders `<date><delimiter><id>` with a Go time layout. CosID uses a
// date-pattern string instead of a Go layout; the caller translates it.
type DatePrefix struct {
	Layout    string
	Delimiter string
	Actual    StringConverter
	Now       func() time.Time
}

// Format implements StringConverter.
func (d DatePrefix) Format(id int64) string {
	now := time.Now
	if d.Now != nil {
		now = d.Now
	}
	return now().Format(d.Layout) + d.Delimiter + d.Actual.Format(id)
}

// GroupedPrefix renders `<group><delimiter><id>`.
type GroupedPrefix struct {
	Group     string
	Delimiter string
	Actual    StringConverter
}

// Format implements StringConverter.
func (g GroupedPrefix) Format(id int64) string {
	return g.Group + g.Delimiter + g.Actual.Format(id)
}

// ToString renders the decimal value, like ToStringIdConverter.
type ToString struct{}

// Format implements StringConverter.
func (ToString) Format(id int64) string { return strconv.FormatInt(id, 10) }

// parse is the inverse of Format.
func (ToString) parse(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, &converterError{"decimal id expected, got " + value}
	}
	return id, nil
}

// SnowflakeFriendly renders a snowflake id the way CosID's
// Millisecond/SecondSnowflakeIdStateParser does:
//
//	<yyyyMMddHHmmss[SSS]>-<machineId>-<sequence>
//
// The timestamp is a *formatted local date-time*, not the raw epoch value, and
// the zone is the parser's zone (CosID defaults to the system zone). It is a
// rendering only: the numeric id is unchanged.
type SnowflakeFriendly struct {
	Layout   Layout
	Millis   bool           // true for millisecond layouts, false for second layouts
	Location *time.Location // nil means time.Local, matching CosID's default
	PadStart bool           // CosID's padStart flag
}

// Format implements StringConverter.
func (f SnowflakeFriendly) Format(id int64) string {
	state := f.Layout.Parse(id)

	location := f.Location
	if location == nil {
		location = time.Local
	}
	instant := time.Unix(0, state.Timestamp*int64(time.Millisecond)).In(location)
	if !f.Millis {
		instant = time.Unix(state.Timestamp, 0).In(location)
	}

	stamp := instant.Format("20060102150405")
	if f.Millis {
		stamp += fmt.Sprintf("%03d", instant.Nanosecond()/int(time.Millisecond))
	}

	machine := decimalField(state.MachineID, f.Layout.MachineBits, f.PadStart)
	sequence := decimalField(state.Sequence, f.Layout.SequenceBits, f.PadStart)
	return stamp + "-" + machine + "-" + sequence
}

// decimalField renders a field, padding to the decimal width of its bit size
// when padStart is set (CosID's RadixIdConverter.maxCharSize(10, bits)).
func decimalField(value int64, bits int, pad bool) string {
	digits := maxDecimalCharSize(bits)
	text := strconv.FormatInt(value, 10)
	if pad && len(text) < digits {
		text = strings.Repeat("0", digits-len(text)) + text
	}
	return text
}

// maxDecimalCharSize is the number of decimal digits 2^bits-1 needs.
func maxDecimalCharSize(bits int) int {
	if bits <= 0 {
		return 1
	}
	max := int64(1)<<uint(bits) - 1
	digits := 0
	for max > 0 {
		digits++
		max /= 10
	}
	if digits == 0 {
		digits = 1
	}
	return digits
}
