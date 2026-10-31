package gcosid

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAtomicGeneratorCounts(t *testing.T) {
	g := NewAtomicGenerator()
	for want := int64(1); want <= 3; want++ {
		if id, _ := g.Next(context.Background()); id != want {
			t.Fatalf("id = %d, want %d", id, want)
		}
	}
}

func TestUUIDGeneratorProducesV4(t *testing.T) {
	g := UUIDGenerator{}
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := g.GenerateAsString(context.Background())
		if err != nil {
			t.Fatalf("uuid: %v", err)
		}
		if len(id) != 36 || strings.Count(id, "-") != 4 {
			t.Fatalf("uuid = %q, want 8-4-4-4-12", id)
		}
		if id[14] != '4' {
			t.Fatalf("uuid %q is not version 4", id)
		}
		if v := id[19]; v != '8' && v != '9' && v != 'a' && v != 'b' {
			t.Fatalf("uuid %q has the wrong variant", id)
		}
		if seen[id] {
			t.Fatalf("duplicate uuid %q", id)
		}
		seen[id] = true
	}

	if _, err := g.Next(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Next err = %v, want ErrInvalidConfig", err)
	}
}

func TestIntegerGeneratorRejectsOverflow(t *testing.T) {
	g := IntegerGenerator{Actual: NewAtomicGenerator()}
	if _, err := g.NextAsInt(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}

	big := IntegerGenerator{Actual: fixedGenerator(1 << 40)}
	if _, err := big.NextAsInt(context.Background()); !errors.Is(err, ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}
	if _, err := big.GenerateAsString(context.Background()); !errors.Is(err, ErrExhausted) {
		t.Fatalf("string err = %v, want ErrExhausted", err)
	}
}

func TestStringOfUsesTheConverter(t *testing.T) {
	radix36, _ := NewRadix(36, 0, false)
	gen := StringOf(fixedGenerator(35), radix36)

	got, err := gen.GenerateAsString(context.Background())
	if err != nil {
		t.Fatalf("string: %v", err)
	}
	if got != "Z" {
		t.Fatalf("id = %q, want Z (35 in base36)", got)
	}
}

// The CosID string family renders fixed-width padded fields, so the total length
// never changes: timestamp 44 bits, machine 20, sequence 16.
func TestStringCosIDLayoutWidths(t *testing.T) {
	cases := map[int]int{36: 17, 62: 15}
	for base, wantLen := range cases {
		parser, err := NewRadixStateParser(base, CosIDStringTimestampBits, CosIDStringMachineBits, CosIDStringSequenceBits)
		if err != nil {
			t.Fatalf("parser: %v", err)
		}
		got := parser.Format(CosIDState{Timestamp: 1, MachineID: 1, Sequence: 1})
		if len(got) != wantLen {
			t.Errorf("base %d: %q has length %d, want %d", base, got, len(got), wantLen)
		}
	}
}

func TestStringCosIDGeneratorRoundTrip(t *testing.T) {
	now := int64(1_700_000_000_000)
	g, err := NewStringCosIDGenerator(62, 7, WithStringCosIDClock(func() int64 { return now }))
	if err != nil {
		t.Fatalf("generator: %v", err)
	}

	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		id, err := g.GenerateAsString(context.Background())
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true

		state, err := g.StateOfString(id)
		if err != nil {
			t.Fatalf("parse %q: %v", id, err)
		}
		if state.Timestamp != now || state.MachineID != 7 {
			t.Fatalf("state = %+v, want timestamp %d machine 7", state, now)
		}
		if _, err := g.Next(context.Background()); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Next err = %v, want ErrInvalidConfig", err)
		}
	}
}

func TestStringCosIDGeneratorValidation(t *testing.T) {
	if _, err := NewStringCosIDGenerator(16, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("base err = %v", err)
	}
	if _, err := NewStringCosIDGenerator(62, -1); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("machine err = %v", err)
	}
	maxMachine := int32(1)<<CosIDStringMachineBits - 1
	if _, err := NewStringCosIDGenerator(62, maxMachine+1); !errors.Is(err, ErrInvalidMachineID) {
		t.Fatalf("machine overflow err = %v", err)
	}
}

func fixedGenerator(v int64) Generator { return fixed(v) }

type fixed int64

func (f fixed) Next(context.Context) (int64, error) { return int64(f), nil }
