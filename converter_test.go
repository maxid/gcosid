package gcosid

import (
	"testing"
	"time"
)

func TestRadixFormattingMatchesCosIDAlphabet(t *testing.T) {
	r62, err := NewRadix(62, 0, false)
	if err != nil {
		t.Fatalf("radix62: %v", err)
	}

	// 10 -> "A", 35 -> "Z", 36 -> "a", 61 -> "z", 62 -> "10".
	cases := map[int64]string{10: "A", 35: "Z", 36: "a", 61: "z", 62: "10", 0: "0"}
	for id, want := range cases {
		if got := r62.Format(id); got != want {
			t.Errorf("Format(%d) = %q, want %q", id, got, want)
		}
	}
}

func TestRadixRoundTrip(t *testing.T) {
	for _, base := range []int{2, 16, 36, 62} {
		r, err := NewRadix(base, 0, false)
		if err != nil {
			t.Fatalf("radix %d: %v", base, err)
		}
		for _, id := range []int64{0, 1, 7, 42, 1 << 20, 1<<40 - 1} {
			got, err := r.Parse(r.Format(id))
			if err != nil {
				t.Fatalf("base %d parse %q: %v", base, r.Format(id), err)
			}
			if got != id {
				t.Errorf("base %d round trip %d -> %d", base, id, got)
			}
		}
	}
}

func TestRadixPadStart(t *testing.T) {
	r, err := NewRadix(62, 11, true)
	if err != nil {
		t.Fatalf("radix: %v", err)
	}
	if got := r.Format(1); got != "00000000001" {
		t.Fatalf("padded = %q, want 00000000001", got)
	}

	unpadded, _ := NewRadix(62, 11, false)
	if got := unpadded.Format(1); got != "1" {
		t.Fatalf("unpadded = %q, want 1", got)
	}
}

func TestRadixRejectsInvalidInput(t *testing.T) {
	if _, err := NewRadix(1, 0, false); err == nil {
		t.Fatal("base 1 must be rejected")
	}
	if _, err := NewRadix(63, 0, false); err == nil {
		t.Fatal("base 63 must be rejected")
	}
	r, _ := NewRadix(10, 0, false)
	if _, err := r.Parse("12a"); err == nil {
		t.Fatal("non-digit input must be rejected")
	}
}

func TestPrefixSuffixAndGrouped(t *testing.T) {
	decimal := ToString{}
	if got := (Prefix{Prefix: "ORD", Delimiter: "-", Actual: decimal}).Format(42); got != "ORD-42" {
		t.Errorf("prefix = %q", got)
	}
	if got := (Suffix{Suffix: "X", Delimiter: "_", Actual: decimal}).Format(42); got != "42_X" {
		t.Errorf("suffix = %q", got)
	}
	if got := (GroupedPrefix{Group: "2026", Delimiter: "-", Actual: decimal}).Format(42); got != "2026-42" {
		t.Errorf("grouped = %q", got)
	}
}

func TestDatePrefix(t *testing.T) {
	d := DatePrefix{
		Layout:    "20060102",
		Delimiter: "-",
		Actual:    ToString{},
		Now:       func() time.Time { return time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC) },
	}
	if got := d.Format(7); got != "20261030-7" {
		t.Fatalf("date prefix = %q", got)
	}
}

func TestSnowflakeFriendlyRendering(t *testing.T) {
	l := MillisecondLayout
	id, err := l.Compose(CosIdEpoch+1000, 3, 5)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}

	got := SnowflakeFriendly{Layout: l}.Format(id)
	want := "1577203201000-3-5"
	if got != want {
		t.Fatalf("friendly = %q, want %q", got, want)
	}

	// Rendering must not change the numeric id.
	if state := l.Parse(id); state.MachineID != 3 || state.Sequence != 5 {
		t.Fatalf("state = %+v", state)
	}
}

func TestToString(t *testing.T) {
	if got := (ToString{}).Format(-42); got != "-42" {
		t.Fatalf("toString = %q", got)
	}
}
