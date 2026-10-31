package gcosid

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Every component must describe itself with a stable, parseable stat — CosID
// exposes the same fields through SegmentIdStat / SnowflakeIdStat / converter
// stats, which operators read from actuator endpoints.
func TestStatIsStableJSON(t *testing.T) {
	stat := NewStat("segment").
		Set("step", 10).
		Set("available", true).
		Wrap(ptr(NewStat("radix62").Set("radix", 62)))

	raw := stat.String()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("stat %q is not valid JSON: %v", raw, err)
	}
	if decoded["kind"] != "segment" {
		t.Fatalf("kind = %v", decoded["kind"])
	}
	if _, ok := decoded["actual"]; !ok {
		t.Fatal("wrapped stat missing")
	}

	names := stat.FieldNames()
	if len(names) != 2 || names[0] != "available" || names[1] != "step" {
		t.Fatalf("field names = %v, want sorted", names)
	}
}

func ptr(s Stat) *Stat { return &s }

func TestSegmentGeneratorStat(t *testing.T) {
	gen, _ := NewSegmentGenerator("order", 10, NewMemoryAllocator(10), WithPrefetchRatio(0))
	defer gen.Close(context.Background())
	if _, err := gen.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	stat := gen.Stat()
	if stat.Kind != "segment" {
		t.Fatalf("kind = %q", stat.Kind)
	}
	if stat.Fields["namespace"] != "order" || stat.Fields["step"] != int64(10) {
		t.Fatalf("fields = %v", stat.Fields)
	}
	if stat.Fields["available"] != true || stat.Fields["overflow"] != false {
		t.Fatalf("availability = %v / %v", stat.Fields["available"], stat.Fields["overflow"])
	}
	if stat.Fields["sequence"] != int64(2) {
		t.Fatalf("sequence = %v, want 2 after one id", stat.Fields["sequence"])
	}
}

func TestChainGeneratorStat(t *testing.T) {
	gen, err := NewChainGenerator("order", 10, NewMemoryAllocator(10))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer gen.Close(context.Background())

	if _, err := gen.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	stat := gen.Stat()
	if stat.Kind != "segmentChain" {
		t.Fatalf("kind = %q", stat.Kind)
	}
	if stat.Fields["safeDistance"] != DefaultSafeDistance {
		t.Fatalf("safeDistance = %v", stat.Fields["safeDistance"])
	}
	if available, ok := stat.Fields["availableSegments"].(int); !ok || available < 1 {
		t.Fatalf("availableSegments = %v", stat.Fields["availableSegments"])
	}
}

func TestSnowflakeStatExposesLayout(t *testing.T) {
	gen, _ := NewSnowflake(MillisecondLayout, 5, WithSnowflakeClock(func() int64 { return CosIdEpoch + 1 }))
	if _, err := gen.Next(context.Background()); err != nil {
		t.Fatalf("next: %v", err)
	}

	stat := gen.Stat()
	if stat.Kind != "snowflake" {
		t.Fatalf("kind = %q", stat.Kind)
	}
	if stat.Fields["epoch"] != CosIdEpoch || stat.Fields["machineId"] != int64(5) {
		t.Fatalf("fields = %v", stat.Fields)
	}
	if stat.Fields["safeJavascript"] != false {
		t.Fatalf("41+10+12 bits must not be JS-safe: %v", stat.Fields["safeJavascript"])
	}

	safe, _ := NewSnowflake(SafeJSMillisecondLayout, 1)
	if safe.Stat().Fields["safeJavascript"] != true {
		t.Fatal("the JS-safe layout must report safeJavascript")
	}
}

func TestConverterStats(t *testing.T) {
	r64, _ := NewRadix(62, 0, false)
	r36, _ := NewRadix(36, 0, false)

	cases := []struct {
		stat Statistical
		kind string
	}{
		{r64, "radix62"},
		{r36, "radix36"},
		{Prefix{Prefix: "ORD", Delimiter: "-", Actual: ToString{}}, "prefix"},
		{Suffix{Suffix: "X", Delimiter: "_", Actual: ToString{}}, "suffix"},
		{DatePrefix{Layout: "20060102", Delimiter: "-", Actual: ToString{}}, "datePrefix"},
		{GroupedPrefix{Group: "2026", Delimiter: "-", Actual: ToString{}}, "groupedPrefix"},
		{SnowflakeFriendly{Layout: MillisecondLayout}, "snowflakeFriendly"},
		{ToString{}, "toString"},
	}
	for _, tc := range cases {
		if got := tc.stat.Stat().Kind; got != tc.kind {
			t.Errorf("kind = %q, want %q", got, tc.kind)
		}
		if !strings.Contains(tc.stat.Stat().String(), tc.kind) {
			t.Errorf("rendered stat does not mention %q", tc.kind)
		}
	}
}

func TestUncertaintyGeneratorScramblesLowBits(t *testing.T) {
	gen, err := NewUncertaintyGenerator(NewAtomicGenerator(), 8)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if gen.Bound() != 256 {
		t.Fatalf("bound = %d, want 256", gen.Bound())
	}

	seenLow := map[int64]bool{}
	for i := 0; i < 50; i++ {
		id, err := gen.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		// The original id occupies the high bits, so ordering still holds.
		original := id >> uint(gen.UncertaintyBits)
		if original != int64(i+1) {
			t.Fatalf("original = %d, want %d", original, i+1)
		}
		seenLow[id&0xff] = true
	}
	if len(seenLow) < 20 {
		t.Fatalf("low bits barely varied: %d distinct values", len(seenLow))
	}
}

// An original id that does not fit the remaining bits must fail rather than be
// silently truncated into someone else's id.
func TestUncertaintyGeneratorRejectsOverflow(t *testing.T) {
	// 16 uncertainty bits leave 47 for the original id, so 2^48 does not fit.
	gen, _ := NewUncertaintyGenerator(fixedGenerator(1<<48), 16)
	if _, err := gen.Next(context.Background()); !errors.Is(err, ErrExhausted) {
		t.Fatalf("err = %v, want ErrExhausted", err)
	}

	// The largest id that fits still works.
	fits, _ := NewUncertaintyGenerator(fixedGenerator(gen.OriginalMaxID()), 16)
	if _, err := fits.Next(context.Background()); err != nil {
		t.Fatalf("max original id: %v", err)
	}
}

func TestNewUncertaintyGeneratorValidates(t *testing.T) {
	if _, err := NewUncertaintyGenerator(nil, 8); err == nil {
		t.Fatal("nil generator must fail")
	}
	if _, err := NewUncertaintyGenerator(NewAtomicGenerator(), 0); err == nil {
		t.Fatal("zero bits must fail")
	}
	if _, err := NewUncertaintyGenerator(NewAtomicGenerator(), TotalBit); err == nil {
		t.Fatal("63 bits must fail")
	}
}

func TestUncertaintyStat(t *testing.T) {
	gen, _ := NewUncertaintyGenerator(NewAtomicGenerator(), 10)
	stat := gen.Stat()
	if stat.Kind != "uncertainty" || stat.Fields["uncertaintyBits"] != 10 {
		t.Fatalf("stat = %v", stat.Fields)
	}
}
