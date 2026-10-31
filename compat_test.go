package gcosid

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// clockSyncFixture is a generator that reports a backwards clock once, then works.
type clockSyncFixture struct{ calls int }

func (f *clockSyncFixture) Next(context.Context) (int64, error) {
	f.calls++
	if f.calls == 1 {
		return 0, fmt.Errorf("%w: last 10, now 5", ErrClockMovedBackwards)
	}
	return int64(f.calls), nil
}

func TestClockSyncGeneratorRetriesAfterWaiting(t *testing.T) {
	actual := &clockSyncFixture{}
	sync := &DefaultClockSync{
		SpinThreshold:   1,
		BrokenThreshold: 500,
		Now:             func() int64 { return time.Now().UnixMilli() },
		Sleep:           func(time.Duration) {},
	}
	wrapped, err := NewClockSyncGenerator(actual, sync)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	id, err := wrapped.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if id == 0 {
		t.Fatal("id was not produced after synchronising")
	}
	if actual.calls != 2 {
		t.Fatalf("calls = %d, want 2 (one retry)", actual.calls)
	}
}

func TestClockSyncGeneratorPassesOtherErrorsThrough(t *testing.T) {
	wrapped, _ := NewClockSyncGenerator(&errorGenerator{err: ErrAllocatorUnavailable}, nil)
	if _, err := wrapped.Next(context.Background()); !errors.Is(err, ErrAllocatorUnavailable) {
		t.Fatalf("err = %v, want the original error", err)
	}
}

type errorGenerator struct{ err error }

func (e *errorGenerator) Next(context.Context) (int64, error) { return 0, e.err }

type stringFixture struct{ calls int }

func (s *stringFixture) GenerateAsString(context.Context) (string, error) {
	s.calls++
	if s.calls == 1 {
		return "", fmt.Errorf("%w: last 1, now 0", ErrClockMovedBackwards)
	}
	return "ok", nil
}

func TestStringClockSyncGenerator(t *testing.T) {
	actual := &stringFixture{}
	wrapped, err := NewStringClockSyncGenerator(actual, &DefaultClockSync{
		SpinThreshold: 1, BrokenThreshold: 500,
		Now: func() int64 { return time.Now().UnixMilli() }, Sleep: func(time.Duration) {},
	})
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	got, err := wrapped.GenerateAsString(context.Background())
	if err != nil || got != "ok" {
		t.Fatalf("got %q, %v", got, err)
	}
	if actual.calls != 2 {
		t.Fatalf("calls = %d, want 2", actual.calls)
	}
}

// StringSegmentID / StringSnowflakeID keep the numeric view and expose the stat
// tree including the converter.
func TestStringIDViews(t *testing.T) {
	seg, _ := NewSegmentGenerator("order", 10, NewMemoryAllocator(10), WithPrefetchRatio(0))
	defer seg.Close(context.Background())

	radix62, _ := NewRadix(62, 0, false)
	stringSeg, err := NewStringSegmentID(seg, radix62)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}

	got, err := stringSeg.GenerateAsString(context.Background())
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if got != "1" {
		t.Fatalf("id = %q, want 1", got)
	}
	if stringSeg.Current().MaxID == 0 {
		t.Fatal("current segment not exposed")
	}
	stat := stringSeg.Stat()
	if stat.Kind != "segment" || stat.Actual == nil || stat.Actual.Kind != "radix62" {
		t.Fatalf("stat = %s", stat.String())
	}

	snow, _ := NewSnowflake(MillisecondLayout, 1, WithSnowflakeClock(func() int64 { return CosIdEpoch + 1 }))
	stringSnow, err := NewStringSnowflakeID(snow, radix62)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if _, err := stringSnow.GenerateAsString(context.Background()); err != nil {
		t.Fatalf("generate: %v", err)
	}
	if stringSnow.Layout().MachineBits != 10 {
		t.Fatal("layout not exposed")
	}
}

func TestStringIDViewsValidate(t *testing.T) {
	radix62, _ := NewRadix(62, 0, false)
	if _, err := NewStringSegmentID(nil, radix62); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil generator err = %v", err)
	}
	seg, _ := NewSegmentGenerator("order", 10, NewMemoryAllocator(10))
	defer seg.Close(context.Background())
	if _, err := NewStringSegmentID(seg, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil converter err = %v", err)
	}
}

// The local state cache is what lets a restart reuse its machine id without a
// remote round trip (CosID's LocalMachineStateStorage).
func TestLocalStateStoragePersistsAcrossRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "machine-state")

	first, err := NewLocalStateStorage(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.Set("order", "node-a", MachineState{MachineID: 7, LastTimestamp: 1234}); err != nil {
		t.Fatalf("set: %v", err)
	}

	second, err := NewLocalStateStorage(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	state, ok := second.Get("order", "node-a")
	if !ok || state.MachineID != 7 || state.LastTimestamp != 1234 {
		t.Fatalf("state = %+v, %v", state, ok)
	}

	if err := second.Remove("order", "node-a"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	third, _ := NewLocalStateStorage(path)
	if _, ok := third.Get("order", "node-a"); ok {
		t.Fatal("entry survived remove")
	}
}

func TestLocalStateStorageToleratesCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "machine-state")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	storage, err := NewLocalStateStorage(path)
	if err != nil {
		t.Fatalf("corrupt cache must not be fatal: %v", err)
	}
	if _, ok := storage.Get("order", "node-a"); ok {
		t.Fatal("corrupt cache produced an entry")
	}
}

func TestLocalHostAddress(t *testing.T) {
	host, err := LocalHostAddress{}.HostAddress()
	if err != nil {
		t.Fatalf("host address: %v", err)
	}
	if host == "" {
		t.Fatal("empty host address")
	}

	instance, err := InstanceIDFrom(nil, 8080, true)
	if err != nil {
		t.Fatalf("instance: %v", err)
	}
	if instance.ID == "" || !instance.Stable {
		t.Fatalf("instance = %+v", instance)
	}
}

// Sharding can start from an id instead of a timestamp: SnowflakeConverter maps
// an id (or its rendered form) back to the instant it was generated.
func TestSnowflakeConverter(t *testing.T) {
	layout := MillisecondLayout
	at := CosIdEpoch + 60_000
	id, err := layout.Compose(at, 3, 5)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}

	converter := SnowflakeConverter{Layout: layout}
	got, err := converter.ToTime(id)
	if err != nil {
		t.Fatalf("convert id: %v", err)
	}
	if got.UnixMilli() != at {
		t.Fatalf("time = %d, want %d", got.UnixMilli(), at)
	}

	radix62, _ := NewRadix(62, 0, false)
	converter.Radix = radix62
	got, err = converter.ToTime(radix62.Format(id))
	if err != nil {
		t.Fatalf("convert string: %v", err)
	}
	if got.UnixMilli() != at {
		t.Fatalf("time from string = %d, want %d", got.UnixMilli(), at)
	}

	if _, err := converter.ToTime(1.5); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("float err = %v", err)
	}
}

func TestStandardConverter(t *testing.T) {
	millis := int64(1_700_000_000_000)
	c := StandardConverter{}
	got, err := c.ToTime(millis)
	if err != nil || got.UnixMilli() != millis {
		t.Fatalf("millis = %v, %v", got, err)
	}

	seconds := StandardConverter{SecondEpoch: true}
	got, err = seconds.ToTime(1_700_000_000)
	if err != nil || got.Unix() != 1_700_000_000 {
		t.Fatalf("seconds = %v, %v", got, err)
	}

	got, err = c.ToTime("2026-10-31T00:00:00Z")
	if err != nil || got.UTC().Year() != 2026 {
		t.Fatalf("rfc3339 = %v, %v", got, err)
	}

	if _, err := c.ToTime("31/10/2026"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("bad string err = %v", err)
	}
	if _, err := c.ToTime(struct{}{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported err = %v", err)
	}
}
