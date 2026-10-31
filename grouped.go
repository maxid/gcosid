package gcosid

import (
	"context"
	"time"
)

// GroupedKey labels an allocation period — CosID's GroupedKey. TTLAt is the unix
// second at which the group ends, so a caller can tell when a counter restarts.
type GroupedKey struct {
	Key   string
	TTLAt int64
}

// NeverGrouped is GroupedKey.NEVER: an ungrouped allocation.
var NeverGrouped = GroupedKey{Key: "", TTLAt: TTLForever}

// TTL returns how many seconds the group has left.
func (k GroupedKey) TTL(now time.Time) int64 { return k.TTLAt - now.Unix() }

// GroupBy resolves the group of a moment in time — CosID's GroupBySupplier.
type GroupBy interface {
	Group(now time.Time) GroupedKey
}

// Period is the granularity of a date-based group.
type Period string

// Supported periods, matching CosID's Year / YearMonth / YearMonthDay suppliers.
const (
	PeriodYear      Period = "year"
	PeriodYearMonth Period = "year-month"
	PeriodYearDay   Period = "year-month-day"
)

// DateGroup groups allocations by calendar period, like CosID's
// AbstractDateGroupBySupplier: the key is the formatted period and the TTL ends
// with it, so the next period starts a fresh counter.
type DateGroup struct {
	Period   Period
	Layout   string // Go layout; defaults follow the period
	Location *time.Location
}

// NewDateGroup builds a date-based grouping.
func NewDateGroup(period Period) (*DateGroup, error) {
	switch period {
	case PeriodYear, PeriodYearMonth, PeriodYearDay:
		return &DateGroup{Period: period}, nil
	default:
		return nil, &configError{"unknown period " + string(period)}
	}
}

type configError struct{ msg string }

func (e *configError) Error() string        { return "gcosid: " + e.msg }
func (e *configError) Is(target error) bool { return target == ErrInvalidConfig }
func (e *configError) Unwrap() error        { return ErrInvalidConfig }

// Group implements GroupBy, filling the key and setting TTLAt to the last second
// of the period (CosID uses LocalTime.MAX, i.e. the period's very end).
func (g *DateGroup) Group(now time.Time) GroupedKey {
	location := g.Location
	if location == nil {
		location = now.Location()
	}
	t := now.In(location)

	var (
		layout string
		end    time.Time
	)
	switch g.Period {
	case PeriodYear:
		layout = g.Layout
		if layout == "" {
			layout = "2006"
		}
		end = time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, location).Add(-time.Second)
	case PeriodYearMonth:
		layout = g.Layout
		if layout == "" {
			layout = "2006-01"
		}
		end = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, location).Add(-time.Second)
	default:
		layout = g.Layout
		if layout == "" {
			layout = "2006-01-02"
		}
		end = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, location).Add(-time.Second)
	}

	return GroupedKey{Key: t.Format(layout), TTLAt: end.Unix()}
}

// GroupedAllocator turns a SegmentAllocator into per-period counters: each period
// gets its own storage key `name@<group>`, so ids restart when the period rolls
// over — CosID's DefaultGroupedIdSegmentDistributor.
type GroupedAllocator struct {
	Source SegmentAllocator
	By     GroupBy
	Now    func() time.Time
}

// NextMaxId implements SegmentAllocator.
func (g *GroupedAllocator) NextMaxId(ctx context.Context, name string, step int64) (int64, error) {
	return g.Source.NextMaxId(ctx, g.NameFor(name), step)
}

// NameFor renders the storage key of the current period.
func (g *GroupedAllocator) NameFor(name string) string {
	return name + "@" + g.Key().Key
}

// Key returns the current group.
func (g *GroupedAllocator) Key() GroupedKey {
	now := time.Now
	if g.Now != nil {
		now = g.Now
	}
	return g.By.Group(now())
}

// GroupedGenerator pairs a generator with its group key, which is what a
// GroupedPrefix converter needs to render `<group>-<id>`. CosID carries the key
// in a ThreadLocal; passing it explicitly keeps this library free of ambient
// state.
type GroupedGenerator struct {
	Allocator *GroupedAllocator
	Generator *ChainGenerator
}

// NewGroupedGenerator builds a chain generator backed by per-period counters.
func NewGroupedGenerator(allocator *GroupedAllocator, step int64, opts ...ChainOption) (*GroupedGenerator, error) {
	if allocator == nil || allocator.Source == nil {
		return nil, &configError{"grouped allocator with a source is required"}
	}
	if allocator.By == nil {
		return nil, &configError{"grouped allocator needs a GroupBy"}
	}
	gen, err := NewChainGenerator(allocator.NameFor("segment"), step, allocator, opts...)
	if err != nil {
		return nil, err
	}
	return &GroupedGenerator{Allocator: allocator, Generator: gen}, nil
}

// Next returns one id together with the group it belongs to.
func (g *GroupedGenerator) Next(ctx context.Context) (int64, GroupedKey, error) {
	key := g.Allocator.Key()
	id, err := g.Generator.Next(ctx)
	return id, key, err
}

// NextString renders the id with a grouped prefix, e.g. `2026-000042`.
func (g *GroupedGenerator) NextString(ctx context.Context, delimiter string, actual StringConverter) (string, error) {
	id, key, err := g.Next(ctx)
	if err != nil {
		return "", err
	}
	return GroupedPrefix{Group: key.Key, Delimiter: delimiter, Actual: actual}.Format(id), nil
}

// Namespace implements NamedGenerator.
func (g *GroupedGenerator) Namespace() string { return g.Generator.Namespace() }

// Close releases the underlying generator.
func (g *GroupedGenerator) Close(ctx context.Context) error { return g.Generator.Close(ctx) }
