package gcosid

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Sharding maps a value (or a value range) to logical node names — CosID's
// sharding package. It is how a deployment spreads snowflake ids of different
// time windows over different storage shards.
type Sharding[T any] interface {
	// Sharding returns the node for a single value.
	Sharding(value T) (string, error)
	// ShardingRange returns the nodes a range touches, deduplicated.
	ShardingRange(lower, upper T, hasLower, hasUpper bool) []string
	// EffectiveNodes lists every node the sharding can produce.
	EffectiveNodes() []string
}

// ModCycle spreads values over `divisor` nodes by modulo — CosID's ModCycle. The
// node name is `prefix + (value mod divisor)`.
type ModCycle struct {
	Divisor int
	Prefix  string

	nodes []string
}

// NewModCycle creates a modulo cycle sharding.
func NewModCycle(divisor int, prefix string) (*ModCycle, error) {
	if divisor <= 0 {
		return nil, fmt.Errorf("%w: divisor must be positive", ErrInvalidConfig)
	}
	nodes := make([]string, divisor)
	for i := range nodes {
		nodes[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return &ModCycle{Divisor: divisor, Prefix: prefix, nodes: nodes}, nil
}

// Sharding implements Sharding for int64 values.
func (m *ModCycle) Sharding(value int64) (string, error) {
	idx := int(value % int64(m.Divisor))
	if idx < 0 {
		idx += m.Divisor
	}
	return m.nodes[idx], nil
}

// ShardingRange implements Sharding. A full (unbounded) range covers every node.
func (m *ModCycle) ShardingRange(lower, upper int64, hasLower, hasUpper bool) []string {
	if !hasLower && !hasUpper {
		return m.EffectiveNodes()
	}
	lo, hi := lower, upper
	if !hasLower {
		lo = 0
	}
	if !hasUpper {
		hi = lo + int64(m.Divisor) - 1
	}
	if hi < lo {
		return nil
	}

	seen := map[string]bool{}
	var out []string
	for v := lo; v <= hi; v++ {
		node, _ := m.Sharding(v)
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
		if len(out) == m.Divisor {
			break
		}
	}
	sort.Strings(out)
	return out
}

// EffectiveNodes implements Sharding.
func (m *ModCycle) EffectiveNodes() []string {
	out := make([]string, len(m.nodes))
	copy(out, m.nodes)
	return out
}

// IntervalStep is one step of an interval timeline: `Amount` units of `Unit`,
// floored to the unit — CosID's IntervalStep.
type IntervalStep struct {
	// Kind is the calendar unit; months and years are not fixed durations, so
	// arithmetic goes through the calendar rather than a time.Duration.
	Kind   StepUnit
	Amount int
}

// StepUnit is the calendar unit of an interval step.
type StepUnit string

// Supported step units.
const (
	StepYear   StepUnit = "year"
	StepMonth  StepUnit = "month"
	StepDay    StepUnit = "day"
	StepHour   StepUnit = "hour"
	StepMinute StepUnit = "minute"
)

// NewIntervalStep validates a step.
func NewIntervalStep(unit StepUnit, amount int) (IntervalStep, error) {
	switch unit {
	case StepYear, StepMonth, StepDay, StepHour, StepMinute:
	default:
		return IntervalStep{}, fmt.Errorf("%w: unknown step unit %q", ErrInvalidConfig, unit)
	}
	if amount <= 0 {
		return IntervalStep{}, fmt.Errorf("%w: step amount must be positive", ErrInvalidConfig)
	}
	return IntervalStep{Kind: unit, Amount: amount}, nil
}

// Next advances one step.
func (s IntervalStep) Next(t time.Time) time.Time {
	switch s.Kind {
	case StepYear:
		return t.AddDate(s.Amount, 0, 0)
	case StepMonth:
		return t.AddDate(0, s.Amount, 0)
	case StepDay:
		return t.AddDate(0, 0, s.Amount)
	case StepHour:
		return t.Add(time.Duration(s.Amount) * time.Hour)
	default:
		return t.Add(time.Duration(s.Amount) * time.Minute)
	}
}

// FloorUnit truncates to the start of the unit, like IntervalStep.floorUnit.
func (s IntervalStep) FloorUnit(t time.Time) time.Time {
	switch s.Kind {
	case StepYear:
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
	case StepMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
	case StepDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	case StepHour:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	default:
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	}
}

// Offset counts whole steps between start and t (IntervalStep.offsetUnit).
func (s IntervalStep) Offset(start, t time.Time) int {
	offset := 0
	cursor := start
	if !t.After(start) {
		for t.Before(cursor) {
			cursor = s.back(cursor)
			offset--
		}
		return offset
	}
	for cursor.AddDate(0, 0, 0).Before(t) {
		next := s.Next(cursor)
		if next.After(t) {
			break
		}
		cursor = next
		offset++
	}
	return offset
}

func (s IntervalStep) back(t time.Time) time.Time {
	switch s.Kind {
	case StepYear:
		return t.AddDate(-s.Amount, 0, 0)
	case StepMonth:
		return t.AddDate(0, -s.Amount, 0)
	case StepDay:
		return t.AddDate(0, 0, -s.Amount)
	case StepHour:
		return t.Add(-time.Duration(s.Amount) * time.Hour)
	default:
		return t.Add(-time.Duration(s.Amount) * time.Minute)
	}
}

// IntervalNode is one node of a timeline with the instant it starts at.
type IntervalNode struct {
	Lower time.Time
	Node  string
}

// IntervalTimeline shards by time window: node names are `prefix + formatted
// window start`, so each window can live in its own storage — CosID's
// IntervalTimeline.
type IntervalTimeline struct {
	Prefix   string
	Layout   string
	Step     IntervalStep
	Lower    time.Time
	Upper    time.Time
	Location *time.Location

	nodes []IntervalNode
}

// NewIntervalTimeline builds a timeline covering [lower, upper] in steps.
func NewIntervalTimeline(prefix, layout string, step IntervalStep, lower, upper time.Time) (*IntervalTimeline, error) {
	if upper.Before(lower) {
		return nil, fmt.Errorf("%w: upper bound %s is before lower bound %s", ErrInvalidConfig, upper, lower)
	}
	if layout == "" {
		layout = "20060102"
	}

	tl := &IntervalTimeline{Prefix: prefix, Layout: layout, Step: step, Lower: lower, Upper: upper}

	cursor := step.FloorUnit(lower)
	last := step.FloorUnit(upper)
	for !cursor.After(last) {
		tl.nodes = append(tl.nodes, IntervalNode{Lower: cursor, Node: prefix + cursor.Format(layout)})
		cursor = step.Next(cursor)
	}
	if len(tl.nodes) == 0 {
		return nil, fmt.Errorf("%w: timeline has no intervals", ErrInvalidConfig)
	}
	return tl, nil
}

// Sharding implements Sharding for instants. Values outside the timeline are an
// error rather than silently landing on the first or last node.
func (tl *IntervalTimeline) Sharding(value time.Time) (string, error) {
	if value.Before(tl.nodes[0].Lower) || value.After(tl.Upper) {
		return "", fmt.Errorf("%w: %s is outside the timeline %s..%s", ErrInvalidConfig, value, tl.Lower, tl.Upper)
	}
	offset := tl.Step.Offset(tl.nodes[0].Lower, value)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(tl.nodes) {
		offset = len(tl.nodes) - 1
	}
	return tl.nodes[offset].Node, nil
}

// ShardingRange implements Sharding.
func (tl *IntervalTimeline) ShardingRange(lower, upper time.Time, hasLower, hasUpper bool) []string {
	if !hasLower && !hasUpper {
		return tl.EffectiveNodes()
	}

	var out []string
	for _, node := range tl.nodes {
		next := tl.Step.Next(node.Lower)
		// A node covers [Lower, next); skip it when that window ends at or
		// before the range starts (touching the boundary is not overlapping).
		if hasLower && !next.After(lower) {
			continue
		}
		if hasUpper && node.Lower.After(upper) {
			continue
		}
		out = append(out, node.Node)
	}
	return out
}

// EffectiveNodes implements Sharding.
func (tl *IntervalTimeline) EffectiveNodes() []string {
	out := make([]string, len(tl.nodes))
	for i, node := range tl.nodes {
		out[i] = node.Node
	}
	return out
}

// CachedSharding memoises range lookups — CosID's CachedSharding. Precise lookups
// are cheap enough to pass through.
type CachedSharding[T any] struct {
	Actual Sharding[T]

	mu    sync.RWMutex
	cache map[string][]string
}

// NewCachedSharding wraps a sharding with a range cache.
func NewCachedSharding[T any](actual Sharding[T]) *CachedSharding[T] {
	return &CachedSharding[T]{Actual: actual, cache: map[string][]string{}}
}

// Sharding implements Sharding.
func (c *CachedSharding[T]) Sharding(value T) (string, error) { return c.Actual.Sharding(value) }

// ShardingRange implements Sharding with memoisation. The key renders the range
// bounds as strings, which is stable because bounds are compared, never mutated.
func (c *CachedSharding[T]) ShardingRange(lower, upper T, hasLower, hasUpper bool) []string {
	key := fmt.Sprintf("%v|%v|%t|%t", lower, upper, hasLower, hasUpper)

	c.mu.RLock()
	if nodes, ok := c.cache[key]; ok {
		c.mu.RUnlock()
		return nodes
	}
	c.mu.RUnlock()

	nodes := c.Actual.ShardingRange(lower, upper, hasLower, hasUpper)

	c.mu.Lock()
	c.cache[key] = nodes
	c.mu.Unlock()
	return nodes
}

// EffectiveNodes implements Sharding.
func (c *CachedSharding[T]) EffectiveNodes() []string { return c.Actual.EffectiveNodes() }

// JoinNodes renders a node list for logs and diagnostics.
func JoinNodes(nodes []string) string { return strings.Join(nodes, ",") }
