package gcosid

import (
	"errors"
	"testing"
	"time"
)

func TestModCycleSharding(t *testing.T) {
	cycle, err := NewModCycle(4, "node_")
	if err != nil {
		t.Fatalf("cycle: %v", err)
	}

	for value, want := range map[int64]string{0: "node_0", 1: "node_1", 3: "node_3", 4: "node_0", 7: "node_3"} {
		got, err := cycle.Sharding(value)
		if err != nil {
			t.Fatalf("sharding %d: %v", value, err)
		}
		if got != want {
			t.Errorf("Sharding(%d) = %q, want %q", value, got, want)
		}
	}

	if nodes := cycle.EffectiveNodes(); len(nodes) != 4 {
		t.Fatalf("nodes = %v", nodes)
	}
}

func TestModCycleShardingRange(t *testing.T) {
	cycle, _ := NewModCycle(4, "node_")

	if nodes := cycle.ShardingRange(0, 0, false, false); len(nodes) != 4 {
		t.Fatalf("full range nodes = %v, want all four", nodes)
	}
	// 0..1 touches only two nodes.
	if nodes := cycle.ShardingRange(0, 1, true, true); len(nodes) != 2 {
		t.Fatalf("0..1 nodes = %v, want 2", nodes)
	}
	// An inverted range is empty, not an error.
	if nodes := cycle.ShardingRange(5, 1, true, true); len(nodes) != 0 {
		t.Fatalf("inverted range nodes = %v, want none", nodes)
	}
}

func TestNewModCycleRejectsBadDivisor(t *testing.T) {
	if _, err := NewModCycle(0, "n"); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("err = %v", err)
	}
}

func TestIntervalStepArithmetic(t *testing.T) {
	month, _ := NewIntervalStep(StepMonth, 1)
	start := time.Date(2026, 1, 15, 13, 45, 0, 0, time.UTC)

	if got := month.FloorUnit(start); !got.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("floor = %s", got)
	}
	if got := month.Next(start); !got.Equal(time.Date(2026, 2, 15, 13, 45, 0, 0, time.UTC)) {
		t.Fatalf("next = %s", got)
	}
	if got := month.Offset(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)); got != 3 {
		t.Fatalf("offset = %d, want 3", got)
	}
	if got := month.Offset(time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); got != -3 {
		t.Fatalf("negative offset = %d, want -3", got)
	}

	if _, err := NewIntervalStep("century", 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unit err = %v", err)
	}
	if _, err := NewIntervalStep(StepDay, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("amount err = %v", err)
	}
}

// A monthly timeline names each node after the month it covers, which is how
// per-month storage shards are addressed.
func TestIntervalTimelineMonthly(t *testing.T) {
	step, _ := NewIntervalStep(StepMonth, 1)
	tl, err := NewIntervalTimeline("order_", "200601",
		step,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 30, 23, 59, 59, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}

	want := []string{"order_202601", "order_202602", "order_202603", "order_202604"}
	got := tl.EffectiveNodes()
	if len(got) != len(want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("nodes = %v, want %v", got, want)
		}
	}

	cases := map[time.Time]string{
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC):    "order_202601",
		time.Date(2026, 2, 28, 23, 0, 0, 0, time.UTC):  "order_202602",
		time.Date(2026, 4, 30, 23, 59, 0, 0, time.UTC): "order_202604",
	}
	for at, wantNode := range cases {
		node, err := tl.Sharding(at)
		if err != nil {
			t.Fatalf("sharding %s: %v", at, err)
		}
		if node != wantNode {
			t.Errorf("Sharding(%s) = %q, want %q", at, node, wantNode)
		}
	}
}

// A value outside the timeline must fail instead of silently landing on an edge
// node: that would write ids to the wrong shard.
func TestIntervalTimelineRejectsOutOfRange(t *testing.T) {
	step, _ := NewIntervalStep(StepMonth, 1)
	tl, _ := NewIntervalTimeline("order_", "200601", step,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	)

	if _, err := tl.Sharding(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("before timeline err = %v", err)
	}
	if _, err := tl.Sharding(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("after timeline err = %v", err)
	}
}

func TestIntervalTimelineShardingRange(t *testing.T) {
	step, _ := NewIntervalStep(StepMonth, 1)
	tl, _ := NewIntervalTimeline("m_", "200601", step,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC),
	)

	if nodes := tl.ShardingRange(time.Time{}, time.Time{}, false, false); len(nodes) != 6 {
		t.Fatalf("full range = %v, want six nodes", nodes)
	}
	nodes := tl.ShardingRange(
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC), true, true)
	if len(nodes) != 2 || nodes[0] != "m_202602" || nodes[1] != "m_202603" {
		t.Fatalf("range nodes = %v, want m_202602,m_202603", nodes)
	}
}

func TestCachedShardingMemoises(t *testing.T) {
	step, _ := NewIntervalStep(StepDay, 1)
	tl, _ := NewIntervalTimeline("d_", "20060102", step,
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
	)
	cached := NewCachedSharding[time.Time](tl)

	lower := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	upper := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)

	first := cached.ShardingRange(lower, upper, true, true)
	second := cached.ShardingRange(lower, upper, true, true)
	if len(first) != len(second) || len(first) != 3 {
		t.Fatalf("cached = %v / %v", first, second)
	}
	if cached.ShardingRange(lower, upper, true, true)[0] != first[0] {
		t.Fatal("cached result changed")
	}
	if len(cached.EffectiveNodes()) != 5 {
		t.Fatalf("nodes = %v", cached.EffectiveNodes())
	}

	if node, err := cached.Sharding(lower); err != nil || node != "d_20260101" {
		t.Fatalf("precise = %q, %v", node, err)
	}
}

func TestNewIntervalTimelineValidates(t *testing.T) {
	step, _ := NewIntervalStep(StepDay, 1)
	if _, err := NewIntervalTimeline("x", "", step,
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("inverted bounds err = %v", err)
	}
}
