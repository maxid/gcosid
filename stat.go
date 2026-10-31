package gcosid

import (
	"encoding/json"
	"sort"
)

// Stat describes a generator or converter for diagnostics — CosID's Stat. It is a
// small tree: a kind plus named fields, optionally pointing at the decorated
// component (`Actual`), which is how CosID renders "generator + converter".
type Stat struct {
	Kind   string         `json:"kind"`
	Fields map[string]any `json:"fields,omitempty"`
	Actual *Stat          `json:"actual,omitempty"`
}

// NewStat starts a stat of the given kind.
func NewStat(kind string) Stat { return Stat{Kind: kind, Fields: map[string]any{}} }

// Set records a field.
func (s Stat) Set(name string, value any) Stat {
	if s.Fields == nil {
		s.Fields = map[string]any{}
	}
	s.Fields[name] = value
	return s
}

// Wrap attaches the decorated component's stat.
func (s Stat) Wrap(actual *Stat) Stat {
	s.Actual = actual
	return s
}

// String renders the stat with sorted keys, so the output is stable in logs and
// golden tests.
func (s Stat) String() string {
	raw, err := json.Marshal(s)
	if err != nil {
		return s.Kind
	}
	return string(raw)
}

// FieldNames lists the recorded field names in sorted order.
func (s Stat) FieldNames() []string {
	names := make([]string, 0, len(s.Fields))
	for name := range s.Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Statistical is implemented by every component that can describe itself.
type Statistical interface {
	Stat() Stat
}

// Stat implements Statistical for a segment generator: the same fields CosID's
// SegmentIdStat exposes, so an operator sees how much of the range is left.
func (g *SegmentGenerator) Stat() Stat {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	return NewStat("segment").
		Set("namespace", g.namespace).
		Set("step", g.step).
		Set("fetchTime", g.current.FetchTime).
		Set("maxId", g.current.MaxID).
		Set("offset", g.current.Offset).
		Set("sequence", g.next).
		Set("expired", g.current.TTL != TTLForever && !g.current.Available(now)).
		Set("overflow", g.next > g.current.MaxID).
		Set("available", g.current.Available(now) && g.next <= g.current.MaxID).
		Set("ttl", g.current.TTL)
}

// Stat implements Statistical for a chain generator.
func (g *ChainGenerator) Stat() Stat {
	now := g.now()
	nodes, gap := 0, g.gapAhead()
	for node := g.head.Load(); node != nil; node = node.next.Load() {
		nodes++
	}

	head := g.head.Load()
	stat := NewStat("segmentChain").
		Set("namespace", g.namespace).
		Set("step", g.step).
		Set("safeDistance", g.safeDistance).
		Set("prefetchDistance", g.prefetchDistance.Load()).
		Set("segments", nodes).
		Set("availableSegments", gap)
	if head != nil {
		stat = stat.
			Set("headVersion", head.version).
			Set("headMaxId", head.segment.MaxID).
			Set("headOffset", head.segment.Offset).
			Set("headSequence", head.seq.Load()).
			Set("headAvailable", head.segment.Available(now) && head.seq.Load() < head.segment.MaxID)
	}
	return stat
}

// Stat implements Statistical for a snowflake generator.
func (g *SnowflakeGenerator) Stat() Stat {
	g.mu.Lock()
	defer g.mu.Unlock()

	return NewStat("snowflake").
		Set("epoch", g.layout.Epoch).
		Set("timestampBit", g.layout.TimestampBits).
		Set("machineBit", g.layout.MachineBits).
		Set("sequenceBit", g.layout.SequenceBits).
		Set("safeJavascript", g.layout.IsSafeJS()).
		Set("machineId", g.machineID).
		Set("lastTimestamp", g.lastTimestamp).
		Set("sequence", g.sequence)
}

// Stat implements Statistical for the CosID string family.
func (g *StringCosIDGenerator) Stat() Stat {
	return NewStat("cosid").
		Set("base", g.base).
		Set("timestampBit", g.timestampBits).
		Set("machineBit", g.machineBits).
		Set("sequenceBit", g.sequenceBits).
		Set("machineId", g.machineID)
}

// Stat implements Statistical for the radix converter.
func (r *Radix) Stat() Stat {
	kind := "radix"
	switch r.Base {
	case 36:
		kind = "radix36"
	case 62:
		kind = "radix62"
	}
	return NewStat(kind).
		Set("radix", r.Base).
		Set("charSize", r.CharSize).
		Set("padStart", r.PadStart)
}

// Stat implements Statistical for the prefix converter.
func (p Prefix) Stat() Stat {
	return NewStat("prefix").
		Set("prefix", p.Prefix).
		Set("delimiter", p.Delimiter)
}

// Stat implements Statistical for the suffix converter.
func (s Suffix) Stat() Stat {
	return NewStat("suffix").
		Set("suffix", s.Suffix).
		Set("delimiter", s.Delimiter)
}

// Stat implements Statistical for the date-prefix converter.
func (d DatePrefix) Stat() Stat {
	return NewStat("datePrefix").
		Set("layout", d.Layout).
		Set("delimiter", d.Delimiter)
}

// Stat implements Statistical for the grouped-prefix converter.
func (g GroupedPrefix) Stat() Stat {
	return NewStat("groupedPrefix").
		Set("group", g.Group).
		Set("delimiter", g.Delimiter)
}

// Stat implements Statistical for the snowflake-friendly converter.
func (f SnowflakeFriendly) Stat() Stat {
	return NewStat("snowflakeFriendly").
		Set("millis", f.Millis).
		Set("padStart", f.PadStart)
}

// Stat implements Statistical for the decimal converter.
func (ToString) Stat() Stat { return NewStat("toString") }
