package core

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
