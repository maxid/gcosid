package gcosid

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
