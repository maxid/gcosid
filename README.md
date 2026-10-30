# gcosid

Segment and snowflake ID generation in Go.

> Status: **experimental scaffold** (v0.0.0-dev).

## Scope: CosID feature parity

The target is a complete port of CosID's capabilities, not one chosen strategy.
Consumers pick a generator; the library never mixes layouts silently.

| CosID capability | Status |
|---|---|
| Segment generator: `IdSegment{offset,maxId,sequence,step,fetchTime,ttl}` arithmetic (CosID 2.15.1) | done |
| Double buffering / chain prefetch with bounded timeout | done |
| `SegmentAllocator` port + in-memory allocator | done |
| DB segment allocator: `cosid(name,last_max_id,last_fetch_time)` reserve-then-read (MySQL, Postgres) | done |
| Segment TTL / expiry abandonment, step tuning | done (TTL) / planned (step) |
| Snowflake: layout/epoch/sequence + ms, second and JS-safe variants | done |
| Machine-id distribution: manual id, leased slots (in-memory + JDBC `cosid_machine`) | done (Redis adapter pending) |
| Clock-moved-backwards policy: strict error or spin/sleep synchroniser | done |
| String converters: radix 2..62, prefix, suffix, date-prefix, grouped, to-string | done |
| Snowflake-friendly rendering `timestamp-machine-sequence` | done |
| CosID golden compatibility vectors (segment bounds, bit layout, strings) | planned |

Rule that will not change: uniqueness is only promised inside a configured
allocation domain. Gaps, restart waste and clock-regression errors are allowed —
"looks continuous" is never a reason to weaken uniqueness.

## Install

```bash
go get github.com/maxid/gcosid
```

## License

MIT. See [LICENSE](LICENSE).
