# gcosid

Segment and snowflake ID generation in Go.

> Status: **experimental scaffold** (v0.0.0-dev).

## Scope: CosID feature parity

The target is a complete port of CosID's capabilities, not one chosen strategy.
Consumers pick a generator; the library never mixes layouts silently.

| CosID capability | Status |
|---|---|
| Segment generator: local range cursor, inclusive bounds | done |
| Double buffering / chain prefetch with bounded timeout | done |
| `SegmentAllocator` port + in-memory allocator | done |
| DB segment allocator (atomic `max_id` bump, dialect SQL) | planned |
| Step start / step size / lazy distributor tuning | planned |
| Snowflake: bit layout, epoch, machine bits, sequence | planned |
| Machine-id distribution: manual / Redis / JDBC | planned |
| Clock-moved-backwards policy (wait vs error) | planned |
| String converters: radix (10/16/32/36/62), prefix, group | planned |
| Snowflake-friendly id (JS-safe rendering) | planned |
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
