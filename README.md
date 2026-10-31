English | [简体中文](README.zh-CN.md)

# gcosid

Segment and snowflake ID generation in Go.

> Status: **experimental scaffold** (v0.0.0-dev).

## Scope: CosID feature parity

The target is a complete port of CosID's capabilities, not one chosen strategy.
Consumers pick a generator; the library never mixes layouts silently.

| CosID capability | Status |
|---|---|
| Segment generator: `IdSegment{offset,maxId,sequence,step,fetchTime,ttl}` arithmetic (CosID 2.15.1) | done |
| Segment chain (`SegmentChainId`): safe distance 2, hunger-adaptive prefetch | done |
| Double buffering / chain prefetch with bounded timeout | done |
| `SegmentAllocator` port + in-memory allocator | done |
| DB segment allocator: `cosid(name,last_max_id,last_fetch_time)` reserve-then-read (MySQL, Postgres) | done |
| Redis segment allocator: `INCRBY cosid:{namespace.name}:adder` | done |
| Segment TTL / expiry abandonment, step tuning | done (TTL) / planned (step) |
| Snowflake: layout/epoch/sequence + ms, second and JS-safe variants | done |
| Machine-id distribution: manual id, leased slots — in-memory, JDBC `cosid_machine`, Redis Lua scripts (verbatim 2.15.1) | done |
| Machine-id guard: periodic lease renewal with failure state (`MachineIdGuarder`) | done |
| Kubernetes StatefulSet distributor (machine id = pod ordinal) | done |
| Storage adapters: MongoDB, ZooKeeper | planned (lowest priority) |
| Clock-moved-backwards policy: strict error or spin/sleep synchroniser | done |
| String converters: radix 2..62, prefix, suffix, date-prefix, grouped, to-string | done |
| Snowflake-friendly rendering `<yyyyMMddHHmmss[SSS]>-<machine>-<sequence>` | done |
| Integer / string generator views (`IntegerIdGenerator`, `StringIdGenerator`) | done |
| `jvm.AtomicLongGenerator`, `jvm.UuidGenerator` | done |
| CosID string family (`Radix36/62CosIdGenerator`, 44/20/16 layout) | done |
| Generator registry (`IdGeneratorProvider`) and `LazyIdGenerator` | done |
| Grouped segments: per-year / month / day counters and `<group>-<id>` rendering | done |
| Sharding: `ModCycle`, `IntervalTimeline` (time windows), `CachedSharding` | done |
| Diagnostics: `Statistical` stats for generators and converters | done |
| `UncertaintyIdGenerator`: random low bits on top of an ordered id | done |
| Struct-tag id injection (`@CosId` + `EnsureId` equivalent) | done |
| CosID golden compatibility vectors (segment bounds, bit layout, strings) | planned |

Rule that will not change: uniqueness is only promised inside a configured
allocation domain. Gaps, restart waste and clock-regression errors are allowed —
"looks continuous" is never a reason to weaken uniqueness.

## Testing

| Command | Covers | Requires |
|---|---|---|
| `go test ./...` | pure logic: segment arithmetic, snowflake layout, converters, dialect SQL | nothing |
| `go test -tags=integration ./...` | storage behaviour against real MySQL and Redis | Docker, or a provided environment |

**Rule**: anything that depends on a database or another middleware is verified
by the integration suite. Unit tests never mock SQL — a mock happily accepts a
statement that duplicates ids in reality (that is exactly how a missing
transaction boundary slipped through once).

Without Docker, point the tests at a provided environment:

```bash
GCOSID_TEST_MYSQL_DSN='test:test@tcp(127.0.0.1:3306)/cosid' \
GCOSID_TEST_REDIS_ADDR=127.0.0.1:6379 \
go test -tags=integration ./...
```

Images default to `mysql:8.4.7` and `redis:7` and can be mirrored:

```bash
GCOSID_MYSQL_IMAGE=registry.example.com/library/mysql:8.4.7 \
GCOSID_REDIS_IMAGE=registry.example.com/library/redis:7 \
go test -tags=integration ./...
```

## Id injection

CosID annotates entity fields with `@CosId` and fills them through an accessor.
Go has no annotations, so the equivalent is a struct tag:

```go
type Order struct {
    ID     int64 `cosid:"order"`
    Amount int64
}

injector := gcosid.NewInjector()
injector.Register("order", chain)          // numeric field
injector.RegisterString("no", orderNoGen)  // string field

filled, err := injector.Ensure(ctx, &order) // fills only when the id is empty
injector.EnsureAll(ctx, []*Order{{}, {}})   // batch
```

See [`examples/inject`](examples/inject/main.go) for a runnable program.

Rules, matching `CosIdAccessor`:

- the tag value is a generator name; `cosid:"-"` opts out;
- only `string` and the integer widths hold ids (`Long`/`Integer`/`String` in CosID);
- an entity that already carries an id is never overwritten (`EnsureId`);
- embedded structs are searched, so a composite entity can inherit its id;
- a string field fed by a numeric generator is rendered as decimal
  (`ToStringIdConverter`).

## Install

```bash
go get github.com/maxid/gcosid
```

## License

MIT. See [LICENSE](LICENSE).
