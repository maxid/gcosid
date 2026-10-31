# CosID 2.15.1 parity notes

Target: **CosID 2.15.1** (`Ahoo-Wang/CosID` tag `v2.15.1`), the version the CosID
platform runs. Everything below is read from that tag, not from memory or the
upstream docs of a different major version.

> Why the version matters: CosID 5.0 is a full rewrite that explicitly keeps no
> backward compatibility (different storage layout, `RangeStore`, lease-based
> machine slots). Porting 5.x semantics would produce ids the CosID side cannot
> continue, so 2.15.1 is the only useful baseline here.

## Segment storage

The reference implementation does **not** use an `id_segment(max_id, step, version)`
row with optimistic locking. It keeps one counter row per namespace:

```sql
create table if not exists cosid
(
    name            varchar(100) not null comment '{namespace}.{name}',
    last_max_id     bigint unsigned not null default 0,
    last_fetch_time bigint unsigned not null default 0,
    constraint cosid_pk primary key (name)
) engine = InnoDB;
```

Allocation (`JdbcIdSegmentDistributor`):

1. `UPDATE cosid SET last_max_id = last_max_id + ?, last_fetch_time = unix_timestamp() WHERE name = ?`
2. `SELECT last_max_id FROM cosid WHERE name = ?`

The returned `last_max_id` is the **end** of the newly reserved range. The range
is `(maxId - step, maxId]`: `DefaultIdSegment.offset = maxId - step`, and the
first id handed out is `offset + 1`.

Consequences for this library:

- No `version` column, no compare-and-set retry loop. The `UPDATE` itself is the
  atomic reservation; a second statement reads it back.
- `name` is `{namespace}.{name}` (`IdSegmentDistributorDefinition`), so the
  namespace is part of the key rather than a separate column.
- Reservation is per `step`, and the caller may ask for a larger step than the
  configured one (multi-segment reservation) — the distributed range still has
  to stay contiguous.

## Segment semantics

`IdSegment` (from `cosid-core/.../segment/IdSegment`):

| Field | Meaning |
|---|---|
| `offset` | `maxId - step`; the first id handed out is `offset + 1` |
| `maxId` | inclusive upper bound |
| `sequence` | next available id, `-1` means overflow (`SEQUENCE_OVERFLOW`) |
| `step` | size of the range |
| `fetchTime` | seconds when the range was fetched |
| `ttl` | `Long.MAX_VALUE` means never expires |

`isAvailable()` = not expired and not overflowed. `SegmentChainId` keeps a chain
of segments with a `safeDistance` lookahead and prefetches the next one before
the current runs out; a fetched range that arrives after the chain moved on is
still usable only while it does not overlap (`NextIdSegmentExpiredException`
otherwise).

## Snowflake

- Epoch `CosId.COSID_EPOCH = 1577203200000` (milliseconds; seconds variant uses
  `1577203200`).
- Layout is `timestampBits | machineBits | sequenceBits`; the JS-safe
  variant limits the sum of the bits so the value stays below
  `JS_MAX_SAFE_NUMBER_BIT`.
- `MillisecondSnowflakeId` / `SecondSnowflakeId` differ only in the time unit;
  `ClockSyncSnowflakeId` wraps a generator with `ClockBackwardsSynchronizer`
  instead of throwing; `SafeJSSnowflakeId` narrows the layout.
- `SnowflakeFriendlyId` is a **formatted local date-time**, not the raw epoch
  value (`SnowflakeIdStateParser.parse`):

  | parser | format | example |
  |---|---|---|
  | `MillisecondSnowflakeIdStateParser` | `yyyyMMddHHmmssSSS-machineId-sequence` | `20191224160001000-3-5` |
  | `SecondSnowflakeIdStateParser` | `yyyyMMddHHmmss-machineId-sequence` | `20191224160001-3-5` |

  The zone is the parser's zone (CosID defaults to the system zone), and
  `padStart` pads machine/sequence to their decimal width
  (`RadixIdConverter.maxCharSize(10, bits)`). It is a *rendering*: the numeric id
  is unchanged. An earlier Go version printed the raw epoch millis — wrong.

## Grouping

`DefaultGroupedIdSegmentDistributor` does not partition one counter — it creates a
**new distributor per period**, named `{name}@{groupKey}`, and `GroupBySupplier`
produces the key plus its expiry:

| supplier | key | ttl ends at |
|---|---|---|
| `YearGroupBySupplier` | `2026` | the last instant of the year |
| `YearMonthGroupBySupplier` | `2026-10` | the last instant of the month |
| `YearMonthDayGroupBySupplier` | `2026-10-31` | the last instant of the day |

`ttlAt` is `LocalDateTime.MAX` of the period in epoch **seconds**
(`toInstant().toEpochMilli() / 1000`). `GroupedPrefixIdConverter` renders
`<groupKey><delimiter><id>`, e.g. `2026-000042`.

One difference in the Go port: CosID reads the current key from a `ThreadLocal`
(`GroupedAccessor`), while this library returns the key next to the id. Ambient
state would make concurrent callers silently pick up each other's group.

## Benchmark method

The tag compiles for **JDK 17** (`build.gradle.kts`, toolchain 17).
Rather than running a JDK container, the golden expectations are read directly
out of the 2.15.1 sources and pinned as literals in the Go tests, each with the
file it came from. That keeps the check offline and reproducible; a JDK container
is only needed if a future version's behaviour cannot be read off the source.

## Segment defaults

- `IdSegmentDistributor.DEFAULT_STEP = 10`, `DEFAULT_OFFSET = 0` — the segment
  step is 10 unless configured, not 100.
- `SegmentChainId.DEFAULT_SAFE_DISTANCE = 2`: the chain keeps **two segments**
  ready ahead of the cursor, prefetched by a `PrefetchJob` on a worker executor.
  A single buffered range (this library's current model) is not the same thing:
  the chain tolerates one slow/failed reservation without stalling.

## Machines

`MachineIdDistributor` assigns the machine id: `ManualMachineIdDistributor`
(fixed), JDBC and Redis stores (leased, with `MachineState`), and a Kubernetes
StatefulSet variant. `MachineIdGuarder` keeps the lease alive; losing it raises
`MachineIdLostException`.

Two details that decide whether Go and Java can share the tables:

1. **Slot name is padded**: `namespacedMachineId(namespace, machineId)` returns
   `namespace + "." + padStart(machineId, 8, '0')`, e.g. `xdmp.00000001`. `name`
   is the primary key of `cosid_machine`, so an unpadded name addresses a
   different row and two runtimes would both see a free slot for the same
   machine id.
2. **`getSafeGuardAt(duration, stable)`**: `stable = true` or a "forever" guard
   yields `0`, which makes the reclaim condition `last_timestamp <= 0` — i.e. no
   slot is ever reclaimed by age. A stable instance (StatefulSet pod) only loses
   its slot when it is explicitly reverted.
3. On a duplicate-key insert the JDBC distributor **re-reads and retries**
   (`distributeMachine` recurses) instead of failing the allocation.

`DefaultMachineIdGuarder` renews every registered slot on a fixed delay
(`initialDelay = DEFAULT_DELAY = 1 minute`) and records a `GuardianState` per
(namespace, instance). A failed guard is not retried silently: it means another
instance may already own the machine id.

`k8s.StatefulSetMachineIdDistributor` derives the machine id from the pod
ordinal: the substring after the **last** `-` of `HOSTNAME` (`xdmp-iot-7` → 7).
Guard and revert are no-ops there — the scheduler owns the ordinal.

## Converters (`cosid-core/.../converter`)

`RadixIdConverter` (bases 2..62), `Radix36IdConverter`, `Radix62IdConverter`,
`PrefixIdConverter`, `SuffixIdConverter`, `DatePrefixIdConverter`,
`GroupedPrefixIdConverter`, `SnowflakeFriendlyIdConverter`, `ToStringIdConverter`.
Converters are pure rendering: the numeric id never depends on them.

## What this means for the Go port

Current `segment.go` differs in three ways and must be aligned:

1. **Storage shape**: it assumed a pluggable `SegmentAllocator` returning
   `[start, end]`. That port is fine, but the JDBC allocator must implement the
   `cosid(name, last_max_id, last_fetch_time)` protocol above — not a
   `max_id + step` with a version column.
2. **Range arithmetic**: switch to `offset = maxId - step`, first id `offset+1`,
   and model the segment as `{offset, maxId, sequence, step, fetchTime, ttl}`
   instead of `{Start, End}`.
3. **Expiry**: `ttl` is part of `isAvailable()`; a segment whose ttl elapsed must
   not be handed out, which the current implementation has no notion of.

Still missing for parity: `SegmentChainId` safe-distance prefetch, snowflake
(three variants + clock synchronizer), machine-id distributors, and all
converters.
