# Parquet 6d-2a.2 — Timestamp + Time Implementation Plan

> Slice plan (compact). Spec: docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Goal:** TIME and TIMESTAMP annotations surface as dedicated `Time`/`Timestamp`
columns; INT96 stops being Raw and decodes to a usable
`Timestamp{unit=nanos, utc=false, origin=int96}`; writes default to modern
INT64 TIMESTAMP with bit-faithful INT96 re-emit behind `emit_int96 = true`.

**Baseline:** 462/462 tests at bfaf941.

## Types (src/dataframe/time.zig, timestamp.zig)

```zig
Time = struct { value: i64, unit: parquet.TimeUnit, utc: bool }
Timestamp = struct { value: i64, unit: parquet.TimeUnit, utc: bool, origin: enum { int64, int96 } }
```
Deviation from the spec table (which omits `utc` on Time): the spec's locked
"full lossless round-trip" decision requires preserving `isAdjustedToUTC`, so
Time carries it like Timestamp does.

Capabilities: `type_name`, `eql` (normalized), `order` (normalized via i128
nanos), `format` (Time `HH:MM:SS[.fraction]`; Timestamp ISO 8601
`YYYY-MM-DDTHH:MM:SS[.fraction]` + `Z` when utc). POD — no deinit/clone/init.

Ops (spec "rich per-type operations"):
- shared: `toNanos(self) i128` (unit-normalized), `addDuration(amount: i64, unit: TimeUnit)`
  (normalizes into the receiver's unit via i128, error on overflow — strictness
  convention), Time `hour()/minute()/second()`.
- Timestamp: `toDate() Date` (civil days via @divFloor of nanos),
  `timeOfDay() Time` (nanos within day, unit nanos),
  `fromDateAndTime(Date, Time, utc) !Timestamp` (nanos, origin=int64),
  Timestamp component accessors via toDate().
- INT96 codec (timestamp.zig, pure + unit-tested both directions):
  `fromInt96Bytes(bytes: *const [12]u8) !Timestamp` — first 8 bytes LE u64 =
  nanos-of-day, last 4 LE u32 = Julian day; epoch nanos =
  (julian − 2440588)·86_400_000_000_000 + nanos_of_day via i128,
  `std.math.cast` to i64 else `error.TimestampOverflow`; result
  `{unit=nanos, utc=false, origin=int96}`.
  `toInt96Bytes(self) ![12]u8` — inverse with @divFloor/@mod (negative-safe);
  julian must fit u32 else error. Bit-faithful: decode∘encode == identity for
  valid inputs (test against data/int96.parquet bytes).

## Tasks

### Task A — Time + Timestamp types + variants
- New time.zig + timestamp.zig per above, with thorough unit tests (unit
  normalization across ms/us/ns, ordering across mixed units, INT96 codec
  identity on fixture bytes + hand vectors + negative epoch values, format
  edge cases, Date↔Timestamp conversions incl. pre-epoch).
- Variant wiring: BoxedSeries `.time`/`.timestamp` + typeName/getType arms;
  toBoxedSeries arms; json_writer `.time/.timestamp => true`; tests.zig
  imports; TEMP error arms in boxedToColumnData (replaced in Task C).

### Task B — read wiring + arena refactor
- `DataframeColumns` scratch mechanism refactor: replace `string_bufs` +
  `i32_bufs` with one `std.heap.ArenaAllocator` — all scratch (slice arrays,
  numeric buffers, future byte buffers) comes from the arena; `deinit` =
  arena deinit. (Borrowed inner slices from String/Raw values are unaffected;
  only arena allocations are freed.) Existing tests pin behavior.
- resolveKind: `.time_`/`.timestamp_` kinds; logical `.time`/`.timestamp`
  carry params into addColumn (NOTE: ResolvedKind is a bare enum — addColumn
  arms re-read `col.logical_type`/`col.converted_type` for unit/utc);
  converted `.time_millis` (INT32 physical, widen to i64) / `.time_micros` /
  `.timestamp_millis` / `.timestamp_micros` (legacy ⇒ utc=true per
  LogicalTypes.md); INT96 physical → `.timestamp_` (no longer `.raw`).
- addColumn arms: Time from int32s (ms) or int64s (us/ns); Timestamp from
  int64s, or from 12-byte byte_arrays via fromInt96Bytes (a decode error on
  malformed INT96 propagates).
- Update pins: int96 adapter test (Raw → Timestamp — rewrite the 6d-2a.0
  round-trip test for the new semantics, keep a Raw test only via a
  VARIANT-style unit test of resolveKind); logical_annotations end-to-end
  ("t" → "Time", "ts" → "Timestamp" with utc=true + micros).
- Extend gen_fixtures.py `logical_annotations` companion fixture
  `data/time_units.parquet`: time32(ms), time64(ns)? (pyarrow: time32("ms"),
  timestamp("ms"), timestamp("ns"), timestamp("us") no tz ⇒ utc=false) —
  pins unit/utc handling breadth. Regenerate ONLY the new file.

### Task C — write wiring + emit_int96
- `parquet.WriteOptions` gains `emit_int96: bool = false` (spec conformance).
- Adapter: `fromDataframe(allocator, df, opts)` where
  `AdapterWriteOptions = struct { emit_int96: bool = false }`; update call
  sites (writer.zig, tests). dataframe `Writer` gains `emit_int96` field +
  `withEmitInt96()` builder, passes through in toString.
- boxedToColumnData arms:
  - `.time`: unit millis → INT32 physical + converted `.time_millis` (narrow
    i64→i32; values < 86.4e6 by construction, but use std.math.cast → error)
    + logical; micros → INT64 + `.time_micros` + logical; nanos → INT64 +
    logical only (no legacy equivalent).
  - `.timestamp`: default INT64 + logical timestamp{unit,utc} + converted
    (.timestamp_millis/.timestamp_micros for ms/us when utc; nanos or
    non-utc → logical only — match what pyarrow emits for non-utc legacy:
    omit converted when utc=false). If `origin == .int96 and opts.emit_int96`
    → INT96 physical, 12-byte values via toInt96Bytes (allocated from the
    DataframeColumns arena), no annotations (INT96 carries none).
  - Mixed-origin columns: if ANY value has origin != int96, the column writes
    as INT64 (origin is per-value but emit decision is per-column: emit INT96
    only when emit_int96 AND all values are origin=int96; document).
- Round-trip tests: (1) logical_annotations ts/t columns lossless (annotations
  + values + utc + unit re-resolve); (2) int96.parquet → df (Timestamp) →
  default write → INT64 TIMESTAMP(nanos, utc=false) → re-read equal nanos;
  (3) int96.parquet → write with emit_int96=true → re-read → original 12-byte
  values bit-identical (compare against a Raw-level read of the original
  file bytes via parquet.readParquet byte_arrays).

### Task D — review, docs, commit
- Combined spec+quality review; fixes.
- docs: roadmap `.2 ✅`; type-system.md rows (Time INT32/INT64+TIME,
  Timestamp INT64+TIMESTAMP / INT96 opt-in); parquet-type-mapping note that
  INT96 now decodes (6c raw-bytes statement superseded).
- Commit: `feat(parquet): Time + Timestamp column types; INT96 decodes to Timestamp (6d-2a.2)`.

## Out of scope
GroupBy on Time/Timestamp keys; null fidelity (Phase 10); INT96 dictionary
write; timezone math beyond the utc flag.
