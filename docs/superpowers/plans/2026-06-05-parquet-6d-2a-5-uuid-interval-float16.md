# Parquet 6d-2a.5 — Uuid + Interval + Float16 Implementation Plan

> Final slice of 6d-2a (compact). Spec: docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Goal:** the last three scalar logical types. `Uuid` (FLBA(16)+UUID) and
`Interval` (FLBA(12)+INTERVAL, legacy converted-only) become dedicated POD
types; `FLOAT16` (FLBA(2)) becomes **native `Series(f16)`** — joining the
numeric family with zero capability decls (typeInfo-keyed `is_numeric`).

**Baseline:** 552 tests at a13aadc (suite shows 345+206 runners).

## Types

`Uuid` (src/dataframe/uuid.zig): `struct { bytes: [16]u8 }`, POD.
`type_name="Uuid"`, `eql`, `order` (bytewise — RFC 4122 lexical), `format`
(canonical lowercase `8-4-4-4-12`), `parse(str) !Uuid` (accepts canonical
hyphenated form, case-insensitive), `toBytes`/`fromBytes` trivial.

`Interval` (src/dataframe/interval.zig): `struct { months: u32, days: u32,
millis: u32 }`, POD. Parquet INTERVAL is 12 bytes = 3× LE u32 (months, days,
millis). `type_name="Interval"`, `eql` (component-wise — NO order: months vs
days have no total ordering; filters limited to eq/neq via... NOTE: without
`order`, indicesWhere's order-arm doesn't fire and the `else` `item < value`
arm would compile-error IF instantiated with T=Interval — but indicesWhere
only instantiates per requested comptime T, so simply don't support
Interval filters this slice), `format` (`P{months}M{days}D{millis}ms`-style
readable form, e.g. "1mo 2d 3000ms"), codec `fromLeBytes(*const [12]u8)` /
`toLeBytes() [12]u8`, ops `addToDate(Date) Date` (calendar month add with
end-of-month day clamping, then +days; millis ignored for Date — document)
and `addToTimestamp(Timestamp) !Timestamp` (months+days via civil math,
millis added in the timestamp's unit via addDuration).

`Float16` = native `f16`. NO new value type. BoxedSeries gains
`float16: *Series(f16)`.

## f16 integration points (the careful part)

- `boxed_series.zig`: `.float16` variant; typeName "f16"; getType f16; ADD
  `.float16` to the explicit numeric tag lists in sum/min/max/sumChecked/
  prod/first/last (they use explicit tags + `else => null/error`, unlike the
  is_numeric-guarded methods which pick f16 up automatically). Pattern:
  `.float32 => |s| ...` arms widen via `@as(f64, s.xxx())` — f16 widens to
  f64 implicitly, so add `.float16` alongside `.float32` in each.
- `series.zig`: toBoxedSeries `f16 => .float16`; getTypeAsString `f16 =>
  "Float16"`; print/printAt/asStringAt float arms become
  `T == f16 or T == f32 or T == f64`.
- `boxed_groupby.zig` + `group.zig`: f16 is is_groupable (numeric) →
  `GroupBy(f16)` instantiates → `BoxedGroupBy` needs a `.float16` variant +
  `toBoxedGroupBy` arm. GroupByContext: add f16 to the float-bitcast hash arm
  (`u16` bits). GroupBy per-column aggregation switches (sum/mean/min/max/...)
  use explicit tag lists with `else => error` — leave f16 value columns
  erroring TypeNot* for now (note in docs), EXCEPT add `.float16` arms where
  it's a one-liner alongside `.float32`.
- `json_writer.zig` isStringSeries: f16 numeric → NOT quoted (no arm needed —
  verify else=>false covers it).

## Tasks

### Task A — Uuid + Interval types + variants (no f16 yet)
uuid.zig + interval.zig per above with thorough tests (uuid parse/format
round-trip incl. uppercase input + bad inputs erroring; interval codec LE
round-trip + hand vectors; addToDate month-clamp cases: Jan31+1mo→Feb28/29,
leap years; addToTimestamp incl. millis in micros-unit timestamp);
BoxedSeries `.uuid`/`.interval` variants + typeName/getType/toBoxedSeries/
json_writer (`true` both) + tests.zig imports; TEMP write arms.

### Task B — f16 integration
All the "f16 integration points" above + tests: Series(f16) numeric ops
(sum/mean/min/max through BoxedSeries return non-null), argSort, groupBy on
f16 keys (count), getTypeAsString "Float16", asStringAt decimal rendering,
json unquoted. TEMP write arm for `.float16`.

### Task C — parquet wiring (all three)
- resolveKind: logical `.uuid => .uuid_` (wins over fixedbytes_), logical
  `.float16 => .float16_`, converted `.interval => .interval_`. ResolvedKind
  +3.
- addColumn: `.uuid_` from 16-byte byte_arrays (`error.InvalidUuid` on wrong
  width); `.interval_` from 12-byte byte_arrays via fromLeBytes; `.float16_`
  from 2-byte byte_arrays via `@bitCast(std.mem.readInt(u16, .., .little))`.
- Write arms: uuid → FLBA(16)+logical uuid; interval → FLBA(12)+converted
  interval (NO logical — none exists); f16 → FLBA(2)+logical float16, bytes
  LE from arena.
- Fixture data/uuid_f16.parquet: pyarrow `pa.uuid()` + `pa.float16()`
  columns (verify on-wire FLBA(16)+UUID / FLBA(2)+FLOAT16 with a python
  schema print; if pa.uuid() unavailable in installed pyarrow, fall back to
  fixed_size_binary(16) + a hand-built-column unit test for uuid resolution,
  reporting the deviation). INTERVAL: pyarrow cannot write it — cover via
  hand-built column resolveKind test + teddy-write→teddy-read round-trip.
- Tests: resolveKind additions (uuid beats fixedbytes_ on FLBA; float16;
  interval converted); df-level fixture reads (uuid formats canonically,
  f16 values approx-equal); round-trips: uuid lossless (bytes + annotation +
  re-resolve), f16 lossless bit-exact, interval teddy→teddy lossless
  (converted annotation + components); FLBA(16) WITHOUT uuid annotation
  still reads FixedBytes (regression pin).

### Task D — review, docs, commit
Combined review; fixes; roadmap `.5 ✅` + **6d-2a phase complete**;
type-system.md rows (Uuid/Interval/f16); parquet-type-mapping note; commit
`feat(parquet): Uuid, Interval, Float16 — 6d-2a scalar logical types complete (6d-2a.5)`.

## Out of scope
VARIANT/GEO/nested (6d-2b); null fidelity (Phase 10).
(As-built notes: Interval DID get a documented nominal-duration `order` —
argSort instantiates for every variant, so the convention stays uniform; and
f16 GroupBy value-column aggregations were fully wired, not deferred.)
