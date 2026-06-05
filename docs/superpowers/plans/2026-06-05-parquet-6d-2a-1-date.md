# Parquet 6d-2a.1 — Date Implementation Plan

> Slice plan (compact — the 6d-2a.0 infra plan documents the conventions).
> Spec: docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Goal:** `Date` becomes a first-class column type: parquet DATE (INT32 days
since epoch, logical or converted annotation) reads as `Series(Date)`, writes
back losslessly, and offers civil-calendar operations.

**Baseline:** 445/445 tests pass at d915233.

## New capability: ordering

Date is the first POD non-numeric type, exposing two gaps the .0 convention
didn't need (Raw/Blob/String all had `toSlice`):
- `argSort`'s `lessThan` else-arm does `a < b` → compile error for structs.
- `indicesWhere`'s comparison block does `item < value` when instantiated.
- `join.zig` unmatched-cell scalar fallback `@as(ValType, 0)` → compile error.

Fixes: value types may declare `order(self: *const T, other: *const T)
std.math.Order`; `argSort`/`indicesWhere` gain a `hasMethod(T, "order")` arm
(before the toSlice arm); join's scalar fallback becomes
`std.mem.zeroes(ValType)` (covers bool/ints/floats/POD structs uniformly).

## Tasks

### Task A — ordering capability + Date type + variant (src/dataframe/)
- `series.zig` argSort lessThan: insert order-capability arm first.
- `boxed_series.zig` indicesWhere: insert order/eql-capability arm first
  (eq/neq via `eql`, lt/lte/gt/gte via `order`).
- `join.zig`: scalar default → `std.mem.zeroes(ValType)`.
- NEW `src/dataframe/date.zig`: `Date = struct { days: i32 }` (days since
  1970-01-01, parquet DATE semantics). Decls: `type_name="Date"`,
  `eql`, `order`, `format` (ISO `YYYY-MM-DD`), plus ops:
  `Civil = struct{year: i32, month: u8, day: u8}`, `fromCivil`, `toCivil`
  (Howard Hinnant civil-calendar algorithms, @divTrunc with the standard
  sign-conditioning), `year()/month()/day()`, `addDays(n: i32)`,
  `diffDays(other) i32`. POD: no deinit/clone/init (zeroes placeholder = epoch).
- Variant wiring: `boxed_series.zig` `.date: *Series(Date)` + typeName/getType
  arms; `series.zig` toBoxedSeries arm; `json_writer.zig` isStringSeries
  `.date => true` (ISO string must be quoted).
- Tests (date.zig + series_test.zig): epoch=1970-01-01; 18262=2020-01-01;
  leap day 2020-02-29; fromCivil∘toCivil round-trip over samples incl.
  pre-1970; addDays/diffDays; order; argSort on Series(Date); indicesWhere
  lt/eq on a boxed date column; join with Date value column compiles.

### Task B — parquet wiring (read + write + round-trip)
- `dataframe/parquet.zig`: `ResolvedKind.date_`; resolveKind: logical `.date`
  AND converted `.date` → `.date_`; addColumn `.date_` arm building
  Series(Date) from `col.int32s` (`orelse return error.UnexpectedPhysicalType`).
- Write side: `DataframeColumns` gains `i32_bufs: [][]i32` (freed in deinit);
  `boxedToColumnData` gains an i32-scratch param + `.date` arm emitting
  physical `.int32`, converted `.date`, logical `.date`, `int32s` = allocated
  days buffer.
- Update `parquet_test.zig` end-to-end pin: column "d" now typeName "Date";
  assert values via toCivil (2020-01-01, 2021-06-15).
- NEW round-trip test: logical_annotations → df → write → read → days
  identical, logical_type `.date` + converted `.date` preserved, re-resolves
  to Date.

### Task C — review, docs, commit
- Combined spec+quality review of the slice diff; apply fixes.
- docs/cleanup-roadmap.md: mark `.1 Date ✅`; docs/type-system.md mapping
  table: Date row (INT32+DATE).
- Commit: `feat(parquet): Date column type — DATE reads/writes as Series(Date) (6d-2a.1)`.

## Out of scope
GroupBy on Date keys (BoxedGroupBy has no .date variant; is_groupable
unchanged — revisit when a slice needs it). Null fidelity (Phase 10).
