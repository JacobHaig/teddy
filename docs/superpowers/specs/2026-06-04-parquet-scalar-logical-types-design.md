# Phase 6d-2a — Scalar Logical Types

**Status:** approved design (brainstorm output). Implementation to follow via a
writing-plans plan. Supersedes the open items for scalar types in
`docs/parquet-type-mapping.md` §5.

## Goal

Surface Parquet logical types as richly-typed dataframe columns instead of raw
bytes/ints, with **lossless read↔write round-trip** and **rich per-type
operations**. Scope is **flat scalar types only**; nested types (LIST/MAP/STRUCT)
are deferred to a separate spec (Phase 6d-2b).

### Decisions locked during brainstorm
- **Representation:** distinct Zig types, each with its own `BoxedSeries` variant
  (chosen over a logical-tag-on-existing-int approach — type-safe, clean home for
  per-type ops).
- **Round-trip:** full lossless read→write→read.
- **API richness:** rich per-type operations (not just store/display).
- **Decimal width:** `i256` unscaled (covers precision ≤ 76, == Arrow decimal256);
  raw-bytes fallback only for precision > 76.
- **INT96:** read decodes to a usable `Timestamp(nanos, utc=false)` tagged with
  `origin=int96`. Write **defaults to modern INT64 TIMESTAMP**; re-emits INT96 only
  when the caller sets the opt-in `WriteOptions.emit_int96 = true`.
- **Deferred types** (nested, VARIANT, GEOMETRY, GEOGRAPHY): `Raw` fallback (bytes
  + metadata) so files still read end-to-end. Full support in 6d-2b.
- **Implementation shape:** shared infra first, then one reviewable vertical slice
  per type-family.

## New types & representations

`TimeUnit = enum { millis, micros, nanos }`.

| Type | Zig representation | Parquet physical | Owns memory |
|---|---|---|---|
| `Date` | `struct { days: i32 }` | INT32 | no |
| `Time` | `struct { value: i64, unit: TimeUnit }` | INT32(ms)/INT64(µs,ns) | no |
| `Timestamp` | `struct { value: i64, unit: TimeUnit, utc: bool, origin: enum{int64,int96} }` | INT64 / INT96 | no |
| `Decimal` | `struct { unscaled: i256, precision: u8, scale: i8 }` | INT32/INT64/FLBA | no |
| `Binary` | owned `[]u8` | BYTE_ARRAY (non-UTF8) | yes |
| `FixedBytes` | owned `[]u8` (column width on the Series) | FLBA(n) | yes |
| `Uuid` | `struct { bytes: [16]u8 }` | FLBA(16) | no |
| `Interval` | `struct { months: u32, days: u32, millis: u32 }` | FLBA(12) | no |
| `Float16` | native `f16` (`Series(f16)`) | FLBA(2) | no |
| `Raw` | owned `[]u8` per value + column metadata (`physical_type`, original `LogicalType`/`ConvertedType`, `type_length`) | any deferred type | yes |

`Float16` uses Zig's native numeric `f16`, so it may also participate in numeric
aggregations; the others are excluded from those.

## Series / BoxedSeries integration

`Series(T)` compiles only the methods actually instantiated for a given `T`, so:

- **Numeric aggregations** (`sum`/`mean`/`min`/`max`/`std`/cumulative/…) do not list
  the non-numeric new variants → never instantiate for them. `Float16` (`f16`) **is**
  added to the numeric arms, since `f16` is a native numeric type and the cost is a
  one-line addition per arm.
- **Generic arms** (`deinit`/`clone`/`print`/`len`/`filterByIndices`/append/validity)
  do instantiate for every variant. Today `Series(T)` hard-codes `if (T == String)`.
  Replace this with a **comptime capability convention**: a value type may declare
  `deinit(allocator)`, `clone(allocator)`, `format(...)`, `eql(other)`, dispatched
  via `@hasDecl(T, …)`.
  - Owning types (`String`, `Binary`, `FixedBytes`, `Raw`) implement `deinit`/`clone`.
  - All new types implement `format` for printing.
  - POD types (`Date`/`Time`/`Timestamp`/`Decimal`/`Uuid`/`Interval`/`f16`) need no
    `deinit`; copy is trivial.

`toBoxedSeries()` and `createSeries()` gain one arm per new variant.

This is a targeted improvement to existing code (removing the `String` special-cases)
that the work requires anyway; it is not unrelated refactoring.

## Thrift layer (enables round-trip)

- Model the modern **`LogicalType` union** and its nested structs: `DecimalType{scale,
  precision}`, `TimeType{isAdjustedToUTC, unit}`, `TimestampType{isAdjustedToUTC, unit}`,
  `IntType{bitWidth, isSigned}`, and the `TimeUnit` union.
- `SchemaElement.decode`: parse **field 10** (currently skipped) into
  `logical_type: ?LogicalType`.
- `SchemaElement.encode`: emit field 10 **and** keep emitting the legacy
  `converted_type` (field 6) where an equivalent exists, for backward compatibility.

## Reader mapping (physical + annotation → Series type)

One resolution function with precedence **logical_type (modern) → converted_type
(legacy) → bare physical**, mapping per the type table. INT96 decodes the 12 bytes
(8-byte nanoseconds-of-day + 4-byte Julian day) into `Timestamp{unit=nanos,
utc=false, origin=int96}`. Decimal reads the unscaled value (native for INT32/INT64;
two's-complement big-endian for FLBA/BYTE_ARRAY) into `i256`. Unknown/deferred
annotations → `Raw`.

## Writer mapping (Series type → physical + annotation)

Reverse mapping, choosing the smallest physical that fits:
- `Date` → INT32 + DATE.
- `Time` → INT32(ms)/INT64(µs,ns) + TIME(unit, utc).
- `Timestamp` → INT64 + TIMESTAMP(unit, utc) by default; if `origin=int96` **and**
  `WriteOptions.emit_int96 == true` → INT96 (bit-faithful).
- `Decimal` → INT32 (precision ≤ 9) / INT64 (≤ 18) / FLBA (else) + DECIMAL(precision,
  scale).
- `Binary` → BYTE_ARRAY. `FixedBytes` → FLBA(width). `Uuid` → FLBA(16) + UUID.
- `Interval` → FLBA(12) + INTERVAL (legacy ConvertedType; no modern logical type).
- `Float16` → FLBA(2) + FLOAT16.
- `Raw` → re-emit its preserved physical type + metadata.

`WriteOptions` gains `emit_int96: bool = false`.

## Rich operations (per type)

Built per slice; representative set:
- **Date**: civil ↔ days, `year`/`month`/`day`, `addDays`, `diffDays`, compare.
- **Time**/**Timestamp**: component extraction, `add`/`sub` durations, unit-normalized
  compare, `Timestamp` ↔ `Date`+`Time`.
- **Decimal**: `add`/`sub` (scale-aligned), `mul`/`div` (scale arithmetic), compare,
  `toF64`/`fromF64`, `rescale`.
- **Uuid**: parse/format canonical `8-4-4-4-12` hex.
- **Interval**: component access, add-to-`Date`/`Timestamp`.
- **Binary**/**FixedBytes**: `len`, `slice`, `hex`, equality.
- **Float16**: native arithmetic + `f32`/`f64` conversion.

Operations propagate nulls consistently with existing Series ops and follow the
project's strictness-level convention where a result could overflow or lose data.

## Testing

For each type: a pyarrow-generated fixture (`src_py/gen_fixtures.py`) + reader test
(typed values correct) + **round-trip test** (teddy read → teddy write → teddy read,
assert identical) + operation unit tests. INT96 covers both the default
modern-write path and `emit_int96 = true`. Decimal covers all three physical
backings (INT32/INT64/FLBA) and a precision > 76 raw fallback.

## Implementation sub-phases (each a reviewable commit)

1. **6d-2a.0 — Shared infra:** `TimeUnit` + `LogicalType` thrift model (parse + encode
   field 10 and nested structs); `Series` comptime capability convention
   (`deinit`/`clone`/`format`/`eql` via `@hasDecl`, replacing the `String`
   special-cases); reader/writer resolution skeletons; `Raw` type + variant + fallback
   wiring.
2. **6d-2a.1 — Date** (read + write + ops + tests).
3. **6d-2a.2 — Timestamp + Time** (units; INT96 origin decode + `emit_int96`).
4. **6d-2a.3 — Decimal** (i256; precision→physical selection; scale-aware ops; >76
   raw fallback).
5. **6d-2a.4 — Binary + FixedBytes** (owned-bytes handling via the new convention).
6. **6d-2a.5 — Uuid + Interval + Float16**.

## Out of scope (this spec)

Nested types (LIST/MAP/STRUCT), VARIANT, GEOMETRY, GEOGRAPHY — these read as `Raw`
here and get full support in Phase 6d-2b (separate spec; requires repetition-level
record assembly and a nested column model).
