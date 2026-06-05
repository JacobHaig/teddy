# Parquet 6d-2a.3 — Decimal Implementation Plan

> Slice plan (compact). Spec: docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Goal:** DECIMAL annotations surface as a dedicated `Decimal` column
(`i256` unscaled — Arrow decimal256 parity, precision ≤ 76; precision > 76
falls back to `Raw`), with scale-aware arithmetic and lossless round-trip
across all three physical backings (INT32/INT64/FLBA).

**Baseline:** 497/497 tests at fb88bc1.

## Type (src/dataframe/decimal.zig)

```zig
Decimal = struct { unscaled: i256, precision: u8, scale: i8 }
```
POD. Capabilities: `type_name="Decimal"`, `eql`/`order` (scale-normalized via
i512 intermediates — rescaling i256 can overflow i256 but never i512),
`format` (sign + integer part + '.' + scale-padded fraction; scale 0 → no
point; negative scale prints trailing zeros).

Ops (spec): `add`/`sub` (scale-aligned to max scale, `error.Overflow` on i256
overflow), `mul` (scales add), `div(quot_scale)` (scale arithmetic, truncating,
`error.DivisionByZero`), `rescale(new_scale)` (strict: `error.LossyRescale` on
truncation, Overflow on widening past i256), `toF64`, `fromF64(precision,
scale)` (strict-ish: NaN/Inf → error), `order`/`eql`.

Byte codec (pure fns, unit-tested): `fromBeBytes(bytes: []const u8) !i256`
(two's-complement big-endian, ≤ 32 bytes, sign-extended) and
`toBeBytes(value: i256, width: usize, out: []u8) !void` (minimal-width helper
`minBytesForPrecision(p: u8) u8` — smallest w with 10^p − 1 ≤ 2^(8w−1) − 1,
computed by loop, table-tested against parquet-mr's values: p=1→1, 2→1, 3→2,
4→2, 9→4, 10→5, 18→8, 38→16, 76→32).

## Wire plumbing this slice unlocks

- `SchemaElement.encode` must emit legacy fields 7 (scale) and 8 (precision)
  when present (decode already reads them) — between fields 6 and 10,
  ascending order.
- `ColumnData` gains `scale: ?i32`, `precision: ?i32`; parquet_writer passes
  them onto the schema element.
- `ParquetColumn` gains `scale: ?i32`, `precision: ?i32`, propagated in
  buildColumn + readLeafConcat (needed for legacy converted-only DECIMAL
  files where precision/scale live in fields 7/8, not in LogicalType).

## Tasks

### Task A — Decimal type + variant
decimal.zig per above with thorough tests (arithmetic incl. scale alignment +
overflow, rescale strictness, codec round-trips incl. negative values and all
widths 1..32, minBytesForPrecision table, format edge cases: scale 0,
value 0, negative, fraction-only like 0.05, precision-76 extremes); BoxedSeries
`.decimal` variant + typeName/getType/toBoxedSeries/json_writer (`true` —
quote it) + tests.zig import; TEMP write arm in boxedToColumnData.

### Task B — wire plumbing + read + write + fixtures
- SchemaElement.encode fields 7/8 + round-trip test (metadata.zig).
- ParquetColumn/ColumnData scale+precision propagation.
- resolveKind: logical `.decimal` with precision ≤ 76 → `.decimal_`,
  precision > 76 → `.raw`; converted `.decimal` → `.decimal_` (precision from
  ParquetColumn.precision; if absent/out-of-range → `.raw`).
- addColumn `.decimal_` arm: precision/scale from logical params else legacy
  fields; unscaled from int32s/int64s natively or byte_arrays via fromBeBytes
  (len > 32 → error.DecimalTooWide… should have resolved to raw — defensive).
- Write arm: precision ≤ 9 → INT32; ≤ 18 → INT64; else FLBA(minBytes) BE from
  the arena; all emit converted `.decimal` + logical decimal{scale, precision}
  + ColumnData.scale/precision. First-value-wins precision/scale contract
  (same doc convention as the .time arm).
- Fixture `data/decimals.parquet` (gen_fixtures.py): pyarrow
  `store_decimal_as_integer=True` for decimal(9,2)-as-INT32 +
  decimal(18,4)-as-INT64, plus decimal(38,10) FLBA and a negative-value
  column; restore byte-churned pre-existing fixtures after regeneration.
- Tests: resolveKind precedence additions (incl. precision-77 → raw via
  hand-built column); logical_annotations "dec" column now typeName "Decimal"
  (update pins; value check 12345678.90 via format or unscaled+scale);
  decimals.parquet end-to-end (all 3 physicals + negative); full lossless
  round-trip (annotations + fields 7/8 + unscaled identical + re-resolve);
  write-physical-selection test (9/2→INT32 wire, 18/4→INT64, 38/10→FLBA(16)).

### Task C — review, docs, commit
Combined review; fixes; roadmap `.3 ✅`; type-system.md row
(`Decimal | .decimal | INT32/64/FLBA+DECIMAL`); commit
`feat(parquet): Decimal column type — i256-backed DECIMAL across all physicals (6d-2a.3)`.

## Out of scope
Decimal as groupBy/join key; banker's rounding (div/rescale truncate, strict
errors instead); precision > 76 (Raw); null fidelity (Phase 10).
