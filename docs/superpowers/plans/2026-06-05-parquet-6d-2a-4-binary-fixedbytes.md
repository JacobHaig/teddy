# Parquet 6d-2a.4 — Binary + FixedBytes Implementation Plan

> Slice plan (compact). Spec: docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Goal:** Non-UTF8 byte payloads get dedicated owning column types:
unannotated `BYTE_ARRAY` (and BSON) → `Binary`; unannotated
`FIXED_LEN_BYTE_ARRAY(n)` → `FixedBytes` with the column width on
`Series.meta`. **Deliberate behavior change:** these previously read as
`String` (garbage for true binary data); UTF8/ENUM/JSON annotations keep
reading as String.

**Baseline:** 526/526 tests at c13776a.

## Types

`Binary` (src/dataframe/binary.zig) — owning, modeled on Raw:
`allocator + bytes: []u8`; capabilities `type_name="Binary"`, `init`,
`fromSlice`, `deinit`, `clone`, `eql`, `toSlice`, `format` (lowercase hex);
ops `len()`, `slice(start, end) []const u8` (bounds-checked, borrowed),
`ColumnMeta = struct { converted_type: ?parquet.ConvertedType = null,
logical_type: ?parquet.LogicalType = null }` (preserves BSON annotation for
lossless re-emit; both null for plain binary).

`FixedBytes` (src/dataframe/fixed_bytes.zig) — same owning surface,
`type_name="FixedBytes"`, plus
`ColumnMeta = struct { width: ?i32 = null }` (the FLBA type_length —
column-level, per the spec's "column width on the Series").

Risk note: existing String reads are SAFE — parquet-cpp/pyarrow always
annotate string columns UTF8 (addresses/multi_rowgroup fixtures verified by
the suite staying green).

## Tasks

### Task A — types + variants
binary.zig + fixed_bytes.zig per above with unit tests (ownership/clone/eql/
slice bounds/format; ColumnMeta defaults); BoxedSeries `.binary`/
`.fixed_bytes` variants + typeName/getType/toBoxedSeries/json_writer (`true` —
hex must be quoted) + tests.zig imports; TEMP write arms in boxedToColumnData.

### Task B — resolution + read + write + tests
- resolveKind: logical `.bson` → `.binary_`; physical defaults change:
  `.byte_array => .binary_`, `.fixed_len_byte_array => .fixedbytes_`
  (UTF8/ENUM/JSON logical or converted still hit `.string` FIRST via
  precedence — verify with tests); converted `.bson` → `.binary_`.
- addColumn arms: `.binary_` from byte_arrays (clone bytes; set ColumnMeta
  from col.converted_type/.logical_type only when bson — else defaults);
  `.fixedbytes_` from byte_arrays + `meta.width = col.type_length`.
- Write arms (replace TEMP): `.binary` → BYTE_ARRAY + preserved annotations
  from meta (null for plain); `.fixed_bytes` → FLBA with
  `type_length = meta.width orelse derive from first value's len` (error
  `MissingWidth` if empty AND no meta width... choose: width from meta, else
  first value length, else error); borrowed `toSlice` slices into an
  arena-allocated slice array (same pattern as `.string`/`.raw`).
- Fixture `data/binary_kinds.parquet` (gen_fixtures.py): one `pa.binary()`
  column with non-UTF8 bytes (e.g. b"\\x00\\x01\\xff") — pins unannotated
  BYTE_ARRAY → Binary. BSON has no pyarrow constructor: cover via
  resolveKind unit test on a hand-built column. FLBA → reuse data/flba.parquet
  (now reads as FixedBytes width 4 — ADD df-level assertions; parquet-level
  tests unchanged).
- Update pins: resolveKind precedence test physical-default rows
  (byte_array bare → `.binary_` now, NOT string — keep a utf8 case proving
  String still wins); any df-level test that read unannotated byte arrays as
  String (audit: grep parquet_test + dataframe tests).
- Round-trips: binary_kinds lossless (bytes identical, no annotation);
  flba.parquet → FixedBytes(width 4) → write → FLBA(4) re-read identical +
  re-resolves; hand-built Binary column with meta bson annotation → write →
  re-read carries bson converted_type.

### Task C — review, docs, commit
Combined review; fixes; roadmap `.4 ✅`; type-system.md rows; commit
`feat(parquet): Binary + FixedBytes column types for non-UTF8 payloads (6d-2a.4)`.

## Out of scope
ENUM → dedicated type (stays String); BYTE_ARRAY-backed decimal write; null
fidelity (Phase 10).
