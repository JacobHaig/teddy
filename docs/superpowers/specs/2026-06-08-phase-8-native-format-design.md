# Phase 8 — Native Dataframe Serialization Format (TDF)

**Status:** design + implementation delivered together for user review (Phase 8
is review-gated). The format is a persistent on-disk contract, so the design
decisions below are the primary review surface — nothing is committed until
approved.

## Goal (from roadmap)

An ergonomic, 1:1, no-processing on-disk format that mirrors teddy's in-memory
dataframe/series structure: save & load with no transformation, uncompressed
by default. The fast path for "teddy wrote it, teddy reads it" — lossless for
EVERY column type including the 6d-2a logical types and nested columns, which
CSV/JSON/parquet-write cannot all round-trip.

## Why a native format (vs. reusing parquet)

- parquet WRITE is lossy/incomplete here: nested write is unimplemented
  (error), Raw/annotation fidelity is partial, and it's processing-heavy
  (thrift, encodings, def/rep). TDF writes the in-memory bytes directly.
- It is the only format that round-trips Nested, and the only one that
  preserves Decimal precision/scale, Timestamp origin, Raw metadata, and
  FixedBytes width exactly.

## Locked format decisions (review these)

1. **Endianness: little-endian throughout.** Matches the dominant target;
   documented in the header so a future big-endian reader can byte-swap.
2. **Magic + versioning.** 8-byte magic `"TEDDYDF1"`. `format_version: u16`
   (=1) lets the layout evolve; an unknown major version → clean
   `error.UnsupportedFormatVersion`. `flags: u16` reserved (0) — the
   extension point for future whole-file compression without a version bump.
3. **Self-describing schema header**, then column data blocks — so load is a
   single forward pass, no seeking. (Random-access/columnar mmap is a possible
   v2; v1 optimizes for simple sequential save/load.)
4. **Lossless for all 16 BoxedSeries variants.** Per-value-metadata types
   (Decimal{precision,scale}, Time/Timestamp{unit,utc,origin}) are
   self-describing in their value bytes; column-level-metadata types
   (FixedBytes width, Raw physical/annotations, Nested SchemaNode) serialize
   their `ColumnMeta` in the schema header.
5. **Uncompressed by default**, no per-column encoding tricks — "no
   processing" is the point. Compression is a future flag (decision 2).
6. **Centralized codec, not a per-type capability.** One reviewable
   `native_format.zig` switches over the known variant set (the type list is
   closed and stable post-6d-2a); recursive `Nested` encoding lives as
   `encode`/`decode` helpers there, not 14 scattered methods.

## File layout (little-endian)

```
HEADER
  magic         : [8]u8  "TEDDYDF1"
  format_version: u16    = 1
  flags         : u16    = 0 (reserved)
  num_columns   : u32
  num_rows      : u64
SCHEMA  (num_columns entries)
  name_len      : u32 ; name bytes
  type_tag      : u8   (see tag table)
  type_meta     : variant-specific (see below)
  has_validity  : u8   (0|1)
DATA  (num_columns blocks, in schema order)
  [if has_validity] validity bitmap: ceil(num_rows/8) bytes, bit i = present
  values        : type-specific (only PRESENT rows? NO — see Values rule)
```

**Values rule:** values are written for ALL rows (including null slots, whose
payload is the type's zero/placeholder), so the values block is a fixed stride
for POD types — trivial bulk read. Validity says which are real. (Trades a
little space for dead-simple, seekable POD blocks; matches the in-memory
ArrayList which also stores placeholders at null slots.)

### type_tag table (u8)
0 bool, 1 i8, 2 i16, 3 i32, 4 i64, 5 i128, 6 isize, 7 u8, 8 u16, 9 u32,
10 u64, 11 u128, 12 usize, 13 f32, 14 f64, 15 f16, 16 String, 17 Date,
18 Time, 19 Timestamp, 20 Decimal, 21 Binary, 22 FixedBytes, 23 Uuid,
24 Interval, 25 Raw, 26 Nested. (Stable; append-only.)

### type_meta by tag
- ints/floats/bool/Date/Time/Timestamp/Decimal/Uuid/Interval/String/Binary:
  none (Time/Timestamp/Decimal carry unit/utc/origin/precision/scale per
  value).
- FixedBytes(22): `width: i32` (the column width; -1 if unset).
- Raw(25): `physical: u8`, `has_converted: u8`+`converted: u8`,
  `logical: ` (1-byte tag + params via encodeLogicalType-equivalent),
  `type_length: i32` (-1 if null).
- Nested(26): the column's `SchemaNode` tree (recursive: name, repetition u8,
  leaf payload opt, children count + children). Optional i32 fields
  (type_length/scale/precision) use an explicit present-flag, NOT a -1
  sentinel, so legitimately-negative scale/precision survive.

### values by tag
- POD fixed-width (bool=1B, i8..i128/u8..u128 at their width, isize/usize as
  fixed 8B i64/u64 for wire portability, f16=2B/f32=4B/f64=8B, Date=4B i32,
  Time=10B {i64 value, u8 unit, u8 utc}, Timestamp=11B {i64, u8 unit, u8 utc,
  u8 origin}, Decimal=34B {i256, u8 precision, i8 scale}, Uuid=16B,
  Interval=12B {u32 months, u32 days, u32 millis}): explicit field-by-field
  LE writes (no struct-padding reliance), num_rows entries. (Exact struct layouts pinned in code + tested, not relying on
  Zig's in-memory padding — explicit field-by-field write.)
- String/Binary/Raw(per-value bytes)/FixedBytes: `len: u32` + bytes, per row.
- Nested: per row, recursive value encoding (tag u8 + payload; list = u32 len
  + elements; strukt = u32 len + fields; map = u32 len + {key,value} pairs;
  scalars as their fixed/length-prefixed forms).

## API / wiring

- `native_format.zig`: `pub fn writeToString(allocator, df) ![]u8` and
  `pub fn parse(allocator, bytes) !*Dataframe`. Untrusted-input hardened like
  the parquet reader (checked casts, bounds before slicing, `error.Corrupt*`,
  never panic — the malformed battery extends to TDF).
- `reader.zig`/`writer.zig`: `FileType` gains `tdf`; Writer.toString and
  Reader.load route to it. Extension convention `.tdf`.
- tests.zig registers `native_format_test.zig`.

## Implementation slices (both land uncommitted for one review)
- **.0**: module + header/schema framing + all NON-nested variants
  (primitives, POD logical types, owning-bytes types, Raw/FixedBytes meta) +
  FileType wiring + round-trip tests (every type, with nulls, empty df,
  multi-column) + malformed-input tests.
- **.1**: Nested recursive value + SchemaNode meta + round-trip tests
  (list/struct/map/nestings, nulls).

## Out of scope
Whole-file or per-column compression (reserved flag); columnar/mmap random
access (possible v2); cross-version migration beyond clean-error on unknown
major version; schema evolution.
