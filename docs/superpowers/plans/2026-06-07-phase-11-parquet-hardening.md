# Phase 11 — Parquet Untrusted-Input Hardening Implementation Plan

> From the 2026-06-04 full-project review, Theme 3 (P1–P8). A parquet reader's
> central promise: arbitrary bytes return an error, never a panic. Today a
> malformed file panics in many independent places. One commit.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Baseline:** 608/608 tests at 9e35733.

## Strategy: validate at the boundary, trust internally

Wire enums stay exhaustive (no `_` member — that would force `_ =>` arms on
every internal switch). Instead, every decode site converts through a checked
helper; internal code keeps exhaustive switches over already-validated values.

## Units

### Unit A — thrift + metadata layer
- `metadata.zig`: a checked enum-decode helper replacing every
  `@enumFromInt(@as(u8, @intCast(...)))` site:
  ```zig
  /// Checked wire-to-enum conversion: unknown/out-of-range values from the
  /// file become an error instead of an @enumFromInt panic (review P3).
  fn enumFromWire(comptime E: type, raw: i32) !E {
      const tag = std.math.cast(@typeInfo(E).@"enum".tag_type, raw) orelse
          return error.InvalidEnumValue;
      return std.meta.intToEnum(E, tag) catch error.InvalidEnumValue;
  }
  ```
  Sweep ALL decode sites in metadata.zig (SchemaElement type_/repetition/
  converted_type, page headers' encodings, ColumnMetaData type_/encodings/
  codec, PageHeader page_type, ...).
- `thrift_reader.zig` (review P4 + P7):
  - `readZigZagI32`/`readZigZagI16`: the varint→u32/u16 narrowing must be
    checked (`std.math.cast` → `error.VarintOverflow`), not `@intCast`.
  - `skip`: bound recursion depth (new depth param or internal counter,
    max 32 → `error.NestingTooDeep`).
  - `pushStruct` past the fixed 16-deep stack: currently silently corrupts
    field-id tracking → return `error.NestingTooDeep` (signature change to
    `!void`; update callers — they all `try` decode paths anyway; the
    writer's pushStruct is on OUR data, leave it).
- Tests: bad enum tags at several fields error; varint > u32 in an i32 field
  errors; 33-deep nested struct skip errors; existing round-trips green.

### Unit B — column/parquet reader layer
- `column_reader.zig`:
  - P1: `decodeDictionary` bounds-checks every index against the dictionary
    length → `error.InvalidDictionaryIndex`.
  - P2: v1 def-level reads: check `decompressed.len >= data_pos + 4` before
    the length prefix and `data_pos + 4 + def_len <= decompressed.len` before
    slicing; same for rep-levels. v2: `rep_len + def_len <= uncompressed_size`
    (and `<= page_data.len`) before the usize subtraction/slices.
  - P5: validate `start_offset <= file_data.len` (and `end_offset` arithmetic
    with `std.math.add` overflow checks) before slicing.
  - P6: `expandWithNulls` byte-array arm frees untransferred accumulator
    entries (`items[vi..]`) before `clearRetainingCapacity`.
  - P4 sweep: every `@intCast` on file-controlled values (`total_compressed_
    size`, `num_values`, page sizes, `dict_header.num_values`, type_length)
    → `std.math.cast` orelse error (`error.CorruptFile` family — pick
    consistent names).
- `parquet_reader.zig`: footer_len checked math; `num_rows` negative →
  error; `@intCast(file_metadata.num_rows)` checked.
- Tests: targeted malformed vectors per fix (hand-built bytes or
  fixture-mutations) — each asserts a specific error, never a panic.

### Unit C — malformed-file battery + review + commit
- New test file src/parquet/malformed_test.zig (register in
  src/parquet/tests.zig): for EACH committed fixture: (a) truncation sweep —
  `readParquet(file[0..n])` for n in a stride over the length must return an
  error or a valid result, never panic; (b) single-byte corruption sweep —
  flip one byte at a stride of offsets, same assertion (accept success too:
  some flips hit padding/values and stay readable — the assertion is
  "no panic, and errors are clean").
  NOTE: keep runtime sane — stride so the battery stays < a few seconds.
- Combined review; fixes; roadmap Phase 11 ✅; commit
  `fix(parquet): harden reader against malformed files — validate at the boundary (Phase 11)`.

## Out of scope
Fuzzing infrastructure (the battery is deterministic); writer-side hardening
(writes trusted data); DataPageV2 write; thrift_writer changes.
