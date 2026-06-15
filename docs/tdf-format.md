# TDF — Teddy DataFrame File Format (v1)

Canonical on-disk reference for teddy's native dataframe format. A 1:1,
uncompressed, **lossless** mirror of an in-memory `Dataframe`: every one of the
16 column types — including `Nested`, `Decimal`, `Timestamp`, `Raw`, and
`FixedBytes` — round-trips exactly, which no other teddy format (CSV / JSON /
parquet-write) achieves.

- **Reference implementation:** `src/dataframe/native_format.zig`
  (`writeToString` / `parse`).
- **Wiring:** `FileType.tdf` in `reader.zig` / `writer.zig`; conventional
  extension `.tdf`.
- **Status:** verified by 29 round-trip + malformed-input tests
  (`src/dataframe/native_format_test.zig`), including a parquet→TDF→render
  end-to-end check over nested data.

---

## 1. Conventions

- **Endianness:** little-endian for every multi-byte integer and float.
  (A future big-endian reader byte-swaps; the format is LE by definition.)
- **Floats** are written as the same-width unsigned integer of their IEEE-754
  bit pattern (`@bitCast` then LE write): `f16`→`u16`, `f32`→`u32`, `f64`→`u64`.
- **Optional integers** in the schema use an explicit 1-byte present flag
  (NOT an in-band sentinel) wherever the value could legitimately be negative
  (SchemaNode `scale`/`precision`/`type_length`). `FixedBytes.width` and
  `Raw.type_length` (non-negative by nature) use a `-1` sentinel.
- **Enums** are written as `@intFromEnum` (1 byte for all wire enums here) and
  **validated on read** — an out-of-range tag is `error.CorruptTdf`, never a
  trap.
- **Lengths** read from the buffer are checked-cast (`std.math.cast`) before
  use; every read is bounds-checked before slicing. Parsing arbitrary bytes
  never panics — it returns an error.
- **Null slots:** a value is written for **every** row, including nulls (the
  null slot holds the type's zero/placeholder). A separate per-column
  validity bitmap records which rows are present. This keeps fixed-width
  (POD) value blocks at a constant stride.

---

## 2. File structure

```
┌─ HEADER ───────────────────────────────────────────────┐
│ magic           [8]u8   "TEDDYDF1"                      │
│ format_version  u16     = 1                             │
│ flags           u16     = 0   (reserved; see §7)        │
│ num_columns     u32                                     │
│ num_rows        u64                                     │
├─ SCHEMA  (num_columns entries, column order) ──────────┤
│ per column:                                             │
│   name_len      u32 ; name bytes (UTF-8)                │
│   type_tag      u8   (§4)                                │
│   type_meta     variant-specific (§5; most types: none) │
│   has_validity  u8   (0 | 1)                            │
├─ DATA  (num_columns blocks, column order) ─────────────┤
│ per column:                                             │
│   [if has_validity] validity bitmap:                    │
│       ceil(num_rows / 8) bytes, bit i set = PRESENT,    │
│       LSB-first within each byte                        │
│   values: num_rows entries (§6), placeholder at nulls   │
└─────────────────────────────────────────────────────────┘
```

A reader is a single forward pass: no seeking, no back-references. The schema
section is fully self-describing, so DATA can be decoded in column order.

`parse` rejects: wrong magic → `error.InvalidTdf`; `format_version != 1` →
`error.UnsupportedFormatVersion`; any structural inconsistency, bad enum, or
truncation → `error.CorruptTdf`; trailing bytes after the last column →
`error.CorruptTdf`; nesting deeper than 64 → `error.NestingTooDeep`.

---

## 3. Validity bitmap

Present only when `has_validity == 1` (i.e. the in-memory series has a
validity bitmap). `ceil(num_rows / 8)` bytes; row `i` is at byte `i / 8`,
bit `i % 8` (LSB-first). A **set** bit means the row is **present**; a clear
bit means null. On read, the bitmap is reconstructed into the series'
`validity` exactly.

---

## 4. Type-tag table (`u8`)

Stable and append-only — new types take the next free tag; existing tags
never change meaning.

| Tag | Type | Tag | Type | Tag | Type |
|----:|------|----:|------|----:|------|
| 0 | bool | 9 | u32 | 18 | Time |
| 1 | i8 | 10 | u64 | 19 | Timestamp |
| 2 | i16 | 11 | u128 | 20 | Decimal |
| 3 | i32 | 12 | usize | 21 | Binary |
| 4 | i64 | 13 | f32 | 22 | FixedBytes |
| 5 | i128 | 14 | f64 | 23 | Uuid |
| 6 | isize | 15 | f16 | 24 | Interval |
| 7 | u8 | 16 | String | 25 | Raw |
| 8 | u16 | 17 | Date | 26 | Nested |

---

## 5. Type-meta (schema header)

Only three tags carry type-meta; all others write nothing here.

### FixedBytes (22)
```
width   i32     (the column's fixed width; -1 if unset)
```

### Raw (25)
```
physical        u8   @intFromEnum(PhysicalType)
has_converted   u8   (0 | 1)
converted       u8   @intFromEnum(ConvertedType)   (0 when has_converted == 0)
logical         LogicalType   (§8)
type_length     i32  (-1 if null)
```

### Nested (26)
```
schema_present  u8   (0 | 1)
schema          SchemaNode   (§5.1, only if schema_present == 1)
```

### 5.1 SchemaNode (recursive)
The nested column's parquet schema subtree, preserved so the column's
structure (field names, repetition, leaf types) survives the round-trip.

```
name          u32 len + bytes
repetition    u8    @intFromEnum(FieldRepetitionType)
physical      u8 present + u8 (@intFromEnum; 0 when absent)
converted     u8 present + u8 (@intFromEnum; 0 when absent)
logical       LogicalType   (§8)
type_length   u8 present + i32
scale         u8 present + i32
precision     u8 present + i32
max_def       u8
max_rep       u8
leaf_index    u8 present + u64
children      u32 count + (count × SchemaNode, recursive)
```

---

## 6. Value layouts (one entry per row)

### Fixed-width (POD)
| Type | Bytes | Encoding |
|------|------:|----------|
| bool | 1 | `0` / `1` |
| i8 / u8 | 1 | LE |
| i16 / u16 | 2 | LE |
| i32 / u32 | 4 | LE |
| i64 / u64 | 8 | LE |
| i128 / u128 | 16 | LE |
| isize / usize | 8 | LE as i64 / u64 (wire-portable; checked-cast on read) |
| f16 | 2 | bit pattern as u16 LE |
| f32 | 4 | bit pattern as u32 LE |
| f64 | 8 | bit pattern as u64 LE |
| Date | 4 | `days` (i32 LE), days since 1970-01-01 |
| Time | 10 | `value` i64 LE, `unit` u8, `utc` u8 |
| Timestamp | 11 | `value` i64 LE, `unit` u8, `utc` u8, `origin` u8 |
| Decimal | 34 | `unscaled` i256 LE (32), `precision` u8, `scale` i8 |
| Uuid | 16 | raw bytes |
| Interval | 12 | `months` u32 LE, `days` u32 LE, `millis` u32 LE |

`unit` is `@intFromEnum(TimeUnit)` (`millis`/`micros`/`nanos`); `origin` is
`@intFromEnum(Timestamp.Origin)` (`int64`/`int96`) — both validated on read.
Structs are written field-by-field (no reliance on Zig in-memory padding), so
the byte counts above are exact and stable.

### Length-prefixed bytes
`String`, `Binary`, `Raw`, `FixedBytes`: per row, `len` (u32 LE) followed by
`len` bytes. (For `FixedBytes` the width is also in type-meta; the per-row
length is still written for a uniform decode path.)

### Nested (tag 26)
Per row, a recursive value (§6.1).

### 6.1 Nested value encoding
```
tag   u8   @intFromEnum(Nested active variant)
payload by tag:
  null_      (nothing)
  boolean    u8
  int        i64 LE          (all signed widths normalize here)
  uint       u64 LE          (all unsigned widths)
  float      f64 LE bits     (f16/f32/f64 widen to f64)
  date       i32 LE days
  time       i64 value, u8 unit, u8 utc
  timestamp  i64 value, u8 unit, u8 utc, u8 origin
  decimal    i256 unscaled (32), u8 precision, i8 scale
  uuid       16 bytes
  interval   u32 months, u32 days, u32 millis
  string     u32 len + bytes
  bytes      u32 len + bytes
  list       u32 count + count × value (recurse)
  strukt     u32 count + count × value (recurse; fields positional —
             names live in the column's SchemaNode)
  map        u32 count + count × { key value } (both recurse)
```
Recursion is bounded at depth 64 on both write and read.

---

## 7. Versioning & extensibility

- `format_version` gates the layout; an unknown version is a clean error, not
  a misparse. v1 is the layout in this document.
- `flags` (u16) is reserved and currently `0`. It is the designated extension
  point for **whole-file compression** — a future reader can honor a flag bit
  without a version bump, keeping uncompressed v1 files readable.
- The type-tag table is append-only.

Out of scope for v1 (possible v2): whole-file or per-column compression,
columnar/mmap random access, schema evolution, cross-version migration beyond
the clean version error.

---

## 8. LogicalType encoding

Shared by Raw type-meta and SchemaNode. `1`-byte present flag; if present, a
`1`-byte tag (`@intFromEnum(LogicalType)`) plus variant params:

| Variant (tag) | Params |
|---|---|
| decimal (5) | `scale` i32 LE, `precision` i32 LE |
| time (7) | `is_adjusted_to_utc` u8, `unit` u8 |
| timestamp (8) | `is_adjusted_to_utc` u8, `unit` u8 |
| integer (10) | `bit_width` i8, `is_signed` u8 |
| all others | none |

The tag is validated against `LogicalType`'s tag set on read.

---

## 9. Error model

`parse` is hardened to the same bar as the parquet reader: every length is
checked-cast, every read is bounds-checked before slicing, every enum is
validated, and recursion is depth-bounded. Arbitrary or truncated input
yields one of `error.InvalidTdf`, `error.UnsupportedFormatVersion`,
`error.CorruptTdf`, or `error.NestingTooDeep` — never a panic or a leak
(verified by truncate-at-every-offset, bad-tag, and deep-nesting tests under
the leak-checking allocator).

---

## 10. Worked header example

A 2-column, 3-row dataframe `{ id: i64 (no nulls), name: String (1 null) }`:

```
54 45 44 44 59 44 46 31   magic "TEDDYDF1"
01 00                     format_version = 1
00 00                     flags = 0
02 00 00 00               num_columns = 2
03 00 00 00 00 00 00 00   num_rows = 3
# schema[0] "id"
02 00 00 00 69 64         name_len=2, "id"
04                        type_tag = 4 (i64)
00                        has_validity = 0
# schema[1] "name"
04 00 00 00 6E 61 6D 65   name_len=4, "name"
10                        type_tag = 16 (String)
01                        has_validity = 1
# data[0] id  (no bitmap): three i64 LE
...
# data[1] name: 1 bitmap byte (bit0,bit2 set → rows 0,2 present), then
#               three len-prefixed strings (the null row writes len=0)
```
