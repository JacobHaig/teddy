# Parquet → Teddy Type Mapping (design note)

**Status:** design / Phase 6a research output. Drives the Parquet reader work
(Phases 6c/6d in `cleanup-roadmap.md`). All structural facts below are verified
against primary Apache sources (`parquet.thrift`, `LogicalTypes.md`,
parquet.apache.org, Arrow docs) — see Sources.

Goal: read *almost any* Parquet file and surface each column as a typed Series
without silently losing semantics (today a Date is read as `i32`, a decimal as a
raw int, and every byte array as `String`).

---

## 1. Physical types (`parquet.thrift` `enum Type`)

Exactly eight. These are all a column can physically be; logical types annotate
them with meaning.

| Code | Physical | What it is | Teddy decodes today |
|---|---|---|---|
| 0 | `BOOLEAN` | 1 bit | ✓ |
| 1 | `INT32` | 32-bit signed | ✓ |
| 2 | `INT64` | 64-bit signed | ✓ |
| 3 | `INT96` | 96-bit (12 bytes), **deprecated** legacy timestamp | ✗ |
| 4 | `FLOAT` | IEEE 32-bit | ✓ |
| 5 | `DOUBLE` | IEEE 64-bit | ✓ |
| 6 | `BYTE_ARRAY` | arbitrarily long bytes | ✓ (always → String) |
| 7 | `FIXED_LEN_BYTE_ARRAY` | fixed-length bytes (length in schema) | ✗ (falls through) |

**INT96** is deprecated — "new Parquet writers should not write data in INT96."
It's the legacy 12-byte timestamp (8-byte nanos-of-day + 4-byte Julian day) from
Impala/Hive/Spark. Modern replacement: `INT64` + `TIMESTAMP`. PyArrow only writes
it under `use_deprecated_int96_timestamps=True` (or `flavor='spark'`). Readers
that want Spark-era files must still handle it.

---

## 2. Logical types

Modern semantics live in the `LogicalType` Thrift **union** (17 members). The
older `ConvertedType` **enum** is deprecated but still present in real files and
maps one-to-one onto the union. (Myth, explicitly refuted in research: there was
*no* "rename from LogicalType in 4.0.0" — they're just old-enum vs new-union.)

| LogicalType | Physical backing | Encoding | Legacy ConvertedType |
|---|---|---|---|
| `STRING` | BYTE_ARRAY | UTF-8 | `UTF8` |
| `ENUM` | BYTE_ARRAY | UTF-8-ish | `ENUM` |
| `JSON` | BYTE_ARRAY | UTF-8 JSON | `JSON` |
| `BSON` | BYTE_ARRAY | BSON bytes | `BSON` |
| `INT(bits,signed)` | INT32 (8/16/32), INT64 (64) | native | `INT_8..64`, `UINT_8..64` |
| `DECIMAL(p,s)` | INT32 (p≤9), INT64 (p≤18), FLBA, BYTE_ARRAY | unscaled × 10⁻ˢ; FLBA/BYTE_ARRAY = two's-complement **big-endian** | `DECIMAL` |
| `DATE` | INT32 | days since 1970-01-01 (signed) | `DATE` |
| `TIME(unit,utc)` | INT32 (MILLIS), INT64 (MICROS/NANOS) | elapsed unit after midnight | `TIME_MILLIS/MICROS` |
| `TIMESTAMP(unit,utc)` | INT64 | elapsed unit since 1970-01-01T00:00:00 | `TIMESTAMP_MILLIS/MICROS` |
| `UUID` | FLBA(16) | 16 bytes **big-endian** | — |
| `INTERVAL` (legacy) | FLBA(12) | 3 × **little-endian** u32: months, days, millis | `INTERVAL` |
| `FLOAT16` | FLBA(2) | IEEE half, **little-endian** | — |
| `MAP` / `LIST` | group nodes | nested | `MAP`/`LIST` |
| `UNKNOWN` / `VARIANT` / `GEOMETRY` / `GEOGRAPHY` | various | newer/extensible | — |

`TimeUnit` union = `MILLIS(1)` / `MICROS(2)` / `NANOS(3)`. `INT(bits,signed)`:
widths 8/16/32 → INT32, width 64 → INT64; bare unannotated ints imply signed.

---

## 3. Recommended Teddy Series type set

### Map onto existing variants (no new type)
| Parquet | Teddy variant |
|---|---|
| BOOLEAN | `bool` |
| INT(8/16/32, signed) | `i8` / `i16` / `i32` |
| INT(64, signed) / bare INT64 | `i64` |
| INT(8/16/32, unsigned) | `u8` / `u16` / `u32` |
| INT(64, unsigned) | `u64` |
| FLOAT | `f32` |
| DOUBLE | `f64` |
| STRING / ENUM / JSON (UTF-8 BYTE_ARRAY) | `String` |

So signed/unsigned INT annotations slot straight into teddy's existing dual
integer families — we just have to *read the annotation* instead of assuming
`i32`/`i64`. (`u128`/`i128`/`usize`/`isize` stay; Parquet never needs them for INT.)

### New variants to add
| New variant | Representation | Source types |
|---|---|---|
| `Date` | `i32` days since epoch | DATE |
| `Time` | `{ unit: enum{millis,micros,nanos}, value: i64 }` (widen MILLIS i32→i64 internally) | TIME |
| `Timestamp` | `{ unit, is_utc: bool, value: i64 }` | TIMESTAMP (and decoded INT96) |
| `Decimal` | `{ unscaled: i256, precision: u8, scale: i8 }` for **precision ≤ 76** (== Arrow decimal256; raw-bytes fallback only beyond 76) | DECIMAL on INT32/INT64/FLBA |
| `Binary` | owned `[]u8` | unannotated BYTE_ARRAY, BSON |
| `FixedBytes` | owned `[]u8` (+ width) | generic FLBA |
| `Uuid` | `[16]u8` | UUID |
| `Interval` | `{ months: u32, days: u32, millis: u32 }` | INTERVAL |
| `Float16` | native Zig `f16` (Zig has it) | FLOAT16 |

### Fallback
A generic `Raw` / `Unsupported` variant carrying **physical type + raw bytes +
LogicalType metadata**, for: INT96 (if not decoded), VARIANT, GEOMETRY,
GEOGRAPHY, and nested MAP/LIST/STRUCT until structured support lands. This is what
lets us claim "reads almost any file" — unknown types round-trip as bytes rather
than erroring or corrupting.

### Notable Zig wins
- Zig's arbitrary-width integers mean `i256` losslessly holds DECIMAL precision
  ≤ 76 (10⁷⁶ < 2²⁵⁵) with no bigint library — matching Arrow `decimal256`.
  Only precision > 76 (nonstandard) falls back to raw two's-complement bytes.
- Zig has native `f16`, so FLOAT16 needs no conversion shim either.

---

## 4. Reference-implementation validation

Arrow's Parquet reader (high confidence): DATE→`date32`, TIME(MILLIS)→`time32[ms]`
/ TIME(MICROS,NANOS)→`time64`, TIMESTAMP→`timestamp[unit,tz]`,
DECIMAL→`decimal128`/`decimal256`, UUID/FLBA→`fixed_size_binary`, unannotated
BYTE_ARRAY→`binary`, STRING→`utf8`, and **INT96 read as a nanosecond timestamp**.
DuckDB and Polars likewise surface these as first-class date/time/timestamp/
decimal/blob columns (medium confidence — well-known behavior, not pinned to a
quoted primary source). The proposed teddy variants line up one-to-one with Arrow.

---

## 5. Open decisions — RESOLVED (2026-06-04, approved 6d-2a spec)

All four are decided in
`docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md`:

1. **Nested types (MAP/LIST/STRUCT):** deferred to a separate spec (Phase
   6d-2b); they read as `Raw` until built.
2. **DECIMAL precision 39–76:** `i256` unscaled (Arrow decimal256 parity);
   raw-bytes fallback only for precision > 76.
3. **VARIANT / GEOMETRY / GEOGRAPHY:** generic `Raw` fallback (full support
   deferred to 6d-2b).
4. **INT96:** fully decode to `Timestamp(nanos, utc=false)` tagged
   `origin=int96`; write defaults to modern INT64 TIMESTAMP, with bit-faithful
   INT96 re-emit behind `WriteOptions.emit_int96 = true`.

---

## Sources
Primary: `apache/parquet-format` [`parquet.thrift`](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift),
[`LogicalTypes.md`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
[parquet.apache.org/docs/file-format/types](https://parquet.apache.org/docs/file-format/types/),
[Arrow PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html),
[Arrow Rust ConvertedType](https://arrow.apache.org/rust/parquet/basic/enum.ConvertedType.html),
[parquet-format#301 (INT96)](https://github.com/apache/parquet-format/issues/301).
