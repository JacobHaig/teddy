# teddy — Project Overview & Technical Write-Up

A dataframe library written in Zig (0.16). teddy reads and writes columnar and
text data across a full type system, with first-class null handling, nested
types, a native serialization format, untrusted-input hardening, and a
pyarrow-referenced regression + benchmark harness that guards correctness and
measures performance.

_Last updated: 2026-06-18 (after Phase 14). Authoritative status:
[docs/cleanup-roadmap.md](cleanup-roadmap.md)._

---

## 1. What teddy is

teddy is a Zig library (consumed via the `dataframe` module; the `teddy`
executable is a demo). Its job: load tabular data from CSV / JSON / Parquet,
hold it in a typed in-memory dataframe, transform it (filter / sort / group /
join / aggregate / cast / …), and write it back out — losslessly where the
format allows. It targets correctness and type fidelity first; it reads
"almost any" Parquet file and writes back what it reads.

```
            ┌─────────── readers ───────────┐      ┌────────── writers ──────────┐
  CSV  ─────┤ csv_reader                     │      │ csv_writer            ─────► CSV
  JSON ─────┤ json_reader (rows/cols/ndjson) ├──►  Dataframe  ──►          │ json_writer          ─────► JSON
  Parquet ──┤ parquet_reader (+ nested)      │   (Series columns)          │ parquet_writer (+nested) ──► Parquet
  TDF  ─────┤ native_format                  │      │ native_format        ─────► TDF
            └────────────────────────────────┘      └──────────────────────────────┘
```

---

## 2. Architecture

### 2.1 The type ladder: `Series(T)` → `BoxedSeries` → `Dataframe`

- **`Series(T)`** (`src/dataframe/series.zig`) — a generic typed column: an
  `ArrayList(T)` of values + an optional lazily-created validity bitmap
  (`?ArrayList(bool)`, present only when the column has nulls) + a name + an
  optional column-level `meta`. All the per-column operations live here
  (append, cast, aggregations, cum*, sort keys, fill-null, …).
- **`BoxedSeries`** (`src/dataframe/boxed_series.zig`) — a tagged union over
  `*Series(T)` for every supported `T`. This is the runtime type erasure that
  lets a dataframe hold heterogeneously-typed columns. Methods dispatch with
  Zig's `inline else`, which instantiates the called `Series(T)` method for
  every variant — a property that drove much of the design (see 2.2).
- **`Dataframe`** (`src/dataframe/dataframe.zig`) — an `ArrayList(BoxedSeries)`
  plus the frame-level operations (filter, sort, join, groupBy, select,
  concat, describe, the I/O entry points, …).

### 2.2 The capability convention

Because `BoxedSeries` dispatch instantiates each `Series(T)` method for every
variant, adding a column type can't require touching dozens of methods. teddy
uses a **comptime capability convention** (`hasMethod(T, name)` in
`series.zig`, gated behind a `@typeInfo` container check so primitives report
false): a value type opts into behavior by *declaring* methods —
`deinit`/`clone` (ownership), `eql`/`order` (compare/sort/hash), `toSlice`,
`format` (rendering), `init` (the null placeholder), `type_name` (display),
`hash`, and `ColumnMeta` (column-level metadata). Numeric behavior is keyed off
`@typeInfo` (`.int`/`.float`), so `f16` joins the numeric family with no
declarations, while struct types are automatically excluded from arithmetic.
`BoxedSeries`'s numeric/cast/group dispatch is comptime-guarded
(`is_numeric`/`is_castable`/`is_groupable`) so a new variant can never
force-instantiate an unsupported method — it returns `error.TypeNot*` instead.

This is why teddy could add nine logical types + a nested type + a Raw fallback
as small, independent slices: each new type is a struct declaring the
capabilities it supports.

---

## 3. The type system

teddy maps the Parquet/Arrow type system onto Zig types, using native Zig types
where they exist and purpose-built ones where they don't. Full reference:
[docs/type-system.md](type-system.md).

| Column type | Backing | Source (Parquet) |
|---|---|---|
| bool, i8–i128, u8–u128, isize/usize | native | BOOLEAN, INT(8–64, signed/unsigned) |
| f32, f64, **f16** | native (`f16` too) | FLOAT, DOUBLE, FLOAT16 |
| String | owned UTF-8 | BYTE_ARRAY + UTF8/ENUM/JSON |
| Date | `i32` days since epoch | INT32 + DATE |
| Time | `{i64 value, unit, utc}` | INT32/INT64 + TIME |
| Timestamp | `{i64 value, unit, utc, origin}` | INT64 + TIMESTAMP, **and decoded INT96** |
| Decimal | **`i256`** unscaled + precision/scale | DECIMAL on INT32/INT64/FLBA (precision ≤ 76) |
| Binary | owned bytes | unannotated BYTE_ARRAY, BSON |
| FixedBytes | owned bytes + width on meta | unannotated FLBA(n) |
| Uuid | `[16]u8` | FLBA(16) + UUID |
| Interval | `{u32 months, days, millis}` | FLBA(12) + INTERVAL |
| Nested | recursive owned tree (list/struct/map) | LIST / MAP / STRUCT |
| Raw | preserved bytes + parquet metadata | VARIANT / GEOMETRY / GEOGRAPHY / unknown |

Design notes that recur: Zig's arbitrary-width ints give `i256` for full
DECIMAL precision with no bigint library; `f16` is native so FLOAT16 is a
first-class numeric; types with no Zig/std equivalent (Date, Decimal, Uuid,
Interval, …) are thin purpose-built structs carrying *Parquet's* exact
semantics (unit ticks, utc flag, INT96 origin) that a generic type wouldn't.
`Raw` is the "don't corrupt, don't error, round-trip bit-faithfully" vessel for
types teddy doesn't decode — it renders as hex and is a preservation surface,
not an interaction one.

---

## 4. I/O formats

- **CSV** — reader with type inference; writer (RFC 4180 quoting). Lossy by
  nature (no type metadata): everything re-reads as String or re-inferred
  numerics.
- **JSON** — reader handling rows `[{…}]`, columns `{c:[…]}`, and NDJSON, with
  format auto-detection; writer for all three. Nested columns write as valid
  JSON (objects via SchemaNode field names, arrays for lists). Reader gaps
  (documented): `\uXXXX` escapes emit raw; nested JSON values aren't read back
  into `Nested`.
- **Parquet** — a from-scratch reader+writer (`src/parquet/`): hand-rolled
  Thrift compact protocol, page/column-chunk handling, PLAIN + RLE/bit-packed
  + dictionary decode, Snappy. Reads multi-row-group files, the full logical
  type system, and **nested LIST/MAP/STRUCT** (Dremel record assembly). Writes
  flat columns and **nested columns** (reverse-Dremel shredding), with
  definition+repetition levels, OPTIONAL columns, and modern+legacy
  annotations. The reader is hardened against malformed input (checked casts,
  bounds-before-slice, validated enums, bounded recursion — a fuzz battery of
  truncation/corruption over every fixture asserts "error, never panic").
- **TDF** (Teddy DataFrame) — teddy's native format
  ([docs/tdf-format.md](tdf-format.md)): a 1:1, uncompressed, **lossless**
  on-disk mirror of the in-memory frame. The only format that round-trips
  *every* type exactly, including Nested, Decimal precision/scale, Timestamp
  origin, Raw annotations, and FixedBytes width. Magic `TEDDYDF1` + a version
  field + a reserved `flags` field (the future compression hook).

---

## 5. Null handling

Nulls are a real value end-to-end (Phase 10). Each `Series` carries a lazily
created validity bitmap; readers call `appendNull` rather than materializing
placeholders; writers render null correctly (CSV empty field, JSON bare
`null`); filters use SQL semantics (null matches nothing); GroupBy drops null
keys and skips nulls in aggregations; joins never match nulls and emit real
nulls for unmatched cells; sort places nulls last. Parquet write emits RLE
definition levels so the null pattern survives `df → parquet → df`.

---

## 6. Operations

Frame-level (`Dataframe`): `filter`, `sort` (nulls-last), `groupBy` /
`groupByMultiple` + aggregations (sum/mean/min/max/count/std/median/…),
`join` (inner/left/right/outer), `select`, `head`/`tail`/`slice`/`limit`,
`concat`, `unique`, `valueCounts`, `dropNulls`/`dropNullsAny`, `fillNull`,
`cast`/`castLossy`, `cumSum`/`cumMin`/`cumMax`/`cumProd`, `shift`, `diff`,
`clip`, `replace`, `describe`, `applyInplace`/`applyNew`, `deepCopy`. Per-column
ops live on `Series(T)`.

Known op-level caveats (surfaced by the Phase 14 suite, on the backlog):
`Dataframe.castSafe` is uncompilable via boxed dispatch (use `cast`/
`castLossy`); `cumProd` panics on integer overflow in debug builds.

---

## 7. Validation & performance methodology

- **Per-module tests** — every module has focused unit tests under the
  testing allocator (leak-checked). `zig build test` runs them all (~782
  tests, ~27s).
- **Regression framework** (`validation/` + `src/dataframe/regression_test.zig`)
  — pyarrow is the reference engine (stronger than pandas, which flattens
  Decimal/Time/etc.). `validation/regression.py` emits a per-type fixture +
  a golden manifest of pyarrow ground-truth semantic values + transform
  results; the Zig harness compares teddy **semantically** per cell and runs
  transforms / TDF / CSV / JSON / **parquet** round-trips, enforcing a
  committed known-divergences allowlist + fidelity matrix (any new/unexpected
  divergence fails the gate). Read parity, TDF and parquet round-trip are
  0-divergence across all types; justified divergences are documented in
  [docs/validation-divergences.md](validation-divergences.md).
- **Wide operations suite** (`src/dataframe/operations_test.zig`) — invariant /
  property tests over wide generated data (50k rows × multiple seeds × null
  densities) covering every operation plus round-trip identities. Arena-backed
  for speed; a leak-canary retains small-scale leak coverage. This is the net
  that guards optimizations.
- **Benchmark harness** (`zig build bench`, `src/bench/main.zig`) — monotonic
  `std.time.Timer`, warmup + median-of-K over generated data. `--json` captures
  a baseline; `--baseline FILE` prints per-op `before | after | Δ%`. Run
  `-Doptimize=ReleaseFast` for real numbers. Methodology + the captured
  baseline live in [docs/benchmarks.md](benchmarks.md). **Every performance
  change must record its before/after Δ% there and stay green on the wide
  operations suite.**

```
   testdata.genDataframe (seeded, all types, controlled nulls)
        │                                   │
        ▼                                   ▼
   operations_test.zig                 src/bench/main.zig
   (invariants, in `zig build test`)   (`zig build bench`, ReleaseFast)
   correctness gate ───────────────►   measure Δ% → docs/benchmarks.md
```

---

## 8. Phase history (how it got here)

- **1–5 Cleanup** — removed template cruft, Zig-ified `.gitignore`, real
  `functions.zig`, strings audit, roadmap triage.
- **6 Parquet read** — multi-row-group concat; FLBA/INT96; unsigned ints; the
  full scalar logical-type system (Date/Time/Timestamp/Decimal/Binary/
  FixedBytes/Uuid/Interval/Float16 + Raw, via the capability convention); and
  nested LIST/MAP/STRUCT read (Dremel assembly).
- **7 JSON fixes** — removed `.auto` unreachable, hardened format detection,
  key unescaping, big-int→float, `\b`/`\f`.
- **8 Native TDF format** — lossless 1:1 save/load.
- **9 Regression framework** — pyarrow-referenced, every type end-to-end, with
  user-justified divergences locked into an allowlist.
- **10–12 Hardening** — null correctness end-to-end (incl. parquet definition
  levels); parquet untrusted-input hardening + fuzz battery; a dataframe
  bug-fix batch.
- **13 Nested parquet write** — reverse-Dremel shredding; pyarrow-conformant.
- **(post-roadmap)** narrow-int parquet write; **14** benchmark harness + wide
  operations regression suite.

---

## 9. Known limitations & deferred items

- Parquet write: dictionary encoding, column statistics (for predicate
  pushdown), DataPageV2, and codecs beyond Snappy/uncompressed are not yet
  implemented.
- 128-bit ints (`i128`/`u128`) can't be written to Parquet (no 128-bit
  integer physical type — a format limitation, not a teddy gap).
- Nested write requires a `SchemaNode` (round-trip case); schema synthesis
  from bare `Nested` values is deferred.
- JSON: `\uXXXX` escapes; reading nested JSON values into `Nested`.
- `Dataframe.castSafe` boxed-dispatch compile issue; `cumProd` debug-overflow
  panic.
- Performance: no SIMD/parallel/lazy/mmap yet — the harness (§7) is the
  prerequisite now in place.

---

## 10. Building & running

```sh
zig build                      # build the library + demo exe
zig build run                  # run the demo (loads data/ files)
zig build test                 # full test suite (~782 tests, ~27s)
zig build bench -Doptimize=ReleaseFast            # benchmarks (real numbers)
zig build bench -Doptimize=ReleaseFast -- --json base.json     # capture baseline
zig build bench -Doptimize=ReleaseFast -- --baseline base.json # before/after Δ%
python3 validation/gen_fixtures.py   # regenerate parquet fixtures (needs pyarrow)
python3 validation/regression.py     # regenerate the regression fixture + golden
```

Consumed as a library via `b.dependency("teddy").module("dataframe")`.
```
