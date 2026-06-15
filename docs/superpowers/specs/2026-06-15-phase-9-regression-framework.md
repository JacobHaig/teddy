# Phase 9 — Python↔Zig Regression Framework

**Status:** design + build. Divergences are surfaced for joint
justification with the user BEFORE the framework's expectations are locked
and committed (the user: "I expect there to be differing behaviours … justify
the behaviour and make sure they are in line with our actual expectations").

## Reference engine: pyarrow (not pandas)

pandas isn't installed and is the *weaker* reference — it flattens Decimal →
`object`, Time → `object`, drops parquet annotations. **pyarrow** (24.0.0,
+`pyarrow.compute`) carries the full Arrow type system (the one parquet
encodes) and provides aggregations/sort, so it is the authoritative reference
for "every data type end-to-end." pandas remains a possible future add for
DataFrame-API parity.

## Architecture: golden-file model (CI needs no Python)

```
validation/                 (reorganized from src_py/)
  gen_fixtures.py           existing data/ fixtures (moved; paths unchanged)
  regression.py            NEW — emits the comprehensive fixture + golden
  README.md                run instructions + the semantic protocol
data/validation/
  alltypes.parquet         one column per teddy type, every type, with nulls
  alltypes.golden.json     pyarrow's ground-truth manifest (semantic + transforms)
src/dataframe/
  regression_test.zig      NEW — loads fixture+golden (std.json), asserts teddy
                           matches each cell SEMANTICALLY; runs transforms;
                           collects ALL divergences; checks them against a
                           committed known-divergences allowlist
docs/
  validation-divergences.md  every teddy-vs-pyarrow divergence + justification
```

Python regenerates the fixture + golden when types change (like the existing
pyarrow fixtures); the Zig test is the committed regression check.

## Semantic value protocol (compare VALUES, not rendering)

Both sides produce an implementation-neutral canonical per cell, so
formatting differences don't masquerade as divergences and real ones stand
out. The golden stores it; the Zig harness extracts the SAME via typed
accessors (not asStringAt).

| Type | golden `v` | teddy extraction |
|---|---|---|
| bool | `"0"`/`"1"` | `@intFromBool` |
| int/uint (all widths) | decimal string | `{d}` of value |
| f16/f32/f64 | JSON number | value, compared with rel-tolerance 1e-6 (f16 looser) |
| String | raw UTF-8 string | `toSlice` |
| Date | days-since-epoch (int string) | `Date.days` |
| Time | nanos-since-midnight (int string) | `Time.toNanos` |
| Timestamp | `"<epoch-nanos>:<utc 0|1>"` | `Timestamp.toNanos` + utc |
| Decimal | `"<unscaled>:<scale>"` | `unscaled`,`scale` |
| Binary/FixedBytes/Uuid | lowercase hex | bytes→hex |
| Interval | `"<months>:<days>:<millis>"` | fields |
| Nested | canonical JSON (lists `[]`, structs `{}` positional, maps as `[[k,v]]`) | `Nested.format` normalized form |
| null | `{"null": true}` | `isNull` |

Per-column the golden also carries `expected_type` (teddy `typeName()`) and
`arrow_type` (pyarrow type str, for the report).

## Coverage: every type, end-to-end

`alltypes.parquet` columns (each with ≥1 null where the type is nullable):
bool; int8/16/32/64 (signed logical INT annotations); uint8/16/32/64; float32;
float64; float16; string(utf8); large_string?(→ String, note); binary
(unannotated → Binary); fixed_size_binary(n) (→ FixedBytes); date32 (→ Date);
time32[ms]+time64[us] (→ Time); timestamp[ms,us,ns] tz + no-tz (→ Timestamp);
decimal128(9,2)/decimal128(38,10) (→ Decimal, INT32/INT64/FLBA backings);
uuid (→ Uuid); float16 (→ Float16); list<int64>, struct, map, list<struct>,
list<list> (→ Nested); INT96 timestamps (→ Timestamp origin int96). Types
teddy maps to Raw (no pyarrow constructor for VARIANT/GEO) are covered by the
existing unit tests, noted as out-of-fixture.

End-to-end stages, each compared to the golden:
1. **read**: parquet → teddy `toDataframe`; semantic value + type parity.
2. **transforms**: numeric columns — sum/mean/min/max vs `pyarrow.compute`
   (tolerance); ascending sort (argSort) order vs `pc.sort_indices` on a chosen
   column.
3. **round-trips**: teddy df → TDF → parse → still matches golden (lossless);
   → CSV → read and → JSON → read where the type survives (document lossy
   ones, e.g. Date→CSV→read returns String).

## Divergence handling (the crux)

The harness COLLECTS every mismatch (it does not stop at the first) into a
report: `(stage, column, row, arrow_value, teddy_value, kind)`. Each is then
triaged into:
- **bug** → fix teddy.
- **justified divergence** → recorded in `docs/validation-divergences.md`
  with rationale, AND encoded in a committed `known_divergences` table the
  test consults so it passes while documenting WHY (e.g. INT96 → Timestamp
  nanos rather than pyarrow's tz-naive datetime; unannotated binary → Binary
  hex rather than a Python `bytes`; float16 precision; CSV round-trip
  re-typing). The user reviews this table before commit.

## Slices
- **9.0**: reorg src_py→validation/ (keep gen_fixtures working); regression.py
  (alltypes fixture + golden, read stage only); Zig harness (std.json load +
  semantic compare + divergence collection) wired into build/tests; prove the
  full type set flows and EMIT the first divergence report.
- **9.1**: transforms + round-trip stages; expand golden.
- **9.2**: divergence triage — fix bugs, write validation-divergences.md +
  the known-divergences allowlist; present to user for sign-off; commit.

## Out of scope
pandas DataFrame-API parity (future); fuzz/property testing; non-parquet
input generation beyond the CSV/JSON round-trip checks; performance.
