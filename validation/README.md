# validation/ — Python↔Zig regression framework

Phase 9. The **reference engine is pyarrow** (24.0.0 + `pyarrow.compute`), not
pandas: pyarrow carries the full Arrow type system that parquet encodes
(Decimal, Time, Timestamp tz, nested), so it is the authoritative ground truth
for "every data type, end-to-end." pandas is a possible future add for
DataFrame-API parity.

## Files

| File | Role |
|---|---|
| `gen_fixtures.py` | Generates the per-feature parquet fixtures under `data/` used by the parquet unit tests (moved here from `src_py/` in 9.0; paths unchanged, still run from repo root). |
| `regression.py` | **NEW (9.0).** Emits the comprehensive `data/validation/alltypes.parquet` fixture + `alltypes.golden.json` manifest (one column per teddy type, with nulls), plus a tiny bonus `int96.parquet` + golden. |
| `main.py` | Legacy pandas one-off (writes addresses parquet). Kept for reference; not part of the regression flow. |

The committed Zig regression check is `src/dataframe/regression_test.zig`
(registered in `src/dataframe/tests.zig`, runs under the existing dataframe
test target — no `build.zig` change needed). **CI needs no Python**: Python
regenerates the golden when types change; the Zig test is the committed gate.

## Regenerate

From the repo root:

```sh
python3 validation/gen_fixtures.py    # per-feature parquet fixtures (data/*.parquet)
python3 validation/regression.py      # data/validation/alltypes.{parquet,golden.json} + int96
zig build test --summary all          # runs the regression harness (+ full suite)
```

`regression.py` prints the column summary it wrote. Re-running is
deterministic.

## Semantic value protocol (compare VALUES, not rendering)

Both sides emit an implementation-neutral canonical per cell, so formatting
differences never masquerade as divergences. The golden stores it as `v`; the
Zig harness extracts the SAME via typed accessors (`teddySemantic`), not
`asStringAt`.

| Type | golden `v` | teddy extraction |
|---|---|---|
| bool | `"0"` / `"1"` | `@intFromBool` |
| int / uint (all widths) | decimal string | `{d}` of value |
| f16 / f32 / f64 | JSON **number** | value; compared with rel-tolerance (1e-6; f16 1e-2) |
| String | raw UTF-8 string | `toSlice` |
| Binary / FixedBytes / Uuid | lowercase hex | bytes → hex |
| Date | days-since-epoch (int string) | `Date.days` |
| Time | nanos-since-midnight (int string) | `Time.toNanos` |
| Timestamp | `"<epoch_nanos>:<utc 0\|1>"` | `Timestamp.toNanos` + `utc` |
| Decimal | `"<unscaled_int>:<scale>"` | `unscaled`,`scale` |
| Interval | `"<months>:<days>:<millis>"` | fields (no pyarrow column; out-of-fixture) |
| Nested | canonical JSON (see below) | `Nested` recursive walk |
| null | `{"null": true}` | `isNull(row)` |

Per column the golden also carries `expected_teddy_type` (teddy `typeName()`)
and `arrow_type` (pyarrow type string, for the report).

### Nested JSON shape (kept in lockstep on both sides)

A nested cell's `v` is a **JSON value** (not a string). The Zig harness renders
its `Nested` tree to the SAME canonical text the golden's JSON renders to, then
string-compares:

- **list** → JSON array of element canonicals: `["1", "2"]`, empty `[]`.
- **struct** → JSON array of field canonicals, **POSITIONAL** (field order =
  parquet schema order; names live in the column schema, not the value):
  `["1", "x"]`.
- **map** → JSON array of `[key_v, value_v]` pairs: `[["a", "1"], ["b", "2"]]`.
- **scalar leaf** inside nesting uses its own protocol canonical: ints/bools as
  the `"..."` strings above, floats as JSON numbers, bytes as hex.
- **null element** (e.g. a null inside a list, or a null struct field) → JSON
  `null`, matching teddy's `.null_` arm.

Both sides serialize a tree to the text form `[a, b, ...]` (lists & structs)
and `[[k, v], ...]` (maps), separated by `", "`, with `null` for nulls. Equal
trees produce byte-identical text.

## Coverage (`alltypes.parquet`, 6 rows, row index 1 is the null row)

29 columns, one per teddy type:

```
c_bool  c_i8 c_i16 c_i32 c_i64  c_u8 c_u16 c_u32 c_u64  c_f32 c_f64 c_f16
c_str  c_binary  c_fixed  c_date  c_time32 c_time64  c_ts_utc c_ts_naive c_ts_ns
c_dec9 c_dec38  c_uuid  c_list c_struct c_map c_list_struct c_list_list
```

Notes / gaps:

- **INT96** can't sit in a column alongside others (pyarrow's
  `use_deprecated_int96_timestamps` is file-wide), so it lives in the separate
  `data/validation/int96.parquet` + `int96.golden.json`. teddy reads INT96 as
  `Timestamp{origin=int96, unit=nanos, utc=false}`, so its canonical is
  `"<epoch_nanos>:0"`. (9.0 ships the fixture; wiring it into the harness loop
  is a 9.1 nicety — the existing `parquet_test.zig` already pins INT96 decode.)
- **Interval** has no pyarrow constructor here → out-of-fixture; covered by the
  hand-built round-trip in `parquet_test.zig`.
- **Raw** (VARIANT/GEOMETRY deferred logical types) has no pyarrow constructor →
  out-of-fixture; covered by `resolveKind` unit tests.
- **large_string** is not included; if added it should expect `String`.

## End-to-end stages (slices 9.0 + 9.1)

The harness runs three stages over the same fixture and collects everything
into one sectioned report:

1. **read** (9.0) — parquet → teddy; per-cell semantic + type parity. Clean.
2. **transforms** (9.1) — for each NUMERIC column the golden carries
   `sum`/`mean`/`min`/`max` computed with `pyarrow.compute` over the non-null
   values (stored as JSON **numbers** — teddy aggregations all return `f64`).
   The harness calls teddy's `BoxedSeries` `sum`/`mean`/`min`/`max` and compares
   with relative tolerance (1e-6; f16 1e-2). The `sum` path replays teddy's
   native-width integer accumulation with `@addWithOverflow` so it DETECTS
   (rather than panics on) the overflow teddy's real `sum()` would hit — that
   overflow is itself a cataloged divergence (pyarrow widens; teddy keeps the
   column's native int width). pyarrow has no float16 aggregation kernel, so the
   generator computes f16 stats manually over the widened values.
3. **sort** (9.1, locked 9.2) — on `c_i32` (which has a null) the golden stores
   `sort_asc_indices` from `pc.sort_indices` (default `null_placement="at_end"`,
   i.e. nulls LAST). The harness calls `argSort(ascending=true)` and records the
   index order. As of Phase 9.2 teddy's `argSort` places nulls last in both
   directions, so this is **PARITY** — the harness expects zero `sort_order`
   entries and FAILS on any.
4. **round-trips** (9.1) — teddy df → format → re-read, as a **fidelity catalog**
   (always recorded), except TDF which is a lossless contract:
   - **TDF**: write → parse → must still match the read-stage golden values +
     types. Any `tdf_roundtrip` entry is a REAL bug. (Currently 0.)
   - **CSV**: whole-frame write → read; records `original_type -> reread_type
     (sample)` per column. Everything re-reads as String or a re-inferred
     numeric.
   - **JSON** (`.rows`): per-column write → read (one column at a time so a
     single un-reparseable column doesn't blank the catalog). Scalars survive
     better than CSV. As of Phase 9.2 the Nested **write** produces valid JSON
     (verified separately via `std.json`), but the `json_reader` still cannot
     RECONSTRUCT a Nested column, so the round-trip reports `InvalidJson` at the
     read step — a documented Phase 7 gap, not a regression.

## Divergence handling

The harness **collects every mismatch** (it does not stop at the first) into a
report — printed to stderr and written to
`data/validation/divergence_report.txt`, with one section per stage:

```
== READ ==           (0 expected)
== TRANSFORMS ==      sum/mean/min/max mismatches
== SORT ==            null-ordering divergence detail
== TDF_ROUNDTRIP ==   lossless contract — should be 0; any entry is a REAL BUG
== CSV_ROUNDTRIP ==   per-column: original_type -> reread_type (sample)
== JSON_ROUNDTRIP ==  per-column: original_type -> reread_type (sample)
```

As of **Phase 9.2 the report is ENFORCED**. The test fails on:
- any **read/transforms/sort** divergence not in the committed
  `known_divergences` allowlist (a NEW, unexpected divergence = regression);
- any **stale** allowlist entry (a justified divergence that is no longer
  observed — keep the allowlist honest);
- any **CSV/JSON fidelity-matrix** change vs the committed `expected_fidelity`
  table (a future change to text-format typing is caught);
- any **tdf_roundtrip** entry (lossless contract);
- any **Nested column whose JSON write is not valid JSON**
  (`enforceNestedJsonValid`).

The framework-can't-run cases (fixture missing, golden column absent,
`num_rows` mismatch) remain HARD failures.

Every justified divergence — `sum()` native-width overflow on u-types, `mean`
f64 precision, the CSV/JSON round-trip retyping matrices, and the Nested
write-valid / read-gap status — is documented with its rationale in
[`docs/validation-divergences.md`](../docs/validation-divergences.md). The
allowlist and the expected fidelity matrix live in
`src/dataframe/regression_test.zig`.
