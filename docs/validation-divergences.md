# teddy ↔ pyarrow — Justified Divergences

Phase 9 runs a Python↔Zig regression framework (see `validation/README.md`)
that compares teddy's behavior, type-by-type, against pyarrow (the Arrow
reference engine). Most behavior is exact parity. The divergences below are
**justified** — each is a deliberate design choice with a documented rationale,
encoded in a committed allowlist so the test passes while a NEW, unexpected
divergence fails it (regression protection).

The harness enforces these in `src/dataframe/regression_test.zig`:
- `known_divergences` — the read/transforms/sort allowlist (table A).
- `expected_fidelity` — the CSV/JSON round-trip type matrix (tables C/D).
- `enforceNestedJsonValid` — Nested JSON write must be valid JSON.

The raw run is cataloged in `data/validation/divergence_report.txt`.

---

## A. Aggregation divergences (transforms stage)

| Stage | Column(s) | Kind | teddy | pyarrow | Rationale |
|---|---|---|---|---|---|
| transforms | `c_u8`, `c_u16`, `c_u32`, `c_u64` | `sum` overflow | native-width accumulator (overflows / panics on extreme data) | widens the accumulator | **User decision:** `sum()` keeps the column's native integer width (fast, predictable type). On data whose total exceeds the native width it overflows — documented and intentional. `BoxedSeries.sumChecked()` is the safe opt-in: it accumulates in the native width with `@addWithOverflow` and returns `error.Overflow` instead of panicking. |
| transforms | `c_i64` | `mean` | `-0.2` (f64 accumulation) | `8.2` (exact) | teddy computes `mean` by summing into an **f64** accumulator then dividing. On boundary values (`c_i64` holds `i64::MIN` and `i64::MAX`) the f64 mantissa cannot represent the exact integer sum, so the result differs from pyarrow's exact integer accumulation. Standard floating-point accumulation behavior. |

### `sum` vs `sumChecked`

- `sum()` / `BoxedSeries.sum()` — native-width accumulation; may overflow on
  extreme data (justified divergence above).
- `sumChecked()` / `BoxedSeries.sumChecked()` — same native width, but returns
  `error.Overflow` on the first overflowing addition. Use this when totals may
  exceed the column's element width.

---

## B. Sort ordering — NOW PARITY (no longer a divergence)

teddy's `argSort` places **nulls last** in both ascending and descending
directions (pandas `na_position='last'`), matching pyarrow's
`pc.sort_indices(..., null_placement="at_end")`. As of Phase 9.2 this is
**parity**; the harness asserts the SORT stage produces zero divergences, so a
regression here fails the test.

---

## C. CSV round-trip fidelity matrix

CSV is a text format with **no type metadata**. On read-back, the CSV reader
re-infers a type from the text. This is expected, documented behavior (not a
bug). The full original-type → re-read-type matrix:

| Column | Original teddy type | Re-read type |
|---|---|---|
| `c_bool` | bool | String |
| `c_i8` / `c_i16` / `c_i32` / `c_i64` | i8 / i16 / i32 / i64 | i64 |
| `c_u8` / `c_u16` / `c_u32` | u8 / u16 / u32 | i64 |
| `c_u64` | u64 | f64 |
| `c_f32` / `c_f64` / `c_f16` | f32 / f64 / f16 | f64 |
| `c_str` | String | String |
| `c_binary` | Binary | String (hex text) |
| `c_fixed` | FixedBytes | String (hex text) |
| `c_date` | Date | String |
| `c_time32` / `c_time64` | Time | String |
| `c_ts_utc` / `c_ts_naive` / `c_ts_ns` | Timestamp | String |
| `c_dec9` / `c_dec38` | Decimal | f64 |
| `c_uuid` | Uuid | String |
| `c_list` / `c_struct` / `c_map` / `c_list_struct` / `c_list_list` | Nested | String (rendered text read back as a string) |

Integer widths collapse to `i64`; `u64` and Decimal widen to `f64`; all
non-numeric types degrade to their text rendering (`String`). No type metadata
survives the text format.

---

## D. JSON round-trip fidelity matrix

JSON preserves more than CSV (numbers stay numeric, `null` stays bare null),
but it still carries no teddy type metadata, so scalars re-type:

| Column | Original teddy type | Re-read type |
|---|---|---|
| `c_bool` | bool | String |
| `c_i8` … `c_i64`, `c_u8` … `c_u32` | int widths | i64 |
| `c_u64` | u64 | f64 |
| `c_f32` / `c_f64` / `c_f16` | float widths | f64 |
| `c_str` | String | String |
| `c_binary` / `c_fixed` / `c_uuid` | Binary / FixedBytes / Uuid | String |
| `c_date` / `c_time*` / `c_ts_*` | Date / Time / Timestamp | String |
| `c_dec9` / `c_dec38` | Decimal | String (quoted for exactness) |
| `c_list` / `c_struct` / `c_map` / `c_list_struct` / `c_list_list` | Nested | **valid JSON on WRITE; not reconstructed on READ** |

### Nested JSON: valid write, no read-back (Phase 7 gap)

As of Phase 9.2, writing a `Series(Nested)` column to JSON produces **valid
JSON** via the dedicated `src/dataframe/nested_json.zig` renderer:
- structs → `{"name": val, ...}` using field names from the column's
  `SchemaNode` (falls back to a positional `[..]` array — still valid JSON —
  when the schema is absent or doesn't line up),
- lists → `[..]`, maps → `{"key": val, ...}`,
- scalars inside nested → JSON strings (string/date/etc.) or bare
  (number/bool/null).

`Nested.format` is unchanged — it stays positional (`{1, "x"}`) for
CSV/print/`asStringAt`. The harness asserts every Nested column's JSON output
parses via `std.json` (`enforceNestedJsonValid`).

**Read-back is NOT supported:** the JSON reader cannot reconstruct a `Nested`
column from JSON (a documented Phase 7 limitation). The per-column JSON
round-trip therefore reports `parse failed: InvalidJson` at the READ step — a
known limitation pinned in the fidelity matrix, not a regression.
