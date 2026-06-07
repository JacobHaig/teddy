# Phase 10 — Null Correctness Implementation Plan

> From the 2026-06-04 full-project review, Theme 1
> (docs/reviews/2026-06-04-full-project-review.md). Two commits: 10a (null
> semantics across ops/adapters/writers) and 10b (parquet writer definition
> levels). Commit policy: Claude commits on green tests; user bulk-reviews.

**Goal:** nulls survive end-to-end. Today every reader materializes nulls as
placeholder values (0/""/false), writers can't render them, GroupBy/joins
fabricate data from them, and filters match them. The `Series` validity
machinery works — everything around it must start using it.

**Baseline:** 579/579 tests at 6d1b7a4 (pushed).

## Locked semantic decisions

| Area | Decision |
|---|---|
| Filters (`indicesWhere`) | SQL-style: null rows never match any comparison (eq/neq/lt/...) |
| `applyInplace`/`applyNew` | skip null slots (placeholders untouched) |
| CSV write | null → empty field (not the literal text "null") |
| JSON write | null → bare `null` for ALL column types (incl. string/quoted types) |
| CSV read | empty cell in numeric/bool/date-like columns → null (was 0/false); empty cell in String columns stays empty string (CSV can't distinguish — documented) |
| JSON read | JSON `null` → `appendNull` for every column type |
| Parquet read | `validity[i] == false` → `appendNull` (every addColumn arm) |
| GroupBy keys | null-key rows are DROPPED (pandas dropna=True semantics; documented; SQL-style "nulls form one group" deferred as a future option) |
| GroupBy aggregations | `isNull` guards added to sum/mean/min/max/stdDev value loops (mean/std denominators = non-null count) — matching prod/first/last/median/nunique which already skip |
| Join keys | nulls never match anything (incl. other nulls); a null-key row behaves as unmatched (kept in left/right/outer per side, dropped in inner) |
| Join unmatched cells | `appendNull` (real nulls), replacing fabricated 0/""/false/epoch |
| Parquet write (10b) | `ColumnData.validity`; columns with nulls become OPTIONAL with RLE definition levels; values buffer holds only non-null entries |

Known test fallout (expected, update the pins): csv_reader "empty cells among
numbers default to zero"; join tests asserting fabricated zeros/epoch
(6d-2a.5's Interval/Date join tests assert epoch placeholders → become
isNull asserts); json reader "null values" test gains real isNull asserts;
main.zig demo output may show `null`.

## Commit 10a units

### Unit A — Series-level ops + writers (foundation)
- `boxed_series.zig` `indicesWhere`: `if (s.isNull(i)) continue;` before match.
- `series.zig` `applyInplace` (and `applyNew` in dataframe.zig if it loops
  values directly — audit): skip null slots.
- `csv_writer.zig`: per-cell `isNull` check → empty field (stop relying on
  asStringAt's "null" literal).
- `json_writer.zig` `appendJsonValue`: `isNull` check FIRST → bare `null`.
- Tests: filter-with-nulls (eq 0 does not match null), applyInplace skips,
  CSV/JSON writer null rendering (build series with appendNull directly).

### Unit B — readers preserve nulls
- `dataframe/parquet.zig`: every addColumn arm replaces the
  `else <placeholder>` branch with `try s.appendNull()` (audit ALL arms incl.
  date/time/timestamp/decimal/binary/fixed_bytes/uuid/interval/float16/raw +
  addNarrowIntColumn/addUintColumn helpers).
- `json_reader.zig` buildDataframe: `.null` → appendNull (all type arms).
- `csv_reader.zig`: empty cell in non-String columns → appendNull.
- Update pinned tests + add round-trips: parquet nullable fixture
  (multi_rowgroup "opt" column!) reads with isNull; CSV round-trip
  numeric-with-empty-cell → null → writes back as empty field; JSON
  round-trip null → bare null.

### Unit C — GroupBy null policy
- `group.zig` setupGroups: skip rows where the key series isNull (doc the
  pandas-like policy at GroupBy + in type-system docs later).
- sum/mean/min/max/stdDev TypedDf helpers: `if (series.isNull(idx)) continue;`
  with corrected denominators for mean/stdDev.
- Tests: null keys dropped (count excludes), aggregations over value columns
  with nulls match the already-correct prod/median behavior.

### Unit D — Join null semantics
- `join.zig` joinTyped: skip null keys when building the right map; null-key
  left rows go to the unmatched branch (left/outer) or drop (inner); null-key
  right rows count as unmatched for right/outer.
- addJoinedColumn: unmatched branch → `try new_series.appendNull();`
  (replaces the init/zeroes synthesis entirely).
- Update the join tests pinned to fabricated values; add null-key tests
  (null != null, null != 0).

→ Commit 10a: `fix(nulls): preserve and render nulls end-to-end (Phase 10a)`.

## Commit 10b — parquet writer definition levels
- `encoding_writer.zig`: RLE level encoder (bit-width-aware, RLE-runs-only
  form: varint(run_len << 1) + value byte — valid per spec, reader already
  decodes it).
- `column_writer.zig`: `ColumnData.validity: ?[]const bool`; when present,
  page = [4-byte LE def-len][RLE levels (0/1, max_def 1)] + values of
  non-null slots only (each physical arm skips invalid indices BEFORE
  width-validation so null placeholders don't trip FLBA checks);
  DataPageHeader.num_values counts ALL rows.
- `parquet_writer.zig`: schema element repetition_type `.optional` when the
  column has validity.
- `dataframe/parquet.zig` boxedToColumnData: every arm passes
  `s.validity` items as `validity` (borrowed; arena not needed).
- Round-trip tests: df with nulls (several types incl. an owning type and a
  scratch-buffer type like Date) → write → read → isNull pattern identical;
  re-read of multi_rowgroup "opt" column → write → read preserves its nulls.

→ Commit 10b: `feat(parquet): write definition levels — nulls survive df->parquet (Phase 10b)`.

As-built notes (10b review): emit_int96 with null INT96 timestamps writes a
zeroed placeholder for null slots (handled in code, no dedicated round-trip
test — add one if legacy-INT96-with-nulls becomes a real input); an all-null
FixedBytes column without `meta.width` errors MissingTypeLength by design
(width cannot be inferred from zero non-null values).

## Out of scope
SQL-style null GROUP BY option; CSV quoted-empty vs bare-empty distinction;
DataPageV2; null-aware sort ordering (argSort currently sorts placeholder
values — note for a future phase); `groupByMultiple` null-key handling (it
builds composite String keys via asStringAt, so null rows group under the
literal "null" rather than being dropped — diverges from the single-key
policy; flagged by the 10a review, track in Phase 12).
