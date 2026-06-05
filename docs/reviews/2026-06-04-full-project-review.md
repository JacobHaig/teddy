# Full-Project Review — 2026-06-04

Five parallel read-only review agents covered: (1) parquet layer, (2) core type
system, (3) IO/adapters, (4) dataframe ops/joins/API, (5) project alignment/docs.
Verification basis: `zig build test --summary all` → **414/414 pass** (226
dataframe + 188 parquet), Zig 0.16.0. Findings below are consolidated and
deduplicated; severities are the reviewers', kept where agents agreed.

## Verdict by area

| Area | Health | Headline |
|---|---|---|
| Parquet layer | ⚠️ Moderate | Happy path solid; **not safe against malformed files** (panics, not errors) |
| Core type system | ⚠️ Moderate | Per-Series null/ownership careful; `is_numeric` + `inline else` will break the build when variants are added (already addressed in the 6d-2a.0 plan) |
| IO/adapters | ⚠️ Moderate | Structurally sound; **nulls silently lost on every read path** |
| Ops/joins/API | ⚠️ Moderate | API coherent; **join null handling fabricates data**; 1 reachable double-free |
| Alignment/docs | ✅ Good | Build/test wiring clean, phases match code; doc drift + repo hygiene items |

---

## Theme 1 — Null fidelity is broken end-to-end (CRITICAL, cross-cutting)

The `Series` validity machinery exists and per-Series ops use it correctly, but
almost everything *around* it drops or fabricates nulls:

| # | Finding | Where |
|---|---|---|
| N1 | All three readers materialize nulls as placeholders (0 / "" / false) instead of `appendNull` — validity read only to pick the placeholder | `dataframe/parquet.zig:33-102`, `json_reader.zig:271-316`, `csv_reader.zig:299-307` |
| N2 | Parquet write side cannot represent nulls at all: `ColumnData` has no validity field; everything written as REQUIRED, no definition levels | `column_writer.zig:25-36`, `parquet.zig:130-236`, `parquet_writer.zig:46-50` |
| N3 | GroupBy keys on raw values — null rows bucket under the placeholder and merge with real 0/"" rows; `sum/mean/min/max/stdDev` aggregators fold placeholder values (prod/first/last/median/nunique correctly skip) | `group.zig:91-105, 194, 236, 308, 357` |
| N4 | Join: null keys match placeholder values on the other side (`NULL == 0`); unmatched left/right/outer cells are appended as real 0/""/false **without** a validity entry — fabricated data visible to all downstream ops | `join.zig:60-94, 147-156` |
| N5 | `indicesWhere`/filter matches null rows by placeholder (`.eq, 0` matches nulls) | `boxed_series.zig:418-463` |
| N6 | `applyInplace` transforms placeholder slots (inconsistent with aggregations that skip) | `series.zig:299` |
| N7 | `asStringAt` returns the literal `"null"` → CSV would emit the text `null` (not empty field); JSON string columns would emit `"null"` (quoted) instead of bare `null` | `series.zig:126`, `csv_writer.zig`, `json_writer.zig:80-97` |
| N8 | Test gap: the only "null values" reader test asserts row count, never `isNull` — N1 passes silently | `json_reader_test.zig:57` |

**Direction:** read-side fixes (N1) are cheap (`appendNull` already works); N3/N4/N5
need a null-policy decision (SQL-style: nulls never match/never aggregate);
N2 needs real parquet-writer work (definition levels). Writers (N7) should
check `isNull` explicitly rather than relying on `asStringAt`.

## Theme 2 — Variant-addition comptime hazards (HIGH; mostly covered by the 6d-2a.0 plan)

`BoxedSeries` `inline else` arms instantiate `Series(T)` methods for **every**
variant, so adding Raw/Date/Decimal/… breaks the build unless gated:

- `is_numeric = !(String or bool)` misclassifies every new struct type → numeric
  methods fail to compile (`series.zig:468`). **Covered:** plan Task 7(b).
- `mean/stdDev/median/quantile/cum*/diff*` use `.string,.bool`-only exclusion
  (`boxed_series.zig:303-355`). **Covered:** plan Task 8.
- `GroupByContext.eql` does `a == b` (compile error for struct variants)
  (`group.zig:30`). **Covered:** plan Task 7(e).
- **NOT covered by the plan — verify during Task 10:** `join.zig:33` dispatches
  `inline else` into `joinTyped`, instantiating `GroupByContext(T)` +
  `HashMap(T,…)` + default-value synthesis for every variant. The
  GroupByContext capability fix may make it compile for `Raw`, but the
  unmatched-cell default-append path must be checked (and join-on-Raw should
  arguably be `error.TypeNotJoinable`).
- Pre-existing trap (not new-variant-related): `BoxedSeries.clip(bool|String, …)`
  is a hard compile error — the `if (comptime *Series(T) == @TypeOf(s))` guard
  doesn't prevent instantiating `Series(bool).clip`'s `@compileError`
  (`boxed_series.zig:358-367`).

## Theme 3 — Parquet reader not hardened against untrusted input (CRITICAL for a file parser)

Systematic "validate-before-trust" gaps; a malformed file panics the process
instead of returning an error:

- P1 **Unchecked dictionary index** → OOB panic (`column_reader.zig:381-389`).
- P2 **Definition-level length prefixes unchecked** → slice-bounds panic; v2
  `uncompressed_size - rep_len - def_len` can underflow usize
  (`column_reader.zig:93-101, 137-166`).
- P3 **`@enumFromInt` on exhaustive enums** traps on any unknown enum value
  (Encoding has no tag 1; ConvertedType incomplete; future codecs)
  (`metadata.zig:63-68` and throughout). Make enums non-exhaustive (`_`) or use
  a checked helper. Directly relevant to 6d-2a: logical types bring richer
  metadata that will hit these.
- P4 **`@intCast` on file-controlled values** panics on overflow/negative:
  `thrift_reader.zig:77,90`; `column_reader.zig:41,59,67-68`;
  `parquet_reader.zig:24,39`. Use `std.math.cast` → error.
- P5 `start_offset` never validated `< file_data.len` before slicing
  (`column_reader.zig:42,63`).
- P6 Leak in `expandWithNulls` byte-array path when def-levels disagree with
  decoded value count (`column_reader.zig:493-513`).
- P7 `skip()` recursion unbounded + fixed-16 field-id stack silently corrupts
  past depth 16 (`thrift_reader.zig:171-225`) — bound nesting depth in the
  6d-2a.0 LogicalType decode too.
- P8 Test gap: zero malformed-file tests ("garbage bytes error, never panic").

## Theme 4 — Isolated correctness bugs

- B1 **`groupByMultiple` double-free** (reachable today): manual
  `composite.deinit()` + `errdefer` both fire on `error.ColumnNotFound`; and
  after ownership transfers to `self.series`, a later failure fires the
  errdefer on a series the dataframe now owns (`dataframe.zig:243-269`). Also
  mutates `self` (adds `_group_key`) non-transactionally.
- B2 `json_reader.zig:274`: `@as(i64, @trunc(f))` is invalid (needs
  `@intFromFloat`) — latent compile error in a currently-dead branch; mixed
  string/number JSON columns silently become `""` (`json_reader.zig:250-255,
  308-313`).
- B3 `tryAppend`/`tryAppendSlice` `[]const u8` branches miss the allocator arg —
  latent compile error, never instantiated (`series.zig:212, 231`).
- B4 `Reader.withPath`/`Writer.withPath` swallow allocation failure
  (`catch return self` / `catch null`) (`reader.zig:53-68`, `writer.zig:54-58`).
- B5 No cross-column length invariant: `height()` trusts column 0; `dropRow`/
  `print` can panic or desync ragged frames (`dataframe.zig:52-57, 91-95`).
- B6 Join: duplicate column names when non-key columns collide; no suffixing
  (`join.zig:114-118`).
- B7 `slice(start>=end)` returns a zero-column frame (loses schema) — inconsistent
  with `head`/`filter` (`dataframe.zig:540`).
- B8 `asStringAt` fixed 128-byte buffer fails on long strings; `groupByMultiple`
  key-building depends on it (`series.zig:129`).
- B9 Error-path leak of `slices` in `fromDataframe` when a later column is
  unsupported (`dataframe/parquet.zig:139-145`).
- B10 Error-set inconsistency: `TypeNotSummable/Averageable/Comparable/Numeric/
  Mismatch` mixed across group.zig/boxed_series.zig.
- B11 `indicesWhere` prints to stderr (`std.debug.print`) inside a library
  function that already returns an error (`boxed_series.zig:457`).

## Theme 5 — Docs & repo hygiene

- D1 **Decimal drift (contradiction):** approved spec says i256 / precision ≤ 76,
  but `docs/parquet-type-mapping.md:91,106-109,126-135` and
  `.claude/memory/parquet-type-mapping.md:17` still say i128 / ≤ 38, and §5
  lists already-resolved decisions as open; `cleanup-roadmap.md:74-75` repeats
  "open decisions before 6d".
- D2 **CONTINUE.md deletion is unstaged** — it's tracked in HEAD; the `rm` alone
  leaves the repo half-deleted.
- D3 `docs/superpowers/plans/` untracked (spec is committed; the plan executing
  it is not).
- D4 `data/addresses{,_snappy}.parquet` have no generator in `gen_fixtures.py`
  although the docstring claims it regenerates committed fixtures.
- D5 `README.md:215-219` broken `Reader.init` example (redeclared, missing `io`
  arg); `README.md:289` says ~385 tests (actual 414); `cleanup-roadmap.md:52`
  says 393→411 (stale).
- D6 `nullable_design.md` orphaned at repo root (other design docs live in
  `docs/`).
- D7 `.DS_Store` tracked despite `.gitignore` entry.
- D8 6d-2a.0 plan baseline says "413/414"; actual is 414/414, 0 skipped.

## Recommended sequencing (proposal — see session discussion)

1. **Hygiene + docs commit (small, now):** D1–D8.
2. **6d-2a.0 as planned** — it already fixes the Theme-2 comptime hazards;
   extend its Task 10/11 verification to include `join.zig` compiling (and
   behaving) with the `Raw` variant.
3. **Null-correctness phase** (Theme 1): read-side `appendNull` fixes + writer
   `isNull` handling + groupBy/join null policy + null round-trip tests.
   Parquet writer definition levels (N2) as its own sub-phase.
4. **Parquet input-hardening phase** (Theme 3): checked narrowing helpers,
   non-exhaustive enums, bounds checks, malformed-file test suite.
5. **Bug-fix batch** (Theme 4): B1 first (reachable double-free), then the rest.
