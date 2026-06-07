# Phase 12 — Bug-Fix Batch Implementation Plan

> Final hardening phase from the 2026-06-04 review (Theme 4, B1–B11) plus
> stragglers flagged by later phase reviews. One commit.
> Commit policy: Claude commits on green tests; user bulk-reviews later.

**Baseline:** 629/629 tests at 715381d.

Already resolved elsewhere (verify, don't redo): B9 fromDataframe error-path
leak (fixed by the 6d-2a.2 arena refactor); the join double-free + unmatched
synthesis (6d-2a.0/Phase 10a); 32-bit varint casts + malformed map coverage
(Phase 11 close-out).

## Unit A — dataframe core (judgment-heavy)

- **B1 groupByMultiple** (dataframe.zig:243-269): double-free on the
  ColumnNotFound path (manual deinit + errdefer both fire) AND errdefer
  firing on a series the dataframe already owns after `addSeries`. Restructure:
  no manual deinit; disarm the errdefer at ownership transfer (moved flag);
  ALSO make it non-mutating-on-failure and fix the null-key gap from the 10a
  review: rows where ANY key column is null are dropped (asStringAt "null"
  composite keys must not form groups) — build the composite key column,
  drop null-component rows, groupBy, and ensure `_group_key` cleanup happens
  even on later failure (or document the mutation contract clearly if a
  non-mutating rewrite is too invasive — judgment call, report it).
- **B5 ragged frames** (dataframe.zig): `dropRow` validates `index <
  height()` AND all columns same length before removing (returns error —
  signature change `!void`; update call sites); `print` bounds-checks per
  column. `addSeries`/`createSeries` stay permissive (building block), but
  `height()` gets a doc comment stating the equal-length invariant.
- **B7 slice() schema loss** (dataframe.zig:540): empty range returns
  `filterByIndices(&.{})` (preserves columns) instead of a zero-column frame.
- **B8 asStringAt fixed buffer** (series.zig): the String arm copies through
  a 128-byte buffer — long strings error NoSpaceLeft and break
  print/groupByMultiple. Fix: String arm appends `toSlice()` directly (no
  buffer); []const u8 arm same; numeric/else arms keep the buffer (bounded).
- **B11** (boxed_series.zig indicesWhere): remove the `std.debug.print` on
  the type-mismatch path (library code must not write stderr on an error it
  returns).
- **B10 error unification**: collapse GroupBy's TypeNotSummable/
  TypeNotAverageable/TypeNotComparable to `error.TypeNotNumeric`;
  keep `error.TypeMismatch` for typed-argument mismatches. Sweep
  group.zig/boxed_series.zig/dataframe.zig; update tests pinning old names.

## Unit B — IO/builder layer

- **B2 json_reader**: `@as(i64, @trunc(f))` latent compile error →
  `@intFromFloat(@trunc(f))` (and decide: arm reachable? keep but correct);
  mixed string/number columns: numbers must STRINGIFY (via std.fmt) instead
  of becoming "" — fix the `.string` build arm's non-string fallback; clean
  the redundant `has_int and has_float` inference branch.
- **B3 tryAppend/tryAppendSlice** (series.zig): add the missing
  `self.allocator` args (latent compile errors in the []const u8 and slice
  paths); add tests instantiating both paths.
- **B4 builders**: Reader.withPath/Writer.withPath stop swallowing
  allocation failure — store a `path_err`? No: simplest honest fix is
  builder methods stay chainable but `load()`/`save()` surface a recorded
  failure: add `invalid: bool` set on alloc failure → load returns
  error.InvalidFilePath... judgment: pick the least-surprising contract,
  document it, test it (simulate is hard — at least unit-test the happy
  path and document the failure contract).
- **B6 join duplicate column names**: non-key name collisions get a
  deterministic `_right` suffix on the right side's column (document; test
  with colliding "v" columns; getSeries returns the left one unchanged).

## Unit C — review + docs + commit
Combined review; roadmap Phase 12 ✅ (hardening track complete); commit
`fix: dataframe bug batch — groupByMultiple double-free, ragged frames, JSON coercion, builder contracts (Phase 12)`.

## Out of scope
Null-aware sort ordering; SQL null-group option; describe/print cosmetics;
Phase 9 regression framework.

## As-built leftovers (final review, all low-severity / OOM-only)
1. groupByMultiple: a narrow double-free window exists only when the
   allocator fails mid-`append` between the value push and the validity push
   (dataframe.zig ~302) — disarm the key errdefer just before append if OOM
   hardening ever matters.
2. join `_right` suffix does not recursively de-collide (left having both
   "v" and "v_right" shadows the renamed right column) — doc note or
   uniquifying loop if it bites.
3. Builder double-withPath-with-failing-second-alloc is verified by
   inspection, not a FailingAllocator(fail_index=2) test.
