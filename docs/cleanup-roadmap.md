# Teddy — Cleanup & Refactor Roadmap

Two tracks: a fast **Cleanup track** (one commit per phase, reviewed) and a
**Build track** (feature work; design pass before coding). Work proceeds one
phase at a time with review + commit at each checkpoint.

---

## Cleanup Track

### Phase 1 — Remove dead boilerplate ✅
- Delete `src/root.zig` (Zig template `add`/`bufferedPrint`, unreferenced, not in build).
- `zig build test` must stay green.
- Commit: `chore: remove unused root.zig template boilerplate`

### Phase 2 — Fix `.gitignore` ✅
- Strip all PHP/Zend/Doctrine/Composer entries.
- Rewrite for Zig: `.zig-cache/`, `zig-out/`, `.DS_Store`, editor/OS noise.
- Resolve conflict: keep `.claude/` ignored but add `!.claude/memory/` (+ `**`) so team memory commits.
- Commit: `chore: rewrite .gitignore for Zig project`

### Phase 3 — `functions.zig`: drop demo, build real version ✅
- Remove `add5a` + its demo test (`dataframe_test.zig:127-138`).
- Brainstorm first: generic, broadly-useful functions for the average library
  user (element-wise math `abs`/`round`/`pow`/`clamp`, predicates, string
  transforms, common reducers) designed to plug into `applyInplace`/`applyNew`.
- Implement agreed set + tests.
- Commit: `feat: replace add5a demo with general-purpose function library`

### Phase 4 — Strings module audit (keep + right-size) ✅
- Map usage of `String` and the deprecated free fns `createString`/`createStringFromSlice`.
- Decide: keep as thin wrappers or fold callers onto `String.init`/`fromSlice`.
- Document realistic scope for future expansion. Commit only if changed.

### Phase 5 — `plan.md` triage ✅
- Review against current code; check off / delete completed items.
- Salvage genuinely-not-done items into this roadmap, then move to `docs/` or delete.
- Commit: `docs: retire completed roadmap`

---

## Build Track (design pass, then code)

### Phase 6 — Parquet reader: read (almost) anything ✅ COMPLETE (2026-06-07)
Sub-phases:
- **6a — Research ✅** — full Parquet type system → Zig mapping documented in
  `docs/parquet-type-mapping.md` (deep-research, primary sources verified).
- **6b — Multi-row-group concat ✅** — unified the single/multi-RG paths into one
  `readLeafConcat` that concatenates every row group's values + validity. Added a
  pyarrow-generated 3-row-group fixture (`data/multi_rowgroup.parquet`,
  `src_py/gen_fixtures.py`) + reader test. Also wired the previously-dead parquet
  reader/writer inline tests into the aggregator.
- **6c — `FIXED_LEN_BYTE_ARRAY` + `INT96` decode ✅** — both now read as raw owned
  bytes (FLBA uses schema `type_length`; INT96 is fixed 12 bytes) across plain,
  dictionary, moveInto/expandWithNulls, and multi-RG concat paths. Semantic
  typing (Decimal/UUID/Timestamp) deferred to 6d. Fixtures `data/flba.parquet` +
  `data/int96.parquet` (`gen_fixtures.py`) + reader tests.
- **6d-1 — Unsigned INT mapping ✅** — bridge now maps `uint_32`→`u32` and
  `uint_64`→`u64` via full-range bitcast (was silently read as signed). Fixture
  `data/unsigned.parquet` + bridge test.
- **6d-2a — New scalar Series types ✅ COMPLETE (2026-06-05)** — add Date/Time/Timestamp/
  Decimal/Binary/FixedBytes/Uuid/Interval/Float16 as distinct types + variants;
  parse the modern `LogicalType` union (SchemaElement field 10); full lossless
  round-trip; rich per-type ops; `Raw` fallback for deferred types. Design approved,
  spec at `docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md`.
  Implemented as per-type vertical slices, ALL DONE (6d-2a.0 infra ✅ →
  .1 Date ✅ → .2 Timestamp/Time ✅ → .3 Decimal ✅ → .4 Binary/FixedBytes ✅ →
  .5 Uuid/Interval/Float16 ✅). Every scalar row of the spec's type table is
  implemented; FLOAT16 is native Series(f16) and participates in numeric
  aggregations + GroupBy. Unannotated BYTE_ARRAY/FLBA now read as Binary/FixedBytes
  (String requires a UTF8/ENUM/JSON annotation — deliberate behavior change in
  .4). Decimal is i256-backed (precision ≤ 76) across all three
  physicals (INT32/INT64/FLBA) with scale-aware strict arithmetic. INT96 now
  decodes to Timestamp(nanos, utc=false, origin=int96);
  writes default to modern INT64, bit-faithful INT96 re-emit behind
  `Writer.withEmitInt96(true)`. Infra slice
  landed: thrift LogicalType parse/encode (field 10 + type_length), Series
  capability convention (hasMethod/ColumnMeta), comptime-safe BoxedSeries
  guards, `Raw` fallback type — INT96 + VARIANT/GEO now read end-to-end and
  round-trip bit-faithfully.
- **6d-2b — Nested types ✅ (2026-06-07; this work IS former item 6e)** —
  LIST/MAP/STRUCT read end-to-end: owned `SchemaNode` trees with correct
  cumulative def/rep levels (fixed the group-skip leaf mis-indexing bug AND a
  latent RLE bit-width bug), Dremel record assembly into the recursive
  `Nested` value type (`Series(Nested)` columns with JSON rendering,
  accessors, deep eql/order/hash), fixtures covering list/struct/map and
  their nestings, fuzz battery through the assembly path. Spec:
  `docs/superpowers/specs/2026-06-07-parquet-nested-types-design.md`.
  Scope decision (locked): READ-side only — nested WRITE returns
  `error.UnsupportedNestedWrite` and is its own backlog item below;
  VARIANT/GEO stay `Raw` (bit-faithful round-trip) until a variant-format
  spec.

The design note's §5 open decisions are all **resolved** by the approved 6d-2a
spec: Decimal = i256 (precision ≤ 76); INT96 decodes to Timestamp(nanos) with
opt-in INT96 re-emit; VARIANT/GEO → `Raw` fallback; nested types → separate
spec (6d-2b).

### Phase 7 — JSON reader/serialization fixes ✅ (2026-06-08; mostly absorbed by Phase 12 B2)
- Fixed `.auto => unreachable` by construction (detectFormat returns a
  no-auto `DetectedFormat`; dispatch is exhaustive).
- Hardened detectFormat (brace-line count, not a `\n{` substring heuristic).
- Object keys are now unescaped + owned (`{"a\"b":1}` → correct column name).
- Integer literals beyond i64 fall back to f64 instead of erroring.
- `\b`/`\f` string escapes decoded. Remaining gap (documented): `\uXXXX`
  unicode escapes emit raw; reading nested JSON values (arrays/objects as
  row values) into `Nested` is its own future feature. (Phase 9.2 added the
  WRITE side: `Series(Nested)` columns now serialize to valid JSON via
  `nested_json.zig`; READ-back reconstruction remains the future feature.)

### Phase 8 — Native Zig serialization format (TDF) ✅ (2026-06-15)
- Ergonomic, 1:1, uncompressed, lossless on-disk format mirroring the
  in-memory dataframe. Round-trips ALL 16 column types incl. Nested,
  Decimal precision/scale, Timestamp origin, Raw annotations, FixedBytes
  width — the only teddy format that does. `FileType.tdf`, `.tdf` extension.
- `src/dataframe/native_format.zig` (writeToString/parse); spec
  `docs/superpowers/specs/2026-06-08-phase-8-native-format-design.md`;
  canonical on-disk reference `docs/tdf-format.md`. 29 round-trip +
  malformed-input tests incl. a parquet→TDF→render end-to-end.
- Magic "TEDDYDF1" + format_version u16; reserved `flags` u16 is the
  future compression hook (no version bump needed). Untrusted-input
  hardened to the parquet-reader bar (checked casts, bounds, validated
  enums, depth-bounded recursion — never panics).

### Phase 9 — Python↔Zig regression framework (new, large) ✅ (2026-06-15)
- Reorganize `src_py/` into a proper folder (`validation/`). ✅ (9.0)
- Harness: transform in pyarrow → same transform in Teddy → diff results.
  ✅ (9.1 transforms + round-trip stages)
- `data/` fixtures stay as-is (intentional test inputs).
- **9.2 — divergence triage + lock-in** ✅: fixed the three behavior decisions
  (`sumChecked` inline prongs + native-width `sum()` kept; `argSort` nulls-last
  parity; Nested → valid JSON via `nested_json.zig`), then converted the soft
  report into an ENFORCED allowlist + committed CSV/JSON fidelity matrix in
  `src/dataframe/regression_test.zig`. Every justified divergence and its
  rationale is documented in [`docs/validation-divergences.md`](validation-divergences.md);
  the test now fails on any new/stale divergence, any fidelity-matrix change,
  any TDF round-trip loss, or invalid Nested JSON.

### Phase 13 — Nested parquet WRITE (new, from 6d-2b scope decision) ⬜
- def/rep level generation from `Nested` row trees + nested schema emission
  (tree-shaped SchemaElements with num_children) + per-leaf chunk writing.
  Comparable in size to the 6d-2b read work. Until then nested columns write
  as a clean `error.UnsupportedNestedWrite`.
- Also from the 6d-2b review: columnar nested representation if perf ever
  matters; file-order column interleaving for mixed nested+flat reads
  (nested columns currently surface after flat ones).

---

## Hardening Track (from the 2026-06-04 full-project review)

Findings, file:line detail, and rationale live in
`docs/reviews/2026-06-04-full-project-review.md`. Planned to land after the
6d-2a slices (the 6d-2a.0 infra commit already fixes the review's Theme-2
comptime hazards).

### Phase 10 — Null-correctness pass ✅ (review Theme 1; landed 2026-06-07 as 10a+10b)
- Readers (parquet/JSON/CSV adapters): call `appendNull` instead of
  materializing placeholders; round-trip tests asserting `isNull`.
- Writers (CSV/JSON): check `isNull` explicitly (empty CSV field, bare JSON
  `null`) instead of relying on `asStringAt`'s `"null"` literal.
- GroupBy: null-key policy + `isNull` guards in sum/mean/min/max/stdDev.
- Join: nulls never match (SQL semantics); unmatched outer-join cells become
  real nulls via `appendNull`, not fabricated 0/"".
- `indicesWhere`/filter and `applyInplace` skip null slots.
- Sub-phase (larger): parquet writer definition levels so nulls survive
  df→parquet (`ColumnData.validity`, OPTIONAL columns).

### Phase 11 — Parquet untrusted-input hardening ✅ (review Theme 3; landed 2026-06-07)
- Checked narrowing helpers replacing `@intCast` on file-controlled values;
  non-exhaustive enums (or checked `@enumFromInt` wrapper).
- Bounds-check every length prefix / offset before slicing (def levels,
  dictionary indices, column offsets); bound thrift `skip` recursion depth.
- Fix `expandWithNulls` byte-array leak on malformed def-levels.
- Malformed-file test suite: "garbage bytes error, never panic".

### Phase 12 — Bug-fix batch ✅ (review Theme 4; landed 2026-06-07 — hardening track complete)
- `groupByMultiple` double-free + non-transactional `_group_key` mutation.
- Latent compile errors: `json_reader` `@trunc` int arm; `tryAppend[Slice]`
  missing allocator; JSON mixed-type columns silently becoming `""`.
- Builder `withPath` error swallowing; cross-column length invariant
  (`height`/`dropRow`/`print` on ragged frames); join duplicate column names;
  `slice()` schema loss; `asStringAt` 128-byte buffer; `fromDataframe`
  error-path leak; unify error sets (`TypeNotNumeric`/`TypeMismatch`).
