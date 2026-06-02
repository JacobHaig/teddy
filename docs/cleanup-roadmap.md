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

### Phase 6 — Parquet reader: read (almost) anything 🟡 in progress
Sub-phases:
- **6a — Research ✅** — full Parquet type system → Zig mapping documented in
  `docs/parquet-type-mapping.md` (deep-research, primary sources verified).
- **6b — Multi-row-group concat ✅** — unified the single/multi-RG paths into one
  `readLeafConcat` that concatenates every row group's values + validity. Added a
  pyarrow-generated 3-row-group fixture (`data/multi_rowgroup.parquet`,
  `src_py/gen_fixtures.py`) + reader test. Also wired the previously-dead parquet
  reader/writer inline tests into the aggregator (393→411 tests run).
- **6c — `FIXED_LEN_BYTE_ARRAY` + `INT96` decode ⬜** (`column_reader.zig:349`).
- **6d — Logical-type mapping ⬜** — add Date/Time/Timestamp/Decimal/Binary/
  FixedBytes/Uuid/Interval/Float16 Series variants per the design note; read
  signed/unsigned INT annotations; `Raw` fallback for unmapped types.
- **6e — Nested schemas ⬜** — MAP/LIST/STRUCT (currently flat-only,
  `parquet_reader.zig:127`); biggest, may split out.

Open decisions before 6d (see design note §5): nested-type strategy,
DECIMAL precision 39–76, VARIANT/GEOMETRY/GEOGRAPHY handling, INT96 decode-vs-raw.

### Phase 7 — JSON reader/serialization fixes ⬜
- Fix `.auto => unreachable` type inference (`json_reader.zig:31`).
- Finish remaining reader/serialization gaps.

### Phase 8 — Native Zig serialization format (new) ⬜
- Ergonomic, 1:1, no-processing on-disk format mirroring the in-memory
  dataframe/series structure. Save & load without transformation, uncompressed
  by default. Design pass (layout, versioning, null bitmap, schema header) first.

### Phase 9 — Python↔Zig regression framework (new, large) ⬜
- Reorganize `src_py/` into a proper folder (e.g. `validation/`).
- Harness: transform in pandas → same transform in Teddy → diff results.
- Expand the pandas helper into fixture-generation + parity-checking basis.
- `data/` fixtures stay as-is (intentional test inputs).
- Break into sub-phases when reached.
