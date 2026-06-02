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

### Phase 5 — `plan.md` triage ⬜
- Review against current code; check off / delete completed items.
- Salvage genuinely-not-done items into this roadmap, then move to `docs/` or delete.
- Commit: `docs: retire completed roadmap`

---

## Build Track (design pass, then code)

### Phase 6 — Parquet reader: read (almost) anything ⬜
- Multi-row-group concatenation (`parquet_reader.zig:74,100`).
- Nested/group schemas (currently flat-only, `:127`).
- `FIXED_LEN_BYTE_ARRAY` proper handling (`column_reader.zig:350`).
- Research: map full Parquet logical/physical type set to type-safe Zig
  representations; define fallback to boxed/dynamic type where static isn't viable.

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
