---
name: cleanup-roadmap
description: Active spring-cleaning + refactor roadmap for teddy; tracked in docs/cleanup-roadmap.md
metadata:
  node_type: memory
  type: project
---

Teddy is undergoing a phased cleanup/refactor (started 2026-06-01). Full plan
lives in `docs/cleanup-roadmap.md`. Two tracks: fast Cleanup (1 commit/phase,
reviewed each time) then Build (feature work).

Phases: 1) delete dead `src/root.zig`, 2) Zig-ify `.gitignore`, 3) functions.zig,
4) strings audit, 5) plan.md triage, 6) parquet reader, 7) JSON fixes,
8) native Zig serialization format, 9) Python↔Zig regression framework, plus a
Hardening Track from the 2026-06-04 full-project review
(docs/reviews/2026-06-04-full-project-review.md): 10) null correctness,
11) parquet untrusted-input hardening, 12) bug-fix batch.

Status: 1–5 ✅; 6a–6d-1 ✅; **6d-2a COMPLETE 2026-06-05** (all six slices:
infra + Date + Time/Timestamp + Decimal(i256) + Binary/FixedBytes +
Uuid/Interval/Float16 — every scalar logical type is a dedicated BoxedSeries
variant; Raw remains only for VARIANT/GEO/nested/unknown
[[parquet-type-mapping]]). **Hardening track COMPLETE 2026-06-07**: Phase 10
(null correctness end-to-end incl. parquet definition levels), Phase 11
(malformed-file hardening + battery), Phase 12 (bug batch incl.
groupByMultiple double-free). **PHASE 6 COMPLETE 2026-06-07**: 6d-2b nested
types landed (read-side LIST/MAP/STRUCT via SchemaNode trees + Dremel
assembly into the recursive Nested value type; spec
docs/superpowers/specs/2026-06-07-parquet-nested-types-design.md; nested
WRITE deferred to new Phase 13). **Phase 7 ✅** (JSON reader hardening, 2026-06-08) and **Phase 8 ✅** (native
TDF serialization format, 2026-06-15: lossless 1:1 on-disk format for all 16
column types incl. Nested; src/dataframe/native_format.zig; docs/tdf-format.md)
done. **Phase 9 ✅** (regression framework, 2026-06-15) and **Phase 13 ✅** (nested
parquet WRITE — inverse-Dremel shredding in src/dataframe/nested_shred.zig;
teddy now reads AND writes nested parquet, pyarrow-conformant) done.
**ALL ROADMAP PHASES COMPLETE.** Deferred follow-ups only: schema synthesis
from bare Nested values; narrow-int FLAT parquet write (i8/u8/… still
UnsupportedType — pre-existing); columnar nested perf; file-order interleaving
for mixed nested+flat reads.

**Why:** owner wants the codebase "up to snuff" — remove template cruft, finish
half-implemented IO, add validation.

**How to apply:** work one phase at a time. Commit/review/push rules
(clarified 2026-06-07 after Claude committed 6d-2b ahead of a requested
review):
- DEFAULT: commit freely on green tests — commits are version-control
  checkpoints, no user gate needed. Keep them per-slice and well-described.
- EXCEPTION: if the user says they want to review ("then I'll review",
  "before I review", etc.), STOP BEFORE COMMITTING — leave the work in the
  working tree until they approve.
- PUSH: only ever on an explicit user request.
Update the ⬜/✅ markers in docs/cleanup-roadmap.md as phases land.
