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
WRITE deferred to new Phase 13). Remaining: Phase 7 JSON fixes (partly
absorbed by Phase 12 B2), Phase 8 native serialization format, Phase 9
Python↔Zig regression framework, Phase 13 nested write.

**Why:** owner wants the codebase "up to snuff" — remove template cruft, finish
half-implemented IO, add validation.

**How to apply:** work one phase at a time. As of 2026-06-05 (after 6d-2a.0)
the review cadence changed: Claude commits each slice on green tests WITHOUT a
per-slice user gate; the user reviews in larger sections afterwards. Keep
commits per-slice and well-described so bulk review stays easy. Pushing to
origin still requires an explicit user request. Update the ⬜/✅ markers in
docs/cleanup-roadmap.md as phases land.
