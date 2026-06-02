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
8) native Zig serialization format, 9) Python↔Zig regression framework.

**Why:** owner wants the codebase "up to snuff" — remove template cruft, finish
half-implemented IO, add validation.

**How to apply:** work one phase at a time; user reviews + commits each phase
before the next. Update the ⬜/✅ markers in docs/cleanup-roadmap.md as phases land.
