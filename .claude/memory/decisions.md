---
name: decisions
description: Standing design decisions for teddy made during the cleanup effort
metadata:
  node_type: memory
  type: project
---

Decisions locked in during the cleanup planning (2026-06-01), see [[cleanup-roadmap]]:

- **Keep the strings module** — it's intended to be expanded, not removed. The
  deprecated `createString`/`createStringFromSlice` free fns are fine to keep
  (right-size scope, don't delete wholesale).
- **`functions.zig` should grow** into a comprehensive set of generic, broadly
  useful functions for the average library user (to feed `applyInplace`/`applyNew`).
  `add5a` was only a throwaway demo and gets removed.
- **Parquet reader goal:** read almost any Parquet file and map columns to
  type-safe Zig representations (research needed on full type coverage).
- **New native Zig serialization format** wanted: ergonomic, 1:1 with in-memory
  structures, save/load with no processing, uncompressed by default.
- **Python helper becomes a regression-testing framework:** same transformation
  in pandas vs Teddy, compare results. `src_py/` to be reorganized.
- **Generated data files in `data/`** (e.g. addresses_out.csv) are intentional
  test fixtures — keep them.
