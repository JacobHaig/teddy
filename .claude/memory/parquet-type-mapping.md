---
name: parquet-type-mapping
description: Verified Parquet type system → Zig Series mapping design note (drives Parquet reader work)
metadata:
  node_type: memory
  type: reference
---

`docs/parquet-type-mapping.md` is the authoritative design note for how Parquet
physical + logical types map to teddy's Series type set. Produced by a verified
deep-research pass (primary Apache sources: parquet.thrift, LogicalTypes.md).

Key conclusions: 8 physical types (INT96 deprecated); logical types layer
semantics via the modern LogicalType union (ConvertedType is the legacy enum,
1:1 mapped — NOT a "rename"). Teddy must add Series variants: Date, Time,
Timestamp, Decimal (i128 unscaled, precision≤38; raw-bytes fallback for >38),
Binary, FixedBytes, Uuid, Interval, Float16 (Zig has native f16/i128), plus a
Raw fallback for INT96/VARIANT/GEO/nested. Signed/unsigned INT annotations map
onto existing i8..i64 / u8..u64.

Drives Phase 6 (6c/6d) in [[cleanup-roadmap]]. Four open decisions in §5 of the
note (nested types, decimal>38, VARIANT/GEO, INT96 decode-vs-raw).
