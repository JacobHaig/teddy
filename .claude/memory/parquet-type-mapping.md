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
Timestamp, Decimal (i256 unscaled, precision≤76 == Arrow decimal256; raw-bytes
fallback only for >76), Binary, FixedBytes, Uuid, Interval, Float16 (Zig has
native f16 and arbitrary-width ints), plus a Raw fallback for VARIANT/GEO/
nested (and INT96 until it decodes to Timestamp). Signed/unsigned INT
annotations map onto existing i8..i64 / u8..u64.

Drives Phase 6 (6c/6d) in [[cleanup-roadmap]]. The note's §5 open decisions are
all resolved by the approved 6d-2a spec
(docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md):
nested → separate 6d-2b spec; decimal i256/≤76; VARIANT/GEO → Raw; INT96 →
decode to Timestamp(nanos), re-emit INT96 only via WriteOptions.emit_int96.
