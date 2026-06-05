# Nullable Column Design

## How Other Libraries Do It

### pandas
Originally used `NaN` (a float IEEE sentinel) for all missing values — which
silently promoted integer columns to float64 when any null appeared. This was
a well-known footgun. Since 1.0, pandas introduced extension types
(`pd.Int64Dtype`, `pd.BooleanDtype`, etc.) that store a separate boolean mask
alongside the value array. Two arrays: `data: ndarray[T]` + `mask: ndarray[bool]`.

### Apache Arrow (foundation for Polars, Spark, DuckDB)
A **validity bitmap** — one bit per row packed into 64-bit words stored
alongside the data buffer. The data buffer always stores a physical value at
every position (usually zero at null slots). Operations check the bitmap
before using the value. This is compact (1 bit per element overhead) and
cache-friendly (data is always dense and packed).

### Polars
Built directly on Arrow. Exposes `Option<T>` at the Rust API boundary. Null
propagation is the default in all operations.

### PySpark
Nullability is a first-class schema concept (`nullable: bool` per field).
Internally uses the Arrow/Parquet validity bitmap model. Operations on null
values return null.

---

## The Design Space in Zig

Four realistic options:

### Option A — `ArrayList(?T)` storage
```zig
values: std.ArrayList(?T)
```
- Accessing: `if (s.values.items[i]) |v| { use(v); }`
- Natural Zig idiom. The compiler enforces null checking at every access point.
- **Problem**: `?i64` is 16 bytes in Zig (value + discriminant + padding).
  `?i32` is 8 bytes. `?bool` is 2 bytes. For integer/float columns this is a
  2x memory overhead — the most common case costs twice as much.
- Iteration also becomes less cache-friendly since values are interleaved with
  validity tags.

### Option B — Separate validity bitmap `ArrayList(bool)`  ← current skeleton
```zig
values:   std.ArrayList(T)      // placeholder at null slots
validity: ?std.ArrayList(bool)  // null ptr = all valid (lazy init)
```
- Data is dense and packed regardless of nulls — good for SIMD, bulk ops.
- 1 byte per element overhead (can upgrade to 1 bit later).
- Public API exposes `getAt(i) → ?T`: checks validity, wraps in optional.
- This is what pandas' extension types, Arrow, Polars, and Spark use internally.
- **Already partially implemented** — structure exists, just not wired up.

### Option C — Bit-packed bitmap `[]u64`
```zig
values:        std.ArrayList(T)
validity_bits: ?[]u64   // 1 bit per element
```
- Exactly what Apache Arrow specifies.
- 64x more compact than the bool array (1 bit vs 1 byte per element).
- More complex to implement (bit shifting, masks, u64 word boundaries).
- Correct direction for a high-performance library but adds implementation
  complexity that isn't the bottleneck right now.

### Option D — Tagged union
```zig
pub fn Nullable(comptime T: type) type {
    return union(enum) { null, value: T };
}
values: std.ArrayList(Nullable(T))
```
- Functionally equivalent to `?T`, just more explicit.
- Same memory overhead as Option A.
- Not worth pursuing over Option A.

---

## Recommendation: Option B (validity bitmap), `?T` at the API surface

Keep the existing `values: ArrayList(T)` + `validity: ?ArrayList(bool)` storage.
Expose `?T` to callers via `getAt`/`setAt`. This gives us:

- Dense storage (no per-element overhead in the hot path)
- Natural Zig optional ergonomics at the call site
- Arrow-compatible storage model (upgrade to bit-packed later if needed)
- The existing structure just needs to be wired up correctly

The bool bitmap can be upgraded to a bit-packed `[]u64` in a later pass without
changing any public API — it's purely an internal storage detail.

---

## What Needs to Be Fixed / Added

### 1. `getAt(i) → ?T`  (new)
The primary null-aware access method. Returns `null` if `isNull(i)`.
```zig
pub fn getAt(self: *Self, i: usize) ?T
```

### 2. `setAt(i, ?T)`  (new)
Set a value or null at a position.
```zig
pub fn setAt(self: *Self, i: usize, value: ?T) !void
```

### 3. Fix `asStringAt` to check validity  (bug fix)
Currently ignores the validity bitmap and returns the placeholder value.
Should return the string `"null"` for null positions.

### 4. Fix `dropRow` to update validity bitmap  (bug fix)
Currently removes from `values` but does not remove the corresponding
entry from `validity`.

### 5. Fix `limit` to truncate validity bitmap  (bug fix)
Same issue — truncates values but not validity.

### 6. Fix `appendSlice` to mark values valid  (bug fix)
If a validity bitmap exists, appending via `appendSlice` should mark
the new entries as valid. Currently it does not touch `validity` at all.

### 7. Fix `compareSeries` to be null-aware  (bug fix)
Currently compares raw values, so two series with the same placeholder
value at a null slot compare as equal even if one is null and the other isn't.

### 8. Fix aggregations to skip nulls  (correctness)
`seriesSum`, `minVal`, `maxVal`, `seriesMean`, `seriesStdDev` all iterate
`values.items` without checking validity. They must skip null rows.
Convention (matching pandas/polars default): **skip nulls, operate on valid values only**.

```zig
// Before (wrong):
for (self.values.items) |v| total += v;

// After (correct):
for (self.values.items, 0..) |v, i| {
    if (!self.isNull(i)) total += v;
}
```

### 9. `fillNull(value) → *Self`  (new)
Return a new series with all null slots replaced by `value`. Resulting
series has no nulls (validity bitmap cleared).

### 10. `fillNullForward() → *Self`  (new)
Carry the last valid value forward into null slots (LOCF). Useful for
time-series gaps.

### 11. `fillNullBackward() → *Self`  (new)
Carry the next valid value backward into null slots (NOCB).

### 12. `dropNulls() → *Self`  (new)
Return a new series with all null rows removed.

### 13. DataFrame-level null operations  (new)
```zig
df.dropNulls(column)      // drop rows where column is null
df.dropNullsAny()         // drop rows where any column is null
df.fillNull(column, value) // fill nulls in one column
```

---

## Null Propagation Rules

Consistent with pandas/polars defaults:

| Operation | Rule |
|---|---|
| Aggregation (sum, min, max, mean, std) | Skip nulls, compute over valid values only |
| `filter` / `filterByIndices` | Null rows are carried through (they stay null) |
| `sort` | Nulls sort last by convention |
| Future: column arithmetic (`a + b`) | If either operand is null, result is null |
| `compareSeries` | null == null is true; null == value is false |
| `asStringAt` on null | Returns `"null"` |

---

## Implementation Order

1. Fix bugs: `dropRow`, `limit`, `appendSlice`, `compareSeries` validity handling
2. Add `getAt` / `setAt`
3. Fix `asStringAt`
4. Fix all aggregations to skip nulls
5. Add `fillNull`, `fillNullForward`, `fillNullBackward`, `dropNulls` on Series
6. Add `dropNulls(col)`, `dropNullsAny()`, `fillNull(col, val)` on DataFrame
7. Tests for all of the above
8. (Later) upgrade bool bitmap to bit-packed `[]u64` for memory efficiency
