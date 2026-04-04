# Teddy

A data manipulation and analysis library for Zig, inspired by pandas and polars. Teddy provides type-safe, null-aware operations on structured tabular data with full control over memory allocation.

## Features

- **Typed Series** — Generic `Series(T)` supporting all integer types, `f32`, `f64`, `bool`, and `String`
- **DataFrame** — Multi-column tabular structure backed by typed series
- **Null support** — Validity bitmap (Apache Arrow model); null-aware aggregations, fill, and drop
- **Type casting** — Three-tier system: `castSafe` (comptime-verified), `cast` (strict runtime), `castLossy` (failures → null)
- **Aggregations** — `sum`, `min`, `max`, `mean`, `stdDev` — all skip nulls
- **GroupBy** — Group by any column; aggregate with `count`, `sum`, `mean`, `min`, `max`, `stdDev`
- **Joins** — Inner, left, right, and outer joins on a key column
- **Sorting & filtering** — `sort`, `filter`, `select`, `head`, `tail`, `slice`, `unique`, `valueCounts`
- **I/O** — CSV, JSON, and Parquet read/write
- **Memory safety** — Explicit allocator everywhere; clear ownership contracts

## Quick Start

```zig
const std = @import("std");
const df_mod = @import("dataframe");
const Dataframe = df_mod.Dataframe;
const Series = df_mod.Series;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Build a DataFrame
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var age = try Series(i32).init(allocator);
    try age.rename("age");
    try age.append(25);
    try age.append(30);
    try age.appendNull(); // missing value
    try age.append(22);
    try df.addSeries(age.toBoxedSeries());

    // Filter rows where age > 24 (nulls are excluded automatically)
    var filtered = try df.filter("age", i32, .gt, 24);
    defer filtered.deinit();

    // Fill nulls before aggregation
    var filled = try df.fillNull("age", i32, 0);
    defer filled.deinit();

    // Cast a column to a different type
    var as_float = try df.cast("age", f64);
    defer as_float.deinit();

    // Summary statistics
    var stats = try df.describe();
    defer stats.deinit();
    try stats.print();
}
```

## Null Handling

Nulls are stored as a validity bitmap alongside values (no memory overhead on null-free columns). The API follows pandas/polars conventions: aggregations skip nulls, and null-aware operations return `?T` at the API surface.

```zig
try s.appendNull();              // mark a slot as missing
s.isNull(i)                     // check a slot
s.nullCount()                   // count missing values
try s.fillNull(0)               // replace nulls with a scalar
try s.fillNullForward()         // carry last valid value forward (LOCF)
try s.fillNullBackward()        // carry next valid value backward
try s.dropNulls()               // new series with null rows removed

try df.dropNulls("col")         // drop rows where col is null
try df.dropNullsAny()           // drop rows where any column is null
try df.fillNull("col", i32, 0)  // fill nulls in one column
```

## Type Casting

Three tiers matching Zig's explicit-cast philosophy:

```zig
// castSafe — comptime guarantee; compile error if the types aren't safely widening
var wide = try s.castSafe(i64);     // i32 → i64: always safe
// var bad = try s.castSafe(i8);    // compile error: may overflow

// cast — strict runtime; errors on overflow, fractional float→int, parse failure
var exact = try s.cast(i32);        // 3.0 → 3 ok; 3.5 → error.LossyCast

// castLossy — permissive; failures become null
var best = try s.castLossy(i8);     // 300 → null, 1 → 1
```

All three variants are available on `Series(T)`, `BoxedSeries`, and `Dataframe`.

## GroupBy

```zig
var gb = try df.groupBy("department");
defer gb.deinit();

var counts = try gb.count();
defer counts.deinit();

var totals = try gb.sum("salary");
defer totals.deinit();
```

## I/O

```zig
// CSV
var df = try Dataframe.Reader.csv(allocator, "data.csv", .{});
defer df.deinit();
const csv_str = try df.toCsvString(.{});
defer allocator.free(csv_str);

// JSON
const json_str = try df.toJsonString(.rows);
defer allocator.free(json_str);

// Parquet
var df2 = try Dataframe.Reader.parquet(allocator, "data.parquet", .{});
defer df2.deinit();
```

## Building

```
zig build          # compile
zig build test     # run all tests (~350 tests)
zig build run      # run the example
```

Requires **Zig 0.14** or later (tested on 0.16.0-dev).

## Project Structure

```
src/
  dataframe/
    dataframe.zig      — DataFrame: columns, filter, sort, join, groupby, I/O
    series.zig         — Series(T): typed column with null support and casting
    boxed_series.zig   — Type-erased union over all Series(T) variants
    boxed_groupby.zig  — Type-erased GroupBy dispatch
    group.zig          — GroupBy(T) aggregation engine
    join.zig           — Inner / left / right / outer join
    strings.zig        — Managed String type used for string columns
    csv_reader.zig     — CSV parser
    csv_writer.zig     — CSV serializer
    json_reader.zig    — JSON parser (rows and columns format)
    json_writer.zig    — JSON serializer
    reader.zig         — Unified Reader builder
    writer.zig         — Unified Writer builder
  parquet/
    parquet.zig        — Parquet reader/writer
```

## Roadmap

See [`plan.md`](plan.md) for the prioritized feature roadmap comparing teddy to pandas and polars.

## License

TBD
