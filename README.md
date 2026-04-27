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
- **I/O** — CSV, JSON (rows, columns, NDJSON/JSONL), and Parquet read/write
- **Memory safety** — Explicit allocator everywhere; clear ownership contracts

---

## Getting Started (Library User)

### Requirements

Zig **0.16.0** or later is required.

### Adding Teddy to Your Project

Fetch the package and save it to your `build.zig.zon`:

```sh
zig fetch --save git+https://github.com/Wisward/teddy
```

Then wire it up in your `build.zig`:

```zig
const teddy_dep = b.dependency("teddy", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("teddy", teddy_dep.module("dataframe"));
```

### First Program

```zig
const std = @import("std");
const teddy = @import("teddy");
const Dataframe = teddy.Dataframe;
const Series = teddy.Series;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Load a CSV file
    var reader = try teddy.Reader.init(allocator, io);
    defer reader.deinit();

    var df = try reader
        .withFileType(.csv)
        .withPath("data.csv")
        .load();
    defer df.deinit();

    // Print the first 5 rows
    const preview = try df.head(5);
    defer preview.deinit();
    try preview.print();
}
```

Run it:

```sh
zig build run
```

---

## Core Concepts

### Building a DataFrame

```zig
var df = try Dataframe.init(allocator);
defer df.deinit();

var age = try Series(i32).init(allocator);
try age.rename("age");
try age.append(25);
try age.append(30);
try age.appendNull(); // missing value
try age.append(22);
try df.addSeries(age.toBoxedSeries());

var name = try Series(teddy.String).init(allocator);
try name.rename("name");
try name.tryAppend("Alice");
try name.tryAppend("Bob");
try name.tryAppend("Carol");
try name.tryAppend("Dave");
try df.addSeries(name.toBoxedSeries());
```

### Filtering and Selecting

```zig
// Keep rows where age > 24 (nulls are excluded automatically)
var adults = try df.filter("age", i32, .gt, 24);
defer adults.deinit();

// Keep only specific columns
var slim = try df.select(&.{ "name", "age" });
defer slim.deinit();
```

Supported filter operators: `.eq`, `.neq`, `.lt`, `.lte`, `.gt`, `.gte`

### Sorting

```zig
// Sort ascending
var sorted_asc = try df.sort("age", true);
defer sorted_asc.deinit();

// Sort descending
var sorted_desc = try df.sort("age", false);
defer sorted_desc.deinit();
```

### Aggregations

```zig
// Summary statistics for all numeric columns
var stats = try df.describe();
defer stats.deinit();
try stats.print();
```

### GroupBy

```zig
var gb = try df.groupBy("department");
defer gb.deinit();

var counts = try gb.count();
defer counts.deinit();

var totals = try gb.sum("salary");
defer totals.deinit();

var averages = try gb.mean("salary");
defer averages.deinit();
```

### Joins

```zig
// left / inner / right / outer
const joined = try left_df.join(right_df, "id", .left);
defer joined.deinit();
```

---

## Null Handling

Nulls are stored as a validity bitmap alongside values — no memory overhead on null-free columns. Aggregations skip nulls automatically.

```zig
try s.appendNull();           // mark a slot as missing
s.isNull(i)                   // check a slot
s.nullCount()                 // count missing values
try s.fillNull(0)             // replace nulls with a scalar
try s.fillNullForward()       // carry last valid value forward (LOCF)
try s.fillNullBackward()      // carry next valid value backward
try s.dropNulls()             // new series with null rows removed

try df.dropNulls("col")       // drop rows where col is null
try df.dropNullsAny()         // drop rows where any column is null
try df.fillNull("col", i32, 0)
```

---

## Type Casting

Three tiers matching Zig's explicit-cast philosophy:

```zig
// castSafe — comptime guarantee; compile error if the types aren't safely widening
var wide = try s.castSafe(i64);     // i32 → i64: always safe
// var bad = try s.castSafe(i8);    // compile error: may overflow

// cast — strict runtime; errors on overflow or fractional float→int
var exact = try s.cast(i32);        // 3.0 → 3 ok; 3.5 → error.LossyCast

// castLossy — permissive; failures become null
var best = try s.castLossy(i8);     // 300 → null, 1 → 1
```

All three variants are available on `Series(T)`, `BoxedSeries`, and `Dataframe`.

---

## I/O

The `Reader` and `Writer` use a fluent builder pattern. File type is set with `.withFileType()`; format-specific options are set with additional calls before `.load()` / `.toString()` / `.save()`.

### Reading

```zig
var reader = try teddy.Reader.init(allocator);
defer reader.deinit();

// CSV (auto-infers column types)
var reader = try teddy.Reader.init(allocator, io);
defer reader.deinit();

var df = try reader
    .withFileType(.csv)
    .withPath("data.csv")
    .withDelimiter(',')   // default; omit if not needed
    .withHeaders(true)    // default
    .load();

// JSON — auto-detects rows `[{...}]`, columns `{"col":[...]}`, or NDJSON
var df2 = try reader.withFileType(.json).withPath("data.json").load();

// Parquet
var df3 = try reader.withFileType(.parquet).withPath("data.parquet").load();
```

### Writing to a File

```zig
var writer = try teddy.Writer.init(allocator, io);
defer writer.deinit();

// CSV
try writer.withFileType(.csv).withPath("out.csv").save(df);

// JSON — rows `[{...}]` format
try writer
    .withFileType(.json)
    .withJsonFormat(.rows)   // .rows | .columns | .ndjson
    .withPath("out.json")
    .save(df);

// Parquet with Snappy compression
try writer
    .withFileType(.parquet)
    .withCompression(.snappy)
    .withPath("out.parquet")
    .save(df);
```

### Writing to a String (in-memory)

```zig
var writer = try teddy.Writer.init(allocator, io);
defer writer.deinit();

const csv_bytes = try writer.withFileType(.csv).toString(df);
defer allocator.free(csv_bytes);

const json_bytes = try writer.withFileType(.json).withJsonFormat(.ndjson).toString(df);
defer allocator.free(json_bytes);
```

### JSON Format Options

| Format | Shape | Example |
|--------|-------|---------|
| `.rows` | Array of objects | `[{"a":1},{"a":2}]` |
| `.columns` | Object of arrays | `{"a":[1,2]}` |
| `.ndjson` | One object per line | `{"a":1}\n{"a":2}` |

Format is auto-detected on read. NDJSON is detected when content starts with `{` and contains `\n{`. For single-object files you can force it: `.{ .format = .ndjson }`.

---

## Building

```sh
zig build          # compile
zig build test     # run all tests (~385 tests)
zig build run      # run the example (loads data/ files)
```

---

## Project Structure

```
src/
  dataframe/
    dataframe.zig       — DataFrame: columns, filter, sort, join, groupby, I/O
    series.zig          — Series(T): typed column with null support and casting
    boxed_series.zig    — Type-erased union over all Series(T) variants
    boxed_groupby.zig   — Type-erased GroupBy dispatch
    group.zig           — GroupBy(T) aggregation engine
    join.zig            — Inner / left / right / outer join
    strings.zig         — Managed String type used for string columns
    reader.zig          — Unified Reader builder
    writer.zig          — Unified Writer builder
    csv_reader.zig      — CSV parser with type inference
    csv_writer.zig      — CSV serializer (RFC 4180)
    json_reader.zig     — JSON parser (rows, columns, NDJSON)
    json_writer.zig     — JSON serializer
    parquet.zig         — Parquet ↔ DataFrame adapter
    *_test.zig          — Per-module test files
    tests.zig           — Test aggregator (imported by build.zig)
  parquet/
    parquet.zig         — Module root (exports readParquet / writeParquet)
    parquet_reader.zig  — File reader (PAR1 magic, footer, column chunks)
    parquet_writer.zig  — File writer
    encoding_*.zig      — PLAIN and RLE/bit-packed codec
    thrift_*.zig        — Thrift metadata serialization
    snappy.zig          — Snappy compression
    *_test.zig          — Per-module test files
    tests.zig           — Test aggregator
  main.zig              — Example program (zig build run)
data/
  addresses.csv / .parquet / _snappy.parquet
  stock_apple.csv
docs/
  architecture.md / file-formats.md / type-system.md / operations.md / reader-writer.md
```

---

## Contributing

### Setup

```sh
git clone https://github.com/Wisward/teddy
cd teddy
zig build test   # all tests should pass before you start
```

### Running Tests

```sh
zig build test --summary all   # shows pass count per test suite
```

Tests live next to the source they cover (`csv_writer_test.zig` beside `csv_writer.zig`, etc.). Each test file is imported by the suite aggregator at `src/dataframe/tests.zig` or `src/parquet/tests.zig` — add your file to the matching aggregator when you create a new one.

### Adding a File Format

1. Create `src/dataframe/<format>_reader.zig` and/or `<format>_writer.zig`
2. Add a variant to the `FileType` union in `reader.zig` / `writer.zig` and wire it into the `load` / `toString` dispatch
3. Add `<format>_reader_test.zig` / `<format>_writer_test.zig` and import them in `src/dataframe/tests.zig`

### Style Notes

- Every public function that allocates takes an `Allocator` and documents ownership in a doc comment
- Prefer zero-copy parsing (slice into the input buffer) over allocating intermediate strings
- Match the existing error type conventions (`error.InvalidJson`, `error.LossyCast`, etc.)

---

## Roadmap

See [`plan.md`](plan.md) for the prioritized feature roadmap comparing teddy to pandas and polars.

## License

TBD
