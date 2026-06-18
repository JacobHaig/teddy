# Benchmarks

The `zig build bench` harness (Phase 14.0) measures teddy's hot-path
DataFrame operations and supports a before/after baseline-comparison workflow.
Every performance change must record a before/after table with a `Δ%` delta,
captured here.

## Methodology

- **Build mode.** Real numbers REQUIRE ReleaseFast:
  `zig build bench -Doptimize=ReleaseFast`. Debug builds compile and run (used
  for CI smoke) but are ~50× slower and are not meaningful timings.
- **Clock.** Monotonic (`std.Io.Clock.awake`), read immediately before and
  after each op.
- **Warmup + median.** Each op is run once untimed (warmup), then `K` timed
  iterations (`--iters`, default 50). We report the **median** ns/op (stable
  against outliers) plus ops/sec.
- **Data.** Generated once, outside the timed loop, by
  `src/dataframe/testdata.zig` (`genDataframe`, seed 0, `null_rate = 0.15`,
  `nested = false`). Data generation is never timed. The join uses a second
  generated frame (seed 1) at `rows/10` to bound the result size; both share
  the `c_key` domain.
- **Result-deinit policy.** For ops that produce a new dataframe (sort, filter,
  groupBy+sum, join, cumSum, cast, dropNulls, deepCopy, round-trips), the result
  **allocation is timed** but the **`deinit` is done after stopping the per-iter
  clock** — allocation cost counts, free cost does not. Pure-read aggregations
  (sum/mean/min/max) allocate nothing.

### Operations and what each measures

| op                | measures |
|-------------------|----------|
| `sort`            | `df.sort("c_i64", asc)` — argsort + gather of all columns |
| `filter`          | `df.filter("c_i64", .gt, median)` — predicate scan + gather |
| `groupBy_sum`     | `df.groupBy("c_str").sum("c_i64")` — hash group + sum agg |
| `join`            | `df.join(right, "c_key", .inner)` — inner hash join (right = rows/10) |
| `cumSum`          | `df.cumSum("c_i64")` — running sum, one column replaced |
| `cast`            | `df.cast("c_i32", i64)` — strict widening cast of one column |
| `sum`/`mean`/`min`/`max` | aggregation over `c_f64` (pure read, no alloc) |
| `dropNulls`       | `df.dropNulls("c_i64")` — null scan + gather (15% nulls) |
| `deepCopy`        | `df.deepCopy()` — full clone of all columns |
| `tdf_roundtrip`   | df → TDF bytes → df (native binary write + parse) |
| `parquet_roundtrip` | df → parquet bytes → df (shred + write + read + rebuild) |

The generated schema (kept stable for both bench and the correctness suite) is
documented in the `src/dataframe/testdata.zig` header: `c_key` (i64, dense
bounded key) plus the numeric, string, temporal and decimal column families.

## Capturing a baseline

```sh
zig build bench -Doptimize=ReleaseFast -- --json baseline.json
```

This writes a machine-readable result set (`name`, `rows`, `iters`,
`median_ns`, `ops_per_sec`).

## Comparing against a baseline

```sh
zig build bench -Doptimize=ReleaseFast -- --baseline baseline.json
```

This re-runs the suite and prints, per op, `before  after  Δ%` where

```
Δ% = (before_ns - after_ns) / before_ns * 100
```

so a **positive Δ% means the op got faster** (e.g. `10.42s → 7.85s` is
`+24.66%`). Tune the workload with `--rows N` (default 100_000) and `--iters K`
(default 50). Use the SAME `--rows`/`--iters` for the baseline and the
comparison run.

> Absolute numbers are machine-dependent and noisy; what matters is the
> methodology and a baseline captured on the same machine you compare on.

## Initial baseline — 2026-06-18

Machine: Apple Silicon (aarch64-macos), `-Doptimize=ReleaseFast`,
`rows=100000`, `iters=50`, seed 0, `null_rate=0.15`. This is the "before" for
all future performance work. Re-capture on your own machine before comparing.

| op                | rows   | iters | median ns/op | ops/sec |
|-------------------|--------|-------|--------------|---------|
| sort              | 100000 | 50    | 24.78 ms     | 40.4    |
| filter            | 100000 | 50    | 4.61 ms      | 217.1   |
| groupBy_sum       | 100000 | 50    | 5.02 ms      | 199.2   |
| join              | 100000 | 50    | 31.52 ms     | 31.7    |
| cumSum            | 100000 | 50    | 9.90 ms      | 101.0   |
| cast              | 100000 | 50    | 9.92 ms      | 100.8   |
| sum               | 100000 | 50    | 225.17 us    | 4441.1  |
| mean              | 100000 | 50    | 225.17 us    | 4441.1  |
| min               | 100000 | 50    | 540.96 us    | 1848.6  |
| max               | 100000 | 50    | 540.96 us    | 1848.6  |
| dropNulls         | 100000 | 50    | 9.43 ms      | 106.0   |
| deepCopy          | 100000 | 50    | 9.85 ms      | 101.5   |
| tdf_roundtrip     | 100000 | 50    | 18.87 ms     | 53.0    |
| parquet_roundtrip | 100000 | 50    | 33.14 ms     | 30.2    |

Each future perf phase appends a dated before/after section below.
