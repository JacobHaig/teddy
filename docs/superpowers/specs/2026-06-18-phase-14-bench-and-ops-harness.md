# Phase 14 — Benchmark Harness + Broad Operations Regression Suite

**Status:** design + build. This is the FOUNDATION the performance work
requires (user mandate: every perf change must document before/after timings
with a % delta, and be guarded by a correctness suite over wide data). It also
becomes the place ops-breadth and parquet-write-quality features assert
correctness. Built before any optimization.

## Why first

Optimization without measurement is guessing, and without a correctness net it
is dangerous. The user requires: (1) before/after timings ("100k iters: 10.42s
→ 7.85s, +24.66%"), (2) regression testing over a wide amount of data, (3) an
assertable operation test suite. Phase 14 delivers exactly those three so the
later phases (DataFrame breadth, parquet write quality, performance) each plug
in: assert correctness in the suite, and for perf changes record the bench
delta.

## Component 1 — shared seeded data generator (`src/dataframe/testdata.zig`)

Deterministic, allocator-based dataframe builder reused by both the bench and
the correctness suite (NO committed multi-MB fixtures; generated in-process,
seeded for reproducibility — `Date.now`/`Math.random` are unavailable in tests
anyway, so use an explicit `seed`):
```zig
pub const GenOptions = struct {
    rows: usize,
    seed: u64 = 0,
    null_rate: f32 = 0.0,    // fraction of null cells per nullable column
    // which column families to include (default: a broad mix)
    numerics: bool = true, strings: bool = true, temporal: bool = true,
    decimal: bool = true, nested: bool = false, // nested off by default (heavier)
};
pub fn genDataframe(allocator, opts: GenOptions) !*Dataframe
```
A linear-congruential or splitmix64 PRNG seeded by `opts.seed` (pure Zig, no
std.Random reliance needed but std.Random.DefaultPrng is fine — it's
deterministic given a seed). Produces columns spanning the type system with
controlled null density and value ranges (incl. some boundary values). Document
the exact schema it emits so both consumers agree.

## Component 2 — benchmark harness (`zig build bench`)

A separate executable (`src/bench/main.zig`) + a `bench` build step. NOT part
of `zig build test` (timing is noisy; the gate is correctness). Must be run
ReleaseFast for real numbers: `zig build bench -Doptimize=ReleaseFast`.

- `std.time.Timer` (monotonic). Per operation: a warmup run, then K iterations
  over a generated dataframe of N rows; record total + per-iter ns; report
  **median** ns/op (stable) plus ops/sec and total. K and N are CLI-tunable
  (`--rows`, `--iters`) with sensible defaults (e.g. N=100_000, K chosen so
  each op runs ~1s).
- Operations benched (the hot paths): sort, filter, groupBy+sum, join,
  cumSum, cast, sum/mean/min/max aggregations, dropNulls, deepCopy, TDF
  write+read round-trip, parquet write+read round-trip.
- **Baseline capture + compare** (this delivers the before/after mandate):
  - `zig build bench -- --json out.json` writes a machine-readable result set
    (op name, rows, iters, median_ns, ops_per_sec).
  - `zig build bench -- --baseline base.json` compares the current run to a
    saved baseline and prints, per op, `before_ns  after_ns  Δ%` (speedup =
    `(before-after)/before*100`). This is the literal "10.42s → 7.85s,
    +24.66%" output.
- `docs/benchmarks.md`: documents the methodology (ReleaseFast, median, warmup,
  how to read Δ%, how to capture a baseline) AND records the INITIAL captured
  baseline table (current performance, the "before" for all future work). Each
  perf phase appends a dated before/after section.

## Component 3 — broad operations correctness suite (`src/dataframe/operations_test.zig`, in `zig build test`)

Property/invariant-based assertions over WIDE generated data (e.g. 50_000 rows,
multiple seeds, with nulls) — robust without a per-op python oracle, and it
runs in the test gate so any optimization that breaks an op FAILS. Cover every
current operation with invariants such as:
- **sort**: output is monotonic per the key (nulls last — Phase 9 rule); it's a
  permutation of the input (same multiset, e.g. equal sum/count/nunique);
  sorting twice == sorting once.
- **filter**: every surviving row satisfies the predicate; count + a re-scan
  agree; `filter(p).height + filter(not p).height == height` (minus null
  semantics — null never matches, so account for it).
- **groupBy + agg**: sum over groups == global sum (non-null); group count
  partitions the rows; mean*count ≈ sum.
- **cumSum**: last element == total sum (no nulls case); length preserved.
- **cast**: castSafe widening round-trips (i32→i64→i32 identity on in-range);
  castLossy never errors.
- **dropNulls**: result has no nulls in the target; height == non-null count.
- **head/tail/slice/limit**: lengths + boundary values.
- **concat**: height adds; schema preserved.
- **round-trip identities**: df → TDF → df equal (compareDataframe); df →
  parquet → df equal for the supported type set; df → CSV/JSON → df where
  lossless-enough (use the Phase 9 fidelity expectations).
- **deepCopy**: equal + independent (mutate original, copy unchanged).
Where a clean pyarrow cross-check is cheap (sum/mean/min/max/sort over the
small alltypes fixture), keep the Phase 9 exact-value goldens; the WIDE suite
is invariant-based. Run each invariant across ≥2 seeds and a couple of row
counts. Keep total runtime reasonable (seconds, not minutes) — the suite is in
the gate.

## Slices
- **14.0**: `testdata.zig` (shared generator) + the `bench` executable/step +
  baseline-capture/compare + `docs/benchmarks.md` with the initial captured
  baseline. (Owns build.zig + the generator.)
- **14.1**: `operations_test.zig` — the wide invariant suite over
  `testdata.genDataframe`, wired into `tests.zig`. (Reuses the generator;
  touches only test files.)

## Out of scope (this phase)
The actual optimizations (later perf phase — they USE this harness); pandas
parity; micro-benchmark statistical rigor beyond median+warmup.
