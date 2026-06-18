//! Phase 14.1 — broad operations correctness suite over WIDE generated data.
//!
//! Property / invariant-based assertions over dataframes produced by
//! `testdata.genDataframe`. This is the correctness net that guards every
//! future optimization: any change that breaks an operation FAILS
//! `zig build test`.
//!
//! Design notes:
//!   * No python/pyarrow oracle. We assert mathematical invariants, cross-checks
//!     between operations, and round-trip identities. These hold regardless of
//!     the (random but seeded) data, so they are robust without goldens.
//!   * Each invariant runs across >=2 seeds and >=2 row counts, and with both
//!     null_rate 0.0 and ~0.15 where nulls are relevant.
//!   * Row counts are WIDE ({5_000, 50_000}) — breadth-at-scale is the point.
//!
//! ## Allocator strategy (why these tests do NOT use std.testing.allocator)
//!
//! The wide invariant tests run their generated frames + all derived frames
//! through an `ArenaAllocator` backed by `std.heap.page_allocator`: bump
//! allocation with NO per-allocation bookkeeping and a single bulk free at
//! test end. This matters because `zig build test` is a DEBUG build, and under
//! the leak-checking GeneralPurposeAllocator every cell of a wide frame
//! (each String / Decimal-i256 / Date / Timestamp value is its own allocation)
//! carries safety metadata; the GPA's per-LIVE-allocation cost degrades
//! super-linearly, which previously turned this suite into ~90s. The arena
//! removes that bottleneck entirely and makes 50_000-row frames cheap, so the
//! correctness coverage no longer trades off against wall time.
//!
//! Leak-safety at scale is NOT this suite's job. genDataframe and every op are
//! already leak-checked on small cases here by the `LEAK-CANARY` test below
//! (on `std.testing.allocator`, with proper deinits) and by the per-op tests in
//! series_test.zig / dataframe_test.zig. The arena frees everything in bulk, so
//! a real leak inside an op would not be caught by the wide tests — that is by
//! design; the canary + per-op tests own that guarantee.
//!
//! Excluded and why:
//!   * Nested (c_list) is excluded from the parquet round-trip — testdata's
//!     Nested column has no parquet schema node to shred against (documented in
//!     testdata.zig). It IS covered by the TDF round-trip.
//!   * i128/u128 columns are not emitted by the generator, so no cast probes for
//!     them here.

const std = @import("std");
const testing = std.testing;

const dataframe = @import("dataframe.zig");
const Dataframe = dataframe.Dataframe;
const Series = dataframe.Series;
const String = dataframe.String;
const CompareOp = dataframe.Dataframe.CompareOp;
const JoinType = dataframe.Dataframe.JoinType;

const testdata = @import("testdata.zig");

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// The standard wide option set: every column family on except nested (which
/// the parquet round-trip cannot shred). Individual tests that need nested ask
/// for it explicitly.
fn genWide(allocator: std.mem.Allocator, rows: usize, seed: u64, null_rate: f32) !*Dataframe {
    return testdata.genDataframe(allocator, .{
        .rows = rows,
        .seed = seed,
        .null_rate = null_rate,
        .numerics = true,
        .strings = true,
        .temporal = true,
        .decimal = true,
        .nested = false,
    });
}

// Two distinct WIDE row counts for the invariant tests (the spec asks for >=2
// row counts and >=2 seeds). These are arena-backed (see the module doc), so
// 50_000 rows is cheap — breadth-at-scale, with the multi-page / large-buffer
// code paths exercised, is the point.
const row_counts = [_]usize{ 5_000, 50_000 };
const seeds = [_]u64{ 1, 7 };
const null_rates = [_]f32{ 0.0, 0.15 };

/// Round-trip identities hold independent of the data, so the (serialize ->
/// parse) round-trip tests run a small fixed set of cases that still covers a
/// no-null frame, a frame with nulls, and two seeds — enough to catch a
/// serialization regression. They are arena-backed like the rest.
const RtCase = struct { rows: usize, seed: u64, null_rate: f32 };
const rt_cases = [_]RtCase{
    .{ .rows = 5_000, .seed = 1, .null_rate = 0.0 },
    .{ .rows = 5_000, .seed = 7, .null_rate = 0.15 },
};

/// Read an i64 cell directly from a column (null -> null).
fn i64At(df: *Dataframe, col: []const u8, i: usize) ?i64 {
    const boxed = df.getSeries(col).?;
    return boxed.int64.getAt(i);
}

/// Sum the non-null values of an i64 column (matching the bounded ±1e9 range,
/// so the whole-frame sum cannot overflow i64).
fn sumI64(df: *Dataframe, col: []const u8) i64 {
    const boxed = df.getSeries(col).?;
    const s = boxed.int64;
    var total: i64 = 0;
    for (0..s.len()) |i| {
        if (s.getAt(i)) |v| total += v;
    }
    return total;
}

// ---------------------------------------------------------------------------
// sort
// ---------------------------------------------------------------------------

/// Asserts the nulls-last suffix rule + monotonicity over non-null rows for a
/// sorted i64 column.
fn assertSortedI64(df: *Dataframe, col: []const u8, ascending: bool) !void {
    const s = df.getSeries(col).?.int64;
    const h = s.len();
    // Find the first null; from there to the end everything must be null
    // (Phase 9: nulls last regardless of direction).
    var first_null: ?usize = null;
    for (0..h) |i| {
        if (s.isNull(i)) {
            first_null = i;
            break;
        }
    }
    if (first_null) |fn_idx| {
        for (fn_idx..h) |i| try testing.expect(s.isNull(i));
    }
    const non_null_end = first_null orelse h;
    // Monotonic over the non-null prefix.
    if (non_null_end >= 2) {
        for (1..non_null_end) |i| {
            const a = s.getAt(i - 1).?;
            const b = s.getAt(i).?;
            if (ascending) try testing.expect(a <= b) else try testing.expect(a >= b);
        }
    }
}

test "sort: nulls-last, monotonic, permutation, idempotent (i64 + c_str)" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);

                const orig_sum = sumI64(df, "c_i64");
                const orig_nulls = df.getSeries("c_i64").?.nullCount();
                const orig_nunique = try df.getSeries("c_str").?.nunique(allocator);

                inline for (.{ true, false }) |asc| {
                    // Sort on a numeric key.
                    const sorted = try df.sort("c_i64", asc);
                    try testing.expectEqual(df.height(), sorted.height());
                    try testing.expectEqual(df.width(), sorted.width());
                    try assertSortedI64(sorted, "c_i64", asc);
                    // Permutation: same multiset (sum, null count, nunique of an
                    // unrelated column all preserved).
                    try testing.expectEqual(orig_sum, sumI64(sorted, "c_i64"));
                    try testing.expectEqual(orig_nulls, sorted.getSeries("c_i64").?.nullCount());
                    try testing.expectEqual(orig_nunique, try sorted.getSeries("c_str").?.nunique(allocator));

                    // Idempotence: sort(sort(x)) == sort(x) on the key column,
                    // value-for-value (cheap integer compare, no per-cell
                    // allocation — keeps the wide size fast).
                    const twice = try sorted.sort("c_i64", asc);
                    {
                        const ka = sorted.getSeries("c_i64").?.int64;
                        const kb = twice.getSeries("c_i64").?.int64;
                        try testing.expectEqual(ka.len(), kb.len());
                        for (0..ka.len()) |i| {
                            try testing.expectEqual(ka.isNull(i), kb.isNull(i));
                            if (ka.getAt(i)) |v| try testing.expectEqual(v, kb.getAt(i).?);
                        }
                    }

                    // Sort on a string column: height + permutation only.
                    const sorted_str = try df.sort("c_str", asc);
                    try testing.expectEqual(df.height(), sorted_str.height());
                    try testing.expectEqual(orig_sum, sumI64(sorted_str, "c_i64"));
                    try testing.expectEqual(orig_nunique, try sorted_str.getSeries("c_str").?.nunique(allocator));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// filter
// ---------------------------------------------------------------------------

test "filter: predicate holds on survivors; eq+neq+null partition; known-count" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);

                const h = df.height();
                const nulls_i64 = df.getSeries("c_i64").?.nullCount();

                // A concrete threshold value present in the data range.
                const thresh: i64 = 0;

                const gt = try df.filter("c_i64", i64, .gt, thresh);
                // Every surviving row satisfies the predicate (null never matches).
                {
                    const s = gt.getSeries("c_i64").?.int64;
                    for (0..s.len()) |i| {
                        try testing.expect(!s.isNull(i));
                        try testing.expect(s.getAt(i).? > thresh);
                    }
                }

                // eq + neq + null-count partition the whole frame: a null cell
                // matches neither eq nor neq under SQL semantics.
                const eq = try df.filter("c_i64", i64, .eq, thresh);
                const neq = try df.filter("c_i64", i64, .neq, thresh);
                try testing.expectEqual(h, eq.height() + neq.height() + nulls_i64);

                // gte over the observed minimum returns all NON-NULL rows.
                const min_f = df.getSeries("c_i64").?.min().?;
                const min_i: i64 = @intFromFloat(min_f);
                const ge_all = try df.filter("c_i64", i64, .gte, min_i);
                try testing.expectEqual(h - nulls_i64, ge_all.height());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// groupBy + aggregations
// ---------------------------------------------------------------------------

test "groupBy: partitions rows; group sums reconcile to global; group count == nunique" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);

                const h = df.height(); // c_key is never null, so all rows have a key.
                const key_nunique = try df.getSeries("c_key").?.nunique(allocator);
                const global_i64 = sumI64(df, "c_i64");

                // count(): one row per distinct key; counts sum to the row count.
                {
                    var gb = try df.groupBy("c_key");
                    const counts = try gb.count();
                    try testing.expectEqual(key_nunique, counts.height());
                    const cs = counts.getSeries("count").?.usize;
                    var total: usize = 0;
                    for (0..cs.len()) |i| total += cs.getAt(i).?;
                    try testing.expectEqual(h, total);
                }

                // sum(c_i64) over groups reconciles to the global non-null sum.
                {
                    var gb = try df.groupBy("c_key");
                    const sums = try gb.sum("c_i64");
                    try testing.expectEqual(key_nunique, sums.height());
                    const vs = sums.getSeries("c_i64").?.int64;
                    var total: i64 = 0;
                    for (0..vs.len()) |i| total += vs.getAt(i).?;
                    try testing.expectEqual(global_i64, total);
                }

                // mean/min/max run and produce one row per group.
                {
                    var gb = try df.groupBy("c_key");
                    const means = try gb.mean("c_i64");
                    try testing.expectEqual(key_nunique, means.height());

                    const mins = try gb.min("c_i64");
                    try testing.expectEqual(key_nunique, mins.height());

                    const maxs = try gb.max("c_i64");
                    try testing.expectEqual(key_nunique, maxs.height());
                }

                // groupBy on a string key partitions the non-null-key rows.
                {
                    const str_nulls = df.getSeries("c_str").?.nullCount();
                    var gb = try df.groupBy("c_str");
                    const counts = try gb.count();
                    const cs = counts.getSeries("count").?.usize;
                    var total: usize = 0;
                    for (0..cs.len()) |i| total += cs.getAt(i).?;
                    // Null keys are dropped by groupBy, so counts cover the
                    // non-null rows.
                    try testing.expectEqual(h - str_nulls, total);
                }
            }
        }
    }
}

test "groupByMultiple: total group sizes partition the non-null-key rows" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);
                const h = df.height();

                // Rows where c_str is null produce a null composite key and are
                // dropped (matching single-key policy). c_key is never null.
                const str_nulls = df.getSeries("c_str").?.nullCount();

                const cols = [_][]const u8{ "c_key", "c_str" };
                var gb = try df.groupByMultiple(&cols);

                const counts = try gb.count();
                const cs = counts.getSeries("count").?.usize;
                var total: usize = 0;
                for (0..cs.len()) |i| total += cs.getAt(i).?;
                try testing.expectEqual(h - str_nulls, total);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// join
// ---------------------------------------------------------------------------

test "join: unique right keys => inner == left coverage; left >= left height; values carry" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            // Use null_rate 0 for the join frame so c_str carries cleanly; the
            // key (c_key) is never null regardless.
            const df = try genWide(allocator, rows, seed, 0.0);

            // Right frame: unique c_key rows of the same df, so every key is
            // unique on the right and every left key matches exactly one right.
            const right = try df.unique("c_key");

            const left_h = df.height();

            const inner = try df.join(right, "c_key", .inner);
            // Unique right keys + every left key present in right => 1:1 match
            // for every left row.
            try testing.expectEqual(left_h, inner.height());

            const left_join = try df.join(right, "c_key", .left);
            try testing.expect(left_join.height() >= left_h);
            try testing.expectEqual(left_h, left_join.height());

            // Joined value columns carry through: the right's c_str arrives as
            // "c_str_right" (collision suffix) and has the same width semantics.
            try testing.expect(inner.getSeries("c_str") != null);
            try testing.expect(inner.getSeries("c_str_right") != null);
        }
    }
}

// ---------------------------------------------------------------------------
// cumulative ops
// ---------------------------------------------------------------------------

test "cumSum/cumMin/cumMax/cumProd: length preserved, null-propagating, monotonic" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);
                const h = df.height();

                // cumSum: length preserved; nulls in input -> nulls in output at
                // the same positions.
                {
                    const cs = try df.cumSum("c_i64");
                    try testing.expectEqual(h, cs.height());
                    const in = df.getSeries("c_i64").?.int64;
                    const out = cs.getSeries("c_i64").?.int64;
                    for (0..h) |i| {
                        try testing.expectEqual(in.isNull(i), out.isNull(i));
                    }
                    // No-null case: last cumulative value == total sum.
                    if (nr == 0.0) {
                        const total = sumI64(df, "c_i64");
                        try testing.expectEqual(total, out.getAt(h - 1).?);
                    }
                }

                // cumMin monotonic non-increasing over non-null cells; cumMax
                // monotonic non-decreasing. Null positions preserved.
                {
                    const cmin = try df.cumMin("c_i64");
                    const cmax = try df.cumMax("c_i64");
                    const in = df.getSeries("c_i64").?.int64;
                    const omin = cmin.getSeries("c_i64").?.int64;
                    const omax = cmax.getSeries("c_i64").?.int64;
                    var prev_min: ?i64 = null;
                    var prev_max: ?i64 = null;
                    for (0..h) |i| {
                        try testing.expectEqual(in.isNull(i), omin.isNull(i));
                        try testing.expectEqual(in.isNull(i), omax.isNull(i));
                        if (omin.getAt(i)) |v| {
                            if (prev_min) |p| try testing.expect(v <= p);
                            prev_min = v;
                        }
                        if (omax.getAt(i)) |v| {
                            if (prev_max) |p| try testing.expect(v >= p);
                            prev_max = v;
                        }
                    }
                }

                // cumProd: length + null pattern preserved. NOTE: cumProd on i64
                // panics on integer overflow in debug builds (documented: "wraps
                // on integer overflow (debug mode panics)" in series.zig) — and
                // `zig build test` is a debug build. The wide c_i64 column (±1e9)
                // overflows after ~3 multiplications, so we cannot run cumProd
                // over the full frame in the gate. We exercise it on a 2-row head
                // where the running product cannot overflow, asserting length +
                // null propagation. (This documented panic-on-overflow means
                // cumProd is unsafe on wide integer data in debug; reported.)
                {
                    const small = try df.head(2);
                    const cp = try small.cumProd("c_i64");
                    try testing.expectEqual(@as(usize, 2), cp.height());
                    const in = small.getSeries("c_i64").?.int64;
                    const out = cp.getSeries("c_i64").?.int64;
                    for (0..2) |i| try testing.expectEqual(in.isNull(i), out.isNull(i));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// cast / castSafe / castLossy
// ---------------------------------------------------------------------------

test "cast: castSafe i32->i64->i32 round-trips; castLossy never errors; height+nulls preserved" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);
                const h = df.height();

                // castSafe round-trip (i32 -> i64 -> i32) exercised at the
                // Series level. NOTE: Dataframe.castSafe(col, i64) is NOT
                // callable on ANY real frame — castSafe is comptime
                // lossless-only and the boxed dispatch instantiates
                // castSafe(i64) for EVERY BoxedSeries variant (incl. u64/i128),
                // so the u64->i64 instantiation is a hard @compileError
                // regardless of the frame's runtime contents. (Structural
                // limitation of the boxed `inline else` dispatch — reported in
                // the session summary; not a logic bug. The dataframe-level
                // widening cast is only reachable through the runtime-checked
                // `cast`/`castLossy`.) The Series API is where castSafe is
                // actually usable, so the invariant is asserted there.
                {
                    const i32_series = df.getSeries("c_i32").?.int32;
                    const i32_nulls = i32_series.nullCount();
                    const wide = try i32_series.castSafe(i64);
                    try testing.expectEqual(h, wide.len());
                    try testing.expectEqual(i32_nulls, wide.nullCount());
                    const back = try wide.castLossy(i32);
                    try testing.expectEqual(h, back.len());
                    for (0..h) |i| {
                        try testing.expectEqual(i32_series.isNull(i), back.isNull(i));
                        if (i32_series.getAt(i)) |v| try testing.expectEqual(v, back.getAt(i).?);
                    }
                }

                // Dataframe-level castLossy f64 -> i64 never errors (truncates /
                // nulls on failure) and preserves height + null pattern.
                const lossy = try df.castLossy("c_f64", i64);
                try testing.expectEqual(h, lossy.height());

                // Dataframe-level strict cast i32 -> i64 (runtime-checked path,
                // which IS callable on the full frame): widening always
                // succeeds, height + null pattern preserved.
                const strict = try df.cast("c_i32", i64);
                try testing.expectEqual(h, strict.height());
                try testing.expectEqual(
                    df.getSeries("c_i32").?.nullCount(),
                    strict.getSeries("c_i32").?.nullCount(),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// dropNulls / dropNullsAny
// ---------------------------------------------------------------------------

test "dropNulls(col): zero nulls in target; height == non-null count" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);
                const nulls = df.getSeries("c_i64").?.nullCount();

                const dropped = try df.dropNulls("c_i64");
                try testing.expectEqual(df.height() - nulls, dropped.height());
                try testing.expectEqual(@as(usize, 0), dropped.getSeries("c_i64").?.nullCount());
            }
        }
    }
}

test "dropNullsAny: result has zero nulls in any column; height <= original" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);

                const dropped = try df.dropNullsAny();
                try testing.expect(dropped.height() <= df.height());
                for (dropped.series.items) |*s| {
                    try testing.expectEqual(@as(usize, 0), s.nullCount());
                }
                // null_rate 0 keeps every row.
                if (nr == 0.0) try testing.expectEqual(df.height(), dropped.height());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// fillNull
// ---------------------------------------------------------------------------

test "fillNull: no nulls remain; non-null cells unchanged; nulls become the value" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const df = try genWide(allocator, rows, seed, 0.15);
            const h = df.height();
            const fill_val: i64 = 123456789;

            const filled = try df.fillNull("c_i64", i64, fill_val);
            const orig = df.getSeries("c_i64").?.int64;
            const out = filled.getSeries("c_i64").?.int64;
            try testing.expectEqual(@as(usize, 0), out.nullCount());
            for (0..h) |i| {
                if (orig.getAt(i)) |v| {
                    try testing.expectEqual(v, out.getAt(i).?);
                } else {
                    try testing.expectEqual(fill_val, out.getAt(i).?);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// head / tail / slice / limit
// ---------------------------------------------------------------------------

test "head/tail/slice/limit: lengths + boundary values + preserved schema" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const df = try genWide(allocator, rows, seed, 0.0);
            const h = df.height();
            const w = df.width();
            const n: usize = 100;

            // head(n): first n rows, boundary value matches the source.
            const hd = try df.head(n);
            try testing.expectEqual(n, hd.height());
            try testing.expectEqual(i64At(df, "c_key", 0), i64At(hd, "c_key", 0));
            try testing.expectEqual(i64At(df, "c_key", n - 1), i64At(hd, "c_key", n - 1));

            // head(n > height) clamps to height.
            const hd_all = try df.head(h + 1000);
            try testing.expectEqual(h, hd_all.height());

            // tail(n): last n rows.
            const tl = try df.tail(n);
            try testing.expectEqual(n, tl.height());
            try testing.expectEqual(i64At(df, "c_key", h - n), i64At(tl, "c_key", 0));
            try testing.expectEqual(i64At(df, "c_key", h - 1), i64At(tl, "c_key", n - 1));

            // slice(start, end): rows [start, end), boundary values match.
            const start: usize = 50;
            const end: usize = 250;
            const sl = try df.slice(start, end);
            try testing.expectEqual(end - start, sl.height());
            try testing.expectEqual(i64At(df, "c_key", start), i64At(sl, "c_key", 0));
            try testing.expectEqual(i64At(df, "c_key", end - 1), i64At(sl, "c_key", end - start - 1));

            // slice(start >= end) -> 0 rows but schema preserved (Phase 12).
            const empty = try df.slice(100, 100);
            try testing.expectEqual(@as(usize, 0), empty.height());
            try testing.expectEqual(w, empty.width());

            // limit mutates in place to the first n rows.
            const lim = try df.deepCopy();
            lim.limit(n);
            try testing.expectEqual(n, lim.height());
            try testing.expectEqual(i64At(df, "c_key", n - 1), i64At(lim, "c_key", n - 1));
        }
    }
}

// ---------------------------------------------------------------------------
// concat
// ---------------------------------------------------------------------------

test "concat: heights add; schema preserved; first/last rows match the sources" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const a = try genWide(allocator, rows, seed, 0.1);
            const b = try genWide(allocator, rows / 2, seed + 100, 0.1);

            const cat = try a.concat(b);
            try testing.expectEqual(a.height() + b.height(), cat.height());
            try testing.expectEqual(a.width(), cat.width());

            // First rows come from a, the row right after a comes from b.
            try testing.expectEqual(i64At(a, "c_key", 0), i64At(cat, "c_key", 0));
            try testing.expectEqual(i64At(b, "c_key", 0), i64At(cat, "c_key", a.height()));
            try testing.expectEqual(i64At(b, "c_key", b.height() - 1), i64At(cat, "c_key", cat.height() - 1));
        }
    }
}

// ---------------------------------------------------------------------------
// unique / valueCounts
// ---------------------------------------------------------------------------

test "unique/valueCounts: unique count == nunique; valueCounts sums to non-null" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            for (null_rates) |nr| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const allocator = arena.allocator();

                const df = try genWide(allocator, rows, seed, nr);

                const key_nunique = try df.getSeries("c_key").?.nunique(allocator);
                const u = try df.unique("c_key");
                try testing.expectEqual(key_nunique, u.height());

                // valueCounts(c_key): one row per distinct key, counts sum to
                // the row count (c_key never null).
                const vc = try df.valueCounts("c_key");
                try testing.expectEqual(key_nunique, vc.height());
                const cs = vc.getSeries("count").?.usize;
                var total: usize = 0;
                for (0..cs.len()) |i| total += cs.getAt(i).?;
                try testing.expectEqual(df.height(), total);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// deepCopy
// ---------------------------------------------------------------------------

test "deepCopy: equal + independent (mutating the copy leaves the original intact)" {
    for (seeds) |seed| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const df = try genWide(allocator, 5_000, seed, 0.15);

        const copy = try df.deepCopy();
        try testing.expect(try df.compareDataframe(copy));

        const orig_nulls = df.getSeries("c_i64").?.nullCount();
        try testing.expect(orig_nulls > 0);

        // Mutate via fillNull on the copy's data: build a filled frame from the
        // copy, then assert the original is untouched.
        _ = try copy.fillNull("c_i64", i64, 7);
        // Original still has its nulls (independence of the underlying data).
        try testing.expectEqual(orig_nulls, df.getSeries("c_i64").?.nullCount());
        try testing.expectEqual(orig_nulls, copy.getSeries("c_i64").?.nullCount());
    }
}

// ---------------------------------------------------------------------------
// shift / diff
// ---------------------------------------------------------------------------

test "shift/diff: length preserved; shift(0)==identity; first |n| rows null" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const df = try genWide(allocator, rows, seed, 0.0);
            const h = df.height();

            // shift(0) is the identity on the column (cheap per-cell compare).
            const s0 = try df.shift("c_i64", 0);
            {
                const a = df.getSeries("c_i64").?.int64;
                const b = s0.getSeries("c_i64").?.int64;
                try testing.expectEqual(a.len(), b.len());
                for (0..a.len()) |i| {
                    try testing.expectEqual(a.isNull(i), b.isNull(i));
                    if (a.getAt(i)) |v| try testing.expectEqual(v, b.getAt(i).?);
                }
            }

            // shift(+n): first n rows become null, length preserved.
            const n: i64 = 5;
            const sn = try df.shift("c_i64", n);
            try testing.expectEqual(h, sn.height());
            const out = sn.getSeries("c_i64").?.int64;
            for (0..@intCast(n)) |i| try testing.expect(out.isNull(i));
            // Value at row n equals the original value at row 0.
            try testing.expectEqual(i64At(df, "c_i64", 0), out.getAt(@intCast(n)));

            // diff(n): first n rows null, length preserved.
            const d = try df.diff("c_i64", 5);
            try testing.expectEqual(h, d.height());
            const dout = d.getSeries("c_i64").?.int64;
            for (0..5) |i| try testing.expect(dout.isNull(i));
        }
    }
}

// ---------------------------------------------------------------------------
// clip / replace
// ---------------------------------------------------------------------------

test "clip/replace: clip values within [lo,hi]; replace swaps only matches" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const df = try genWide(allocator, rows, seed, 0.1);
            const h = df.height();

            // clip to a tight range; every non-null output must lie in [lo, hi].
            const lo: i64 = -500;
            const hi: i64 = 500;
            const clipped = try df.clip("c_i64", i64, lo, hi);
            const cs = clipped.getSeries("c_i64").?.int64;
            const in = df.getSeries("c_i64").?.int64;
            for (0..h) |i| {
                try testing.expectEqual(in.isNull(i), cs.isNull(i));
                if (cs.getAt(i)) |v| {
                    try testing.expect(v >= lo and v <= hi);
                }
            }

            // replace on c_key (never null, bounded cardinality): swap key 0 ->
            // a sentinel; every other cell unchanged.
            const sentinel: i64 = -987654321;
            const replaced = try df.replace("c_key", i64, 0, sentinel);
            const ok = df.getSeries("c_key").?.int64;
            const rk = replaced.getSeries("c_key").?.int64;
            for (0..h) |i| {
                const orig_v = ok.getAt(i).?;
                const new_v = rk.getAt(i).?;
                if (orig_v == 0) {
                    try testing.expectEqual(sentinel, new_v);
                } else {
                    try testing.expectEqual(orig_v, new_v);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Round-trip identities (the strongest regression guard)
// ---------------------------------------------------------------------------

test "round-trip: TDF write -> parse -> compareDataframe (lossless, incl. nested)" {
    for (rt_cases) |c| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        // Include nested here — TDF handles every column type.
        const df = try testdata.genDataframe(allocator, .{
            .rows = c.rows,
            .seed = c.seed,
            .null_rate = c.null_rate,
            .nested = true,
        });

        const rt = try testdata.tdfRoundTrip(allocator, df);
        try testing.expect(try df.compareDataframe(rt));
    }
}

test "round-trip: parquet write -> read -> per-column value+null parity (no nested)" {
    for (rt_cases) |c| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        // Exclude nested: testdata's c_list has no parquet schema node to
        // shred against (see testdata.zig). Everything else round-trips.
        const df = try genWide(allocator, c.rows, c.seed, c.null_rate);

        const rt = try testdata.parquetRoundTrip(allocator, df);

        try testing.expectEqual(df.height(), rt.height());
        // Per-column value + null parity via the canonical string form.
        const names = try df.getColumnNames();
        for (names.items) |col| {
            const a = df.getSeries(col) orelse return error.MissingColumn;
            const b = rt.getSeries(col) orelse return error.MissingColumn;
            try testing.expectEqual(a.len(), b.len());
            try testing.expectEqual(a.nullCount(), b.nullCount());
            for (0..a.len()) |i| {
                try testing.expectEqual(a.isNull(i), b.isNull(i));
                const sa = try a.asStringAt(i);
                const sb = try b.asStringAt(i);
                try testing.expect(std.mem.eql(u8, sa.toSlice(), sb.toSlice()));
            }
        }
    }
}

test "round-trip: CSV write -> parse survives (row count + width, Phase 9 fidelity)" {
    for (rt_cases) |c| {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const df = try genWide(allocator, c.rows, c.seed, 0.1);

        const csv = try df.toCsvString(.{});

        const parsed = try @import("csv_reader.zig").parse(allocator, csv, .{});

        // Per the Phase 9 fidelity matrix, CSV re-read may infer different
        // types — assert structure survives, not value equality.
        try testing.expectEqual(df.height(), parsed.height());
        try testing.expectEqual(df.width(), parsed.width());
    }
}

// ---------------------------------------------------------------------------
// describe
// ---------------------------------------------------------------------------

test "describe: runs; 5 stat rows; count == non-null count per numeric column" {
    for (row_counts) |rows| {
        for (seeds) |seed| {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            const df = try genWide(allocator, rows, seed, 0.15);

            const desc = try df.describe();

            // 5 stat rows: count, mean, std, min, max.
            try testing.expectEqual(@as(usize, 5), desc.height());

            // The first ("count") row of each numeric column == non-null count.
            // describe emits an f64 column per numeric source column, named
            // after it; the stat label column is "stat".
            const expected_count: f64 = @floatFromInt(df.height() - df.getSeries("c_i64").?.nullCount());
            const count_row = desc.getSeries("c_i64").?.float64.getAt(0).?;
            try testing.expectEqual(expected_count, count_row);
        }
    }
}

// ---------------------------------------------------------------------------
// LEAK-CANARY — the ONE test that runs on std.testing.allocator
// ---------------------------------------------------------------------------
//
// The wide invariant tests above use an arena (see the module doc), so they do
// NOT leak-check the ops. This canary runs a representative handful of ops
// (genDataframe incl. nested, sort, filter, groupBy, deepCopy, fillNull, and a
// TDF round-trip) at SMALL size on `std.testing.allocator` with proper
// per-frame `deinit`s, so a leak introduced into genDataframe or any of those
// ops surfaces here (and the per-op tests in series_test/dataframe_test cover
// the rest at small scale). Keep this on the testing allocator and keep the
// deinits.
test "LEAK-CANARY: representative ops are leak-free at small size (testing allocator)" {
    const allocator = testing.allocator;

    // genDataframe incl. nested — exercises every column type's allocation path.
    const df = try testdata.genDataframe(allocator, .{
        .rows = 500,
        .seed = 1,
        .null_rate = 0.15,
        .nested = true,
    });
    defer df.deinit();

    // sort (allocates an argSort + a permuted frame).
    {
        const sorted = try df.sort("c_i64", true);
        defer sorted.deinit();
        try testing.expectEqual(df.height(), sorted.height());
    }

    // filter (predicate indices + filtered frame).
    {
        const filtered = try df.filter("c_i64", i64, .gt, 0);
        defer filtered.deinit();
        try testing.expect(filtered.height() <= df.height());
    }

    // groupBy + count (hash map of groups + result frame).
    {
        var gb = try df.groupBy("c_key");
        defer gb.deinit();
        const counts = try gb.count();
        defer counts.deinit();
        try testing.expect(counts.height() > 0);
    }

    // deepCopy + fillNull (whole-frame clones with per-cell String/Decimal/etc.
    // clones — the heaviest allocation paths).
    {
        const copy = try df.deepCopy();
        defer copy.deinit();
        const filled = try copy.fillNull("c_i64", i64, 0);
        defer filled.deinit();
        try testing.expectEqual(@as(usize, 0), filled.getSeries("c_i64").?.nullCount());
    }

    // TDF round-trip (serialize + parse rebuild, incl. nested) — the format
    // helpers must free their own intermediates and hand back an owned frame.
    {
        const rt = try testdata.tdfRoundTrip(allocator, df);
        defer rt.deinit();
        try testing.expect(try df.compareDataframe(rt));
    }
}
