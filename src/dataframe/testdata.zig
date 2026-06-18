//! Phase 14.0 — shared seeded data generator.
//!
//! Deterministic, allocator-based dataframe builder reused by the benchmark
//! harness (`src/bench/main.zig`) and the wide correctness suite (Phase 14.1,
//! `operations_test.zig`). No committed multi-MB fixtures: data is generated
//! in-process and is fully reproducible given `(rows, seed)`.
//!
//! ## Emitted schema (BOTH consumers must agree on this — keep it stable)
//!
//! Columns are emitted in this fixed order; a family is omitted entirely when
//! its `GenOptions` flag is false. Every data column EXCEPT `c_key` is nullable
//! and obeys `null_rate`; `c_key` is always non-null so joins/groupBy have a
//! dense, bounded-cardinality key.
//!
//! | column        | type      | family    | notes                                   |
//! |---------------|-----------|-----------|-----------------------------------------|
//! | c_key         | i64       | (always)  | join/group key, cardinality ~ rows/16, never null |
//! | c_i32         | i32       | numerics  | full i32 range, occasional min/max boundary |
//! | c_i64         | i64       | numerics  | wide range, occasional min/max boundary |
//! | c_u32         | u32       | numerics  | full u32 range, occasional 0/max boundary |
//! | c_u64         | u64       | numerics  | wide range, occasional 0/max boundary   |
//! | c_f64         | f64       | numerics  | [-1e6, 1e6], occasional 0 / tiny / large |
//! | c_str         | String    | strings   | short ascii (len 1..8), drawn from a small pool so groupBy/unique are meaningful |
//! | c_date        | Date      | temporal  | days since epoch, ~ 2000-01-01 .. 2030  |
//! | c_timestamp   | Timestamp | temporal  | micros since epoch, utc, int64 origin   |
//! | c_dec         | Decimal   | decimal   | precision 9, scale 2 (unscaled in [-9_999_999, 9_999_999]) |
//! | c_list        | Nested    | nested    | list<i64>, len 0..4 (ONLY when opts.nested) |
//!
//! Determinism: a `std.Random.DefaultPrng` seeded by `opts.seed`. Same
//! `(rows, seed)` => byte-identical dataframe (verified by an in-file test via
//! `compareDataframe`). `null_rate == 0` => no nulls in any column.
//!
//! NOTE on round-trips: the `c_list` (Nested) column requires a parquet schema
//! node to be shredded for parquet, so the parquet round-trip benches generate
//! with `nested = false`. TDF handles every column including nested.

const std = @import("std");

const dataframe = @import("dataframe.zig");
pub const Dataframe = dataframe.Dataframe;
const Series = dataframe.Series;
const String = dataframe.String;

const Date = @import("date.zig").Date;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Nested = @import("nested.zig").Nested;
const TimeUnit = @import("parquet").TimeUnit;

const native = @import("native_format.zig");
const parquet_mod = @import("parquet");
const parquet_adapter = @import("parquet.zig");

pub const GenOptions = struct {
    rows: usize,
    seed: u64 = 0,
    /// Fraction of null cells per nullable column (0.0 .. 1.0).
    null_rate: f32 = 0.0,
    numerics: bool = true,
    strings: bool = true,
    temporal: bool = true,
    decimal: bool = true,
    /// Nested off by default (heavier; and parquet round-trips skip it).
    nested: bool = false,
};

/// A small pool of short ascii strings. Repeats across rows make groupBy /
/// unique / value_counts meaningful (bounded cardinality).
const string_pool = [_][]const u8{
    "alpha", "beta",  "gamma", "delta", "eps",  "zeta",   "eta",   "theta",
    "iota",  "kappa", "lam",   "mu",    "nu",   "xi",     "omi",   "pi",
};

/// Deterministic given `(rows, seed)`. Caller owns the returned `*Dataframe`
/// and must call `deinit` (which frees all columns).
pub fn genDataframe(allocator: std.mem.Allocator, opts: GenOptions) !*Dataframe {
    var prng = std.Random.DefaultPrng.init(opts.seed);
    const rand = prng.random();

    const df = try Dataframe.init(allocator);
    errdefer df.deinit();

    const n = opts.rows;
    // Bounded key cardinality so joins/groupBy produce sane result sizes.
    const key_card: u64 = @max(@as(u64, 1), @as(u64, @intCast(n)) / 16);

    // Helper: returns true when this cell should be null.
    const shouldNull = struct {
        fn f(r: std.Random, rate: f32) bool {
            if (rate <= 0.0) return false;
            return r.float(f32) < rate;
        }
    }.f;

    // c_key — always present, never null.
    {
        var s = try df.createSeries(i64);
        try s.rename("c_key");
        for (0..n) |_| {
            try s.append(@intCast(rand.uintLessThan(u64, key_card)));
        }
    }

    if (opts.numerics) {
        {
            var s = try df.createSeries(i32);
            try s.rename("c_i32");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else if (rand.float(f32) < 0.02) {
                    try s.append(if (rand.boolean()) std.math.maxInt(i32) else std.math.minInt(i32));
                } else {
                    try s.append(rand.int(i32));
                }
            }
        }
        {
            // Bounded range (±1e9): cumSum / groupBy-sum over this column must
            // not integer-overflow even across the whole frame. (i32/u32 carry
            // the min/max boundary probes; they are only widened/cast, never
            // summed, by the bench.)
            var s = try df.createSeries(i64);
            try s.rename("c_i64");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else {
                    try s.append(rand.intRangeAtMost(i64, -1_000_000_000, 1_000_000_000));
                }
            }
        }
        {
            var s = try df.createSeries(u32);
            try s.rename("c_u32");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else if (rand.float(f32) < 0.02) {
                    try s.append(if (rand.boolean()) std.math.maxInt(u32) else 0);
                } else {
                    try s.append(rand.int(u32));
                }
            }
        }
        {
            var s = try df.createSeries(u64);
            try s.rename("c_u64");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else if (rand.float(f32) < 0.02) {
                    try s.append(if (rand.boolean()) std.math.maxInt(u64) else 0);
                } else {
                    try s.append(rand.uintLessThan(u64, 2_000_000_000));
                }
            }
        }
        {
            var s = try df.createSeries(f64);
            try s.rename("c_f64");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else {
                    const roll = rand.float(f32);
                    if (roll < 0.01) {
                        try s.append(0.0);
                    } else if (roll < 0.02) {
                        try s.append(1e-9);
                    } else if (roll < 0.03) {
                        try s.append(1e12);
                    } else {
                        try s.append((rand.float(f64) - 0.5) * 2_000_000.0);
                    }
                }
            }
        }
    }

    if (opts.strings) {
        var s = try df.createSeries(String);
        try s.rename("c_str");
        for (0..n) |_| {
            if (shouldNull(rand, opts.null_rate)) {
                try s.appendNull();
            } else {
                // ~70% from the pool (repeats), ~30% short random ascii.
                if (rand.float(f32) < 0.7) {
                    const pick = string_pool[rand.uintLessThan(usize, string_pool.len)];
                    try s.tryAppend(pick);
                } else {
                    var buf: [8]u8 = undefined;
                    const len = 1 + rand.uintLessThan(usize, 8);
                    for (0..len) |i| {
                        buf[i] = 'a' + rand.uintLessThan(u8, 26);
                    }
                    const slice: []const u8 = buf[0..len];
                    try s.tryAppend(slice);
                }
            }
        }
    }

    if (opts.temporal) {
        {
            var s = try df.createSeries(Date);
            try s.rename("c_date");
            // 2000-01-01 is day 10957; ~30 years span.
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else {
                    const day: i32 = 10_957 + rand.intRangeAtMost(i32, 0, 10_957);
                    try s.append(.{ .days = day });
                }
            }
        }
        {
            var s = try df.createSeries(Timestamp);
            try s.rename("c_timestamp");
            for (0..n) |_| {
                if (shouldNull(rand, opts.null_rate)) {
                    try s.appendNull();
                } else {
                    const v = rand.intRangeAtMost(i64, 946_684_800_000_000, 1_893_456_000_000_000);
                    try s.append(.{ .value = v, .unit = .micros, .utc = true, .origin = .int64 });
                }
            }
        }
    }

    if (opts.decimal) {
        var s = try df.createSeries(Decimal);
        try s.rename("c_dec");
        for (0..n) |_| {
            if (shouldNull(rand, opts.null_rate)) {
                try s.appendNull();
            } else {
                const unscaled = rand.intRangeAtMost(i256, -9_999_999, 9_999_999);
                try s.append(.{ .unscaled = unscaled, .precision = 9, .scale = 2 });
            }
        }
    }

    if (opts.nested) {
        var s = try df.createSeries(Nested);
        try s.rename("c_list");
        for (0..n) |_| {
            if (shouldNull(rand, opts.null_rate)) {
                try s.appendNull();
            } else {
                const len = rand.uintLessThan(usize, 5); // 0..4
                const items = try allocator.alloc(Nested, len);
                for (items) |*it| {
                    it.* = .{ .int = rand.intRangeAtMost(i64, -1000, 1000) };
                }
                try s.append(.{ .list = .{ .allocator = allocator, .items = items } });
            }
        }
    }

    return df;
}

// --- Round-trip helpers (used by the bench; in the module so they can reach
//     native_format / parquet directly) -------------------------------------

/// TDF round-trip: serialize `df` to the native binary format and parse it
/// back. Caller owns the returned `*Dataframe`.
pub fn tdfRoundTrip(allocator: std.mem.Allocator, df: *Dataframe) !*Dataframe {
    const bytes = try native.writeToString(allocator, df);
    defer allocator.free(bytes);
    return native.parse(allocator, bytes);
}

/// Parquet round-trip: shred `df` to columns, write a parquet buffer, read it
/// back, and rebuild a dataframe. Caller owns the returned `*Dataframe`.
/// `df` must not contain a Nested column (parquet shredding needs a schema
/// node); generate with `nested = false`.
pub fn parquetRoundTrip(allocator: std.mem.Allocator, df: *Dataframe) !*Dataframe {
    var cols = try parquet_adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const buf = try parquet_mod.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(buf);
    var result = try parquet_mod.readParquet(allocator, buf);
    defer result.deinit();
    return parquet_adapter.toDataframe(allocator, &result);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "genDataframe is deterministic for the same (rows, seed)" {
    const a = try genDataframe(testing.allocator, .{ .rows = 1000, .seed = 1 });
    defer a.deinit();
    const b = try genDataframe(testing.allocator, .{ .rows = 1000, .seed = 1 });
    defer b.deinit();
    try testing.expect(try a.compareDataframe(b));
}

test "genDataframe width/height correct; default families" {
    const df = try genDataframe(testing.allocator, .{ .rows = 500, .seed = 7 });
    defer df.deinit();
    try testing.expectEqual(@as(usize, 500), df.height());
    // c_key + numerics(5) + strings(1) + temporal(2) + decimal(1) = 10
    try testing.expectEqual(@as(usize, 10), df.width());
}

test "genDataframe with nested adds c_list column" {
    const df = try genDataframe(testing.allocator, .{ .rows = 50, .seed = 3, .nested = true });
    defer df.deinit();
    try testing.expectEqual(@as(usize, 11), df.width());
    try testing.expect(df.getSeries("c_list") != null);
}

test "genDataframe null_rate=0 produces no nulls" {
    const df = try genDataframe(testing.allocator, .{ .rows = 800, .seed = 2, .null_rate = 0.0, .nested = true });
    defer df.deinit();
    for (df.series.items) |*s| {
        try testing.expectEqual(@as(usize, 0), s.nullCount());
    }
}

test "genDataframe null_rate>0 produces some nulls in nullable columns" {
    const df = try genDataframe(testing.allocator, .{ .rows = 2000, .seed = 5, .null_rate = 0.3 });
    defer df.deinit();
    // c_key is never null.
    try testing.expectEqual(@as(usize, 0), df.getSeries("c_key").?.nullCount());
    // A nullable column should have at least one null at this rate/size.
    try testing.expect(df.getSeries("c_i64").?.nullCount() > 0);
}

test "tdfRoundTrip preserves the frame" {
    const df = try genDataframe(testing.allocator, .{ .rows = 300, .seed = 9, .null_rate = 0.1 });
    defer df.deinit();
    const rt = try tdfRoundTrip(testing.allocator, df);
    defer rt.deinit();
    try testing.expect(try df.compareDataframe(rt));
}
