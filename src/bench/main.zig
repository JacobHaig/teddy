//! Phase 14.0 — benchmark harness (`zig build bench`).
//!
//! A standalone executable that times teddy's hot-path operations over a
//! seeded, generated dataframe (see `src/dataframe/testdata.zig`).
//!
//! ## Methodology
//! - Monotonic clock (`std.Io.Clock.awake`). Each op is WARMED UP once (untimed), then
//!   run for K iterations, each iteration timed individually. We report the
//!   MEDIAN ns/op (stable against outliers) plus ops/sec and the K-sample total.
//! - The input dataframe(s) are generated ONCE, OUTSIDE the timed loop — data
//!   generation is never timed.
//! - For ops that PRODUCE a new dataframe (sort, filter, join, ...), the result
//!   alloc IS timed; the result `deinit` is done AFTER stopping the per-iter
//!   timer, so allocation cost counts and free cost does not. Pure-read ops
//!   (sum/mean/min/max) allocate nothing. Round-trip ops time the full
//!   write+read+rebuild and deinit the rebuilt frame untimed.
//! - REAL numbers require ReleaseFast: `zig build bench -Doptimize=ReleaseFast`.
//!   Debug builds compile and run (CI smoke) but are not meaningful timings.
//!
//! ## Usage
//!   zig build bench -Doptimize=ReleaseFast -- [--rows N] [--iters K]
//!                                              [--json out.json] [--baseline base.json]
//! - `--rows N`     rows in the generated frame (default 100_000)
//! - `--iters K`    timed iterations per op (default 50)
//! - `--json PATH`  write machine-readable results
//! - `--baseline P` load a prior `--json` result and print before/after Δ%

const std = @import("std");
const teddy = @import("teddy");
const testdata = teddy.testdata;
const Dataframe = teddy.Dataframe;

const Result = struct {
    name: []const u8,
    rows: usize,
    iters: usize,
    median_ns: u64,

    fn opsPerSec(self: Result) f64 {
        if (self.median_ns == 0) return 0;
        return 1_000_000_000.0 / @as(f64, @floatFromInt(self.median_ns));
    }
};

const Config = struct {
    rows: usize = 100_000,
    iters: usize = 50,
    json_path: ?[]const u8 = null,
    baseline_path: ?[]const u8 = null,
};

/// Times a single op `f` over K iterations and returns the median ns/op.
/// `f` receives the shared context and must perform exactly one operation,
/// freeing any dataframe it produces (the result-deinit timing policy is
/// documented in the module header; callers stop the timer before deinit).
fn benchOp(
    allocator: std.mem.Allocator,
    io: std.Io,
    name: []const u8,
    rows: usize,
    iters: usize,
    ctx: anytype,
    comptime f: fn (@TypeOf(ctx)) anyerror!void,
) !Result {
    // Warmup (untimed).
    try f(ctx);

    const samples = try allocator.alloc(u64, iters);
    defer allocator.free(samples);

    const Clock = std.Io.Clock;
    for (samples) |*slot| {
        const t0 = Clock.Timestamp.now(io, .awake);
        try f(ctx);
        const t1 = Clock.Timestamp.now(io, .awake);
        const ns = t0.durationTo(t1).raw.toNanoseconds();
        slot.* = if (ns < 0) 0 else @intCast(ns);
    }

    std.mem.sort(u64, samples, {}, std.sort.asc(u64));
    const median = samples[iters / 2];
    return .{ .name = name, .rows = rows, .iters = iters, .median_ns = median };
}

// --- Operation closures ----------------------------------------------------
// Each takes a context struct and performs exactly one op. Result frames are
// produced inside the timed region; freed after (see benchOp / header policy).

const Ctx = struct {
    alloc: std.mem.Allocator,
    df: *Dataframe,
    right: *Dataframe,
    f64_median: f64,
    i64_median: i64,
};

fn opSort(c: *Ctx) !void {
    const r = try c.df.sort("c_i64", true);
    r.deinit();
}
fn opFilter(c: *Ctx) !void {
    const r = try c.df.filter("c_i64", i64, .gt, c.i64_median);
    r.deinit();
}
fn opGroupBySum(c: *Ctx) !void {
    var gb = try c.df.groupBy("c_str");
    defer gb.deinit();
    const r = try gb.sum("c_i64");
    r.deinit();
}
fn opJoin(c: *Ctx) !void {
    const r = try c.df.join(c.right, "c_key", .inner);
    r.deinit();
}
fn opCumSum(c: *Ctx) !void {
    const r = try c.df.cumSum("c_i64");
    r.deinit();
}
fn opCast(c: *Ctx) !void {
    const r = try c.df.cast("c_i32", i64);
    r.deinit();
}
fn opSum(c: *Ctx) !void {
    std.mem.doNotOptimizeAway(c.df.getSeries("c_f64").?.sum());
}
fn opMean(c: *Ctx) !void {
    std.mem.doNotOptimizeAway(c.df.getSeries("c_f64").?.mean());
}
fn opMin(c: *Ctx) !void {
    std.mem.doNotOptimizeAway(c.df.getSeries("c_f64").?.min());
}
fn opMax(c: *Ctx) !void {
    std.mem.doNotOptimizeAway(c.df.getSeries("c_f64").?.max());
}
fn opDropNulls(c: *Ctx) !void {
    const r = try c.df.dropNulls("c_i64");
    r.deinit();
}
fn opDeepCopy(c: *Ctx) !void {
    const r = try c.df.deepCopy();
    r.deinit();
}
fn opTdfRoundTrip(c: *Ctx) !void {
    const r = try testdata.tdfRoundTrip(c.alloc, c.df);
    r.deinit();
}
fn opParquetRoundTrip(c: *Ctx) !void {
    const r = try testdata.parquetRoundTrip(c.alloc, c.df);
    r.deinit();
}

fn median_f64(allocator: std.mem.Allocator, df: *Dataframe, col: []const u8) !f64 {
    const s = df.getSeries(col) orelse return 0;
    const n = s.len();
    var vals = try std.ArrayList(f64).initCapacity(allocator, n);
    defer vals.deinit(allocator);
    for (0..n) |i| {
        if (s.isNull(i)) continue;
        var str = try s.asStringAt(i);
        defer str.deinit();
        const v = std.fmt.parseFloat(f64, str.toSlice()) catch continue;
        try vals.append(allocator, v);
    }
    if (vals.items.len == 0) return 0;
    std.mem.sort(f64, vals.items, {}, std.sort.asc(f64));
    return vals.items[vals.items.len / 2];
}

/// Args slices live in the process arena for the program's lifetime, so the
/// `?[]const u8` paths borrow them directly (no dupe, no free).
fn parseArgs(args: []const [:0]const u8) !Config {
    var cfg = Config{};
    var i: usize = 1; // skip exe name
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--rows")) {
            i += 1;
            if (i >= args.len) return error.MissingArg;
            cfg.rows = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, a, "--iters")) {
            i += 1;
            if (i >= args.len) return error.MissingArg;
            cfg.iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, a, "--json")) {
            i += 1;
            if (i >= args.len) return error.MissingArg;
            cfg.json_path = args[i];
        } else if (std.mem.eql(u8, a, "--baseline")) {
            i += 1;
            if (i >= args.len) return error.MissingArg;
            cfg.baseline_path = args[i];
        } else {
            std.debug.print("unknown arg: {s}\n", .{a});
            return error.UnknownArg;
        }
    }
    if (cfg.iters == 0) cfg.iters = 1;
    return cfg;
}

/// Appends `fmt`-formatted text to an unmanaged ArrayList(u8).
fn appendFmt(buf: *std.ArrayList(u8), allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) !void {
    const s = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(s);
    try buf.appendSlice(allocator, s);
}

fn writeJson(allocator: std.mem.Allocator, io: std.Io, path: []const u8, results: []const Result) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    try appendFmt(&buf, allocator, "{{\n  \"results\": [\n", .{});
    for (results, 0..) |r, i| {
        try appendFmt(
            &buf,
            allocator,
            "    {{ \"name\": \"{s}\", \"rows\": {d}, \"iters\": {d}, \"median_ns\": {d}, \"ops_per_sec\": {d:.3} }}{s}\n",
            .{ r.name, r.rows, r.iters, r.median_ns, r.opsPerSec(), if (i + 1 < results.len) "," else "" },
        );
    }
    try appendFmt(&buf, allocator, "  ]\n}}\n", .{});

    const cwd = std.Io.Dir.cwd();
    try cwd.writeFile(io, .{ .sub_path = path, .data = buf.items });
}

const Baseline = struct {
    entries: std.StringHashMapUnmanaged(u64) = .{}, // name -> median_ns

    fn deinit(self: *Baseline, allocator: std.mem.Allocator) void {
        var it = self.entries.iterator();
        while (it.next()) |e| allocator.free(e.key_ptr.*);
        self.entries.deinit(allocator);
    }
};

fn loadBaseline(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Baseline {
    const cwd = std.Io.Dir.cwd();
    const content = try cwd.readFileAlloc(io, path, allocator, .unlimited);
    defer allocator.free(content);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, content, .{});
    defer parsed.deinit();

    var bl = Baseline{};
    errdefer bl.deinit(allocator);

    const root = parsed.value.object;
    const arr = (root.get("results") orelse return error.BadBaseline).array;
    for (arr.items) |item| {
        const obj = item.object;
        const name = (obj.get("name") orelse continue).string;
        const median = (obj.get("median_ns") orelse continue).integer;
        const key = try allocator.dupe(u8, name);
        try bl.entries.put(allocator, key, @intCast(median));
    }
    return bl;
}

fn fmtNs(buf: []u8, ns: u64) []const u8 {
    // Human-friendly: ns / us / ms / s.
    if (ns < 1_000) return std.fmt.bufPrint(buf, "{d} ns", .{ns}) catch buf[0..0];
    if (ns < 1_000_000) return std.fmt.bufPrint(buf, "{d:.2} us", .{@as(f64, @floatFromInt(ns)) / 1e3}) catch buf[0..0];
    if (ns < 1_000_000_000) return std.fmt.bufPrint(buf, "{d:.2} ms", .{@as(f64, @floatFromInt(ns)) / 1e6}) catch buf[0..0];
    return std.fmt.bufPrint(buf, "{d:.3} s", .{@as(f64, @floatFromInt(ns)) / 1e9}) catch buf[0..0];
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const cfg = try parseArgs(args);

    // Build inputs ONCE (untimed). Nested off so parquet round-trip works.
    // ~15% nulls so dropNulls has work to do.
    var df = try testdata.genDataframe(allocator, .{ .rows = cfg.rows, .seed = 0, .null_rate = 0.15 });
    defer df.deinit();
    // Smaller right frame for join (bounds result size); same key domain.
    const right_rows = @max(@as(usize, 1), cfg.rows / 10);
    var right = try testdata.genDataframe(allocator, .{ .rows = right_rows, .seed = 1, .null_rate = 0.0 });
    defer right.deinit();

    var ctx = Ctx{
        .alloc = allocator,
        .df = df,
        .right = right,
        .f64_median = try median_f64(allocator, df, "c_f64"),
        .i64_median = blk: {
            const m = try median_f64(allocator, df, "c_i64");
            break :blk @intFromFloat(m);
        },
    };

    var results: std.ArrayList(Result) = .empty;
    defer results.deinit(allocator);

    const Op = struct { name: []const u8, f: fn (*Ctx) anyerror!void };
    const ops = [_]Op{
        .{ .name = "sort", .f = opSort },
        .{ .name = "filter", .f = opFilter },
        .{ .name = "groupBy_sum", .f = opGroupBySum },
        .{ .name = "join", .f = opJoin },
        .{ .name = "cumSum", .f = opCumSum },
        .{ .name = "cast", .f = opCast },
        .{ .name = "sum", .f = opSum },
        .{ .name = "mean", .f = opMean },
        .{ .name = "min", .f = opMin },
        .{ .name = "max", .f = opMax },
        .{ .name = "dropNulls", .f = opDropNulls },
        .{ .name = "deepCopy", .f = opDeepCopy },
        .{ .name = "tdf_roundtrip", .f = opTdfRoundTrip },
        .{ .name = "parquet_roundtrip", .f = opParquetRoundTrip },
    };

    inline for (ops) |op| {
        const r = try benchOp(allocator, io, op.name, cfg.rows, cfg.iters, &ctx, op.f);
        try results.append(allocator, r);
    }

    try report(allocator, io, cfg, results.items);

    if (cfg.json_path) |p| {
        try writeJson(allocator, io, p, results.items);
        std.debug.print("\nwrote results to {s}\n", .{p});
    }
}

fn report(allocator: std.mem.Allocator, io: std.Io, cfg: Config, results: []const Result) !void {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);

    var baseline: ?Baseline = null;
    defer if (baseline) |*b| b.deinit(allocator);
    if (cfg.baseline_path) |bp| {
        baseline = loadBaseline(allocator, io, bp) catch |e| blk: {
            std.debug.print("warning: could not load baseline {s}: {s}\n", .{ bp, @errorName(e) });
            break :blk null;
        };
    }

    try appendFmt(&out, allocator, "\nteddy benchmark — rows={d} iters={d} (median-of-K)\n", .{ cfg.rows, cfg.iters });

    var nb: [32]u8 = undefined;
    var ob: [32]u8 = undefined;

    if (baseline) |bl| {
        try appendFmt(&out, allocator, "{s:<20} {s:>14} {s:>14} {s:>10}\n", .{ "op", "before", "after", "delta%" });
        try appendFmt(&out, allocator, "{s}\n", .{"-" ** 60});
        for (results) |r| {
            const after = r.median_ns;
            const after_s = fmtNs(&nb, after);
            if (bl.entries.get(r.name)) |before| {
                const before_s = fmtNs(&ob, before);
                const delta = (@as(f64, @floatFromInt(before)) - @as(f64, @floatFromInt(after))) /
                    @as(f64, @floatFromInt(before)) * 100.0;
                try appendFmt(&out, allocator, "{s:<20} {s:>14} {s:>14} {d:>9.2}%\n", .{ r.name, before_s, after_s, delta });
            } else {
                try appendFmt(&out, allocator, "{s:<20} {s:>14} {s:>14} {s:>10}\n", .{ r.name, "(none)", after_s, "n/a" });
            }
        }
    } else {
        try appendFmt(&out, allocator, "{s:<20} {s:>10} {s:>8} {s:>14} {s:>14}\n", .{ "op", "rows", "iters", "median ns/op", "ops/sec" });
        try appendFmt(&out, allocator, "{s}\n", .{"-" ** 70});
        for (results) |r| {
            const m = fmtNs(&nb, r.median_ns);
            try appendFmt(&out, allocator, "{s:<20} {d:>10} {d:>8} {s:>14} {d:>14.1}\n", .{ r.name, r.rows, r.iters, m, r.opsPerSec() });
        }
    }

    var stdout_buf: [4096]u8 = undefined;
    var fw = std.Io.File.stdout().writer(io, &stdout_buf);
    try fw.interface.writeAll(out.items);
    try fw.interface.flush();
}
