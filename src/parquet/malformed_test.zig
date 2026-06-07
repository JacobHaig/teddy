//! Malformed-input battery (Phase 11, Unit C): every committed fixture is
//! truncated and bit-flipped at deterministic offsets; readParquet must return
//! an error or a valid result — never panic, never leak. A panic here means a
//! missed validate-at-the-boundary site.

const std = @import("std");
const reader = @import("parquet_reader.zig");

const fixtures = [_][]const u8{
    "data/addresses.parquet",
    "data/addresses_snappy.parquet",
    "data/multi_rowgroup.parquet",
    "data/flba.parquet",
    "data/int96.parquet",
    "data/unsigned.parquet",
    "data/logical_annotations.parquet",
    "data/time_units.parquet",
    "data/decimals.parquet",
    "data/binary_kinds.parquet",
    "data/uuid_f16.parquet",
    "data/nested_smoke.parquet",
    "data/nested_kinds.parquet",
};

fn readFixture(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    return cwd.readFileAlloc(io, path, allocator, .unlimited);
}

/// Result must be deinited when read succeeds; both outcomes are acceptable.
fn expectNoPanic(allocator: std.mem.Allocator, data: []const u8) void {
    var result = reader.readParquet(allocator, data) catch return; // clean error: fine
    result.deinit(); // surprising success (corruption hit padding): also fine
}

test "malformed: truncation sweep over every fixture" {
    const allocator = std.testing.allocator;
    for (fixtures) |path| {
        const data = try readFixture(allocator, path);
        defer allocator.free(data);
        // Sweep ~64 truncation points per fixture, plus the structurally
        // interesting tails (footer length field, magic).
        const stride = @max(1, data.len / 64);
        var n: usize = 0;
        while (n < data.len) : (n += stride) {
            expectNoPanic(allocator, data[0..n]);
        }
        // Exact boundaries around the footer trailer.
        if (data.len >= 12) {
            expectNoPanic(allocator, data[0 .. data.len - 1]);
            expectNoPanic(allocator, data[0 .. data.len - 4]);
            expectNoPanic(allocator, data[0 .. data.len - 8]);
            expectNoPanic(allocator, data[0 .. data.len - 12]);
        }
    }
}

test "malformed: single-byte corruption sweep over every fixture" {
    const allocator = std.testing.allocator;
    for (fixtures) |path| {
        const data = try readFixture(allocator, path);
        defer allocator.free(data);
        const mutable = try allocator.dupe(u8, data);
        defer allocator.free(mutable);
        // Flip one byte at ~64 offsets per fixture (footer region is densest
        // in structure, so add a focused tail sweep too).
        const stride = @max(1, data.len / 64);
        var off: usize = 0;
        while (off < data.len) : (off += stride) {
            mutable[off] ^= 0xFF;
            expectNoPanic(allocator, mutable);
            mutable[off] = data[off]; // restore
        }
        // Dense sweep over the last 64 bytes (footer + trailer).
        const tail_start = if (data.len > 64) data.len - 64 else 0;
        for (tail_start..data.len) |o| {
            mutable[o] ^= 0xFF;
            expectNoPanic(allocator, mutable);
            mutable[o] = data[o];
        }
    }
}

test "malformed: pathological inputs" {
    const allocator = std.testing.allocator;
    // All zeros with valid magic wrappers.
    var zeros = [_]u8{0} ** 64;
    @memcpy(zeros[0..4], "PAR1");
    @memcpy(zeros[60..64], "PAR1");
    expectNoPanic(allocator, &zeros);
    // Footer length pointing past start of file.
    var self_ref = [_]u8{0} ** 16;
    @memcpy(self_ref[0..4], "PAR1");
    std.mem.writeInt(u32, self_ref[8..12], 0xFFFF_FFFF, .little);
    @memcpy(self_ref[12..16], "PAR1");
    expectNoPanic(allocator, &self_ref);
    // 0xFF everywhere between magics (max varints, max deltas).
    var ones = [_]u8{0xFF} ** 64;
    @memcpy(ones[0..4], "PAR1");
    std.mem.writeInt(u32, ones[56..60], 44, .little);
    @memcpy(ones[60..64], "PAR1");
    expectNoPanic(allocator, &ones);
}
