//! Timestamp column type (Phase 6d-2a.2): parquet TIMESTAMP annotation and
//! decoded legacy INT96. value counts `unit` ticks since the Unix epoch;
//! `utc` mirrors isAdjustedToUTC; `origin` records the on-disk physical so
//! WriteOptions.emit_int96 can re-emit legacy files bit-faithfully.

const std = @import("std");
const TimeUnit = @import("parquet").TimeUnit;
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;

const ns_per_day: i128 = 86_400_000_000_000;
/// Julian day number of 1970-01-01.
const julian_epoch: i128 = 2_440_588;

pub const Timestamp = struct {
    value: i64,
    unit: TimeUnit,
    utc: bool,
    origin: Origin = .int64,

    pub const Origin = enum { int64, int96 };
    pub const type_name = "Timestamp";

    pub fn toNanos(self: Timestamp) i128 {
        return @as(i128, self.value) * Time.nanosPerUnit(self.unit);
    }

    pub fn addDuration(self: Timestamp, amount: i64, unit: TimeUnit) !Timestamp {
        const total: i128 = self.toNanos() + @as(i128, amount) * Time.nanosPerUnit(unit);
        const per = Time.nanosPerUnit(self.unit);
        if (@rem(total, per) != 0) return error.LossyDuration;
        const new_value = std.math.cast(i64, @divTrunc(total, per)) orelse return error.Overflow;
        return .{ .value = new_value, .unit = self.unit, .utc = self.utc, .origin = self.origin };
    }

    /// Calendar date of the instant (floor — correct for pre-epoch too).
    /// Precondition: the day count must fit i32 (always true for values that
    /// came from parquet; a hand-built extreme value panics — harden to an
    /// error union when a fallible caller path appears).
    pub fn toDate(self: Timestamp) Date {
        return .{ .days = @intCast(@divFloor(self.toNanos(), ns_per_day)) };
    }

    /// Time-of-day within the instant's date, in nanos.
    pub fn timeOfDay(self: Timestamp) Time {
        return .{ .value = @intCast(@mod(self.toNanos(), ns_per_day)), .unit = .nanos, .utc = self.utc };
    }

    /// Compose from a calendar date + time-of-day. Result is nanos, origin int64.
    pub fn fromDateAndTime(date: Date, time: Time, utc: bool) !Timestamp {
        const total: i128 = @as(i128, date.days) * ns_per_day + time.toNanos();
        const v = std.math.cast(i64, total) orelse return error.Overflow;
        return .{ .value = v, .unit = .nanos, .utc = utc, .origin = .int64 };
    }

    pub fn eql(self: *const Timestamp, other: *const Timestamp) bool {
        return self.toNanos() == other.toNanos();
    }

    pub fn order(self: *const Timestamp, other: *const Timestamp) std.math.Order {
        return std.math.order(self.toNanos(), other.toNanos());
    }

    /// Decode legacy INT96: bytes[0..8] LE u64 nanos-of-day, bytes[8..12] LE
    /// u32 Julian day. Produces {unit=nanos, utc=false, origin=int96}.
    pub fn fromInt96Bytes(bytes: *const [12]u8) !Timestamp {
        const nanos_of_day = std.mem.readInt(u64, bytes[0..8], .little);
        const julian = std.mem.readInt(u32, bytes[8..12], .little);
        const total: i128 = (@as(i128, julian) - julian_epoch) * ns_per_day + nanos_of_day;
        const v = std.math.cast(i64, total) orelse return error.TimestampOverflow;
        return .{ .value = v, .unit = .nanos, .utc = false, .origin = .int96 };
    }

    /// Inverse of fromInt96Bytes — bit-faithful for any value it produced.
    pub fn toInt96Bytes(self: Timestamp) ![12]u8 {
        const total = self.toNanos();
        const days = @divFloor(total, ns_per_day);
        const nanos_of_day: u64 = @intCast(@mod(total, ns_per_day));
        const julian_i = days + julian_epoch;
        if (julian_i < 0 or julian_i > std.math.maxInt(u32)) return error.TimestampOverflow;
        var out: [12]u8 = undefined;
        std.mem.writeInt(u64, out[0..8], nanos_of_day, .little);
        std.mem.writeInt(u32, out[8..12], @intCast(julian_i), .little);
        return out;
    }

    /// ISO 8601; 'Z' suffix when utc.
    pub fn format(self: Timestamp, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        const d = self.toDate();
        try d.format(writer);
        try writer.print("T", .{});
        const t = self.timeOfDay();
        try t.format(writer);
        if (self.utc) try writer.print("Z", .{});
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Timestamp: epoch == 1970-01-01T00:00:00" {
    const ts = Timestamp{ .value = 0, .unit = .nanos, .utc = false };
    const d = ts.toDate();
    try std.testing.expectEqual(@as(i32, 0), d.days);
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 1970), c.year);
    try std.testing.expectEqual(@as(u8, 1), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);

    const t = ts.timeOfDay();
    try std.testing.expectEqual(@as(i64, 0), t.value);
}

test "Timestamp: pre-epoch negative value gives 1969-12-31 with correct time" {
    // -1ns is still on 1969-12-31 (floor division)
    const ts = Timestamp{ .value = -1, .unit = .nanos, .utc = false };
    const d = ts.toDate();
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 1969), c.year);
    try std.testing.expectEqual(@as(u8, 12), c.month);
    try std.testing.expectEqual(@as(u8, 31), c.day);

    // time-of-day = -1 mod ns_per_day = ns_per_day - 1 = 86399999999999
    const t = ts.timeOfDay();
    try std.testing.expectEqual(@as(i64, 86_399_999_999_999), t.value);
}

test "Timestamp: pre-epoch half-day (1969-12-31 12:00:00)" {
    // -12h in nanos
    const half_day: i64 = -43_200_000_000_000;
    const ts = Timestamp{ .value = half_day, .unit = .nanos, .utc = false };
    const d = ts.toDate();
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 1969), c.year);
    try std.testing.expectEqual(@as(u8, 12), c.month);
    try std.testing.expectEqual(@as(u8, 31), c.day);

    const t = ts.timeOfDay();
    // @mod(-43_200_000_000_000, 86_400_000_000_000) = 43_200_000_000_000
    try std.testing.expectEqual(@as(i64, 43_200_000_000_000), t.value);
}

test "Timestamp: fromDateAndTime/toDate/timeOfDay identity" {
    const date = Date.fromCivil(.{ .year = 2020, .month = 6, .day = 15 });
    const time = Time{ .value = 3_723_000_000_000, .unit = .nanos, .utc = false }; // 01:02:03
    const ts = try Timestamp.fromDateAndTime(date, time, false);

    try std.testing.expectEqual(TimeUnit.nanos, ts.unit);
    try std.testing.expectEqual(Timestamp.Origin.int64, ts.origin);

    const d2 = ts.toDate();
    try std.testing.expectEqual(date.days, d2.days);

    const t2 = ts.timeOfDay();
    try std.testing.expectEqual(time.value, t2.value);
}

test "Timestamp: eql across units" {
    const a = Timestamp{ .value = 1_000_000_000, .unit = .nanos, .utc = false };
    const b = Timestamp{ .value = 1_000_000, .unit = .micros, .utc = false };
    const c = Timestamp{ .value = 1_000, .unit = .millis, .utc = false };
    try std.testing.expect(a.eql(&b));
    try std.testing.expect(b.eql(&c));
    try std.testing.expect(a.eql(&c));
}

test "Timestamp: order across units" {
    const a = Timestamp{ .value = 1_500, .unit = .millis, .utc = false }; // 1.5s
    const b = Timestamp{ .value = 1_000_000, .unit = .micros, .utc = false }; // 1s
    try std.testing.expectEqual(std.math.Order.gt, a.order(&b));
    try std.testing.expectEqual(std.math.Order.lt, b.order(&a));
}

test "Timestamp: format epoch" {
    const ts = Timestamp{ .value = 0, .unit = .nanos, .utc = false };
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{ts});
    try std.testing.expectEqualStrings("1970-01-01T00:00:00", out);
}

test "Timestamp: format utc suffix" {
    const ts = Timestamp{ .value = 0, .unit = .nanos, .utc = true };
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{ts});
    try std.testing.expectEqualStrings("1970-01-01T00:00:00Z", out);
}

test "Timestamp: format micros fraction" {
    // 1970-01-01T00:00:01.000001 UTC = 1_000_001 micros
    // timeOfDay() returns unit=nanos, so the fraction renders in nanos (9 digits).
    // 1 us = 1_000 ns → fraction = 000001000 (9 digits)
    const ts = Timestamp{ .value = 1_000_001, .unit = .micros, .utc = true };
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{ts});
    try std.testing.expectEqualStrings("1970-01-01T00:00:01.000001000Z", out);
}

test "Timestamp: INT96 codec — julian=2440588 (epoch), nanos_of_day=0" {
    // Encode epoch manually: nanos_of_day = 0, julian = 2440588
    var bytes: [12]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], 0, .little);
    std.mem.writeInt(u32, bytes[8..12], 2_440_588, .little);

    const ts = try Timestamp.fromInt96Bytes(&bytes);
    try std.testing.expectEqual(@as(i64, 0), ts.value);
    try std.testing.expectEqual(TimeUnit.nanos, ts.unit);
    try std.testing.expect(!ts.utc);
    try std.testing.expectEqual(Timestamp.Origin.int96, ts.origin);

    const out = try ts.toInt96Bytes();
    try std.testing.expectEqualSlices(u8, &bytes, &out);
}

test "Timestamp: INT96 codec — julian=2458849 (2020-01-01)" {
    // 2020-01-01: days from epoch = 18262, julian = 2440588 + 18262 = 2458850
    // Actually: 2020-01-01 is day 18262 from epoch. Julian = 2440588 + 18262 = 2458850
    const julian: u32 = 2_440_588 + 18_262; // = 2458850
    var bytes: [12]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], 0, .little);
    std.mem.writeInt(u32, bytes[8..12], julian, .little);

    const ts = try Timestamp.fromInt96Bytes(&bytes);
    const d = ts.toDate();
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c.year);
    try std.testing.expectEqual(@as(u8, 1), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);

    // Round-trip
    const out = try ts.toInt96Bytes();
    try std.testing.expectEqualSlices(u8, &bytes, &out);
}

test "Timestamp: INT96 codec — pre-epoch julian (julian < 2440588)" {
    // 1969-12-31: days = -1, julian = 2440588 - 1 = 2440587
    const julian: u32 = 2_440_587;
    const nanos_of_day: u64 = 43_200_000_000_000; // 12:00:00
    var bytes: [12]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], nanos_of_day, .little);
    std.mem.writeInt(u32, bytes[8..12], julian, .little);

    const ts = try Timestamp.fromInt96Bytes(&bytes);
    const d = ts.toDate();
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 1969), c.year);
    try std.testing.expectEqual(@as(u8, 12), c.month);
    try std.testing.expectEqual(@as(u8, 31), c.day);

    // Round-trip
    const out = try ts.toInt96Bytes();
    try std.testing.expectEqualSlices(u8, &bytes, &out);
}

test "Timestamp: INT96 codec — decode∘encode identity on varied vectors" {
    // A set of hand-crafted (julian, nanos_of_day) pairs
    const vectors = [_]struct { julian: u32, nod: u64 }{
        .{ .julian = 2_440_588, .nod = 0 },
        .{ .julian = 2_440_588, .nod = 86_399_999_999_999 }, // just before midnight
        .{ .julian = 2_458_849, .nod = 3_661_000_000_000 }, // some timestamp in 2019
        .{ .julian = 2_440_587, .nod = 0 }, // 1969-12-31T00:00:00
        .{ .julian = 2_415_021, .nod = 43_200_000_000_000 }, // ~1900-01-01 noon
    };
    for (vectors) |v| {
        var bytes: [12]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], v.nod, .little);
        std.mem.writeInt(u32, bytes[8..12], v.julian, .little);

        const ts = try Timestamp.fromInt96Bytes(&bytes);
        const out = try ts.toInt96Bytes();
        try std.testing.expectEqualSlices(u8, &bytes, &out);
    }
}

test "Timestamp: addDuration same unit" {
    const ts = Timestamp{ .value = 0, .unit = .millis, .utc = false };
    const ts2 = try ts.addDuration(86_400_000, .millis);
    try std.testing.expectEqual(@as(i64, 86_400_000), ts2.value);
}

test "Timestamp: addDuration LossyDuration" {
    const ts = Timestamp{ .value = 0, .unit = .millis, .utc = false };
    try std.testing.expectError(error.LossyDuration, ts.addDuration(1, .micros));
}

// ---------------------------------------------------------------------------
// Fixture-based codec test — reads data/int96.parquet
// ---------------------------------------------------------------------------

test "Timestamp: INT96 fixture codec — decode∘encode roundtrip" {
    const parquet = @import("parquet");
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();

    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();

    // The first column contains INT96 values (12-byte arrays)
    const col = &result.columns[0];
    const arrays = col.byte_arrays orelse return error.NoByteArrays;

    for (arrays) |raw_bytes| {
        try std.testing.expectEqual(@as(usize, 12), raw_bytes.len);
        const bytes_ptr: *const [12]u8 = raw_bytes[0..12];
        const ts = try Timestamp.fromInt96Bytes(bytes_ptr);
        const out = try ts.toInt96Bytes();
        try std.testing.expectEqualSlices(u8, raw_bytes, &out);
    }
}

// ---------------------------------------------------------------------------
// Capability smoke tests: Series(Timestamp) argSort + asStringAt
// ---------------------------------------------------------------------------

test "Timestamp: Series(Timestamp) argSort across mixed units" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Timestamp).init(allocator);
    defer s.deinit();
    try s.rename("ts");

    // Append in reverse order: 3s, 1s, 2s (mixed units)
    try s.append(.{ .value = 3_000, .unit = .millis, .utc = false });
    try s.append(.{ .value = 1_000_000_000, .unit = .nanos, .utc = false });
    try s.append(.{ .value = 2_000, .unit = .millis, .utc = false });

    var indices = try s.argSort(allocator, true);
    defer indices.deinit(allocator);

    // 1s < 2s < 3s → indices [1, 2, 0]
    try std.testing.expectEqual(@as(usize, 1), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
    try std.testing.expectEqual(@as(usize, 0), indices.items[2]);
}

test "Timestamp: Series(Timestamp) asStringAt via format" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Timestamp).init(allocator);
    defer s.deinit();
    try s.rename("ts");
    try s.append(.{ .value = 0, .unit = .nanos, .utc = true });

    var str = try s.asStringAt(0);
    defer str.deinit();
    try std.testing.expectEqualStrings("1970-01-01T00:00:00Z", str.toSlice());
}
