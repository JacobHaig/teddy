//! Time-of-day column type (Phase 6d-2a.2): parquet TIME annotation.
//! value is in `unit` ticks since midnight; `utc` mirrors isAdjustedToUTC
//! (kept for lossless round-trip).
//!
//! NOTE: hour/minute/second/format assume non-negative time-of-day values
//! (total nanos >= 0), which is the parquet TIME contract — TIME stores
//! elapsed ticks since midnight, never a negative offset.

const std = @import("std");
const TimeUnit = @import("parquet").TimeUnit;

pub const Time = struct {
    value: i64,
    unit: TimeUnit,
    utc: bool,

    pub const type_name = "Time";

    pub fn nanosPerUnit(unit: TimeUnit) i128 {
        return switch (unit) {
            .millis => 1_000_000,
            .micros => 1_000,
            .nanos => 1,
        };
    }

    /// Unit-normalized total nanoseconds (i128 — cannot overflow).
    pub fn toNanos(self: Time) i128 {
        return @as(i128, self.value) * nanosPerUnit(self.unit);
    }

    /// Add a duration given in any unit. Result stays in self's unit;
    /// sub-unit remainders error (strict — no silent truncation).
    pub fn addDuration(self: Time, amount: i64, unit: TimeUnit) !Time {
        const total: i128 = self.toNanos() + @as(i128, amount) * nanosPerUnit(unit);
        const per = nanosPerUnit(self.unit);
        if (@rem(total, per) != 0) return error.LossyDuration;
        const new_value = std.math.cast(i64, @divTrunc(total, per)) orelse return error.Overflow;
        return .{ .value = new_value, .unit = self.unit, .utc = self.utc };
    }

    pub fn hour(self: Time) u8 {
        return @intCast(@divTrunc(self.toNanos(), 3_600_000_000_000));
    }

    pub fn minute(self: Time) u8 {
        return @intCast(@rem(@divTrunc(self.toNanos(), 60_000_000_000), 60));
    }

    pub fn second(self: Time) u8 {
        return @intCast(@rem(@divTrunc(self.toNanos(), 1_000_000_000), 60));
    }

    /// Semantic equality across units (1s == 1000ms).
    pub fn eql(self: *const Time, other: *const Time) bool {
        return self.toNanos() == other.toNanos();
    }

    /// Unit-normalized total ordering.
    pub fn order(self: *const Time, other: *const Time) std.math.Order {
        return std.math.order(self.toNanos(), other.toNanos());
    }

    /// HH:MM:SS, plus .mmm / .uuuuuu / .nnnnnnnnn when sub-second ticks exist.
    pub fn format(self: Time, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        const ns = self.toNanos();
        const h: u32 = @intCast(@divTrunc(ns, 3_600_000_000_000));
        const m: u32 = @intCast(@rem(@divTrunc(ns, 60_000_000_000), 60));
        const s: u32 = @intCast(@rem(@divTrunc(ns, 1_000_000_000), 60));
        try writer.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ h, m, s });
        const frac_ns: u32 = @intCast(@rem(ns, 1_000_000_000));
        if (frac_ns != 0) {
            switch (self.unit) {
                .millis => try writer.print(".{d:0>3}", .{frac_ns / 1_000_000}),
                .micros => try writer.print(".{d:0>6}", .{frac_ns / 1_000}),
                .nanos => try writer.print(".{d:0>9}", .{frac_ns}),
            }
        }
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Time: unit normalization — 1s expressed in each unit" {
    const ms = Time{ .value = 1_000, .unit = .millis, .utc = false };
    const us = Time{ .value = 1_000_000, .unit = .micros, .utc = false };
    const ns = Time{ .value = 1_000_000_000, .unit = .nanos, .utc = false };

    // All three are equal via eql (semantic, unit-normalized)
    try std.testing.expect(ms.eql(&us));
    try std.testing.expect(us.eql(&ns));
    try std.testing.expect(ms.eql(&ns));

    // order: equal
    try std.testing.expectEqual(std.math.Order.eq, ms.order(&us));
    try std.testing.expectEqual(std.math.Order.eq, us.order(&ns));
}

test "Time: mixed-unit ordering" {
    // 1.5s in millis vs 1s in micros
    const a = Time{ .value = 1_500, .unit = .millis, .utc = false };
    const b = Time{ .value = 1_000_000, .unit = .micros, .utc = false };
    try std.testing.expectEqual(std.math.Order.gt, a.order(&b));
    try std.testing.expectEqual(std.math.Order.lt, b.order(&a));
}

test "Time: addDuration — same unit" {
    const t = Time{ .value = 3_600_000, .unit = .millis, .utc = false }; // 1h
    const t2 = try t.addDuration(60_000, .millis); // +1m
    try std.testing.expectEqual(@as(i64, 3_660_000), t2.value);
    try std.testing.expectEqual(TimeUnit.millis, t2.unit);
}

test "Time: addDuration — finer unit exact" {
    // millis time + microseconds that exactly land on a millis boundary
    const t = Time{ .value = 1_000, .unit = .millis, .utc = false }; // 1s
    const t2 = try t.addDuration(500_000, .micros); // +0.5s = 1.5s
    try std.testing.expectEqual(@as(i64, 1_500), t2.value);
    try std.testing.expectEqual(TimeUnit.millis, t2.unit);
}

test "Time: addDuration — LossyDuration when adding 1us to millis time" {
    // 1us = 1000ns; millis per unit = 1_000_000ns; 1000 % 1_000_000 != 0
    const t = Time{ .value = 0, .unit = .millis, .utc = false };
    const result = t.addDuration(1, .micros);
    try std.testing.expectError(error.LossyDuration, result);
}

test "Time: addDuration — Overflow" {
    // max i64 millis + large amount
    const t = Time{ .value = std.math.maxInt(i64), .unit = .millis, .utc = false };
    const result = t.addDuration(1, .millis);
    try std.testing.expectError(error.Overflow, result);
}

test "Time: hour/minute/second for 01:02:03.5 in each unit" {
    // 01:02:03.500 = 1*3600 + 2*60 + 3 = 3723 seconds + 0.5 = 3723500ms
    const ms = Time{ .value = 3_723_500, .unit = .millis, .utc = false };
    try std.testing.expectEqual(@as(u8, 1), ms.hour());
    try std.testing.expectEqual(@as(u8, 2), ms.minute());
    try std.testing.expectEqual(@as(u8, 3), ms.second());

    const us = Time{ .value = 3_723_500_000, .unit = .micros, .utc = false };
    try std.testing.expectEqual(@as(u8, 1), us.hour());
    try std.testing.expectEqual(@as(u8, 2), us.minute());
    try std.testing.expectEqual(@as(u8, 3), us.second());

    const ns = Time{ .value = 3_723_500_000_000, .unit = .nanos, .utc = false };
    try std.testing.expectEqual(@as(u8, 1), ns.hour());
    try std.testing.expectEqual(@as(u8, 2), ns.minute());
    try std.testing.expectEqual(@as(u8, 3), ns.second());
}

test "Time: format — no fraction" {
    const t = Time{ .value = 3_723_000, .unit = .millis, .utc = false }; // 01:02:03 exact
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{t});
    try std.testing.expectEqualStrings("01:02:03", out);
}

test "Time: format — millis fraction" {
    const t = Time{ .value = 3_723_500, .unit = .millis, .utc = false }; // 01:02:03.500
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{t});
    try std.testing.expectEqualStrings("01:02:03.500", out);
}

test "Time: format — micros fraction" {
    // 13:59:59.000001 = (13*3600 + 59*60 + 59)*1_000_000 + 1 us
    const secs: i64 = 13 * 3600 + 59 * 60 + 59;
    const t = Time{ .value = secs * 1_000_000 + 1, .unit = .micros, .utc = false };
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{t});
    try std.testing.expectEqualStrings("13:59:59.000001", out);
}

test "Time: format — nanos fraction" {
    // 01:02:03.000000001
    const secs: i64 = 1 * 3600 + 2 * 60 + 3;
    const t = Time{ .value = secs * 1_000_000_000 + 1, .unit = .nanos, .utc = false };
    var buf: [32]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{t});
    try std.testing.expectEqualStrings("01:02:03.000000001", out);
}
