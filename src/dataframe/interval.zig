//! Interval column type (Phase 6d-2a.5): Parquet INTERVAL annotation.
//! Wire format: 12 bytes = three little-endian u32 in order: months, days, millis.
//! This matches the Parquet specification for FIXED_LEN_BYTE_ARRAY(12) with the
//! INTERVAL converted_type.
//!
//! Note on ordering: calendar months and days are incommensurable in the general
//! case (a month can be 28–31 days). For storage/sort ordering purposes, `order`
//! normalises to a nominal millisecond duration using the average Gregorian month
//! (2 629 746 000 ms ≈ 365.2425 days × 24h × 3600s × 1000ms / 12) and the exact
//! day (86 400 000 ms). This gives a TOTAL but SEMANTICALLY-ARBITRARY ordering —
//! correct for sort stability, unsuitable for calendar-correct comparisons.
//! Documented deviation from the plan's "no order" intent: giving Interval an
//! `order` function is required because argSort's `else` arm uses `a < b` which
//! is not valid for a struct type. The plan acknowledged this risk and the
//! decision is to provide the nominal ordering rather than a comptime guard.

const std = @import("std");
const Date = @import("date.zig").Date;
const Timestamp = @import("timestamp.zig").Timestamp;

/// Average Gregorian month in milliseconds: 365.2425 * 24 * 3600 * 1000 / 12.
const avg_month_ms: u64 = 2_629_746_000;
/// Exact day in milliseconds.
const day_ms: u64 = 86_400_000;

pub const Interval = struct {
    months: u32,
    days: u32,
    millis: u32,

    pub const type_name = "Interval";

    // -----------------------------------------------------------------------
    // Wire codec
    // -----------------------------------------------------------------------

    /// Decode from the 12-byte Parquet INTERVAL little-endian representation.
    pub fn fromLeBytes(b: *const [12]u8) Interval {
        return .{
            .months = std.mem.readInt(u32, b[0..4], .little),
            .days   = std.mem.readInt(u32, b[4..8], .little),
            .millis = std.mem.readInt(u32, b[8..12], .little),
        };
    }

    /// Encode to the 12-byte Parquet INTERVAL little-endian representation.
    pub fn toLeBytes(self: Interval) [12]u8 {
        var out: [12]u8 = undefined;
        std.mem.writeInt(u32, out[0..4], self.months, .little);
        std.mem.writeInt(u32, out[4..8], self.days,   .little);
        std.mem.writeInt(u32, out[8..12], self.millis, .little);
        return out;
    }

    // -----------------------------------------------------------------------
    // Comparison
    // -----------------------------------------------------------------------

    /// Component-wise equality.
    pub fn eql(self: *const Interval, other: *const Interval) bool {
        return self.months == other.months and
               self.days   == other.days   and
               self.millis == other.millis;
    }

    /// Storage/sort ordering — SEMANTICALLY ARBITRARY, not calendar-correct.
    /// Normalises to a nominal millisecond count using the average Gregorian
    /// month (2 629 746 000 ms) and exact day (86 400 000 ms), then compares
    /// as u128. Values that differ only in the months vs. days split may
    /// compare in a surprising order relative to calendar truth.
    pub fn order(self: *const Interval, other: *const Interval) std.math.Order {
        const a: u128 = @as(u128, self.months)  * avg_month_ms +
                        @as(u128, self.days)    * day_ms       +
                        @as(u128, self.millis);
        const b: u128 = @as(u128, other.months) * avg_month_ms +
                        @as(u128, other.days)   * day_ms       +
                        @as(u128, other.millis);
        return std.math.order(a, b);
    }

    // -----------------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------------

    /// Readable form: "{months}mo {days}d {millis}ms" e.g. "1mo 2d 3000ms".
    pub fn format(self: Interval, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print("{d}mo {d}d {d}ms", .{ self.months, self.days, self.millis });
    }

    // -----------------------------------------------------------------------
    // Calendar arithmetic
    // -----------------------------------------------------------------------

    /// Add this interval to a Date. Month addition uses end-of-month clamping
    /// (e.g. Jan 31 + 1 month → Feb 28 or Feb 29 in a leap year). The `millis`
    /// field is IGNORED for Date arithmetic — an Interval's sub-day component
    /// cannot be represented in a date-only value.
    pub fn addToDate(self: Interval, d: Date) Date {
        const c = d.toCivil();
        // Convert absolute month count, add, then split back.
        const total_months: i64 = @as(i64, c.year) * 12 + (@as(i64, c.month) - 1) + @as(i64, self.months);
        const new_year: i32  = @intCast(@divFloor(total_months, 12));
        const new_month: u8  = @intCast(@mod(total_months, 12) + 1);
        const max_day = daysInMonth(new_year, new_month);
        const new_day: u8 = if (c.day > max_day) max_day else c.day;
        const date_after_months = Date.fromCivil(.{ .year = new_year, .month = new_month, .day = new_day });
        return date_after_months.addDays(@intCast(self.days));
    }

    /// Add this interval to a Timestamp. The result unit is always nanoseconds
    /// (fromDateAndTime always returns nanos-unit). Month and day components are
    /// applied via civil-calendar arithmetic (with end-of-month clamping),
    /// millis are added precisely via addDuration.
    pub fn addToTimestamp(self: Interval, ts: Timestamp) !Timestamp {
        // Apply months + days via civil date math on the date part.
        const date_part = self.addToDate(ts.toDate());
        // Reassemble: date_part + original time-of-day → nanos-unit Timestamp.
        const rebuilt = try Timestamp.fromDateAndTime(date_part, ts.timeOfDay(), ts.utc);
        // Add millis on top of the nanos-unit Timestamp.
        return rebuilt.addDuration(@intCast(self.millis), .millis);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Number of days in a given month of a given year (proleptic Gregorian).
    fn daysInMonth(year: i32, month: u8) u8 {
        return switch (month) {
            1, 3, 5, 7, 8, 10, 12 => 31,
            4, 6, 9, 11            => 30,
            2 => if (isLeap(year)) 29 else 28,
            else => unreachable,
        };
    }

    /// Proleptic Gregorian leap year: divisible by 4, except centuries unless
    /// divisible by 400.
    fn isLeap(year: i32) bool {
        return @rem(year, 4) == 0 and (@rem(year, 100) != 0 or @rem(year, 400) == 0);
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Interval: codec round-trip" {
    const iv = Interval{ .months = 1, .days = 2, .millis = 3000 };
    const bytes = iv.toLeBytes();
    // Hand vector: months=1 → 0x01 00 00 00, days=2 → 0x02 00 00 00,
    // millis=3000=0xBB8 → 0xB8 0x0B 0x00 0x00 (LE)
    try std.testing.expectEqual(@as(u8, 0x01), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[1]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[2]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[3]);
    try std.testing.expectEqual(@as(u8, 0x02), bytes[4]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[5]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[6]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[7]);
    try std.testing.expectEqual(@as(u8, 0xB8), bytes[8]);
    try std.testing.expectEqual(@as(u8, 0x0B), bytes[9]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[10]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[11]);

    const back = Interval.fromLeBytes(&bytes);
    try std.testing.expectEqual(@as(u32, 1), back.months);
    try std.testing.expectEqual(@as(u32, 2), back.days);
    try std.testing.expectEqual(@as(u32, 3000), back.millis);
}

test "Interval: addToDate 2020-01-31 + 1 month -> 2020-02-29 (leap year)" {
    const iv = Interval{ .months = 1, .days = 0, .millis = 0 };
    const d = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 31 });
    const result = iv.addToDate(d);
    const c = result.toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c.year);
    try std.testing.expectEqual(@as(u8, 2), c.month);
    try std.testing.expectEqual(@as(u8, 29), c.day);
}

test "Interval: addToDate 2021-01-31 + 1 month -> 2021-02-28 (non-leap year)" {
    const iv = Interval{ .months = 1, .days = 0, .millis = 0 };
    const d = Date.fromCivil(.{ .year = 2021, .month = 1, .day = 31 });
    const result = iv.addToDate(d);
    const c = result.toCivil();
    try std.testing.expectEqual(@as(i32, 2021), c.year);
    try std.testing.expectEqual(@as(u8, 2), c.month);
    try std.testing.expectEqual(@as(u8, 28), c.day);
}

test "Interval: addToDate +13 months crosses year boundary" {
    const iv = Interval{ .months = 13, .days = 0, .millis = 0 };
    const d = Date.fromCivil(.{ .year = 2020, .month = 3, .day = 1 });
    const result = iv.addToDate(d);
    const c = result.toCivil();
    try std.testing.expectEqual(@as(i32, 2021), c.year);
    try std.testing.expectEqual(@as(u8, 4), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);
}

test "Interval: addToDate months + days composition" {
    // 2020-01-31 + 1mo + 5d = 2020-02-29 + 5 = 2020-03-05
    const iv = Interval{ .months = 1, .days = 5, .millis = 0 };
    const d = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 31 });
    const result = iv.addToDate(d);
    const c = result.toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c.year);
    try std.testing.expectEqual(@as(u8, 3), c.month);
    try std.testing.expectEqual(@as(u8, 5), c.day);
}

test "Interval: addToTimestamp adds millis" {
    const parquet = @import("parquet");
    // 2020-01-01 00:00:00 UTC in millis since epoch
    const epoch_ms: i64 = 18262 * 86_400_000; // 2020-01-01 in ms
    const ts = Timestamp{ .value = epoch_ms, .unit = .millis, .utc = true };
    const iv = Interval{ .months = 0, .days = 0, .millis = 5000 };
    const result = try iv.addToTimestamp(ts);
    // Result is nanos-unit (fromDateAndTime always returns nanos).
    // 2020-01-01 00:00:05.000 UTC = epoch_ms * 1_000_000 ns + 5_000_000_000 ns
    const expected_nanos: i64 = @as(i64, epoch_ms) * 1_000_000 + 5_000_000_000;
    try std.testing.expectEqual(expected_nanos, result.value);
    _ = parquet; // suppress unused if import needed for TimeUnit only
}

test "Interval: eql" {
    const a = Interval{ .months = 1, .days = 2, .millis = 3000 };
    const b = Interval{ .months = 1, .days = 2, .millis = 3000 };
    const c = Interval{ .months = 1, .days = 2, .millis = 4000 };
    try std.testing.expect(a.eql(&b));
    try std.testing.expect(!a.eql(&c));
}

test "Interval: format" {
    const iv = Interval{ .months = 1, .days = 2, .millis = 3000 };
    var buf: [64]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{iv});
    try std.testing.expectEqualStrings("1mo 2d 3000ms", out);
}

test "Interval: order (nominal storage ordering)" {
    // A larger nominal duration should be greater.
    const small = Interval{ .months = 0, .days = 0, .millis = 1000 };
    const large = Interval{ .months = 1, .days = 0, .millis = 0 }; // 1 avg month >> 1 second
    try std.testing.expectEqual(std.math.Order.lt, small.order(&large));
    try std.testing.expectEqual(std.math.Order.gt, large.order(&small));
    const same = Interval{ .months = 0, .days = 0, .millis = 1000 };
    try std.testing.expectEqual(std.math.Order.eq, small.order(&same));
}

test "Interval: Series(Interval) asStringAt" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Interval).init(allocator);
    defer s.deinit();
    try s.rename("dur");
    try s.append(.{ .months = 1, .days = 2, .millis = 3000 });

    var str = try s.asStringAt(0);
    defer str.deinit();
    try std.testing.expectEqualStrings("1mo 2d 3000ms", str.toSlice());
}
