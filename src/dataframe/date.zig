//! Date column type (Phase 6d-2a.1): days since 1970-01-01 (Unix epoch),
//! matching parquet DATE (INT32 + DATE annotation) semantics exactly.
//! Civil-calendar conversions use Howard Hinnant's algorithms
//! (https://howardhinnant.github.io/date_algorithms.html), valid for the
//! entire i32 day range (proleptic Gregorian).

const std = @import("std");

pub const Date = struct {
    days: i32,

    pub const type_name = "Date";

    pub const Civil = struct { year: i32, month: u8, day: u8 };

    /// Hinnant days_from_civil.
    pub fn fromCivil(c: Civil) Date {
        const y: i64 = @as(i64, c.year) - @as(i64, @intFromBool(c.month <= 2));
        const m: i64 = c.month;
        const d: i64 = c.day;
        const era: i64 = @divTrunc(if (y >= 0) y else y - 399, 400);
        const yoe: i64 = y - era * 400; // [0, 399]
        const doy: i64 = @divTrunc(153 * (m + (if (m > 2) @as(i64, -3) else 9)) + 2, 5) + d - 1; // [0, 365]
        const doe: i64 = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy; // [0, 146096]
        return .{ .days = @intCast(era * 146097 + doe - 719468) };
    }

    /// Hinnant civil_from_days.
    pub fn toCivil(self: Date) Civil {
        const z: i64 = @as(i64, self.days) + 719468;
        const era: i64 = @divTrunc(if (z >= 0) z else z - 146096, 146097);
        const doe: i64 = z - era * 146097; // [0, 146096]
        const yoe: i64 = @divTrunc(doe - @divTrunc(doe, 1460) + @divTrunc(doe, 36524) - @divTrunc(doe, 146096), 365); // [0, 399]
        const y: i64 = yoe + era * 400;
        const doy: i64 = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100)); // [0, 365]
        const mp: i64 = @divTrunc(5 * doy + 2, 153); // [0, 11]
        const d: i64 = doy - @divTrunc(153 * mp + 2, 5) + 1; // [1, 31]
        const m: i64 = if (mp < 10) mp + 3 else mp - 9; // [1, 12]
        return .{
            .year = @intCast(y + @as(i64, @intFromBool(m <= 2))),
            .month = @intCast(m),
            .day = @intCast(d),
        };
    }

    pub fn year(self: Date) i32 {
        return self.toCivil().year;
    }

    pub fn month(self: Date) u8 {
        return self.toCivil().month;
    }

    pub fn day(self: Date) u8 {
        return self.toCivil().day;
    }

    pub fn addDays(self: Date, n: i32) Date {
        return .{ .days = self.days + n };
    }

    /// Signed distance in days (self - other).
    pub fn diffDays(self: Date, other: Date) i32 {
        return self.days - other.days;
    }

    pub fn eql(self: *const Date, other: *const Date) bool {
        return self.days == other.days;
    }

    /// Total ordering — drives argSort and filter comparisons.
    pub fn order(self: *const Date, other: *const Date) std.math.Order {
        return std.math.order(self.days, other.days);
    }

    /// ISO 8601 YYYY-MM-DD.
    pub fn format(self: Date, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        const c = self.toCivil();
        // Year: handle negative (proleptic Gregorian) with explicit sign only
        // for BC dates. The magnitude must be unsigned before "{d:0>4}" —
        // a signed value makes the formatter pad around the sign character.
        if (c.year < 0) {
            try writer.print("-{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(-c.year)), c.month, c.day });
        } else {
            try writer.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u32, @intCast(c.year)), c.month, c.day });
        }
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Date: epoch is 1970-01-01" {
    const epoch = Date{ .days = 0 };
    const c = epoch.toCivil();
    try std.testing.expectEqual(@as(i32, 1970), c.year);
    try std.testing.expectEqual(@as(u8, 1), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);

    const from = Date.fromCivil(.{ .year = 1970, .month = 1, .day = 1 });
    try std.testing.expectEqual(@as(i32, 0), from.days);
}

test "Date: 2020-01-01 == days 18262" {
    const d = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });
    try std.testing.expectEqual(@as(i32, 18262), d.days);

    const c = (Date{ .days = 18262 }).toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c.year);
    try std.testing.expectEqual(@as(u8, 1), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);
}

test "Date: leap day 2020-02-29" {
    const leap = Date.fromCivil(.{ .year = 2020, .month = 2, .day = 29 });
    // Round-trip
    const c = leap.toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c.year);
    try std.testing.expectEqual(@as(u8, 2), c.month);
    try std.testing.expectEqual(@as(u8, 29), c.day);

    // +1 day should be 2020-03-01
    const next = leap.addDays(1);
    const nc = next.toCivil();
    try std.testing.expectEqual(@as(i32, 2020), nc.year);
    try std.testing.expectEqual(@as(u8, 3), nc.month);
    try std.testing.expectEqual(@as(u8, 1), nc.day);
}

test "Date: pre-epoch 1969-12-31 == days -1" {
    const d = Date.fromCivil(.{ .year = 1969, .month = 12, .day = 31 });
    try std.testing.expectEqual(@as(i32, -1), d.days);

    const c = (Date{ .days = -1 }).toCivil();
    try std.testing.expectEqual(@as(i32, 1969), c.year);
    try std.testing.expectEqual(@as(u8, 12), c.month);
    try std.testing.expectEqual(@as(u8, 31), c.day);
}

test "Date: pre-epoch 1900-03-01" {
    const d = Date.fromCivil(.{ .year = 1900, .month = 3, .day = 1 });
    // 1900-03-01 is 70 years + a few days before epoch; verify round-trip
    const c = d.toCivil();
    try std.testing.expectEqual(@as(i32, 1900), c.year);
    try std.testing.expectEqual(@as(u8, 3), c.month);
    try std.testing.expectEqual(@as(u8, 1), c.day);
}

test "Date: fromCivil/toCivil round-trip over sample days" {
    // A spread of ±50000 days relative to epoch, covering many centuries.
    const samples = [_]i32{
        -50000, -25000, -10000, -5000, -1000, -365, -1,
        0,      1,      365,    1000,  5000,  10000, 18262,
        18628,  25000,  36524,  50000,
    };
    for (samples) |days| {
        const d = Date{ .days = days };
        const c = d.toCivil();
        const back = Date.fromCivil(c);
        try std.testing.expectEqual(days, back.days);
    }
}

test "Date: year/month/day accessors" {
    const d = Date.fromCivil(.{ .year = 2021, .month = 6, .day = 15 });
    try std.testing.expectEqual(@as(i32, 2021), d.year());
    try std.testing.expectEqual(@as(u8, 6), d.month());
    try std.testing.expectEqual(@as(u8, 15), d.day());
}

test "Date: addDays and diffDays" {
    const a = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });
    const b = a.addDays(366); // 2020 is a leap year, so +366 = 2021-01-01
    const bc = b.toCivil();
    try std.testing.expectEqual(@as(i32, 2021), bc.year);
    try std.testing.expectEqual(@as(u8, 1), bc.month);
    try std.testing.expectEqual(@as(u8, 1), bc.day);

    try std.testing.expectEqual(@as(i32, 366), b.diffDays(a));
    try std.testing.expectEqual(@as(i32, -366), a.diffDays(b));
}

test "Date: eql and order" {
    const a = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });
    const b = Date.fromCivil(.{ .year = 2020, .month = 6, .day = 15 });
    const a2 = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });

    try std.testing.expect(a.eql(&a2));
    try std.testing.expect(!a.eql(&b));

    try std.testing.expectEqual(std.math.Order.lt, a.order(&b));
    try std.testing.expectEqual(std.math.Order.gt, b.order(&a));
    try std.testing.expectEqual(std.math.Order.eq, a.order(&a2));
}

test "Date: format ISO 8601" {
    const d = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });
    var buf: [16]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{d});
    try std.testing.expectEqualStrings("2020-01-01", out);
}

test "Date: format pads small and negative years" {
    var buf: [16]u8 = undefined;

    const y1 = Date.fromCivil(.{ .year = 1, .month = 2, .day = 3 });
    try std.testing.expectEqualStrings("0001-02-03", try std.fmt.bufPrint(&buf, "{f}", .{y1}));

    const y999 = Date.fromCivil(.{ .year = 999, .month = 12, .day = 31 });
    try std.testing.expectEqualStrings("0999-12-31", try std.fmt.bufPrint(&buf, "{f}", .{y999}));

    const bc1 = Date.fromCivil(.{ .year = -1, .month = 1, .day = 1 });
    try std.testing.expectEqualStrings("-0001-01-01", try std.fmt.bufPrint(&buf, "{f}", .{bc1}));

    const bc44 = Date.fromCivil(.{ .year = -44, .month = 3, .day = 15 });
    try std.testing.expectEqualStrings("-0044-03-15", try std.fmt.bufPrint(&buf, "{f}", .{bc44}));
}
