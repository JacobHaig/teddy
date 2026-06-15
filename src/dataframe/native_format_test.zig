//! Round-trip + malformed-input tests for the native TDF format (Phase 8.0).

const std = @import("std");
const testing = std.testing;

const native = @import("native_format.zig");
const Dataframe = @import("dataframe.zig").Dataframe;
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;
const Raw = @import("raw.zig").Raw;
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Binary = @import("binary.zig").Binary;
const FixedBytes = @import("fixed_bytes.zig").FixedBytes;
const Uuid = @import("uuid.zig").Uuid;
const Interval = @import("interval.zig").Interval;
const Nested = @import("nested.zig").Nested;
const parquet = @import("parquet");
const adapter = @import("parquet.zig");

const Writer = @import("writer.zig").Writer;
const Reader = @import("reader.zig").Reader;

/// Serialize then parse; caller owns the returned dataframe (deinit).
fn roundTrip(allocator: std.mem.Allocator, df: *Dataframe) !*Dataframe {
    const bytes = try native.writeToString(allocator, df);
    defer allocator.free(bytes);
    return native.parse(allocator, bytes);
}

// ---------------------------------------------------------------------------
// Primitive scalar columns
// ---------------------------------------------------------------------------

test "tdf: integer columns round-trip with a null each" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    inline for (.{ i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize }) |T| {
        var col = try df.createSeries(T);
        try col.rename(@typeName(T));
        try col.append(1);
        try col.appendNull();
        try col.append(42);
    }

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    try testing.expectEqual(@as(usize, 12), out.width());
    try testing.expectEqual(@as(usize, 3), out.height());

    inline for (.{ i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize }) |T| {
        const boxed = out.getSeries(@typeName(T)).?;
        const s: *Series(T) = switch (boxed.*) {
            inline else => |p| if (@TypeOf(p) == *Series(T)) p else unreachable,
        };
        try testing.expectEqual(@as(T, 1), s.values.items[0]);
        try testing.expect(s.isNull(1));
        try testing.expect(!s.isNull(0));
        try testing.expectEqual(@as(T, 42), s.values.items[2]);
    }
}

test "tdf: bool and float columns round-trip with nulls" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var b = try df.createSeries(bool);
    try b.rename("b");
    try b.append(true);
    try b.appendNull();
    try b.append(false);

    var f32c = try df.createSeries(f32);
    try f32c.rename("f32");
    try f32c.append(1.5);
    try f32c.appendNull();
    try f32c.append(-2.25);

    var f64c = try df.createSeries(f64);
    try f64c.rename("f64");
    try f64c.append(3.14159);
    try f64c.appendNull();
    try f64c.append(-0.5);

    var f16c = try df.createSeries(f16);
    try f16c.rename("f16");
    try f16c.append(1.0);
    try f16c.appendNull();
    try f16c.append(0.5);

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const ob = out.getSeries("b").?.bool;
    try testing.expectEqual(true, ob.values.items[0]);
    try testing.expect(ob.isNull(1));
    try testing.expectEqual(false, ob.values.items[2]);

    const o32 = out.getSeries("f32").?.float32;
    try testing.expectEqual(@as(f32, 1.5), o32.values.items[0]);
    try testing.expect(o32.isNull(1));
    try testing.expectEqual(@as(f32, -2.25), o32.values.items[2]);

    const o64 = out.getSeries("f64").?.float64;
    try testing.expectEqual(@as(f64, 3.14159), o64.values.items[0]);
    try testing.expect(o64.isNull(1));

    const o16 = out.getSeries("f16").?.float16;
    try testing.expectEqual(@as(f16, 1.0), o16.values.items[0]);
    try testing.expect(o16.isNull(1));
    try testing.expectEqual(@as(f16, 0.5), o16.values.items[2]);
}

// ---------------------------------------------------------------------------
// Owning-bytes + logical types
// ---------------------------------------------------------------------------

test "tdf: String column round-trips with null" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(String);
    try s.rename("s");
    try s.append(try String.fromSlice(allocator, "hello"));
    try s.appendNull();
    try s.append(try String.fromSlice(allocator, "world!"));

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const os = out.getSeries("s").?.string;
    try testing.expectEqualStrings("hello", os.values.items[0].toSlice());
    try testing.expect(os.isNull(1));
    try testing.expectEqualStrings("world!", os.values.items[2].toSlice());
    try testing.expectEqualStrings("String", out.getSeries("s").?.typeName());
}

test "tdf: Date / Time / Timestamp round-trip with nulls" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var d = try df.createSeries(Date);
    try d.rename("d");
    try d.append(Date.fromCivil(.{ .year = 2020, .month = 6, .day = 15 }));
    try d.appendNull();

    var t = try df.createSeries(Time);
    try t.rename("t");
    try t.append(.{ .value = 3_723_500, .unit = .millis, .utc = true });
    try t.appendNull();

    var ts = try df.createSeries(Timestamp);
    try ts.rename("ts");
    try ts.append(.{ .value = 1_000_001, .unit = .micros, .utc = true, .origin = .int96 });
    try ts.appendNull();

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const od = out.getSeries("d").?.date;
    const dc = od.values.items[0].toCivil();
    try testing.expectEqual(@as(i32, 2020), dc.year);
    try testing.expectEqual(@as(u8, 6), dc.month);
    try testing.expectEqual(@as(u8, 15), dc.day);
    try testing.expect(od.isNull(1));

    const ot = out.getSeries("t").?.time;
    try testing.expectEqual(@as(i64, 3_723_500), ot.values.items[0].value);
    try testing.expectEqual(parquet.TimeUnit.millis, ot.values.items[0].unit);
    try testing.expectEqual(true, ot.values.items[0].utc);
    try testing.expect(ot.isNull(1));

    const ots = out.getSeries("ts").?.timestamp;
    try testing.expectEqual(@as(i64, 1_000_001), ots.values.items[0].value);
    try testing.expectEqual(parquet.TimeUnit.micros, ots.values.items[0].unit);
    try testing.expectEqual(true, ots.values.items[0].utc);
    try testing.expectEqual(Timestamp.Origin.int96, ots.values.items[0].origin);
    try testing.expect(ots.isNull(1));
}

test "tdf: Decimal round-trips precision + scale + extreme magnitude" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var dec = try df.createSeries(Decimal);
    try dec.rename("dec");
    try dec.append(.{ .unscaled = 1234567890, .precision = 10, .scale = 2 });
    try dec.appendNull();
    const cap: i256 = @intCast(comptime blk: {
        var r: i512 = 1;
        var i: u16 = 0;
        while (i < 76) : (i += 1) r *= 10;
        break :blk r - 1;
    });
    try dec.append(.{ .unscaled = -cap, .precision = 76, .scale = -3 });

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const od = out.getSeries("dec").?.decimal;
    try testing.expectEqual(@as(i256, 1234567890), od.values.items[0].unscaled);
    try testing.expectEqual(@as(u8, 10), od.values.items[0].precision);
    try testing.expectEqual(@as(i8, 2), od.values.items[0].scale);
    try testing.expect(od.isNull(1));
    try testing.expectEqual(-cap, od.values.items[2].unscaled);
    try testing.expectEqual(@as(u8, 76), od.values.items[2].precision);
    try testing.expectEqual(@as(i8, -3), od.values.items[2].scale);
}

test "tdf: Uuid / Interval / Binary round-trip with nulls" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var u = try df.createSeries(Uuid);
    try u.rename("u");
    try u.append(try Uuid.parse("550e8400-e29b-41d4-a716-446655440000"));
    try u.appendNull();
    try u.append(try Uuid.parse("00000000-0000-0000-0000-000000000001"));

    var iv = try df.createSeries(Interval);
    try iv.rename("iv");
    try iv.append(.{ .months = 1, .days = 2, .millis = 3000 });
    try iv.appendNull();
    try iv.append(.{ .months = 0, .days = 0, .millis = 1 });

    var bin = try df.createSeries(Binary);
    try bin.rename("bin");
    try bin.append(try Binary.fromSlice(allocator, &.{ 0xDE, 0xAD, 0xBE, 0xEF }));
    try bin.appendNull();
    try bin.append(try Binary.fromSlice(allocator, &.{}));

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const ou = out.getSeries("u").?.uuid;
    const want = try Uuid.parse("550e8400-e29b-41d4-a716-446655440000");
    try testing.expectEqualSlices(u8, &want.bytes, &ou.values.items[0].bytes);
    try testing.expect(ou.isNull(1));

    const oiv = out.getSeries("iv").?.interval;
    try testing.expectEqual(@as(u32, 1), oiv.values.items[0].months);
    try testing.expectEqual(@as(u32, 2), oiv.values.items[0].days);
    try testing.expectEqual(@as(u32, 3000), oiv.values.items[0].millis);
    try testing.expect(oiv.isNull(1));

    const obin = out.getSeries("bin").?.binary;
    try testing.expectEqualSlices(u8, &.{ 0xDE, 0xAD, 0xBE, 0xEF }, obin.values.items[0].toSlice());
    try testing.expect(obin.isNull(1));
    try testing.expectEqual(@as(usize, 0), obin.values.items[2].toSlice().len);
}

test "tdf: FixedBytes preserves width meta" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var fb = try df.createSeries(FixedBytes);
    try fb.rename("fb");
    fb.meta = .{ .width = 4 };
    try fb.append(try FixedBytes.fromSlice(allocator, &.{ 0xCA, 0xFE, 0xBA, 0xBE }));
    try fb.appendNull();
    try fb.append(try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02, 0x03, 0x04 }));

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const ofb = out.getSeries("fb").?.fixed_bytes;
    try testing.expect(ofb.meta.width != null);
    try testing.expectEqual(@as(i32, 4), ofb.meta.width.?);
    try testing.expectEqualSlices(u8, &.{ 0xCA, 0xFE, 0xBA, 0xBE }, ofb.values.items[0].toSlice());
    try testing.expect(ofb.isNull(1));
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03, 0x04 }, ofb.values.items[2].toSlice());
}

test "tdf: FixedBytes with null width meta round-trips as null" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var fb = try df.createSeries(FixedBytes);
    try fb.rename("fb");
    try fb.append(try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02 }));

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const ofb = out.getSeries("fb").?.fixed_bytes;
    try testing.expect(ofb.meta.width == null);
}

test "tdf: Raw preserves physical + converted + logical + type_length meta" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var raw = try df.createSeries(Raw);
    try raw.rename("raw");
    raw.meta = .{
        .physical_type = .int96,
        .converted_type = .timestamp_micros,
        .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .micros } },
        .type_length = 12,
    };
    try raw.append(try Raw.fromSlice(allocator, &.{ 0x01, 0x02, 0x03 }));
    try raw.appendNull();

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const oraw = out.getSeries("raw").?.raw;
    try testing.expectEqual(parquet.PhysicalType.int96, oraw.meta.physical_type);
    try testing.expectEqual(parquet.ConvertedType.timestamp_micros, oraw.meta.converted_type.?);
    try testing.expect(oraw.meta.logical_type != null);
    switch (oraw.meta.logical_type.?) {
        .timestamp => |p| {
            try testing.expectEqual(true, p.is_adjusted_to_utc);
            try testing.expectEqual(parquet.TimeUnit.micros, p.unit);
        },
        else => return error.WrongLogicalType,
    }
    try testing.expectEqual(@as(i32, 12), oraw.meta.type_length.?);
    try testing.expectEqualSlices(u8, &.{ 0x01, 0x02, 0x03 }, oraw.values.items[0].toSlice());
    try testing.expect(oraw.isNull(1));
}

test "tdf: Raw with decimal/integer logical types + absent converted/logical" {
    const allocator = testing.allocator;

    // decimal logical type, no converted type
    {
        var df = try Dataframe.init(allocator);
        defer df.deinit();
        var raw = try df.createSeries(Raw);
        try raw.rename("r");
        raw.meta = .{
            .physical_type = .fixed_len_byte_array,
            .converted_type = null,
            .logical_type = .{ .decimal = .{ .scale = 4, .precision = 18 } },
            .type_length = 8,
        };
        try raw.append(try Raw.fromSlice(allocator, &.{ 0xFF, 0x00 }));

        var out = try roundTrip(allocator, df);
        defer out.deinit();
        const o = out.getSeries("r").?.raw;
        try testing.expect(o.meta.converted_type == null);
        switch (o.meta.logical_type.?) {
            .decimal => |p| {
                try testing.expectEqual(@as(i32, 4), p.scale);
                try testing.expectEqual(@as(i32, 18), p.precision);
            },
            else => return error.WrongLogicalType,
        }
    }

    // integer logical type
    {
        var df = try Dataframe.init(allocator);
        defer df.deinit();
        var raw = try df.createSeries(Raw);
        try raw.rename("r");
        raw.meta = .{
            .physical_type = .int32,
            .logical_type = .{ .integer = .{ .bit_width = 32, .is_signed = false } },
        };
        try raw.append(try Raw.fromSlice(allocator, &.{0xAB}));

        var out = try roundTrip(allocator, df);
        defer out.deinit();
        const o = out.getSeries("r").?.raw;
        try testing.expectEqual(parquet.PhysicalType.int32, o.meta.physical_type);
        switch (o.meta.logical_type.?) {
            .integer => |p| {
                try testing.expectEqual(@as(i8, 32), p.bit_width);
                try testing.expectEqual(false, p.is_signed);
            },
            else => return error.WrongLogicalType,
        }
        try testing.expect(o.meta.type_length == null);
    }

    // no logical type at all (parameterless path)
    {
        var df = try Dataframe.init(allocator);
        defer df.deinit();
        var raw = try df.createSeries(Raw);
        try raw.rename("r");
        raw.meta = .{ .physical_type = .byte_array };
        try raw.append(try Raw.fromSlice(allocator, &.{0x01}));

        var out = try roundTrip(allocator, df);
        defer out.deinit();
        const o = out.getSeries("r").?.raw;
        try testing.expect(o.meta.logical_type == null);
        try testing.expect(o.meta.converted_type == null);
    }
}

// ---------------------------------------------------------------------------
// Structural edge cases
// ---------------------------------------------------------------------------

test "tdf: empty dataframe (0 columns)" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var out = try roundTrip(allocator, df);
    defer out.deinit();
    try testing.expectEqual(@as(usize, 0), out.width());
    try testing.expectEqual(@as(usize, 0), out.height());
}

test "tdf: columns but 0 rows" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i32);
    try c.rename("x");
    var s = try df.createSeries(String);
    try s.rename("s");

    var out = try roundTrip(allocator, df);
    defer out.deinit();
    try testing.expectEqual(@as(usize, 2), out.width());
    try testing.expectEqual(@as(usize, 0), out.height());
    try testing.expectEqualStrings("x", out.getSeries("x").?.name());
    try testing.expectEqualStrings("s", out.getSeries("s").?.name());
}

test "tdf: all-null column round-trips" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i64);
    try c.rename("x");
    try c.appendNull();
    try c.appendNull();
    try c.appendNull();

    var out = try roundTrip(allocator, df);
    defer out.deinit();
    const o = out.getSeries("x").?.int64;
    try testing.expectEqual(@as(usize, 3), o.len());
    try testing.expect(o.isNull(0));
    try testing.expect(o.isNull(1));
    try testing.expect(o.isNull(2));
    try testing.expectEqual(@as(usize, 3), o.nullCount());
}

test "tdf: column with no validity bitmap (no nulls) has none after parse" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i32);
    try c.rename("x");
    try c.append(1);
    try c.append(2);
    try testing.expect(c.validity == null);

    var out = try roundTrip(allocator, df);
    defer out.deinit();
    const o = out.getSeries("x").?.int32;
    try testing.expect(o.validity == null);
    try testing.expectEqual(@as(i32, 1), o.values.items[0]);
    try testing.expectEqual(@as(i32, 2), o.values.items[1]);
}

// ---------------------------------------------------------------------------
// Malformed / hardened input
// ---------------------------------------------------------------------------

test "tdf: bad magic -> error.InvalidTdf" {
    const allocator = testing.allocator;
    const bad = "NOTTEDDY" ++ ([_]u8{0} ** 16);
    try testing.expectError(error.InvalidTdf, native.parse(allocator, bad));
}

test "tdf: bumped version -> error.UnsupportedFormatVersion" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i32);
    try c.rename("x");
    try c.append(1);

    const bytes = try native.writeToString(allocator, df);
    defer allocator.free(bytes);
    // version is the u16 right after the 8-byte magic.
    bytes[8] = 99;
    try testing.expectError(error.UnsupportedFormatVersion, native.parse(allocator, bytes));
}

test "tdf: truncation at every offset returns an error, never panics" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var ints = try df.createSeries(i64);
    try ints.rename("ints");
    try ints.append(7);
    try ints.appendNull();
    var strs = try df.createSeries(String);
    try strs.rename("strs");
    try strs.append(try String.fromSlice(allocator, "abc"));
    try strs.appendNull();
    var fb = try df.createSeries(FixedBytes);
    try fb.rename("fb");
    fb.meta = .{ .width = 2 };
    try fb.append(try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02 }));
    try fb.appendNull();

    const bytes = try native.writeToString(allocator, df);
    defer allocator.free(bytes);

    // Every strict prefix must fail cleanly (no panic, no leak under testing alloc).
    var cut: usize = 0;
    while (cut < bytes.len) : (cut += 1) {
        const r = native.parse(allocator, bytes[0..cut]);
        if (r) |ok| {
            ok.deinit();
            return error.TruncationShouldHaveFailed;
        } else |_| {}
    }
    // The full buffer must parse.
    var ok = try native.parse(allocator, bytes);
    ok.deinit();
}

test "tdf: trailing garbage after a valid frame -> error.CorruptTdf" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i32);
    try c.rename("x");
    try c.append(1);

    const bytes = try native.writeToString(allocator, df);
    defer allocator.free(bytes);

    const extended = try allocator.alloc(u8, bytes.len + 1);
    defer allocator.free(extended);
    @memcpy(extended[0..bytes.len], bytes);
    extended[bytes.len] = 0xFF;
    try testing.expectError(error.CorruptTdf, native.parse(allocator, extended));
}

// ---------------------------------------------------------------------------
// Reader / Writer wiring
// ---------------------------------------------------------------------------

test "tdf: Writer.toString(.tdf) routes to native_format and parses back" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i32);
    try c.rename("x");
    try c.append(10);
    try c.append(20);

    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.tdf);

    const bytes = try w.toString(df);
    defer allocator.free(bytes);

    var out = try native.parse(allocator, bytes);
    defer out.deinit();
    const o = out.getSeries("x").?.int32;
    try testing.expectEqual(@as(i32, 10), o.values.items[0]);
    try testing.expectEqual(@as(i32, 20), o.values.items[1]);
}

test "tdf: Writer.save + Reader.load file round-trip" {
    const allocator = testing.allocator;
    const io = std.Io.Threaded.global_single_threaded.io();

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i64);
    try c.rename("val");
    try c.append(100);
    try c.appendNull();
    try c.append(300);
    var s = try df.createSeries(String);
    try s.rename("label");
    try s.append(try String.fromSlice(allocator, "a"));
    try s.append(try String.fromSlice(allocator, "b"));
    try s.appendNull();

    const path = "test_tdf_roundtrip.tdf";
    const cwd = std.Io.Dir.cwd();
    defer cwd.deleteFile(io, path) catch {};

    var w = try Writer.init(allocator, io);
    defer w.deinit();
    _ = w.withFileType(.tdf).withPath(path);
    try w.save(df);

    var r = try Reader.init(allocator, io);
    defer r.deinit();
    _ = r.withFileType(.tdf).withPath(path);
    var out = try r.load();
    defer out.deinit();

    try testing.expectEqual(@as(usize, 2), out.width());
    try testing.expectEqual(@as(usize, 3), out.height());
    const ov = out.getSeries("val").?.int64;
    try testing.expectEqual(@as(i64, 100), ov.values.items[0]);
    try testing.expect(ov.isNull(1));
    try testing.expectEqual(@as(i64, 300), ov.values.items[2]);
    const ol = out.getSeries("label").?.string;
    try testing.expectEqualStrings("a", ol.values.items[0].toSlice());
    try testing.expect(ol.isNull(2));
}

// ---------------------------------------------------------------------------
// Nested columns (Phase 8.1)
// ---------------------------------------------------------------------------

/// Build a list<i64> value from the given ints.
fn intList(allocator: std.mem.Allocator, ints: []const i64) !Nested {
    const items = try allocator.alloc(Nested, ints.len);
    for (ints, 0..) |v, i| items[i] = .{ .int = v };
    return .{ .list = .{ .allocator = allocator, .items = items } };
}

test "tdf: list<i64> column round-trips (with a null row and an empty list)" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(Nested);
    try col.rename("lst");
    try col.append(try intList(allocator, &.{ 1, 2 })); // row 0
    try col.appendNull(); // row 1 (validity null, stored .null_)
    try col.append(try intList(allocator, &.{})); // row 2 empty list
    try col.append(try intList(allocator, &.{3})); // row 3

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    try testing.expectEqual(@as(usize, 1), out.width());
    try testing.expectEqual(@as(usize, 4), out.height());

    const s = out.getSeries("lst").?.nested;
    try testing.expect(!s.isNull(0));
    try testing.expect(s.isNull(1));
    try testing.expect(!s.isNull(2));

    try testing.expectEqual(@as(usize, 2), try s.values.items[0].listLen());
    try testing.expectEqual(@as(i64, 1), (try s.values.items[0].listAt(0)).int);
    try testing.expectEqual(@as(i64, 2), (try s.values.items[0].listAt(1)).int);
    try testing.expectEqual(@as(usize, 0), try s.values.items[2].listLen());
    try testing.expectEqual(@as(usize, 1), try s.values.items[3].listLen());
    try testing.expectEqual(@as(i64, 3), (try s.values.items[3].listAt(0)).int);
}

test "tdf: struct{a:i64, b:String} column round-trips fields positionally" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(Nested);
    try col.rename("st");
    {
        const fields = try allocator.alloc(Nested, 2);
        fields[0] = .{ .int = 7 };
        fields[1] = .{ .string = try String.fromSlice(allocator, "hi") };
        try col.append(.{ .strukt = .{ .allocator = allocator, .fields = fields } });
    }
    {
        const fields = try allocator.alloc(Nested, 2);
        fields[0] = .{ .int = -9 };
        fields[1] = .{ .string = try String.fromSlice(allocator, "") };
        try col.append(.{ .strukt = .{ .allocator = allocator, .fields = fields } });
    }

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const s = out.getSeries("st").?.nested;
    try testing.expectEqual(@as(i64, 7), (try s.values.items[0].structAt(0)).int);
    try testing.expectEqualStrings("hi", (try s.values.items[0].structAt(1)).string.toSlice());
    try testing.expectEqual(@as(i64, -9), (try s.values.items[1].structAt(0)).int);
    try testing.expectEqualStrings("", (try s.values.items[1].structAt(1)).string.toSlice());
}

test "tdf: map<String,i64> column round-trips (empty map + null-value entry)" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(Nested);
    try col.rename("m");
    {
        const entries = try allocator.alloc(Nested.MapEntry, 2);
        entries[0] = .{
            .key = .{ .string = try String.fromSlice(allocator, "a") },
            .value = .{ .int = 1 },
        };
        entries[1] = .{
            .key = .{ .string = try String.fromSlice(allocator, "b") },
            .value = .null_, // null value
        };
        try col.append(.{ .map = .{ .allocator = allocator, .entries = entries } });
    }
    try col.append(.{ .map = .{ .allocator = allocator, .entries = &.{} } }); // empty map

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const s = out.getSeries("m").?.nested;
    try testing.expectEqual(@as(usize, 2), try s.values.items[0].mapLen());
    const e0 = try s.values.items[0].mapAt(0);
    try testing.expectEqualStrings("a", e0.key.string.toSlice());
    try testing.expectEqual(@as(i64, 1), e0.value.int);
    const e1 = try s.values.items[0].mapAt(1);
    try testing.expectEqualStrings("b", e1.key.string.toSlice());
    try testing.expectEqual(Nested.Tag.null_, e1.value.kind());
    try testing.expectEqual(@as(usize, 0), try s.values.items[1].mapLen());
}

test "tdf: list<list<i64>> nesting round-trips" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(Nested);
    try col.rename("ll");
    {
        const outer = try allocator.alloc(Nested, 2);
        outer[0] = try intList(allocator, &.{ 1, 2 });
        outer[1] = try intList(allocator, &.{3});
        try col.append(.{ .list = .{ .allocator = allocator, .items = outer } });
    }

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const s = out.getSeries("ll").?.nested;
    const row = &s.values.items[0];
    try testing.expectEqual(@as(usize, 2), try row.listLen());
    const inner0 = try row.listAt(0);
    try testing.expectEqual(@as(usize, 2), try inner0.listLen());
    try testing.expectEqual(@as(i64, 1), (try inner0.listAt(0)).int);
    try testing.expectEqual(@as(i64, 2), (try inner0.listAt(1)).int);
    const inner1 = try row.listAt(1);
    try testing.expectEqual(@as(usize, 1), try inner1.listLen());
    try testing.expectEqual(@as(i64, 3), (try inner1.listAt(0)).int);
}

test "tdf: Nested column with meta.schema survives round-trip" {
    const allocator = testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    // Hand-build a small SchemaNode tree: optional group "mylist" with one
    // required int64 leaf "elem".
    const child = try allocator.alloc(parquet.types.SchemaNode, 1);
    child[0] = .{
        .name = try allocator.dupe(u8, "elem"),
        .repetition = .required,
        .physical = .int64,
        .logical = .{ .integer = .{ .bit_width = 64, .is_signed = true } },
        .scale = null,
        .precision = 18,
        .max_def = 1,
        .max_rep = 1,
        .leaf_index = 0,
        .children = &.{},
    };
    const root = try allocator.create(parquet.types.SchemaNode);
    root.* = .{
        .name = try allocator.dupe(u8, "mylist"),
        .repetition = .optional,
        .children = child,
    };

    var col = try df.createSeries(Nested);
    try col.rename("mylist");
    col.meta = .{ .schema = root, .allocator = allocator };
    try col.append(try intList(allocator, &.{ 10, 20 }));

    var out = try roundTrip(allocator, df);
    defer out.deinit();

    const s = out.getSeries("mylist").?.nested;
    try testing.expect(s.meta.schema != null);
    const rs = s.meta.schema.?;
    try testing.expectEqualStrings("mylist", rs.name);
    try testing.expectEqual(parquet.types.FieldRepetitionType.optional, rs.repetition);
    try testing.expectEqual(@as(usize, 1), rs.children.len);
    const leaf = &rs.children[0];
    try testing.expectEqualStrings("elem", leaf.name);
    try testing.expectEqual(parquet.types.FieldRepetitionType.required, leaf.repetition);
    try testing.expectEqual(parquet.PhysicalType.int64, leaf.physical.?);
    try testing.expectEqual(@as(i32, 64), leaf.logical.?.integer.bit_width);
    try testing.expect(leaf.logical.?.integer.is_signed);
    try testing.expectEqual(@as(i32, 18), leaf.precision.?);
    try testing.expectEqual(@as(?i32, null), leaf.scale);
    try testing.expectEqual(@as(u8, 1), leaf.max_def);
    try testing.expectEqual(@as(u8, 1), leaf.max_rep);
    try testing.expectEqual(@as(usize, 0), leaf.leaf_index.?);

    // Values still intact.
    try testing.expectEqual(@as(usize, 2), try s.values.items[0].listLen());
    try testing.expectEqual(@as(i64, 10), (try s.values.items[0].listAt(0)).int);
}

test "tdf: parquet nested_kinds.parquet -> TDF -> parse renders identically" {
    const allocator = testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_kinds.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    // Round-trip the whole dataframe through TDF.
    var out = try roundTrip(allocator, df);
    defer out.deinit();

    try testing.expectEqual(df.width(), out.width());
    try testing.expectEqual(df.height(), out.height());

    // For each Nested column, per-row rendering must match column-for-column.
    var found_nested = false;
    for (df.series.items, 0..) |*boxed, col_idx| {
        if (boxed.* != .nested) continue;
        found_nested = true;
        const orig = boxed.nested;
        const rt = out.series.items[col_idx].nested;
        try testing.expectEqualStrings(orig.name.toSlice(), rt.name.toSlice());
        try testing.expectEqual(orig.len(), rt.len());

        var row: usize = 0;
        while (row < orig.len()) : (row += 1) {
            try testing.expectEqual(orig.isNull(row), rt.isNull(row));
            var a = try orig.asStringAt(row);
            defer a.deinit();
            var b = try rt.asStringAt(row);
            defer b.deinit();
            try testing.expectEqualStrings(a.toSlice(), b.toSlice());
        }
    }
    try testing.expect(found_nested);
}

// ---------------------------------------------------------------------------
// Malformed nested input
// ---------------------------------------------------------------------------

/// Build a minimal valid TDF buffer holding one list<i64> nested column.
fn buildOneListTdf(allocator: std.mem.Allocator) ![]u8 {
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try df.createSeries(Nested);
    try col.rename("lst");
    try col.append(try intList(allocator, &.{ 1, 2, 3 }));
    return native.writeToString(allocator, df);
}

test "tdf: truncating a nested buffer at any stride errors, never panics" {
    const allocator = testing.allocator;
    const bytes = try buildOneListTdf(allocator);
    defer allocator.free(bytes);

    var len: usize = 0;
    while (len < bytes.len) : (len += 1) {
        const slice = bytes[0..len];
        if (native.parse(allocator, slice)) |df| {
            df.deinit();
            try testing.expect(false); // a truncated buffer must not parse
        } else |_| {}
    }
}

test "tdf: nested value tag byte = 99 -> error.CorruptTdf" {
    const allocator = testing.allocator;
    const bytes = try buildOneListTdf(allocator);
    defer allocator.free(bytes);

    const buf = try allocator.dupe(u8, bytes);
    defer allocator.free(buf);

    // Replace the last list-tag byte (the outer list value's tag) with 99, an
    // out-of-range Nested tag, which decodeNested must reject.
    const list_tag: u8 = @intFromEnum(Nested.Tag.list);
    var i: usize = bytes.len;
    var done = false;
    while (i > 0) {
        i -= 1;
        if (buf[i] == list_tag) {
            buf[i] = 99;
            done = true;
            break;
        }
    }
    try testing.expect(done);
    try testing.expectError(error.CorruptTdf, native.parse(allocator, buf));
}

test "tdf: pathologically deep nesting -> error.NestingTooDeep" {
    const allocator = testing.allocator;

    // Hand-craft a TDF buffer: header + 1 nested column (no schema, no validity)
    // + 1 row that is a list nested far deeper than MAX_NESTING_DEPTH (64).
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "TEDDYDF1");
    try appendIntLE(allocator, &buf, u16, 1); // version
    try appendIntLE(allocator, &buf, u16, 0); // flags
    try appendIntLE(allocator, &buf, u32, 1); // num_columns
    try appendIntLE(allocator, &buf, u64, 1); // num_rows
    // schema: name "x", tag nested, schema_present=0, has_validity=0
    try appendIntLE(allocator, &buf, u32, 1);
    try buf.append(allocator, 'x');
    try buf.append(allocator, 26); // Tag.nested_
    try buf.append(allocator, 0); // schema present = no
    try buf.append(allocator, 0); // has_validity = no
    // value: 200 nested lists each of count 1, then an int leaf.
    const list_tag: u8 = @intFromEnum(Nested.Tag.list);
    var depth: usize = 0;
    while (depth < 200) : (depth += 1) {
        try buf.append(allocator, list_tag);
        try appendIntLE(allocator, &buf, u32, 1); // one child
    }
    try buf.append(allocator, @intFromEnum(Nested.Tag.int));
    try appendIntLE(allocator, &buf, i64, 42);

    try testing.expectError(error.NestingTooDeep, native.parse(allocator, buf.items));
}

fn appendIntLE(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), comptime T: type, value: T) !void {
    var tmp: [@divExact(@typeInfo(T).int.bits, 8)]u8 = undefined;
    std.mem.writeInt(T, &tmp, value, .little);
    try buf.appendSlice(allocator, &tmp);
}
