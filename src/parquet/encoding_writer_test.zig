const std = @import("std");
const PlainEncoder = @import("encoding_writer.zig").PlainEncoder;
const encodeRleLevels = @import("encoding_writer.zig").encodeRleLevels;
const PlainDecoder = @import("encoding_reader.zig").PlainDecoder;
const RleBitPackedDecoder = @import("encoding_reader.zig").RleBitPackedDecoder;

test "PlainEncoder: writeInt32 round-trip" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeInt32(42);
    try enc.writeInt32(-100);
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqual(@as(i32, 42), try dec.readInt32());
    try std.testing.expectEqual(@as(i32, -100), try dec.readInt32());
}

test "PlainEncoder: writeInt64 round-trip" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeInt64(123456789);
    try enc.writeInt64(-987654321);
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqual(@as(i64, 123456789), try dec.readInt64());
    try std.testing.expectEqual(@as(i64, -987654321), try dec.readInt64());
}

test "PlainEncoder: writeFloat round-trip" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeFloat(3.14);
    try enc.writeFloat(-2.5);
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqual(@as(f32, 3.14), try dec.readFloat());
    try std.testing.expectEqual(@as(f32, -2.5), try dec.readFloat());
}

test "PlainEncoder: writeDouble round-trip" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeDouble(2.71828);
    try enc.writeDouble(-1.414);
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqual(@as(f64, 2.71828), try dec.readDouble());
    try std.testing.expectEqual(@as(f64, -1.414), try dec.readDouble());
}

test "PlainEncoder: writeByteArray round-trip" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeByteArray("hello");
    try enc.writeByteArray("world");
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqualStrings("hello", try dec.readByteArray());
    try std.testing.expectEqualStrings("world", try dec.readByteArray());
}

test "PlainEncoder: writeByteArray empty" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeByteArray("");
    var dec = PlainDecoder.init(enc.written());
    const result = try dec.readByteArray();
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "PlainEncoder: writeBooleans round-trip" {
    const allocator = std.testing.allocator;
    var enc = PlainEncoder.init(allocator);
    defer enc.deinit();
    const bools = [_]bool{ true, false, true, true, false, false, true, false, true, true };
    try enc.writeBooleans(&bools);
    var dec = PlainDecoder.init(enc.written());
    const decoded = try dec.readBooleans(10, allocator);
    defer allocator.free(decoded);
    for (bools, 0..) |expected, i| {
        try std.testing.expectEqual(expected, decoded[i]);
    }
}

test "PlainEncoder: writeBooleans single" {
    const allocator = std.testing.allocator;
    var enc = PlainEncoder.init(allocator);
    defer enc.deinit();
    try enc.writeBooleans(&.{true});
    var dec = PlainDecoder.init(enc.written());
    const decoded = try dec.readBooleans(1, allocator);
    defer allocator.free(decoded);
    try std.testing.expect(decoded[0] == true);
}

test "PlainEncoder: mixed types sequential" {
    var enc = PlainEncoder.init(std.testing.allocator);
    defer enc.deinit();
    try enc.writeInt32(42);
    try enc.writeByteArray("abc");
    var dec = PlainDecoder.init(enc.written());
    try std.testing.expectEqual(@as(i32, 42), try dec.readInt32());
    try std.testing.expectEqualStrings("abc", try dec.readByteArray());
}

// ============================================================
// encodeRleLevels (definition-level encoder)
// ============================================================

test "encodeRleLevels: single run of present" {
    const allocator = std.testing.allocator;
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    const levels = [_]u1{ 1, 1, 1 };
    try encodeRleLevels(allocator, &levels, &out);
    // varint(3 << 1) = 6 (0x06), then value byte 0x01.
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x06, 0x01 }, out.items);
}

test "encodeRleLevels: alternating values produce three runs" {
    const allocator = std.testing.allocator;
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    const levels = [_]u1{ 1, 0, 1 };
    try encodeRleLevels(allocator, &levels, &out);
    // run1: len1 present -> 0x02,0x01 ; run2: len1 null -> 0x02,0x00 ;
    // run3: len1 present -> 0x02,0x01
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x02, 0x01, 0x02, 0x00, 0x02, 0x01 }, out.items);
}

fn roundtripLevels(allocator: std.mem.Allocator, levels: []const u1) !void {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try encodeRleLevels(allocator, levels, &out);
    var dec = RleBitPackedDecoder.init(out.items, 1);
    const decoded = try dec.readBatch(levels.len, allocator);
    defer allocator.free(decoded);
    for (levels, 0..) |lvl, i| {
        try std.testing.expectEqual(@as(u32, lvl), decoded[i]);
    }
}

test "encodeRleLevels: round-trips through RleBitPackedDecoder" {
    const allocator = std.testing.allocator;
    try roundtripLevels(allocator, &[_]u1{ 1, 1, 1 });
    try roundtripLevels(allocator, &[_]u1{ 1, 0, 1 });
    try roundtripLevels(allocator, &[_]u1{ 0, 0, 0, 1, 1, 0, 1 });
    try roundtripLevels(allocator, &[_]u1{1});
    try roundtripLevels(allocator, &[_]u1{0});
}

test "encodeRleLevels: long run forces multi-byte varint" {
    const allocator = std.testing.allocator;
    // 200 present values: 200 << 1 = 400 = 0x190 -> varint 0x90, 0x03.
    var levels: [200]u1 = undefined;
    @memset(&levels, 1);
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try encodeRleLevels(allocator, &levels, &out);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x90, 0x03, 0x01 }, out.items);
    try roundtripLevels(allocator, &levels);
}

test "encodeRleLevels: alternating 150 values round-trips" {
    const allocator = std.testing.allocator;
    var levels: [150]u1 = undefined;
    for (&levels, 0..) |*l, i| l.* = @intCast(i % 2);
    try roundtripLevels(allocator, &levels);
}

test "encodeRleLevels: empty input emits nothing" {
    const allocator = std.testing.allocator;
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try encodeRleLevels(allocator, &.{}, &out);
    try std.testing.expectEqual(@as(usize, 0), out.items.len);
}
