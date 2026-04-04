const std = @import("std");
const PlainEncoder = @import("encoding_writer.zig").PlainEncoder;
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
