const std = @import("std");
const Allocator = std.mem.Allocator;
const PlainDecoder = @import("encoding_reader.zig").PlainDecoder;

// ============================================================
// PLAIN Encoding Writer
// ============================================================

/// Encodes values in Parquet PLAIN format into a growable buffer.
/// Exact inverse of PlainDecoder.
pub const PlainEncoder = struct {
    buf: std.ArrayList(u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) PlainEncoder {
        return .{ .buf = .{}, .allocator = allocator };
    }

    pub fn deinit(self: *PlainEncoder) void {
        self.buf.deinit(self.allocator);
    }

    pub fn toOwnedSlice(self: *PlainEncoder) ![]u8 {
        return self.buf.toOwnedSlice(self.allocator);
    }

    pub fn written(self: *const PlainEncoder) []const u8 {
        return self.buf.items;
    }

    pub fn writeInt32(self: *PlainEncoder, val: i32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes, val, .little);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeInt64(self: *PlainEncoder, val: i64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, val, .little);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeFloat(self: *PlainEncoder, val: f32) !void {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @bitCast(val), .little);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    pub fn writeDouble(self: *PlainEncoder, val: f64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, @bitCast(val), .little);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    /// Write a BYTE_ARRAY: 4-byte LE length prefix + raw bytes.
    pub fn writeByteArray(self: *PlainEncoder, data: []const u8) !void {
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(data.len), .little);
        try self.buf.appendSlice(self.allocator, &len_bytes);
        try self.buf.appendSlice(self.allocator, data);
    }

    /// Write PLAIN-encoded booleans: bit-packed, LSB first.
    pub fn writeBooleans(self: *PlainEncoder, bools: []const bool) !void {
        const num_bytes = (bools.len + 7) / 8;
        const start = self.buf.items.len;
        try self.buf.appendNTimes(self.allocator, 0, num_bytes);
        for (bools, 0..) |b, i| {
            if (b) {
                const byte_idx = i / 8;
                const bit_idx: u3 = @intCast(i % 8);
                self.buf.items[start + byte_idx] |= @as(u8, 1) << bit_idx;
            }
        }
    }
};

// ============================================================
// Tests
// ============================================================

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
