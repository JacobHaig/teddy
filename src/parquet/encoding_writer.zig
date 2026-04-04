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
        return .{ .buf = .empty, .allocator = allocator };
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

