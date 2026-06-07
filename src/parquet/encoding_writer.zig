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

    /// Write a FIXED_LEN_BYTE_ARRAY / INT96 value: raw bytes, no length prefix.
    /// The caller is responsible for validating the value width.
    /// Contrast with writeByteArray, which prefixes a 4-byte length.
    pub fn writeFixedByteArray(self: *PlainEncoder, data: []const u8) !void {
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
// RLE/Bit-Packing Hybrid Level Encoder
// ============================================================

/// Encode definition/repetition levels as an RLE-hybrid stream using RLE runs
/// only (no bit-packed groups — pure-RLE is valid per the Parquet spec, and the
/// reader's RleBitPackedDecoder decodes it). For max_def_level==1 the levels are
/// 0 (null) / 1 (present) and fit one byte, so the run value is a single byte.
///
/// RLE run header: varint(run_len << 1); the low bit 0 marks an RLE run (a
/// bit-packed group would set it). The run value follows in ceil(bit_width/8)
/// bytes; for bit_width 1..8 that is exactly one byte. Adjacent equal levels
/// coalesce into one run; differing values start new runs.
pub fn encodeRleLevels(allocator: Allocator, levels: []const u1, out: *std.ArrayList(u8)) !void {
    var i: usize = 0;
    while (i < levels.len) {
        const run_val = levels[i];
        var run_len: usize = 1;
        while (i + run_len < levels.len and levels[i + run_len] == run_val) run_len += 1;
        // RLE run header: varint(count << 1); low bit 0 = RLE run.
        var header: u64 = @as(u64, run_len) << 1;
        while (header >= 0x80) {
            try out.append(allocator, @intCast((header & 0x7F) | 0x80));
            header >>= 7;
        }
        try out.append(allocator, @intCast(header));
        // Run value: 1 byte for bit_width <= 8 (levels are 0/1).
        try out.append(allocator, @intFromBool(run_val == 1));
        i += run_len;
    }
}

// ============================================================
// Tests
// ============================================================

