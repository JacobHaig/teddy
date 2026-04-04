const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

// ============================================================
// PLAIN Encoding Decoder
// ============================================================

/// Decodes PLAIN-encoded Parquet values from a byte buffer.
pub const PlainDecoder = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) PlainDecoder {
        return .{ .data = data, .pos = 0 };
    }

    pub fn readInt32(self: *PlainDecoder) !i32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEof;
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return val;
    }

    pub fn readInt64(self: *PlainDecoder) !i64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEof;
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }

    pub fn readFloat(self: *PlainDecoder) !f32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEof;
        const bits = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return @bitCast(bits);
    }

    pub fn readDouble(self: *PlainDecoder) !f64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEof;
        const bits = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return @bitCast(bits);
    }

    /// Read a BYTE_ARRAY: 4-byte LE length + raw bytes.
    pub fn readByteArray(self: *PlainDecoder) ![]const u8 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEof;
        const len: usize = @intCast(std.mem.readInt(u32, self.data[self.pos..][0..4], .little));
        self.pos += 4;
        if (self.pos + len > self.data.len) return error.UnexpectedEof;
        const slice = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return slice;
    }

    /// Read a FIXED_LEN_BYTE_ARRAY of known length.
    pub fn readFixedByteArray(self: *PlainDecoder, len: usize) ![]const u8 {
        if (self.pos + len > self.data.len) return error.UnexpectedEof;
        const slice = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return slice;
    }

    // ---- Batch readers ----

    pub fn readInt32Batch(self: *PlainDecoder, count: usize, allocator: Allocator) ![]i32 {
        const result = try allocator.alloc(i32, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            result[i] = try self.readInt32();
        }
        return result;
    }

    pub fn readInt64Batch(self: *PlainDecoder, count: usize, allocator: Allocator) ![]i64 {
        const result = try allocator.alloc(i64, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            result[i] = try self.readInt64();
        }
        return result;
    }

    pub fn readFloatBatch(self: *PlainDecoder, count: usize, allocator: Allocator) ![]f32 {
        const result = try allocator.alloc(f32, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            result[i] = try self.readFloat();
        }
        return result;
    }

    pub fn readDoubleBatch(self: *PlainDecoder, count: usize, allocator: Allocator) ![]f64 {
        const result = try allocator.alloc(f64, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            result[i] = try self.readDouble();
        }
        return result;
    }

    /// Read count BYTE_ARRAYs, copying each into allocator-owned memory.
    pub fn readByteArrayBatch(self: *PlainDecoder, count: usize, allocator: Allocator) ![][]const u8 {
        const result = try allocator.alloc([]const u8, count);
        var initialized: usize = 0;
        errdefer {
            for (0..initialized) |i| allocator.free(result[i]);
            allocator.free(result);
        }
        for (0..count) |i| {
            const src = try self.readByteArray();
            const copy = try allocator.alloc(u8, src.len);
            @memcpy(copy, src);
            result[i] = copy;
            initialized += 1;
        }
        return result;
    }

    /// Read PLAIN-encoded booleans: bit-packed, LSB first.
    pub fn readBooleans(self: *PlainDecoder, count: usize, allocator: Allocator) ![]bool {
        const result = try allocator.alloc(bool, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            const byte_idx = i / 8;
            const bit_idx: u3 = @intCast(i % 8);
            if (self.pos + byte_idx >= self.data.len) return error.UnexpectedEof;
            result[i] = (self.data[self.pos + byte_idx] >> bit_idx) & 1 == 1;
        }
        self.pos += (count + 7) / 8;
        return result;
    }
};

// ============================================================
// RLE / Bit-Packing Hybrid Decoder
// ============================================================

/// Decodes RLE/Bit-packing hybrid encoded values.
/// Used for definition levels, repetition levels, and dictionary indices.
pub const RleBitPackedDecoder = struct {
    data: []const u8,
    pos: usize,
    bit_width: u8,
    end: usize, // end position in data

    // Current run state
    run_remaining: u32,
    is_rle_run: bool,
    rle_value: u32,

    // Bit-packed run state
    bit_buffer: u64,
    bits_in_buffer: u8,

    pub fn init(data: []const u8, bit_width: u8) RleBitPackedDecoder {
        return .{
            .data = data,
            .pos = 0,
            .bit_width = bit_width,
            .end = data.len,
            .run_remaining = 0,
            .is_rle_run = false,
            .rle_value = 0,
            .bit_buffer = 0,
            .bits_in_buffer = 0,
        };
    }

    /// Initialize from data that starts with a 4-byte LE length prefix.
    /// Used for definition/repetition levels in DataPage v1.
    pub fn initWithLengthPrefix(data: []const u8) RleBitPackedDecoder {
        if (data.len < 4) {
            return init(data, 0);
        }
        const len: usize = @intCast(std.mem.readInt(u32, data[0..4], .little));
        const actual_end = @min(4 + len, data.len);
        // bit_width must be set by caller after init
        return .{
            .data = data,
            .pos = 4,
            .bit_width = 0,
            .end = actual_end,
            .run_remaining = 0,
            .is_rle_run = false,
            .rle_value = 0,
            .bit_buffer = 0,
            .bits_in_buffer = 0,
        };
    }

    /// Read the next value.
    pub fn next(self: *RleBitPackedDecoder) !u32 {
        if (self.run_remaining == 0) {
            try self.readRunHeader();
        }

        self.run_remaining -= 1;

        if (self.is_rle_run) {
            return self.rle_value;
        }

        // Bit-packed: extract bit_width bits
        return self.readBitPacked();
    }

    /// Read a batch of values.
    pub fn readBatch(self: *RleBitPackedDecoder, count: usize, allocator: Allocator) ![]u32 {
        const result = try allocator.alloc(u32, count);
        errdefer allocator.free(result);
        for (0..count) |i| {
            result[i] = try self.next();
        }
        return result;
    }

    fn readRunHeader(self: *RleBitPackedDecoder) !void {
        const header = try self.readVlq();

        if (header & 1 == 1) {
            // Bit-packed run: count = (header >> 1) * 8
            self.run_remaining = (header >> 1) * 8;
            self.is_rle_run = false;
            // Reset bit buffer for new run
            self.bit_buffer = 0;
            self.bits_in_buffer = 0;
        } else {
            // RLE run: count = header >> 1
            self.run_remaining = header >> 1;
            self.is_rle_run = true;
            // Read value in ceil(bit_width/8) bytes LE
            self.rle_value = try self.readRleValue();
        }
    }

    fn readRleValue(self: *RleBitPackedDecoder) !u32 {
        const byte_count = (@as(usize, self.bit_width) + 7) / 8;
        if (byte_count == 0) return 0;
        if (self.pos + byte_count > self.end) return error.UnexpectedEof;
        var value: u32 = 0;
        for (0..byte_count) |i| {
            value |= @as(u32, self.data[self.pos + i]) << @intCast(i * 8);
        }
        self.pos += byte_count;
        return value;
    }

    fn readBitPacked(self: *RleBitPackedDecoder) !u32 {
        // Fill bit buffer if needed
        while (self.bits_in_buffer < self.bit_width) {
            if (self.pos >= self.end) return error.UnexpectedEof;
            self.bit_buffer |= @as(u64, self.data[self.pos]) << @intCast(self.bits_in_buffer);
            self.pos += 1;
            self.bits_in_buffer += 8;
        }

        const mask: u64 = if (self.bit_width == 32) 0xFFFFFFFF else (@as(u64, 1) << @intCast(self.bit_width)) - 1;
        const value: u32 = @intCast(self.bit_buffer & mask);
        self.bit_buffer >>= @intCast(self.bit_width);
        self.bits_in_buffer -= self.bit_width;
        return value;
    }

    /// Read a VLQ unsigned integer (same encoding as Thrift varint).
    fn readVlq(self: *RleBitPackedDecoder) !u32 {
        var result: u32 = 0;
        var shift: u5 = 0;
        while (true) {
            if (self.pos >= self.end) return error.UnexpectedEof;
            const byte = self.data[self.pos];
            self.pos += 1;
            result |= @as(u32, byte & 0x7F) << shift;
            if (byte & 0x80 == 0) return result;
            shift += 7;
            if (shift >= 32) return error.VarintTooLong;
        }
    }
};

