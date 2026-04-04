const std = @import("std");
const thrift = @import("thrift_reader.zig");
const CompactType = thrift.CompactType;
const ThriftReader = thrift.ThriftReader;

// ============================================================
// Thrift Compact Protocol Writer
// ============================================================

/// Encodes Thrift Compact Protocol data into a growable buffer.
/// Exact inverse of ThriftReader.
pub const ThriftWriter = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    last_field_id: i16,
    field_id_stack: [16]i16,
    stack_depth: usize,

    pub fn init(allocator: std.mem.Allocator) ThriftWriter {
        return .{
            .buf = .empty,
            .allocator = allocator,
            .last_field_id = 0,
            .field_id_stack = [_]i16{0} ** 16,
            .stack_depth = 0,
        };
    }

    pub fn deinit(self: *ThriftWriter) void {
        self.buf.deinit(self.allocator);
    }

    /// Return the written bytes as an owned slice and reset the writer.
    pub fn toOwnedSlice(self: *ThriftWriter) ![]u8 {
        return self.buf.toOwnedSlice(self.allocator);
    }

    /// Get the current written bytes (non-owning view).
    pub fn written(self: *const ThriftWriter) []const u8 {
        return self.buf.items;
    }

    // ---- Primitive writers ----

    /// Write an unsigned variable-length integer (7 bits per byte, MSB = continuation).
    pub fn writeVarint(self: *ThriftWriter, value: u64) !void {
        var v = value;
        while (v >= 0x80) {
            try self.buf.append(self.allocator, @intCast((v & 0x7F) | 0x80));
            v >>= 7;
        }
        try self.buf.append(self.allocator, @intCast(v));
    }

    /// Write a zigzag-encoded i32 as a varint.
    pub fn writeZigZagI32(self: *ThriftWriter, value: i32) !void {
        try self.writeVarint(zigzagEncode32(value));
    }

    /// Write a zigzag-encoded i64 as a varint.
    pub fn writeZigZagI64(self: *ThriftWriter, value: i64) !void {
        try self.writeVarint(zigzagEncode64(value));
    }

    /// Write a zigzag-encoded i16 as a varint.
    pub fn writeZigZagI16(self: *ThriftWriter, value: i16) !void {
        try self.writeVarint(zigzagEncode32(@as(i32, value)));
    }

    /// Write a single byte.
    pub fn writeByte(self: *ThriftWriter, b: u8) !void {
        try self.buf.append(self.allocator, b);
    }

    /// Write an IEEE 754 double (8 bytes, little-endian).
    pub fn writeDouble(self: *ThriftWriter, value: f64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, @bitCast(value), .little);
        try self.buf.appendSlice(self.allocator, &bytes);
    }

    /// Write a binary field: varint length prefix followed by raw bytes.
    pub fn writeBinary(self: *ThriftWriter, data: []const u8) !void {
        try self.writeVarint(@intCast(data.len));
        try self.buf.appendSlice(self.allocator, data);
    }

    /// Alias for writeBinary — Thrift strings are encoded the same as binary.
    pub fn writeString(self: *ThriftWriter, s: []const u8) !void {
        try self.writeBinary(s);
    }

    // ---- Struct / field navigation ----

    /// Write a field header. Uses delta encoding when the gap from last field is 1-15.
    pub fn writeFieldHeader(self: *ThriftWriter, field_id: i16, field_type: CompactType) !void {
        const delta = field_id - self.last_field_id;
        if (delta > 0 and delta <= 15) {
            try self.buf.append(self.allocator, @as(u8, @intCast(delta)) << 4 | @intFromEnum(field_type));
        } else {
            try self.buf.append(self.allocator, @intFromEnum(field_type));
            try self.writeZigZagI16(field_id);
        }
        self.last_field_id = field_id;
    }

    /// Write a boolean field (type is encoded in the field header itself).
    pub fn writeBoolField(self: *ThriftWriter, field_id: i16, value: bool) !void {
        const field_type: CompactType = if (value) .boolean_true else .boolean_false;
        try self.writeFieldHeader(field_id, field_type);
    }

    /// Write a struct stop marker.
    pub fn writeFieldStop(self: *ThriftWriter) !void {
        try self.buf.append(self.allocator, 0x00);
    }

    /// Write a list header: element type + size.
    pub fn writeListHeader(self: *ThriftWriter, elem_type: CompactType, size: u32) !void {
        if (size < 15) {
            try self.buf.append(self.allocator, @as(u8, @intCast(size)) << 4 | @intFromEnum(elem_type));
        } else {
            try self.buf.append(self.allocator, @as(u8, 0xF0) | @intFromEnum(elem_type));
            try self.writeVarint(@as(u64, size));
        }
    }

    /// Push the current field ID state before entering a nested struct.
    pub fn pushStruct(self: *ThriftWriter) void {
        if (self.stack_depth < self.field_id_stack.len) {
            self.field_id_stack[self.stack_depth] = self.last_field_id;
            self.stack_depth += 1;
        }
        self.last_field_id = 0;
    }

    /// Pop the field ID state after leaving a nested struct.
    pub fn popStruct(self: *ThriftWriter) void {
        if (self.stack_depth > 0) {
            self.stack_depth -= 1;
            self.last_field_id = self.field_id_stack[self.stack_depth];
        }
    }
};

// ============================================================
// Helpers
// ============================================================

pub fn zigzagEncode32(n: i32) u64 {
    const unsigned: u32 = @bitCast(n);
    return @as(u64, (unsigned << 1) ^ @as(u32, @bitCast(n >> 31)));
}

fn zigzagEncode64(n: i64) u64 {
    const unsigned: u64 = @bitCast(n);
    return (unsigned << 1) ^ @as(u64, @bitCast(n >> 63));
}

