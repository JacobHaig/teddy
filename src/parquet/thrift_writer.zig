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

fn zigzagEncode32(n: i32) u64 {
    const unsigned: u32 = @bitCast(n);
    return @as(u64, (unsigned << 1) ^ @as(u32, @bitCast(n >> 31)));
}

fn zigzagEncode64(n: i64) u64 {
    const unsigned: u64 = @bitCast(n);
    return (unsigned << 1) ^ @as(u64, @bitCast(n >> 63));
}

// ============================================================
// Tests
// ============================================================

test "writeVarint: single byte" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeVarint(5);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(u64, 5), try r.readVarint());
}

test "writeVarint: multi-byte (150)" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeVarint(150);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(u64, 150), try r.readVarint());
}

test "writeVarint: zero" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeVarint(0);
    try std.testing.expectEqual(@as(usize, 1), w.written().len);
    try std.testing.expectEqual(@as(u8, 0x00), w.written()[0]);
}

test "writeVarint: large value (300)" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeVarint(300);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(u64, 300), try r.readVarint());
}

test "zigzag encode round-trip" {
    try std.testing.expectEqual(@as(u64, 0), zigzagEncode32(0));
    try std.testing.expectEqual(@as(u64, 1), zigzagEncode32(-1));
    try std.testing.expectEqual(@as(u64, 2), zigzagEncode32(1));
    try std.testing.expectEqual(@as(u64, 3), zigzagEncode32(-2));
    try std.testing.expectEqual(@as(u64, 4), zigzagEncode32(2));
}

test "writeZigZagI32: round-trip positive" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeZigZagI32(42);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(i32, 42), try r.readZigZagI32());
}

test "writeZigZagI32: round-trip negative" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeZigZagI32(-100);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(i32, -100), try r.readZigZagI32());
}

test "writeZigZagI64: round-trip" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeZigZagI64(123456789);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(i64, 123456789), try r.readZigZagI64());
}

test "writeZigZagI64: round-trip negative" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeZigZagI64(-999999);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(i64, -999999), try r.readZigZagI64());
}

test "writeZigZagI16: round-trip" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeZigZagI16(100);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(i16, 100), try r.readZigZagI16());
}

test "writeByte: round-trip" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeByte(0xAB);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(u8, 0xAB), try r.readByte());
}

test "writeDouble: round-trip" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeDouble(3.14);
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqual(@as(f64, 3.14), try r.readDouble());
}

test "writeBinary: round-trip" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeBinary("hello");
    var r = ThriftReader.init(w.written());
    try std.testing.expectEqualStrings("hello", try r.readBinary());
}

test "writeBinary: empty" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeBinary("");
    var r = ThriftReader.init(w.written());
    const result = try r.readBinary();
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "writeFieldHeader: delta encoding" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeFieldHeader(1, .i32);
    try w.writeFieldHeader(3, .i32);
    try w.writeFieldStop();

    var r = ThriftReader.init(w.written());
    const fh1 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), fh1.field_id);
    try std.testing.expectEqual(CompactType.i32, fh1.field_type);

    const fh2 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 3), fh2.field_id);
    try std.testing.expectEqual(CompactType.i32, fh2.field_type);

    const fh3 = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, fh3.field_type);
}

test "writeFieldHeader: absolute encoding (large gap)" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeFieldHeader(100, .i32);

    var r = ThriftReader.init(w.written());
    const fh = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 100), fh.field_id);
    try std.testing.expectEqual(CompactType.i32, fh.field_type);
}

test "writeBoolField: true and false" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeBoolField(1, true);
    try w.writeBoolField(2, false);
    try w.writeFieldStop();

    var r = ThriftReader.init(w.written());
    const fh1 = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.boolean_true, fh1.field_type);
    const fh2 = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.boolean_false, fh2.field_type);
}

test "writeListHeader: small list" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeListHeader(.i32, 3);

    var r = ThriftReader.init(w.written());
    const hdr = try r.readListHeader();
    try std.testing.expectEqual(@as(u32, 3), hdr.size);
    try std.testing.expectEqual(CompactType.i32, hdr.elem_type);
}

test "writeListHeader: large list" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    try w.writeListHeader(.i32, 20);

    var r = ThriftReader.init(w.written());
    const hdr = try r.readListHeader();
    try std.testing.expectEqual(@as(u32, 20), hdr.size);
    try std.testing.expectEqual(CompactType.i32, hdr.elem_type);
}

test "pushStruct/popStruct preserves field ID" {
    var w = ThriftWriter.init(std.testing.allocator);
    defer w.deinit();
    w.last_field_id = 5;
    w.pushStruct();
    try std.testing.expectEqual(@as(i16, 0), w.last_field_id);
    w.last_field_id = 10;
    w.popStruct();
    try std.testing.expectEqual(@as(i16, 5), w.last_field_id);
}

test "full struct round-trip: SchemaElement-like" {
    const allocator = std.testing.allocator;
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    w.pushStruct();
    try w.writeFieldHeader(1, .i32);
    try w.writeZigZagI32(2); // INT64
    try w.writeFieldHeader(3, .i32);
    try w.writeZigZagI32(1); // OPTIONAL
    try w.writeFieldHeader(4, .binary);
    try w.writeString("age");
    try w.writeFieldStop();
    w.popStruct();

    var r = ThriftReader.init(w.written());
    r.pushStruct();

    const fh1 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), fh1.field_id);
    try std.testing.expectEqual(@as(i32, 2), try r.readZigZagI32());

    const fh2 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 3), fh2.field_id);
    try std.testing.expectEqual(@as(i32, 1), try r.readZigZagI32());

    const fh3 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 4), fh3.field_id);
    try std.testing.expectEqualStrings("age", try r.readString());

    const fh4 = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, fh4.field_type);
    r.popStruct();
}

test "nested struct round-trip" {
    const allocator = std.testing.allocator;
    var w = ThriftWriter.init(allocator);
    defer w.deinit();

    w.pushStruct();
    try w.writeFieldHeader(1, .i32);
    try w.writeZigZagI32(42);
    try w.writeFieldHeader(2, .@"struct");
    w.pushStruct();
    try w.writeFieldHeader(1, .binary);
    try w.writeString("inner");
    try w.writeFieldStop();
    w.popStruct();
    try w.writeFieldHeader(3, .i64);
    try w.writeZigZagI64(99);
    try w.writeFieldStop();
    w.popStruct();

    var r = ThriftReader.init(w.written());
    r.pushStruct();

    const fh1 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), fh1.field_id);
    try std.testing.expectEqual(@as(i32, 42), try r.readZigZagI32());

    const fh2 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 2), fh2.field_id);
    try std.testing.expectEqual(CompactType.@"struct", fh2.field_type);
    r.pushStruct();
    const inner_fh = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), inner_fh.field_id);
    try std.testing.expectEqualStrings("inner", try r.readString());
    const inner_stop = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, inner_stop.field_type);
    r.popStruct();

    const fh3 = try r.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 3), fh3.field_id);
    try std.testing.expectEqual(@as(i64, 99), try r.readZigZagI64());

    const stop = try r.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, stop.field_type);
    r.popStruct();
}
