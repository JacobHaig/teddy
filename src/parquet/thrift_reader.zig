const std = @import("std");

// ============================================================
// Thrift Compact Protocol Types
// ============================================================

pub const CompactType = enum(u4) {
    stop = 0,
    boolean_true = 1,
    boolean_false = 2,
    i8 = 3,
    i16 = 4,
    i32 = 5,
    i64 = 6,
    double = 7,
    binary = 8,
    list = 9,
    set = 10,
    map = 11,
    @"struct" = 12,
};

pub const FieldHeader = struct {
    field_id: i16,
    field_type: CompactType,
};

pub const ListHeader = struct {
    elem_type: CompactType,
    size: u32,
};

// ============================================================
// Thrift Compact Protocol Reader
// ============================================================

/// Decodes Thrift Compact Protocol encoded data.
/// All reads are from a byte slice; no allocations are performed.
pub const ThriftReader = struct {
    data: []const u8,
    pos: usize,
    last_field_id: i16,
    // Stack for nested struct field ID tracking
    field_id_stack: [16]i16,
    stack_depth: usize,

    pub fn init(data: []const u8) ThriftReader {
        return .{
            .data = data,
            .pos = 0,
            .last_field_id = 0,
            .field_id_stack = [_]i16{0} ** 16,
            .stack_depth = 0,
        };
    }

    // ---- Primitive readers ----

    /// Read an unsigned variable-length integer (1-10 bytes, 7 bits per byte, MSB = continuation).
    pub fn readVarint(self: *ThriftReader) !u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            if (self.pos >= self.data.len) return error.UnexpectedEof;
            const byte = self.data[self.pos];
            self.pos += 1;
            result |= @as(u64, byte & 0x7F) << shift;
            if (byte & 0x80 == 0) return result;
            shift += 7;
            if (shift >= 64) return error.VarintTooLong;
        }
    }

    /// Read a zigzag-encoded i32 (stored as varint).
    pub fn readZigZagI32(self: *ThriftReader) !i32 {
        const n = try self.readVarint();
        const unsigned: u32 = @intCast(n);
        return zigzagDecode32(unsigned);
    }

    /// Read a zigzag-encoded i64 (stored as varint).
    pub fn readZigZagI64(self: *ThriftReader) !i64 {
        const n = try self.readVarint();
        return zigzagDecode64(n);
    }

    /// Read a zigzag-encoded i16 (stored as varint).
    pub fn readZigZagI16(self: *ThriftReader) !i16 {
        const n = try self.readVarint();
        const unsigned: u32 = @intCast(n);
        return @intCast(zigzagDecode32(unsigned));
    }

    /// Read a single byte.
    pub fn readByte(self: *ThriftReader) !u8 {
        if (self.pos >= self.data.len) return error.UnexpectedEof;
        const b = self.data[self.pos];
        self.pos += 1;
        return b;
    }

    /// Read an IEEE 754 double (8 bytes, little-endian).
    pub fn readDouble(self: *ThriftReader) !f64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEof;
        const bytes = self.data[self.pos..][0..8];
        self.pos += 8;
        return @bitCast(std.mem.readInt(u64, bytes, .little));
    }

    /// Read a binary field: varint length prefix followed by raw bytes.
    /// Returns a slice into the underlying data buffer (zero-copy).
    pub fn readBinary(self: *ThriftReader) ![]const u8 {
        const len: usize = @intCast(try self.readVarint());
        if (self.pos + len > self.data.len) return error.UnexpectedEof;
        const slice = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return slice;
    }

    /// Alias for readBinary — Thrift strings are encoded the same as binary.
    pub fn readString(self: *ThriftReader) ![]const u8 {
        return self.readBinary();
    }

    // ---- Struct / field navigation ----

    /// Read a field header. Returns the field ID and type.
    /// If the type is .stop, the struct is complete.
    pub fn readFieldHeader(self: *ThriftReader) !FieldHeader {
        const byte = try self.readByte();
        const type_nibble: u4 = @intCast(byte & 0x0F);
        const field_type: CompactType = @enumFromInt(type_nibble);

        if (field_type == .stop) {
            return .{ .field_id = 0, .field_type = .stop };
        }

        const delta_nibble: u4 = @intCast((byte >> 4) & 0x0F);

        var field_id: i16 = undefined;
        if (delta_nibble == 0) {
            // Absolute field ID: read zigzag i16
            field_id = try self.readZigZagI16();
        } else {
            // Delta field ID
            field_id = self.last_field_id + @as(i16, delta_nibble);
        }

        self.last_field_id = field_id;
        return .{ .field_id = field_id, .field_type = field_type };
    }

    /// Read a list header: element type + size.
    pub fn readListHeader(self: *ThriftReader) !ListHeader {
        const byte = try self.readByte();
        const size_nibble: u4 = @intCast((byte >> 4) & 0x0F);
        const elem_type: CompactType = @enumFromInt(@as(u4, @intCast(byte & 0x0F)));

        var size: u32 = undefined;
        if (size_nibble == 0x0F) {
            // Large list: read varint for size
            size = @intCast(try self.readVarint());
        } else {
            size = size_nibble;
        }

        return .{ .elem_type = elem_type, .size = size };
    }

    /// Push the current field ID state before entering a nested struct.
    pub fn pushStruct(self: *ThriftReader) void {
        if (self.stack_depth < self.field_id_stack.len) {
            self.field_id_stack[self.stack_depth] = self.last_field_id;
            self.stack_depth += 1;
        }
        self.last_field_id = 0;
    }

    /// Pop the field ID state after leaving a nested struct.
    pub fn popStruct(self: *ThriftReader) void {
        if (self.stack_depth > 0) {
            self.stack_depth -= 1;
            self.last_field_id = self.field_id_stack[self.stack_depth];
        }
    }

    /// Skip an unknown field of the given type.
    pub fn skip(self: *ThriftReader, compact_type: CompactType) !void {
        switch (compact_type) {
            .stop => {},
            .boolean_true, .boolean_false => {},
            .i8 => self.pos += 1,
            .i16, .i32, .i64 => _ = try self.readVarint(),
            .double => self.pos += 8,
            .binary => {
                const len: usize = @intCast(try self.readVarint());
                self.pos += len;
            },
            .list, .set => {
                const header = try self.readListHeader();
                for (0..header.size) |_| {
                    try self.skip(header.elem_type);
                }
            },
            .map => {
                const size: u32 = @intCast(try self.readVarint());
                if (size > 0) {
                    const type_byte = try self.readByte();
                    const key_type: CompactType = @enumFromInt(@as(u4, @intCast((type_byte >> 4) & 0x0F)));
                    const val_type: CompactType = @enumFromInt(@as(u4, @intCast(type_byte & 0x0F)));
                    for (0..size) |_| {
                        try self.skip(key_type);
                        try self.skip(val_type);
                    }
                }
            },
            .@"struct" => {
                self.pushStruct();
                while (true) {
                    const fh = try self.readFieldHeader();
                    if (fh.field_type == .stop) break;
                    try self.skip(fh.field_type);
                }
                self.popStruct();
            },
        }
    }

    /// Check if we've consumed all data.
    pub fn isEof(self: *const ThriftReader) bool {
        return self.pos >= self.data.len;
    }
};

// ============================================================
// Helpers
// ============================================================

fn zigzagDecode32(n: u32) i32 {
    return @bitCast((n >> 1) ^ (-%((n & 1))));
}

fn zigzagDecode64(n: u64) i64 {
    return @bitCast((n >> 1) ^ (-%((n & 1))));
}

// ============================================================
// Tests
// ============================================================

test "readVarint: single byte" {
    var reader = ThriftReader.init(&.{0x05});
    try std.testing.expectEqual(@as(u64, 5), try reader.readVarint());
}

test "readVarint: multi-byte (150)" {
    // 150 = 0b10010110 → encoded as [0x96, 0x01]
    var reader = ThriftReader.init(&.{ 0x96, 0x01 });
    try std.testing.expectEqual(@as(u64, 150), try reader.readVarint());
}

test "readVarint: zero" {
    var reader = ThriftReader.init(&.{0x00});
    try std.testing.expectEqual(@as(u64, 0), try reader.readVarint());
}

test "zigzag decode" {
    // 0 -> 0, 1 -> -1, 2 -> 1, 3 -> -2, 4 -> 2
    try std.testing.expectEqual(@as(i32, 0), zigzagDecode32(0));
    try std.testing.expectEqual(@as(i32, -1), zigzagDecode32(1));
    try std.testing.expectEqual(@as(i32, 1), zigzagDecode32(2));
    try std.testing.expectEqual(@as(i32, -2), zigzagDecode32(3));
    try std.testing.expectEqual(@as(i32, 2), zigzagDecode32(4));
}

test "readZigZagI32" {
    // Zigzag(2) = varint(4) = [0x04]
    var reader = ThriftReader.init(&.{0x04});
    try std.testing.expectEqual(@as(i32, 2), try reader.readZigZagI32());
}

test "readZigZagI32: negative" {
    // Zigzag(-1) = varint(1) = [0x01]
    var reader = ThriftReader.init(&.{0x01});
    try std.testing.expectEqual(@as(i32, -1), try reader.readZigZagI32());
}

test "readByte" {
    var reader = ThriftReader.init(&.{ 0xAB, 0xCD });
    try std.testing.expectEqual(@as(u8, 0xAB), try reader.readByte());
    try std.testing.expectEqual(@as(u8, 0xCD), try reader.readByte());
}

test "readDouble" {
    // 3.14 in IEEE 754 LE
    const bytes = std.mem.toBytes(@as(u64, @bitCast(@as(f64, 3.14))));
    var reader = ThriftReader.init(&bytes);
    try std.testing.expectEqual(@as(f64, 3.14), try reader.readDouble());
}

test "readBinary" {
    // length=5 (varint), then "hello"
    var reader = ThriftReader.init(&.{ 0x05, 'h', 'e', 'l', 'l', 'o' });
    const result = try reader.readBinary();
    try std.testing.expectEqualStrings("hello", result);
}

test "readFieldHeader: delta encoding" {
    // Byte 0x15 = high nibble 1 (delta=1), low nibble 5 (i32)
    // Byte 0x25 = high nibble 2 (delta=2), low nibble 5 (i32)
    var reader = ThriftReader.init(&.{ 0x15, 0x25, 0x00 });
    const fh1 = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), fh1.field_id);
    try std.testing.expectEqual(CompactType.i32, fh1.field_type);

    const fh2 = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 3), fh2.field_id);
    try std.testing.expectEqual(CompactType.i32, fh2.field_type);

    const fh3 = try reader.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, fh3.field_type);
}

test "readFieldHeader: boolean true/false in type nibble" {
    // 0x11 = delta=1, type=boolean_true
    // 0x12 = delta=1, type=boolean_false
    var reader = ThriftReader.init(&.{ 0x11, 0x12 });
    const fh1 = try reader.readFieldHeader();
    try std.testing.expectEqual(CompactType.boolean_true, fh1.field_type);
    const fh2 = try reader.readFieldHeader();
    try std.testing.expectEqual(CompactType.boolean_false, fh2.field_type);
}

test "readListHeader: small list" {
    // 0x35 = size=3, elem_type=5 (i32)
    var reader = ThriftReader.init(&.{0x35});
    const header = try reader.readListHeader();
    try std.testing.expectEqual(@as(u32, 3), header.size);
    try std.testing.expectEqual(CompactType.i32, header.elem_type);
}

test "readListHeader: large list" {
    // 0xF5 = size=0xF (large), elem_type=5, followed by varint(20)
    var reader = ThriftReader.init(&.{ 0xF5, 0x14 });
    const header = try reader.readListHeader();
    try std.testing.expectEqual(@as(u32, 20), header.size);
    try std.testing.expectEqual(CompactType.i32, header.elem_type);
}

test "pushStruct/popStruct preserves field ID" {
    var reader = ThriftReader.init(&.{});
    reader.last_field_id = 5;
    reader.pushStruct();
    try std.testing.expectEqual(@as(i16, 0), reader.last_field_id);
    reader.last_field_id = 10;
    reader.popStruct();
    try std.testing.expectEqual(@as(i16, 5), reader.last_field_id);
}

test "skip: various types" {
    // i32 varint (value 100 = zigzag 200 = varint [0xC8, 0x01])
    // binary (length=3, "abc")
    // struct with one i8 field + stop
    const data = [_]u8{
        0xC8, 0x01, // i32 (varint)
        0x03, 'a', 'b', 'c', // binary
        0x13, 0xFF, 0x00, // struct: field delta=1 type=i8, value=0xFF, stop
    };
    var reader = ThriftReader.init(&data);
    try reader.skip(.i32);
    try reader.skip(.binary);
    try reader.skip(.@"struct");
    try std.testing.expect(reader.isEof());
}

test "readVarint: max single byte (127)" {
    var reader = ThriftReader.init(&.{0x7F});
    try std.testing.expectEqual(@as(u64, 127), try reader.readVarint());
}

test "readVarint: boundary 128" {
    // 128 = 0x80 → encoded as [0x80, 0x01]
    var reader = ThriftReader.init(&.{ 0x80, 0x01 });
    try std.testing.expectEqual(@as(u64, 128), try reader.readVarint());
}

test "readVarint: large value (300)" {
    // 300 = 0x012C → encoded as [0xAC, 0x02]
    var reader = ThriftReader.init(&.{ 0xAC, 0x02 });
    try std.testing.expectEqual(@as(u64, 300), try reader.readVarint());
}

test "readVarint: EOF returns error" {
    var reader = ThriftReader.init(&.{});
    try std.testing.expectError(error.UnexpectedEof, reader.readVarint());
}

test "readVarint: incomplete multi-byte returns error" {
    // Continuation bit set but no more bytes
    var reader = ThriftReader.init(&.{0x80});
    try std.testing.expectError(error.UnexpectedEof, reader.readVarint());
}

test "readZigZagI64" {
    // zigzag(100) = 200 = varint [0xC8, 0x01]
    var reader = ThriftReader.init(&.{ 0xC8, 0x01 });
    try std.testing.expectEqual(@as(i64, 100), try reader.readZigZagI64());
}

test "readZigZagI64: negative" {
    // zigzag(-100) = 199 = varint [0xC7, 0x01]
    var reader = ThriftReader.init(&.{ 0xC7, 0x01 });
    try std.testing.expectEqual(@as(i64, -100), try reader.readZigZagI64());
}

test "readZigZagI16" {
    // zigzag(10) = 20 = varint [0x14]
    var reader = ThriftReader.init(&.{0x14});
    try std.testing.expectEqual(@as(i16, 10), try reader.readZigZagI16());
}

test "readByte: EOF returns error" {
    var reader = ThriftReader.init(&.{});
    try std.testing.expectError(error.UnexpectedEof, reader.readByte());
}

test "readDouble: EOF returns error" {
    var reader = ThriftReader.init(&.{ 0x00, 0x00, 0x00 }); // only 3 bytes
    try std.testing.expectError(error.UnexpectedEof, reader.readDouble());
}

test "readBinary: EOF on length returns error" {
    var reader = ThriftReader.init(&.{});
    try std.testing.expectError(error.UnexpectedEof, reader.readBinary());
}

test "readBinary: EOF on data returns error" {
    // Length = 10 but only 3 bytes of data
    var reader = ThriftReader.init(&.{ 0x0A, 'a', 'b', 'c' });
    try std.testing.expectError(error.UnexpectedEof, reader.readBinary());
}

test "readBinary: empty string" {
    var reader = ThriftReader.init(&.{0x00});
    const result = try reader.readBinary();
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "readFieldHeader: absolute field ID (delta=0)" {
    // When high nibble is 0, read zigzag i16 for absolute field ID
    // 0x05 = delta=0, type=i32(5), then zigzag(100)=200=varint [0xC8, 0x01]
    var reader = ThriftReader.init(&.{ 0x05, 0xC8, 0x01 });
    const fh = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 100), fh.field_id);
    try std.testing.expectEqual(CompactType.i32, fh.field_type);
}

test "isEof: empty reader" {
    var reader = ThriftReader.init(&.{});
    try std.testing.expect(reader.isEof());
}

test "isEof: after reading all data" {
    var reader = ThriftReader.init(&.{0x05});
    try std.testing.expect(!reader.isEof());
    _ = try reader.readVarint();
    try std.testing.expect(reader.isEof());
}

test "skip: list of i32" {
    // List header: 3 elements of type i32 = 0x35
    // 3 zigzag values: 0x00, 0x02, 0x04 → 0, 1, 2
    const data = [_]u8{ 0x35, 0x00, 0x02, 0x04 };
    var reader = ThriftReader.init(&data);
    try reader.skip(.list);
    try std.testing.expect(reader.isEof());
}

test "skip: double" {
    const data = [_]u8{0} ** 8;
    var reader = ThriftReader.init(&data);
    try reader.skip(.double);
    try std.testing.expect(reader.isEof());
}

test "skip: boolean types consume no bytes" {
    var reader = ThriftReader.init(&.{});
    try reader.skip(.boolean_true);
    try reader.skip(.boolean_false);
    try reader.skip(.stop);
    try std.testing.expect(reader.isEof());
}

test "skip: map" {
    // Empty map: size=0 (varint)
    var reader = ThriftReader.init(&.{0x00});
    try reader.skip(.map);
    try std.testing.expect(reader.isEof());
}

test "skip: non-empty map" {
    // Map with 1 entry: size=1, key_type=i32(5)|val_type=i32(5) = 0x55
    // key: varint 0x00, value: varint 0x02
    const data = [_]u8{ 0x01, 0x55, 0x00, 0x02 };
    var reader = ThriftReader.init(&data);
    try reader.skip(.map);
    try std.testing.expect(reader.isEof());
}

test "nested struct: two levels of pushStruct/popStruct" {
    var reader = ThriftReader.init(&.{});
    reader.last_field_id = 5;
    reader.pushStruct(); // level 1
    reader.last_field_id = 3;
    reader.pushStruct(); // level 2
    try std.testing.expectEqual(@as(i16, 0), reader.last_field_id);
    reader.last_field_id = 7;
    reader.popStruct(); // back to level 1
    try std.testing.expectEqual(@as(i16, 3), reader.last_field_id);
    reader.popStruct(); // back to top
    try std.testing.expectEqual(@as(i16, 5), reader.last_field_id);
}

test "multiple readFieldHeaders track sequential field IDs" {
    // Fields 1, 2, 3 with delta encoding
    // 0x15 = delta=1, type=i32; then a varint value
    // 0x15 = delta=1, type=i32; then a varint value
    // 0x15 = delta=1, type=i32; then a varint value
    // 0x00 = stop
    const data = [_]u8{
        0x15, 0x00, // field 1, value=0
        0x15, 0x00, // field 2, value=0
        0x15, 0x00, // field 3, value=0
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    reader.pushStruct();

    const fh1 = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 1), fh1.field_id);
    _ = try reader.readVarint(); // consume value

    const fh2 = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 2), fh2.field_id);
    _ = try reader.readVarint();

    const fh3 = try reader.readFieldHeader();
    try std.testing.expectEqual(@as(i16, 3), fh3.field_id);
    _ = try reader.readVarint();

    const fh4 = try reader.readFieldHeader();
    try std.testing.expectEqual(CompactType.stop, fh4.field_type);
    reader.popStruct();
}
