const std = @import("std");
const PlainDecoder = @import("encoding_reader.zig").PlainDecoder;
const RleBitPackedDecoder = @import("encoding_reader.zig").RleBitPackedDecoder;

test "PlainDecoder: readInt32" {
    const data = [_]u8{ 0x2A, 0x00, 0x00, 0x00 }; // 42 LE
    var dec = PlainDecoder.init(&data);
    try std.testing.expectEqual(@as(i32, 42), try dec.readInt32());
}

test "PlainDecoder: readInt64" {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, 123456789, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(@as(i64, 123456789), try dec.readInt64());
}

test "PlainDecoder: readFloat" {
    const val: f32 = 3.14;
    const bits: u32 = @bitCast(val);
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, bits, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(val, try dec.readFloat());
}

test "PlainDecoder: readDouble" {
    const val: f64 = 2.71828;
    const bits: u64 = @bitCast(val);
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, bits, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(val, try dec.readDouble());
}

test "PlainDecoder: readByteArray" {
    // length=5, "hello"
    const data = [_]u8{ 0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o' };
    var dec = PlainDecoder.init(&data);
    const result = try dec.readByteArray();
    try std.testing.expectEqualStrings("hello", result);
}

test "PlainDecoder: readBooleans" {
    const allocator = std.testing.allocator;
    // 0b00000101 = bits: true, false, true, false, false, false, false, false
    const data = [_]u8{0x05};
    var dec = PlainDecoder.init(&data);
    const bools = try dec.readBooleans(3, allocator);
    defer allocator.free(bools);
    try std.testing.expect(bools[0] == true);
    try std.testing.expect(bools[1] == false);
    try std.testing.expect(bools[2] == true);
}

test "PlainDecoder: readInt32Batch" {
    const allocator = std.testing.allocator;
    var buf: [12]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 10, .little);
    std.mem.writeInt(i32, buf[4..8], 20, .little);
    std.mem.writeInt(i32, buf[8..12], 30, .little);
    var dec = PlainDecoder.init(&buf);
    const batch = try dec.readInt32Batch(3, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(i32, 10), batch[0]);
    try std.testing.expectEqual(@as(i32, 20), batch[1]);
    try std.testing.expectEqual(@as(i32, 30), batch[2]);
}

test "RleBitPackedDecoder: RLE run" {
    const allocator = std.testing.allocator;
    // RLE header: count=3, so header = (3 << 1) | 0 = 6 = varint [0x06]
    // Value: bit_width=8, so 1 byte: 0x07 (value=7)
    const data = [_]u8{ 0x06, 0x07 };
    var dec = RleBitPackedDecoder.init(&data, 8);
    const batch = try dec.readBatch(3, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(u32, 7), batch[0]);
    try std.testing.expectEqual(@as(u32, 7), batch[1]);
    try std.testing.expectEqual(@as(u32, 7), batch[2]);
}

test "RleBitPackedDecoder: bit-packed run" {
    const allocator = std.testing.allocator;
    // Bit-packed header: count=8 values, so header = (1 << 1) | 1 = 3 = varint [0x03]
    // bit_width=1, 8 values packed into 1 byte: 0b10101010 = 0xAA
    // Values (LSB first): 0,1,0,1,0,1,0,1
    const data = [_]u8{ 0x03, 0xAA };
    var dec = RleBitPackedDecoder.init(&data, 1);
    const batch = try dec.readBatch(8, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(u32, 0), batch[0]);
    try std.testing.expectEqual(@as(u32, 1), batch[1]);
    try std.testing.expectEqual(@as(u32, 0), batch[2]);
    try std.testing.expectEqual(@as(u32, 1), batch[3]);
}

test "RleBitPackedDecoder: bit_width=0 (all zeros)" {
    const allocator = std.testing.allocator;
    // RLE header: count=5, header = (5 << 1) | 0 = 10 = [0x0A]
    // Value with 0 bytes (bit_width=0) → always 0
    const data = [_]u8{0x0A};
    var dec = RleBitPackedDecoder.init(&data, 0);
    const batch = try dec.readBatch(5, allocator);
    defer allocator.free(batch);
    for (batch) |v| {
        try std.testing.expectEqual(@as(u32, 0), v);
    }
}

test "PlainDecoder: readFixedByteArray" {
    const data = [_]u8{ 'a', 'b', 'c', 'd' };
    var dec = PlainDecoder.init(&data);
    const result = try dec.readFixedByteArray(3);
    try std.testing.expectEqualStrings("abc", result);
    // Remaining byte
    const result2 = try dec.readFixedByteArray(1);
    try std.testing.expectEqualStrings("d", result2);
}

test "PlainDecoder: readFixedByteArray EOF" {
    const data = [_]u8{ 'a', 'b' };
    var dec = PlainDecoder.init(&data);
    try std.testing.expectError(error.UnexpectedEof, dec.readFixedByteArray(3));
}

test "PlainDecoder: readByteArrayBatch" {
    const allocator = std.testing.allocator;
    // Two byte arrays: "hi" (len=2) and "bye" (len=3)
    const data = [_]u8{
        0x02, 0x00, 0x00, 0x00, 'h', 'i',
        0x03, 0x00, 0x00, 0x00, 'b', 'y', 'e',
    };
    var dec = PlainDecoder.init(&data);
    const batch = try dec.readByteArrayBatch(2, allocator);
    defer {
        for (batch) |item| allocator.free(item);
        allocator.free(batch);
    }
    try std.testing.expectEqualStrings("hi", batch[0]);
    try std.testing.expectEqualStrings("bye", batch[1]);
}

test "PlainDecoder: readFloat negative" {
    const val: f32 = -2.5;
    const bits: u32 = @bitCast(val);
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, bits, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(val, try dec.readFloat());
}

test "PlainDecoder: readDouble negative" {
    const val: f64 = -123.456;
    const bits: u64 = @bitCast(val);
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, bits, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(val, try dec.readDouble());
}

test "PlainDecoder: readInt32 EOF" {
    const data = [_]u8{ 0x01, 0x02 }; // only 2 bytes
    var dec = PlainDecoder.init(&data);
    try std.testing.expectError(error.UnexpectedEof, dec.readInt32());
}

test "PlainDecoder: readInt64 EOF" {
    const data = [_]u8{ 0x01, 0x02, 0x03, 0x04 }; // only 4 bytes
    var dec = PlainDecoder.init(&data);
    try std.testing.expectError(error.UnexpectedEof, dec.readInt64());
}

test "PlainDecoder: readByteArray EOF on length" {
    const data = [_]u8{ 0x01, 0x02 }; // only 2 bytes for 4-byte length
    var dec = PlainDecoder.init(&data);
    try std.testing.expectError(error.UnexpectedEof, dec.readByteArray());
}

test "PlainDecoder: readByteArray EOF on data" {
    // Length=10 but only 2 data bytes
    const data = [_]u8{ 0x0A, 0x00, 0x00, 0x00, 'a', 'b' };
    var dec = PlainDecoder.init(&data);
    try std.testing.expectError(error.UnexpectedEof, dec.readByteArray());
}

test "PlainDecoder: readInt32 negative value" {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &buf, -42, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(@as(i32, -42), try dec.readInt32());
}

test "PlainDecoder: readInt64 large value" {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &buf, 9999999999, .little);
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(@as(i64, 9999999999), try dec.readInt64());
}

test "PlainDecoder: readBooleans multiple bytes" {
    const allocator = std.testing.allocator;
    // 10 booleans across 2 bytes
    // byte 0: 0b11001010 → false, true, false, true, false, false, true, true
    // byte 1: 0b00000010 → false, true
    const data = [_]u8{ 0xCA, 0x02 };
    var dec = PlainDecoder.init(&data);
    const bools = try dec.readBooleans(10, allocator);
    defer allocator.free(bools);
    // byte 0 LSB first: bit0=0, bit1=1, bit2=0, bit3=1, bit4=0, bit5=0, bit6=1, bit7=1
    try std.testing.expect(bools[0] == false);
    try std.testing.expect(bools[1] == true);
    try std.testing.expect(bools[2] == false);
    try std.testing.expect(bools[3] == true);
    try std.testing.expect(bools[4] == false);
    try std.testing.expect(bools[5] == false);
    try std.testing.expect(bools[6] == true);
    try std.testing.expect(bools[7] == true);
    // byte 1: bit0=0, bit1=1
    try std.testing.expect(bools[8] == false);
    try std.testing.expect(bools[9] == true);
}

test "PlainDecoder: readInt64Batch" {
    const allocator = std.testing.allocator;
    var buf: [16]u8 = undefined;
    std.mem.writeInt(i64, buf[0..8], 100, .little);
    std.mem.writeInt(i64, buf[8..16], -200, .little);
    var dec = PlainDecoder.init(&buf);
    const batch = try dec.readInt64Batch(2, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(i64, 100), batch[0]);
    try std.testing.expectEqual(@as(i64, -200), batch[1]);
}

test "PlainDecoder: readFloatBatch" {
    const allocator = std.testing.allocator;
    var buf: [8]u8 = undefined;
    const v1: f32 = 1.5;
    const v2: f32 = -3.0;
    std.mem.writeInt(u32, buf[0..4], @bitCast(v1), .little);
    std.mem.writeInt(u32, buf[4..8], @bitCast(v2), .little);
    var dec = PlainDecoder.init(&buf);
    const batch = try dec.readFloatBatch(2, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(v1, batch[0]);
    try std.testing.expectEqual(v2, batch[1]);
}

test "PlainDecoder: readDoubleBatch" {
    const allocator = std.testing.allocator;
    var buf: [16]u8 = undefined;
    const v1: f64 = 2.718;
    const v2: f64 = -1.414;
    std.mem.writeInt(u64, buf[0..8], @bitCast(v1), .little);
    std.mem.writeInt(u64, buf[8..16], @bitCast(v2), .little);
    var dec = PlainDecoder.init(&buf);
    const batch = try dec.readDoubleBatch(2, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(v1, batch[0]);
    try std.testing.expectEqual(v2, batch[1]);
}

test "PlainDecoder: sequential reads" {
    // Read an int32 then a byte_array from the same decoder
    var buf: [11]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 42, .little);
    // byte_array: length=3, "abc"
    std.mem.writeInt(u32, buf[4..8], 3, .little);
    buf[8] = 'a';
    buf[9] = 'b';
    buf[10] = 'c';
    var dec = PlainDecoder.init(&buf);
    try std.testing.expectEqual(@as(i32, 42), try dec.readInt32());
    try std.testing.expectEqualStrings("abc", try dec.readByteArray());
}

test "RleBitPackedDecoder: RLE run with bit_width=16" {
    const allocator = std.testing.allocator;
    // RLE header: count=2, header = (2 << 1) | 0 = 4 = [0x04]
    // Value: 2 bytes LE for bit_width=16: 0x00, 0x01 → 256
    const data = [_]u8{ 0x04, 0x00, 0x01 };
    var dec = RleBitPackedDecoder.init(&data, 16);
    const batch = try dec.readBatch(2, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(u32, 256), batch[0]);
    try std.testing.expectEqual(@as(u32, 256), batch[1]);
}

test "RleBitPackedDecoder: bit-packed with bit_width=2" {
    const allocator = std.testing.allocator;
    // Bit-packed header: 1 group of 8 values → header = (1 << 1) | 1 = 3 = [0x03]
    // bit_width=2, 8 values = 16 bits = 2 bytes
    // Values: 0, 1, 2, 3, 0, 1, 2, 3
    // Packed: 00 01 10 11 00 01 10 11 = 0b11100100 0b11100100 = 0xE4 0xE4
    const data = [_]u8{ 0x03, 0xE4, 0xE4 };
    var dec = RleBitPackedDecoder.init(&data, 2);
    const batch = try dec.readBatch(8, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(u32, 0), batch[0]);
    try std.testing.expectEqual(@as(u32, 1), batch[1]);
    try std.testing.expectEqual(@as(u32, 2), batch[2]);
    try std.testing.expectEqual(@as(u32, 3), batch[3]);
    try std.testing.expectEqual(@as(u32, 0), batch[4]);
    try std.testing.expectEqual(@as(u32, 1), batch[5]);
    try std.testing.expectEqual(@as(u32, 2), batch[6]);
    try std.testing.expectEqual(@as(u32, 3), batch[7]);
}

test "RleBitPackedDecoder: mixed RLE and bit-packed runs" {
    const allocator = std.testing.allocator;
    // RLE run of 3 × value 5, then bit-packed run of 8 values
    // bit_width = 4

    // RLE: header = (3 << 1) | 0 = 6 = [0x06], value = 0x05 (1 byte for bw=4)
    // Bit-packed: header = (1 << 1) | 1 = 3 = [0x03]
    //   8 values × 4 bits = 32 bits = 4 bytes
    //   Values: 0,1,2,3,4,5,6,7
    //   Packed LSB first: nibble pairs: 0x10, 0x32, 0x54, 0x76
    const data = [_]u8{
        0x06, 0x05, // RLE: 3 × 5
        0x03, 0x10, 0x32, 0x54, 0x76, // Bit-packed: 0,1,2,3,4,5,6,7
    };
    var dec = RleBitPackedDecoder.init(&data, 4);
    const batch = try dec.readBatch(11, allocator);
    defer allocator.free(batch);
    // RLE values
    try std.testing.expectEqual(@as(u32, 5), batch[0]);
    try std.testing.expectEqual(@as(u32, 5), batch[1]);
    try std.testing.expectEqual(@as(u32, 5), batch[2]);
    // Bit-packed values
    try std.testing.expectEqual(@as(u32, 0), batch[3]);
    try std.testing.expectEqual(@as(u32, 1), batch[4]);
    try std.testing.expectEqual(@as(u32, 2), batch[5]);
    try std.testing.expectEqual(@as(u32, 3), batch[6]);
    try std.testing.expectEqual(@as(u32, 4), batch[7]);
    try std.testing.expectEqual(@as(u32, 5), batch[8]);
    try std.testing.expectEqual(@as(u32, 6), batch[9]);
    try std.testing.expectEqual(@as(u32, 7), batch[10]);
}

test "RleBitPackedDecoder: single value RLE" {
    const allocator = std.testing.allocator;
    // RLE header: count=1, header = (1 << 1) | 0 = 2 = [0x02]
    // Value: bit_width=8, 1 byte: 0xFF (255)
    const data = [_]u8{ 0x02, 0xFF };
    var dec = RleBitPackedDecoder.init(&data, 8);
    const val = try dec.next();
    _ = allocator;
    try std.testing.expectEqual(@as(u32, 255), val);
}

test "RleBitPackedDecoder: initWithLengthPrefix" {
    const allocator = std.testing.allocator;
    // 4-byte LE length prefix = 2, then RLE data
    // RLE: count=3, value=1, bit_width=1
    const data = [_]u8{
        0x02, 0x00, 0x00, 0x00, // length prefix = 2
        0x06, 0x01, // RLE: 3 × 1
    };
    var dec = RleBitPackedDecoder.initWithLengthPrefix(&data);
    dec.bit_width = 1;
    const batch = try dec.readBatch(3, allocator);
    defer allocator.free(batch);
    try std.testing.expectEqual(@as(u32, 1), batch[0]);
    try std.testing.expectEqual(@as(u32, 1), batch[1]);
    try std.testing.expectEqual(@as(u32, 1), batch[2]);
}
