const std = @import("std");
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const thrift = @import("thrift_reader.zig");
const CompactType = thrift.CompactType;
const ThriftReader = thrift.ThriftReader;
const zigzagEncode32 = @import("thrift_writer.zig").zigzagEncode32;

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
