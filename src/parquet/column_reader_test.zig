const std = @import("std");
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const encoding = @import("encoding_reader.zig");
const column_reader = @import("column_reader.zig");
const DictionaryStore = column_reader.DictionaryStore;
const ValueAccumulator = column_reader.ValueAccumulator;

test "DictionaryStore: init and deinit with int32 values" {
    const allocator = std.testing.allocator;
    // PLAIN-encoded int32 dictionary: 3 values (10, 20, 30)
    var buf: [12]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 10, .little);
    std.mem.writeInt(i32, buf[4..8], 20, .little);
    std.mem.writeInt(i32, buf[8..12], 30, .little);

    var dict = try DictionaryStore.init(allocator, &buf, .int32, 0, 3);
    defer dict.deinit();

    try std.testing.expectEqual(@as(usize, 3), dict.int32s.items.len);
    try std.testing.expectEqual(@as(i32, 10), dict.int32s.items[0]);
    try std.testing.expectEqual(@as(i32, 20), dict.int32s.items[1]);
    try std.testing.expectEqual(@as(i32, 30), dict.int32s.items[2]);
}

test "DictionaryStore: init with byte_array values" {
    const allocator = std.testing.allocator;
    // Two PLAIN byte_arrays: "hi" (len=2) and "bye" (len=3)
    const buf = [_]u8{
        0x02, 0x00, 0x00, 0x00, 'h', 'i',
        0x03, 0x00, 0x00, 0x00, 'b', 'y', 'e',
    };

    var dict = try DictionaryStore.init(allocator, &buf, .byte_array, 0, 2);
    defer dict.deinit();

    try std.testing.expectEqual(@as(usize, 2), dict.byte_arrays.items.len);
    try std.testing.expectEqualStrings("hi", dict.byte_arrays.items[0]);
    try std.testing.expectEqualStrings("bye", dict.byte_arrays.items[1]);
}

test "DictionaryStore: init with float values" {
    const allocator = std.testing.allocator;
    var buf: [8]u8 = undefined;
    const v1: f32 = 1.5;
    const v2: f32 = -3.0;
    std.mem.writeInt(u32, buf[0..4], @bitCast(v1), .little);
    std.mem.writeInt(u32, buf[4..8], @bitCast(v2), .little);

    var dict = try DictionaryStore.init(allocator, &buf, .float, 0, 2);
    defer dict.deinit();

    try std.testing.expectEqual(@as(usize, 2), dict.floats.items.len);
    try std.testing.expectEqual(v1, dict.floats.items[0]);
    try std.testing.expectEqual(v2, dict.floats.items[1]);
}

test "DictionaryStore: init with double values" {
    const allocator = std.testing.allocator;
    var buf: [16]u8 = undefined;
    const v1: f64 = 2.718;
    const v2: f64 = 3.14;
    std.mem.writeInt(u64, buf[0..8], @bitCast(v1), .little);
    std.mem.writeInt(u64, buf[8..16], @bitCast(v2), .little);

    var dict = try DictionaryStore.init(allocator, &buf, .double, 0, 2);
    defer dict.deinit();

    try std.testing.expectEqual(@as(usize, 2), dict.doubles.items.len);
    try std.testing.expectEqual(v1, dict.doubles.items[0]);
    try std.testing.expectEqual(v2, dict.doubles.items[1]);
}

test "DictionaryStore: init with int64 values" {
    const allocator = std.testing.allocator;
    var buf: [16]u8 = undefined;
    std.mem.writeInt(i64, buf[0..8], 1000000, .little);
    std.mem.writeInt(i64, buf[8..16], -999, .little);

    var dict = try DictionaryStore.init(allocator, &buf, .int64, 0, 2);
    defer dict.deinit();

    try std.testing.expectEqual(@as(usize, 2), dict.int64s.items.len);
    try std.testing.expectEqual(@as(i64, 1000000), dict.int64s.items[0]);
    try std.testing.expectEqual(@as(i64, -999), dict.int64s.items[1]);
}

test "ValueAccumulator: decodePlain int32" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .int32, 0);
    defer acc.deinit();

    var buf: [8]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 42, .little);
    std.mem.writeInt(i32, buf[4..8], -7, .little);
    var dec = encoding.PlainDecoder.init(&buf);

    try acc.decodePlain(&dec, 2, allocator);
    try std.testing.expectEqual(@as(usize, 2), acc.int32s.items.len);
    try std.testing.expectEqual(@as(i32, 42), acc.int32s.items[0]);
    try std.testing.expectEqual(@as(i32, -7), acc.int32s.items[1]);
}

test "ValueAccumulator: decodePlain byte_array" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .byte_array, 0);
    defer acc.deinit();

    const buf = [_]u8{
        0x03, 0x00, 0x00, 0x00, 'f', 'o', 'o',
        0x03, 0x00, 0x00, 0x00, 'b', 'a', 'r',
    };
    var dec = encoding.PlainDecoder.init(&buf);

    try acc.decodePlain(&dec, 2, allocator);
    try std.testing.expectEqual(@as(usize, 2), acc.byte_arrays.items.len);
    try std.testing.expectEqualStrings("foo", acc.byte_arrays.items[0]);
    try std.testing.expectEqualStrings("bar", acc.byte_arrays.items[1]);
}

test "ValueAccumulator: decodePlain boolean" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .boolean, 0);
    defer acc.deinit();

    // 0b00000101 → true, false, true
    const buf = [_]u8{0x05};
    var dec = encoding.PlainDecoder.init(&buf);

    try acc.decodePlain(&dec, 3, allocator);
    try std.testing.expectEqual(@as(usize, 3), acc.booleans.items.len);
    try std.testing.expect(acc.booleans.items[0] == true);
    try std.testing.expect(acc.booleans.items[1] == false);
    try std.testing.expect(acc.booleans.items[2] == true);
}

test "ValueAccumulator: decodeDictionary int32" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .int32, 0);
    defer acc.deinit();

    // Dictionary: [100, 200, 300]
    var dict_buf: [12]u8 = undefined;
    std.mem.writeInt(i32, dict_buf[0..4], 100, .little);
    std.mem.writeInt(i32, dict_buf[4..8], 200, .little);
    std.mem.writeInt(i32, dict_buf[8..12], 300, .little);
    var dict = try DictionaryStore.init(allocator, &dict_buf, .int32, 0, 3);
    defer dict.deinit();

    // RLE indices: 3 values all index 2 → value 300
    // RLE header: count=3, header = (3 << 1) | 0 = 6, value = 2 (1 byte for bw=8)
    const rle_buf = [_]u8{ 0x06, 0x02 };
    var rle_dec = encoding.RleBitPackedDecoder.init(&rle_buf, 8);

    try acc.decodeDictionary(&rle_dec, &dict, 3, .int32, allocator);
    try std.testing.expectEqual(@as(usize, 3), acc.int32s.items.len);
    try std.testing.expectEqual(@as(i32, 300), acc.int32s.items[0]);
    try std.testing.expectEqual(@as(i32, 300), acc.int32s.items[1]);
    try std.testing.expectEqual(@as(i32, 300), acc.int32s.items[2]);
}

test "ValueAccumulator: decodeDictionary byte_array" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .byte_array, 0);
    defer acc.deinit();

    // Dictionary: ["hello", "world"]
    const dict_buf = [_]u8{
        0x05, 0x00, 0x00, 0x00, 'h', 'e', 'l', 'l', 'o',
        0x05, 0x00, 0x00, 0x00, 'w', 'o', 'r', 'l', 'd',
    };
    var dict = try DictionaryStore.init(allocator, &dict_buf, .byte_array, 0, 2);
    defer dict.deinit();

    // RLE indices: [1, 0, 1] → "world", "hello", "world"
    // Bit-packed: 1 group of 8, bit_width=1
    // header = (1 << 1) | 1 = 3
    // Values: 1, 0, 1 + 5 padding zeros = 0b00000101 = 0x05
    const rle_buf = [_]u8{ 0x03, 0x05 };
    var rle_dec = encoding.RleBitPackedDecoder.init(&rle_buf, 1);

    try acc.decodeDictionary(&rle_dec, &dict, 3, .byte_array, allocator);
    try std.testing.expectEqual(@as(usize, 3), acc.byte_arrays.items.len);
    try std.testing.expectEqualStrings("world", acc.byte_arrays.items[0]);
    try std.testing.expectEqualStrings("hello", acc.byte_arrays.items[1]);
    try std.testing.expectEqualStrings("world", acc.byte_arrays.items[2]);
}

test "ValueAccumulator: moveInto transfers int32 ownership" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .int32, 0);
    // Don't defer acc.deinit() — moveInto transfers ownership

    var buf: [4]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 99, .little);
    var dec = encoding.PlainDecoder.init(&buf);
    try acc.decodePlain(&dec, 1, allocator);

    var col = types.ParquetColumn.initEmpty(allocator);
    const name = try allocator.alloc(u8, 1);
    name[0] = 'x';
    col.name = name;
    acc.moveInto(&col);
    defer col.deinit();

    try std.testing.expect(col.int32s != null);
    try std.testing.expectEqual(@as(i32, 99), col.int32s.?[0]);

    // Accumulator should now be empty (deinit is safe)
    acc.deinit();
}

test "ValueAccumulator: expandWithNulls inserts defaults" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .int32, 0);
    defer acc.deinit();

    // Accumulate 2 non-null values
    var buf: [8]u8 = undefined;
    std.mem.writeInt(i32, buf[0..4], 10, .little);
    std.mem.writeInt(i32, buf[4..8], 20, .little);
    var dec = encoding.PlainDecoder.init(&buf);
    try acc.decodePlain(&dec, 2, allocator);

    // Validity: [true, false, true, false] → 4 rows, 2 non-null
    const validity = [_]bool{ true, false, true, false };
    var col = types.ParquetColumn.initEmpty(allocator);
    const name = try allocator.alloc(u8, 1);
    name[0] = 'v';
    col.name = name;

    var vi: usize = 0;
    try acc.expandWithNulls(allocator, &validity, 4, &vi, &col);
    defer col.deinit();

    try std.testing.expect(col.int32s != null);
    const vals = col.int32s.?;
    try std.testing.expectEqual(@as(usize, 4), vals.len);
    try std.testing.expectEqual(@as(i32, 10), vals[0]); // valid
    try std.testing.expectEqual(@as(i32, 0), vals[1]); // null → default
    try std.testing.expectEqual(@as(i32, 20), vals[2]); // valid
    try std.testing.expectEqual(@as(i32, 0), vals[3]); // null → default
}

test "ValueAccumulator: expandWithNulls byte_array" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .byte_array, 0);
    defer acc.deinit();

    // 1 non-null string
    const buf = [_]u8{ 0x02, 0x00, 0x00, 0x00, 'h', 'i' };
    var dec = encoding.PlainDecoder.init(&buf);
    try acc.decodePlain(&dec, 1, allocator);

    // Validity: [false, true, false] → 3 rows, 1 non-null
    const validity = [_]bool{ false, true, false };
    var col = types.ParquetColumn.initEmpty(allocator);
    const name_buf = try allocator.alloc(u8, 1);
    name_buf[0] = 's';
    col.name = name_buf;

    var vi: usize = 0;
    try acc.expandWithNulls(allocator, &validity, 3, &vi, &col);
    defer col.deinit();

    try std.testing.expect(col.byte_arrays != null);
    const arrs = col.byte_arrays.?;
    try std.testing.expectEqual(@as(usize, 3), arrs.len);
    try std.testing.expectEqual(@as(usize, 0), arrs[0].len); // null → empty
    try std.testing.expectEqualStrings("hi", arrs[1]); // valid
    try std.testing.expectEqual(@as(usize, 0), arrs[2].len); // null → empty
}

// ============================================================
// Phase 11 Unit B — malformed-input hardening
// ============================================================

test "P1: decodeDictionary rejects out-of-range index" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .int32, 0);
    defer acc.deinit();

    // Dictionary has 2 entries (indices 0,1 are valid).
    var dict_buf: [8]u8 = undefined;
    std.mem.writeInt(i32, dict_buf[0..4], 100, .little);
    std.mem.writeInt(i32, dict_buf[4..8], 200, .little);
    var dict = try DictionaryStore.init(allocator, &dict_buf, .int32, 0, 2);
    defer dict.deinit();

    // RLE stream of index value 5 (out of range): RLE run, count=1,
    // header = (1<<1)|0 = 2, value byte = 5 (bit_width 8 → 1 value byte).
    const rle_buf = [_]u8{ 0x02, 0x05 };
    var rle_dec = encoding.RleBitPackedDecoder.init(&rle_buf, 8);

    try std.testing.expectError(
        error.InvalidDictionaryIndex,
        acc.decodeDictionary(&rle_dec, &dict, 1, .int32, allocator),
    );
}

test "P1: decodeDictionary rejects out-of-range index for byte_array" {
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .byte_array, 0);
    defer acc.deinit();

    // Single-entry byte_array dictionary: only index 0 is valid.
    const dict_buf = [_]u8{ 0x02, 0x00, 0x00, 0x00, 'h', 'i' };
    var dict = try DictionaryStore.init(allocator, &dict_buf, .byte_array, 0, 1);
    defer dict.deinit();

    // RLE run of index 3 (out of range).
    const rle_buf = [_]u8{ 0x02, 0x03 };
    var rle_dec = encoding.RleBitPackedDecoder.init(&rle_buf, 8);

    try std.testing.expectError(
        error.InvalidDictionaryIndex,
        acc.decodeDictionary(&rle_dec, &dict, 1, .byte_array, allocator),
    );
}

test "P6: expandWithNulls frees untransferred byte_array tail (no leak)" {
    // Decode MORE byte_arrays than the validity array marks present. The leftover
    // accumulator entries must be freed before clearRetainingCapacity, or the
    // testing allocator reports a leak.
    const allocator = std.testing.allocator;
    var acc = ValueAccumulator.init(allocator, .byte_array, 0);
    defer acc.deinit();

    // Accumulate 3 strings.
    const buf = [_]u8{
        0x01, 0x00, 0x00, 0x00, 'a',
        0x01, 0x00, 0x00, 0x00, 'b',
        0x01, 0x00, 0x00, 0x00, 'c',
    };
    var dec = encoding.PlainDecoder.init(&buf);
    try acc.decodePlain(&dec, 3, allocator);

    // Validity marks only 1 row present → vi stops at 1, items[1..] (b, c) are
    // never transferred and must be freed by the fix.
    const validity = [_]bool{true};
    var col = types.ParquetColumn.initEmpty(allocator);
    const name_buf = try allocator.alloc(u8, 1);
    name_buf[0] = 's';
    col.name = name_buf;

    var vi: usize = 0;
    try acc.expandWithNulls(allocator, &validity, 1, &vi, &col);
    defer col.deinit();

    try std.testing.expect(col.byte_arrays != null);
    try std.testing.expectEqual(@as(usize, 1), col.byte_arrays.?.len);
    try std.testing.expectEqualStrings("a", col.byte_arrays.?[0]);
}

test "P4/P5: readColumnChunk rejects negative num_values" {
    const allocator = std.testing.allocator;
    // total_compressed_size 0, but num_values = -1 → CorruptFile before any read.
    const cmd = metadata.ColumnMetaData{
        .type_ = .int32,
        .num_values = -1,
        .total_compressed_size = 0,
        .data_page_offset = 4,
    };
    const chunk = metadata.ColumnChunk{ .meta_data = cmd };
    const leaf = column_reader.LeafColumn{
        .schema_element = .{ .name = "c", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    const file_data = [_]u8{ 'P', 'A', 'R', '1', 0, 0, 0, 0 };
    try std.testing.expectError(
        error.CorruptFile,
        column_reader.readColumnChunk(allocator, &file_data, chunk, leaf, 0),
    );
}

test "P5: readColumnChunk rejects start_offset beyond file end" {
    const allocator = std.testing.allocator;
    // data_page_offset points past the (tiny) file → CorruptFile/OutOfBounds.
    const cmd = metadata.ColumnMetaData{
        .type_ = .int32,
        .num_values = 1,
        .total_compressed_size = 4,
        .data_page_offset = 1000, // well past EOF
    };
    const chunk = metadata.ColumnChunk{ .meta_data = cmd };
    const leaf = column_reader.LeafColumn{
        .schema_element = .{ .name = "c", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    const file_data = [_]u8{ 'P', 'A', 'R', '1', 0, 0, 0, 0 };
    try std.testing.expectError(
        error.ColumnDataOutOfBounds,
        column_reader.readColumnChunk(allocator, &file_data, chunk, leaf, 1),
    );
}

test "P5: readColumnChunk rejects negative offset" {
    const allocator = std.testing.allocator;
    const cmd = metadata.ColumnMetaData{
        .type_ = .int32,
        .num_values = 1,
        .total_compressed_size = 4,
        .data_page_offset = -5, // negative → cast fails
    };
    const chunk = metadata.ColumnChunk{ .meta_data = cmd };
    const leaf = column_reader.LeafColumn{
        .schema_element = .{ .name = "c", .type_ = .int32, .repetition_type = .required },
        .max_def_level = 0,
        .max_rep_level = 0,
        .column_index = 0,
    };
    const file_data = [_]u8{ 'P', 'A', 'R', '1', 0, 0, 0, 0 };
    try std.testing.expectError(
        error.CorruptFile,
        column_reader.readColumnChunk(allocator, &file_data, chunk, leaf, 1),
    );
}
