const std = @import("std");
const column_writer = @import("column_writer.zig");
const writeColumn = column_writer.writeColumn;
const ColumnData = column_writer.ColumnData;
const metadata = @import("metadata.zig");
const ThriftReader = @import("thrift_reader.zig").ThriftReader;
const encoding = @import("encoding_reader.zig");

test "writeColumn: int32 column" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 10, 20, 30 };
    const col = ColumnData{
        .name = "x",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 3,
    };
    var result = try writeColumn(allocator, col, .uncompressed);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 3), result.num_values);
    try std.testing.expect(result.data.len > 0);
}

test "writeColumn: int64 column" {
    const allocator = std.testing.allocator;
    const vals = [_]i64{ 100, 200 };
    const col = ColumnData{
        .name = "y",
        .physical_type = .int64,
        .int64s = &vals,
        .num_values = 2,
    };
    var result = try writeColumn(allocator, col, .uncompressed);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 2), result.num_values);
}

test "writeColumn: byte_array column" {
    const allocator = std.testing.allocator;
    const vals = [_][]const u8{ "hello", "world" };
    const col = ColumnData{
        .name = "name",
        .physical_type = .byte_array,
        .byte_arrays = &vals,
        .num_values = 2,
    };
    var result = try writeColumn(allocator, col, .uncompressed);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 2), result.num_values);
}

test "writeColumn: boolean column" {
    const allocator = std.testing.allocator;
    const vals = [_]bool{ true, false, true };
    const col = ColumnData{
        .name = "flag",
        .physical_type = .boolean,
        .booleans = &vals,
        .num_values = 3,
    };
    var result = try writeColumn(allocator, col, .uncompressed);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 3), result.num_values);
}

// levelBitWidth replica for the test (mirrors column_reader.zig).
fn tLevelBitWidth(max_level: u8) u8 {
    if (max_level == 0) return 0;
    return @intCast(@as(u8, 8) - @as(u8, @clz(max_level)));
}

test "writeColumn: nested-leaf body has [rep][def][values] in v1 read order" {
    const allocator = std.testing.allocator;
    // list<i64> shape: max_def 3, max_rep 1. Fixture mirrors the assembly pins:
    // rows [1,2]/null/[] → def {3,3,0,1} rep {0,1,0,0} present values {1,2}.
    const def = [_]u32{ 3, 3, 0, 1 };
    const rep = [_]u32{ 0, 1, 0, 0 };
    const vals = [_]i64{ 1, 2 };
    const col = ColumnData{
        .name = "element",
        .physical_type = .int64,
        .int64s = &vals,
        .num_values = def.len, // total level count
        .def_levels = &def,
        .rep_levels = &rep,
        .max_def = 3,
        .max_rep = 1,
    };
    var result = try writeColumn(allocator, col, .uncompressed);
    defer result.deinit();

    // num_values in the page header == the total level count.
    try std.testing.expectEqual(@as(i64, def.len), result.num_values);

    // Parse the page header off the front to find where the body begins.
    var tr = ThriftReader.init(result.data);
    const header = try metadata.PageHeader.decode(&tr);
    try std.testing.expectEqual(@as(i32, def.len), header.data_page_header.?.num_values);
    const body = result.data[tr.pos..];

    // Read order: [4B rep_len][rep RLE @ bw(max_rep)][4B def_len][def RLE @ bw(max_def)][values]
    var pos: usize = 0;
    const rep_len = std.mem.readInt(u32, body[pos..][0..4], .little);
    pos += 4;
    {
        var dec = encoding.RleBitPackedDecoder.init(body[pos .. pos + rep_len], tLevelBitWidth(col.max_rep));
        const got = try dec.readBatch(def.len, allocator);
        defer allocator.free(got);
        for (rep, 0..) |r, i| try std.testing.expectEqual(r, got[i]);
    }
    pos += rep_len;

    const def_len = std.mem.readInt(u32, body[pos..][0..4], .little);
    pos += 4;
    {
        var dec = encoding.RleBitPackedDecoder.init(body[pos .. pos + def_len], tLevelBitWidth(col.max_def));
        const got = try dec.readBatch(def.len, allocator);
        defer allocator.free(got);
        for (def, 0..) |d, i| try std.testing.expectEqual(d, got[i]);
    }
    pos += def_len;

    // Values: present-only, PLAIN i64.
    var pd = encoding.PlainDecoder.init(body[pos..]);
    try std.testing.expectEqual(@as(i64, 1), try pd.readInt64());
    try std.testing.expectEqual(@as(i64, 2), try pd.readInt64());
}

test "writeColumn: snappy compression" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 1, 2, 3, 4, 5 };
    const col = ColumnData{
        .name = "x",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 5,
    };
    var result = try writeColumn(allocator, col, .snappy);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 5), result.num_values);
    try std.testing.expect(result.data.len > 0);
}
