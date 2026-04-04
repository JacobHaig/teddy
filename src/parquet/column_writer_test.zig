const std = @import("std");
const column_writer = @import("column_writer.zig");
const writeColumn = column_writer.writeColumn;
const ColumnData = column_writer.ColumnData;

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
