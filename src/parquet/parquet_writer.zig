const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const column_writer = @import("column_writer.zig");
const ColumnData = column_writer.ColumnData;
const reader_mod = @import("parquet_reader.zig");

// ============================================================
// Parquet File Writer
// ============================================================

pub const WriteOptions = struct {
    compression: types.CompressionCodec = .uncompressed,
};

/// Write a complete Parquet file from column data.
/// Returns an allocator-owned buffer containing the entire file.
///
/// File layout:
///   4 bytes: "PAR1" magic
///   Column chunks (page header + page data for each column)
///   Thrift-encoded FileMetaData footer
///   4 bytes: footer length (LE u32)
///   4 bytes: "PAR1" magic
pub fn writeParquet(allocator: Allocator, columns: []const ColumnData, options: WriteOptions) ![]u8 {
    const num_rows: i64 = if (columns.len > 0) @intCast(columns[0].num_values) else 0;

    // Write each column chunk
    var col_results = try allocator.alloc(column_writer.ColumnWriteResult, columns.len);
    defer {
        for (col_results) |*cr| cr.deinit();
        allocator.free(col_results);
    }
    for (columns, 0..) |col, i| {
        col_results[i] = try column_writer.writeColumn(allocator, col, options.compression);
    }

    // Build schema elements: root + one per column
    const schema = try allocator.alloc(metadata.SchemaElement, columns.len + 1);
    defer allocator.free(schema);
    schema[0] = .{ .name = "schema", .num_children = @intCast(columns.len) };
    for (columns, 0..) |col, i| {
        schema[i + 1] = .{
            .type_ = col.physical_type,
            .repetition_type = .required,
            .name = col.name,
            .converted_type = col.converted_type,
        };
    }

    // Calculate offsets: column data starts after 4-byte magic
    var current_offset: i64 = 4; // "PAR1"
    const col_chunks = try allocator.alloc(metadata.ColumnChunk, columns.len);
    defer allocator.free(col_chunks);

    var total_byte_size: i64 = 0;
    for (columns, 0..) |col, i| {
        var encs_buf = [_]types.Encoding{.plain};
        var path_buf = [_][]const u8{col.name};
        col_chunks[i] = .{
            .file_offset = current_offset,
            .meta_data = .{
                .type_ = col.physical_type,
                .encodings = &encs_buf,
                .path_in_schema = &path_buf,
                .codec = options.compression,
                .num_values = col_results[i].num_values,
                .total_uncompressed_size = col_results[i].total_uncompressed_size,
                .total_compressed_size = col_results[i].total_compressed_size,
                .data_page_offset = current_offset,
            },
        };
        current_offset += @intCast(col_results[i].data.len);
        total_byte_size += col_results[i].total_compressed_size;
    }

    // Build row group
    var row_groups = [_]metadata.RowGroup{.{
        .columns = col_chunks,
        .total_byte_size = total_byte_size,
        .num_rows = num_rows,
    }};

    // Build file metadata
    const file_meta = metadata.FileMetaData{
        .version = 2,
        .schema = schema,
        .num_rows = num_rows,
        .row_groups = &row_groups,
        .created_by = "teddy (Zig)",
    };

    // Encode footer
    var footer_writer = ThriftWriter.init(allocator);
    defer footer_writer.deinit();
    try file_meta.encode(&footer_writer);
    const footer_bytes = footer_writer.written();
    const footer_len: u32 = @intCast(footer_bytes.len);

    // Assemble complete file
    var total_size: usize = 4; // PAR1
    for (col_results) |cr| total_size += cr.data.len;
    total_size += footer_bytes.len;
    total_size += 4; // footer length
    total_size += 4; // PAR1

    const output = try allocator.alloc(u8, total_size);
    var pos: usize = 0;

    // Magic
    @memcpy(output[pos .. pos + 4], "PAR1");
    pos += 4;

    // Column chunks
    for (col_results) |cr| {
        @memcpy(output[pos .. pos + cr.data.len], cr.data);
        pos += cr.data.len;
    }

    // Footer
    @memcpy(output[pos .. pos + footer_bytes.len], footer_bytes);
    pos += footer_bytes.len;

    // Footer length (LE u32)
    std.mem.writeInt(u32, output[pos..][0..4], footer_len, .little);
    pos += 4;

    // Magic
    @memcpy(output[pos .. pos + 4], "PAR1");
    pos += 4;

    return output;
}

// ============================================================
// Tests
// ============================================================

test "writeParquet: minimal int32 file" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 10, 20, 30 };
    const cols = [_]ColumnData{.{
        .name = "x",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 3,
    }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    // Verify magic bytes
    try std.testing.expectEqualStrings("PAR1", output[0..4]);
    try std.testing.expectEqualStrings("PAR1", output[output.len - 4 ..]);

    // Verify it can be read back
    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    try std.testing.expectEqual(@as(usize, 1), result.columns.len);
    try std.testing.expectEqualStrings("x", result.columns[0].name);
    try std.testing.expectEqual(@as(i32, 10), result.columns[0].int32s.?[0]);
    try std.testing.expectEqual(@as(i32, 20), result.columns[0].int32s.?[1]);
    try std.testing.expectEqual(@as(i32, 30), result.columns[0].int32s.?[2]);
}

test "writeParquet: multi-column mixed types" {
    const allocator = std.testing.allocator;
    const ints = [_]i64{ 1, 2, 3 };
    const floats = [_]f64{ 1.1, 2.2, 3.3 };
    const strs = [_][]const u8{ "a", "bb", "ccc" };
    const cols = [_]ColumnData{
        .{ .name = "id", .physical_type = .int64, .int64s = &ints, .num_values = 3 },
        .{ .name = "val", .physical_type = .double, .doubles = &floats, .num_values = 3 },
        .{ .name = "name", .physical_type = .byte_array, .converted_type = .utf8, .byte_arrays = &strs, .num_values = 3 },
    };
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    try std.testing.expectEqual(@as(usize, 3), result.columns.len);
    try std.testing.expectEqual(@as(i64, 2), result.columns[0].int64s.?[1]);
    try std.testing.expectEqual(@as(f64, 2.2), result.columns[1].doubles.?[1]);
    try std.testing.expectEqualStrings("ccc", result.columns[2].byte_arrays.?[2]);
}

test "writeParquet: snappy compression round-trip" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 42, 99, -7 };
    const cols = [_]ColumnData{.{
        .name = "v",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 3,
    }};
    const output = try writeParquet(allocator, &cols, .{ .compression = .snappy });
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(i32, 42), result.columns[0].int32s.?[0]);
    try std.testing.expectEqual(@as(i32, 99), result.columns[0].int32s.?[1]);
    try std.testing.expectEqual(@as(i32, -7), result.columns[0].int32s.?[2]);
}

test "writeParquet: boolean column" {
    const allocator = std.testing.allocator;
    const vals = [_]bool{ true, false, true };
    const cols = [_]ColumnData{.{
        .name = "flag",
        .physical_type = .boolean,
        .booleans = &vals,
        .num_values = 3,
    }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(true, result.columns[0].booleans.?[0]);
    try std.testing.expectEqual(false, result.columns[0].booleans.?[1]);
    try std.testing.expectEqual(true, result.columns[0].booleans.?[2]);
}

test "writeParquet: empty file" {
    const allocator = std.testing.allocator;
    const output = try writeParquet(allocator, &.{}, .{});
    defer allocator.free(output);

    try std.testing.expectEqualStrings("PAR1", output[0..4]);
    try std.testing.expectEqualStrings("PAR1", output[output.len - 4 ..]);
}
