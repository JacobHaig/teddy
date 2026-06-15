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
    /// Re-emit Timestamp columns whose values all originate from legacy INT96
    /// as INT96 (bit-faithful). Default writes modern INT64 TIMESTAMP.
    /// Consumed by the dataframe adapter when choosing the physical type;
    /// has no effect at this layer.
    emit_int96: bool = false,
};

/// A top-level column to write. A `flat` column is one leaf with one schema
/// element (the legacy shape). A `nested` column expands to a schema SUBTREE
/// (pre-order SchemaElement list, NOT including the file root) plus one
/// ColumnData per leaf, in leaf order. `num_rows` is the column's top-level row
/// count (which differs from any leaf's level count for repeated/nested data).
pub const WriteColumn = union(enum) {
    flat: ColumnData,
    nested: struct {
        schema: []const metadata.SchemaElement,
        leaves: []const ColumnData,
        num_rows: usize,
    },

    /// Number of leaf chunks this column contributes to the row group.
    fn leafCount(self: WriteColumn) usize {
        return switch (self) {
            .flat => 1,
            .nested => |n| n.leaves.len,
        };
    }

    /// Number of pre-order schema elements (excluding the file root).
    fn schemaCount(self: WriteColumn) usize {
        return switch (self) {
            .flat => 1,
            .nested => |n| n.schema.len,
        };
    }

    /// The column's top-level row count.
    fn rowCount(self: WriteColumn) usize {
        return switch (self) {
            .flat => |cd| cd.num_values,
            .nested => |n| n.num_rows,
        };
    }
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
pub fn writeParquet(allocator: Allocator, cols: []const WriteColumn, options: WriteOptions) ![]u8 {
    // num_rows = the top-level row count. Flat → its num_values; nested → its
    // stored num_rows. All columns are assumed to agree (same row count); we
    // derive from the first column as the legacy flat path did, generalized.
    const num_rows: i64 = if (cols.len > 0) @intCast(cols[0].rowCount()) else 0;

    // Flatten the top-level columns into per-leaf ColumnData (flat → its one
    // ColumnData; nested → its leaves in order). Borrowed slices — no copies.
    var total_leaves: usize = 0;
    var total_schema: usize = 0;
    for (cols) |c| {
        total_leaves += c.leafCount();
        total_schema += c.schemaCount();
    }

    const leaf_cols = try allocator.alloc(ColumnData, total_leaves);
    defer allocator.free(leaf_cols);
    {
        var li: usize = 0;
        for (cols) |c| {
            switch (c) {
                .flat => |cd| {
                    leaf_cols[li] = cd;
                    li += 1;
                },
                .nested => |n| {
                    for (n.leaves) |cd| {
                        leaf_cols[li] = cd;
                        li += 1;
                    }
                },
            }
        }
    }

    // Write each leaf column chunk
    var col_results = try allocator.alloc(column_writer.ColumnWriteResult, total_leaves);
    var col_results_written: usize = 0;
    defer {
        for (col_results[0..col_results_written]) |*cr| cr.deinit();
        allocator.free(col_results);
    }
    for (leaf_cols, 0..) |col, i| {
        col_results[i] = try column_writer.writeColumn(allocator, col, options.compression);
        col_results_written = i + 1;
    }

    // Build schema elements: root + each column's contribution, pre-order.
    // flat → one element (as before); nested → its pre-order subtree slice.
    const schema = try allocator.alloc(metadata.SchemaElement, total_schema + 1);
    defer allocator.free(schema);
    schema[0] = .{ .name = "schema", .num_children = @intCast(cols.len) };
    {
        var si: usize = 1;
        for (cols) |c| {
            switch (c) {
                .flat => |col| {
                    schema[si] = .{
                        .type_ = col.physical_type,
                        .type_length = col.type_length,
                        // OPTIONAL exactly when def levels are written (validity
                        // present); the reader gates def-level reads on
                        // max_def_level > 0, derived from repetition_type ==
                        // .optional in resolveSchema.
                        .repetition_type = if (col.validity != null) .optional else .required,
                        .name = col.name,
                        .converted_type = col.converted_type,
                        .scale = col.scale,
                        .precision = col.precision,
                        .logical_type = col.logical_type,
                    };
                    si += 1;
                },
                .nested => |n| {
                    for (n.schema) |elem| {
                        schema[si] = elem;
                        si += 1;
                    }
                },
            }
        }
    }

    // Calculate offsets: column data starts after 4-byte magic. One ColumnChunk
    // per leaf, in leaf order.
    var current_offset: i64 = 4; // "PAR1"
    const col_chunks = try allocator.alloc(metadata.ColumnChunk, total_leaves);
    defer allocator.free(col_chunks);

    // Per-leaf backing for the encodings + path_in_schema slices. These MUST
    // outlive the loop (their addresses are stored into col_chunks and read
    // at footer-encode time below) — a per-iteration stack local would leave
    // every chunk aliasing the LAST leaf's arrays. path_in_schema is not
    // load-bearing for teddy reads (leaves resolve positionally by
    // column_index), but other readers expect the per-leaf name, so keep it
    // correct.
    const encs_bufs = try allocator.alloc([1]types.Encoding, total_leaves);
    defer allocator.free(encs_bufs);
    const path_bufs = try allocator.alloc([1][]const u8, total_leaves);
    defer allocator.free(path_bufs);

    var total_byte_size: i64 = 0;
    for (leaf_cols, 0..) |col, i| {
        encs_bufs[i] = [_]types.Encoding{.plain};
        path_bufs[i] = [_][]const u8{col.name};
        col_chunks[i] = .{
            .file_offset = current_offset,
            .meta_data = .{
                .type_ = col.physical_type,
                .encodings = &encs_bufs[i],
                .path_in_schema = &path_bufs[i],
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
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "x",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 3,
    } }};
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
    const cols = [_]WriteColumn{
        .{ .flat = .{ .name = "id", .physical_type = .int64, .int64s = &ints, .num_values = 3 } },
        .{ .flat = .{ .name = "val", .physical_type = .double, .doubles = &floats, .num_values = 3 } },
        .{ .flat = .{ .name = "name", .physical_type = .byte_array, .converted_type = .utf8, .byte_arrays = &strs, .num_values = 3 } },
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
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "v",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 3,
    } }};
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
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "flag",
        .physical_type = .boolean,
        .booleans = &vals,
        .num_values = 3,
    } }};
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

test "writeParquet: FIXED_LEN_BYTE_ARRAY round-trip with type_length" {
    const allocator = std.testing.allocator;
    const vals = [_][]const u8{ "abcd", "efgh" };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "fb",
        .physical_type = .fixed_len_byte_array,
        .type_length = 4,
        .byte_arrays = &vals,
        .num_values = 2,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(types.PhysicalType.fixed_len_byte_array, result.columns[0].physical_type);
    try std.testing.expectEqual(@as(i32, 4), result.columns[0].type_length.?);
    try std.testing.expectEqualStrings("abcd", result.columns[0].byte_arrays.?[0]);
    try std.testing.expectEqualStrings("efgh", result.columns[0].byte_arrays.?[1]);
}

test "writeParquet: INT96 round-trip preserves raw 12-byte values" {
    const allocator = std.testing.allocator;
    const v1 = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const v2 = [_]u8{ 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    const vals = [_][]const u8{ &v1, &v2 };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "t",
        .physical_type = .int96,
        .byte_arrays = &vals,
        .num_values = 2,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(types.PhysicalType.int96, result.columns[0].physical_type);
    try std.testing.expectEqualSlices(u8, &v1, result.columns[0].byte_arrays.?[0]);
    try std.testing.expectEqualSlices(u8, &v2, result.columns[0].byte_arrays.?[1]);
}

test "writeParquet: FLBA value width mismatch errors" {
    const allocator = std.testing.allocator;
    const vals = [_][]const u8{"abc"}; // 3 bytes, width says 4
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "fb",
        .physical_type = .fixed_len_byte_array,
        .type_length = 4,
        .byte_arrays = &vals,
        .num_values = 1,
    } }};
    try std.testing.expectError(error.FixedLengthMismatch, writeParquet(allocator, &cols, .{}));
}

test "writeParquet: INT96 value width mismatch errors" {
    const allocator = std.testing.allocator;
    const v = [_]u8{ 1, 2, 3 }; // 3 bytes, not 12
    const vals = [_][]const u8{&v};
    const cols = [_]WriteColumn{.{ .flat = .{ .name = "t", .physical_type = .int96, .byte_arrays = &vals, .num_values = 1 } }};
    try std.testing.expectError(error.FixedLengthMismatch, writeParquet(allocator, &cols, .{}));
}

test "writeParquet: optional int64 column with nulls round-trips" {
    const allocator = std.testing.allocator;
    // Values buffer holds a placeholder at the null slot; validity marks it.
    const vals = [_]i64{ 10, 0, 30 };
    const valid = [_]bool{ true, false, true };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "x",
        .physical_type = .int64,
        .int64s = &vals,
        .num_values = 3,
        .validity = &valid,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    const col = result.columns[0];
    try std.testing.expect(col.validity != null);
    try std.testing.expectEqual(true, col.validity.?[0]);
    try std.testing.expectEqual(false, col.validity.?[1]);
    try std.testing.expectEqual(true, col.validity.?[2]);
    // Reader re-expands to full row count (placeholder at the null slot).
    try std.testing.expectEqual(@as(usize, 3), col.int64s.?.len);
    try std.testing.expectEqual(@as(i64, 10), col.int64s.?[0]);
    try std.testing.expectEqual(@as(i64, 30), col.int64s.?[2]);
}

test "writeParquet: optional column round-trips with snappy compression" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 1, 0, 0, 4 };
    const valid = [_]bool{ true, false, false, true };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "v",
        .physical_type = .int32,
        .int32s = &vals,
        .num_values = 4,
        .validity = &valid,
    } }};
    const output = try writeParquet(allocator, &cols, .{ .compression = .snappy });
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    const col = result.columns[0];
    try std.testing.expect(col.validity != null);
    try std.testing.expectEqualSlices(bool, &valid, col.validity.?);
    try std.testing.expectEqual(@as(usize, 4), col.int32s.?.len);
    try std.testing.expectEqual(@as(i32, 1), col.int32s.?[0]);
    try std.testing.expectEqual(@as(i32, 4), col.int32s.?[3]);
}

test "writeParquet: optional FLBA with a null placeholder does not trip width check" {
    const allocator = std.testing.allocator;
    // Null slot is an empty slice — would fail FixedLengthMismatch if not skipped.
    const vals = [_][]const u8{ "abcd", "", "efgh" };
    const valid = [_]bool{ true, false, true };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "fb",
        .physical_type = .fixed_len_byte_array,
        .type_length = 4,
        .byte_arrays = &vals,
        .num_values = 3,
        .validity = &valid,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    const col = result.columns[0];
    try std.testing.expectEqualSlices(bool, &valid, col.validity.?);
    try std.testing.expectEqual(@as(usize, 3), col.byte_arrays.?.len);
    try std.testing.expectEqualStrings("abcd", col.byte_arrays.?[0]);
    try std.testing.expectEqualStrings("efgh", col.byte_arrays.?[2]);
}

test "writeParquet: required column (no validity) byte layout is unchanged" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 10, 20, 30 };
    const required = [_]WriteColumn{.{ .flat = .{ .name = "x", .physical_type = .int32, .int32s = &vals, .num_values = 3 } }};
    const opt_valid = [_]bool{ true, true, true };
    const optional = [_]WriteColumn{.{ .flat = .{ .name = "x", .physical_type = .int32, .int32s = &vals, .num_values = 3, .validity = &opt_valid } }};

    const req_out = try writeParquet(allocator, &required, .{});
    defer allocator.free(req_out);
    const opt_out = try writeParquet(allocator, &optional, .{});
    defer allocator.free(opt_out);

    // Optional adds the def-level section + OPTIONAL schema bit, so it must be
    // strictly larger; the required path stays byte-for-byte as before.
    try std.testing.expect(opt_out.len > req_out.len);

    var result = try reader_mod.readParquet(allocator, req_out);
    defer result.deinit();
    try std.testing.expect(result.columns[0].validity == null);
}

test "writeParquet: nested list<i64> column round-trips through the reader" {
    const allocator = std.testing.allocator;
    // root → optional group l (list) { repeated group list { optional int64 element } }
    // rows [1,2]/null/[] → def {3,3,0,1} rep {0,1,0,0} present values {1,2}.
    const subschema = [_]metadata.SchemaElement{
        .{ .name = "l", .repetition_type = .optional, .num_children = 1, .converted_type = .list },
        .{ .name = "list", .repetition_type = .repeated, .num_children = 1 },
        .{ .name = "element", .type_ = .int64, .repetition_type = .optional },
    };
    const def = [_]u32{ 3, 3, 0, 1 };
    const rep = [_]u32{ 0, 1, 0, 0 };
    const vals = [_]i64{ 1, 2 };
    const leaves = [_]ColumnData{.{
        .name = "element",
        .physical_type = .int64,
        .int64s = &vals,
        .num_values = def.len,
        .def_levels = &def,
        .rep_levels = &rep,
        .max_def = 3,
        .max_rep = 1,
    }};
    const cols = [_]WriteColumn{.{ .nested = .{
        .schema = &subschema,
        .leaves = &leaves,
        .num_rows = 3,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    // 3 top-level rows; the leaf is surfaced as a nested column, not flat.
    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    try std.testing.expectEqual(@as(usize, 0), result.columns.len);
    try std.testing.expectEqual(@as(usize, 1), result.nested_columns.len);
    const leaf = result.nested_columns[0];
    try std.testing.expect(leaf.nested);
    // Present values preserved verbatim.
    try std.testing.expectEqual(@as(i64, 1), leaf.int64s.?[0]);
    try std.testing.expectEqual(@as(i64, 2), leaf.int64s.?[1]);
    // Raw def/rep streams round-trip.
    try std.testing.expectEqual(@as(usize, def.len), leaf.def_levels.?.len);
    for (def, 0..) |d, i| try std.testing.expectEqual(@as(u16, @intCast(d)), leaf.def_levels.?[i]);
    for (rep, 0..) |r, i| try std.testing.expectEqual(@as(u16, @intCast(r)), leaf.rep_levels.?[i]);
}

test "writeParquet: logical_type lands in the schema and reads back" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 18262, 18263 };
    const cols = [_]WriteColumn{.{ .flat = .{
        .name = "d",
        .physical_type = .int32,
        .converted_type = .date,
        .logical_type = .date,
        .int32s = &vals,
        .num_values = 2,
    } }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expect(result.columns[0].logical_type.? == .date);
    try std.testing.expectEqual(types.ConvertedType.date, result.columns[0].converted_type.?);
}
