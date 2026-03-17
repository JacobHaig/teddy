const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const column_reader = @import("column_reader.zig");
const ThriftReader = @import("thrift.zig").ThriftReader;

// ============================================================
// Parquet File Reader
// ============================================================

const MAGIC = "PAR1";

/// Read a Parquet file from an in-memory buffer and return decoded columns.
/// The buffer must contain the complete file contents.
/// Caller owns the returned ParquetResult and must call deinit().
pub fn readParquet(allocator: Allocator, file_data: []const u8) !types.ParquetResult {
    // 1. Validate minimum size and magic bytes
    if (file_data.len < 12) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, file_data[0..4], MAGIC)) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, file_data[file_data.len - 4 ..], MAGIC)) return error.InvalidParquetFile;

    // 2. Read footer length (4 bytes LE, just before closing magic)
    const footer_len: usize = @intCast(std.mem.readInt(u32, file_data[file_data.len - 8 ..][0..4], .little));
    if (footer_len + 8 > file_data.len) return error.InvalidParquetFile;

    // 3. Decode FileMetaData from footer
    const footer_start = file_data.len - 8 - footer_len;
    const footer_data = file_data[footer_start .. footer_start + footer_len];

    var thrift_reader = ThriftReader.init(footer_data);
    var file_metadata = try metadata.FileMetaData.decode(&thrift_reader, allocator);
    defer file_metadata.deinit(allocator);

    // 4. Resolve schema: find leaf columns
    const leaves = try resolveSchema(allocator, file_metadata.schema);
    defer allocator.free(leaves);

    const num_rows: usize = @intCast(file_metadata.num_rows);

    // 5. Read all columns across all row groups
    const columns = try allocator.alloc(types.ParquetColumn, leaves.len);
    var cols_initialized: usize = 0;
    errdefer {
        for (0..cols_initialized) |i| {
            var c = columns[i];
            c.deinit();
        }
        allocator.free(columns);
    }

    // Initialize columns
    for (0..leaves.len) |i| {
        columns[i] = types.ParquetColumn.initEmpty(allocator);
    }

    // For single row group (most common), read directly
    // For multiple row groups, we'd need to concatenate — handle the simple case first
    if (file_metadata.row_groups.len == 1) {
        const rg = file_metadata.row_groups[0];
        for (leaves, 0..) |leaf, i| {
            if (leaf.column_index >= rg.columns.len) return error.ColumnIndexOutOfBounds;
            columns[i] = try column_reader.readColumnChunk(
                allocator,
                file_data,
                rg.columns[leaf.column_index],
                leaf,
                num_rows,
            );
            cols_initialized += 1;
        }
    } else {
        // Multiple row groups: read each and accumulate
        // For now, read just the first row group's worth for each column,
        // then concatenate subsequent row groups
        for (file_metadata.row_groups) |rg| {
            const rg_rows: usize = @intCast(rg.num_rows);
            for (leaves, 0..) |leaf, i| {
                if (leaf.column_index >= rg.columns.len) return error.ColumnIndexOutOfBounds;
                var chunk_col = try column_reader.readColumnChunk(
                    allocator,
                    file_data,
                    rg.columns[leaf.column_index],
                    leaf,
                    rg_rows,
                );
                defer chunk_col.deinit();

                if (cols_initialized <= i) {
                    // First row group: move data
                    const name_copy = try allocator.alloc(u8, leaf.schema_element.name.len);
                    @memcpy(name_copy, leaf.schema_element.name);

                    columns[i] = chunk_col;
                    columns[i].name = name_copy;
                    // Prevent deinit of chunk_col from freeing the data we just moved
                    chunk_col = types.ParquetColumn.initEmpty(allocator);
                    cols_initialized += 1;
                }
                // TODO: concatenate subsequent row groups' data onto columns[i]
            }
        }
    }

    return .{
        .columns = columns,
        .num_rows = num_rows,
        .allocator = allocator,
    };
}

/// Walk the schema tree to identify leaf (data) columns.
/// The schema is a flattened tree: the root element has num_children,
/// and we skip group nodes to find leaves.
fn resolveSchema(allocator: Allocator, schema: []const metadata.SchemaElement) ![]column_reader.LeafColumn {
    if (schema.len == 0) return error.EmptySchema;

    var leaves = std.ArrayList(column_reader.LeafColumn).empty;
    defer leaves.deinit(allocator);

    // Index 0 is the root (message) node
    var col_idx: usize = 0;
    var i: usize = 1; // skip root
    while (i < schema.len) {
        const elem = schema[i];
        if (elem.num_children != null and elem.num_children.? > 0) {
            // Group node — skip (for now we only support flat schemas)
            i += 1;
            continue;
        }

        // Leaf column
        const max_def: u8 = if (elem.repetition_type) |rt|
            (if (rt == .optional) @as(u8, 1) else @as(u8, 0))
        else
            0;

        try leaves.append(allocator, .{
            .schema_element = elem,
            .max_def_level = max_def,
            .max_rep_level = 0, // flat schema, no repetition
            .column_index = col_idx,
        });
        col_idx += 1;
        i += 1;
    }

    return try leaves.toOwnedSlice(allocator);
}

// ============================================================
// Tests
// ============================================================

test "readParquet: addresses.parquet (uncompressed)" {
    const allocator = std.testing.allocator;

    // Read the file at runtime
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // addresses.parquet: 7 columns × 7 rows
    try std.testing.expectEqual(@as(usize, 7), result.num_rows);
    try std.testing.expectEqual(@as(usize, 7), result.columns.len);

    // Check column names
    try std.testing.expectEqualStrings("First Name", result.columns[0].name);
    try std.testing.expectEqualStrings("Last Name", result.columns[1].name);
    try std.testing.expectEqualStrings("Age", result.columns[2].name);
    try std.testing.expectEqualStrings("Address", result.columns[3].name);
    try std.testing.expectEqualStrings("City", result.columns[4].name);
    try std.testing.expectEqualStrings("State", result.columns[5].name);
    try std.testing.expectEqualStrings("Zip", result.columns[6].name);

    // Check first column values (First Name = byte_array)
    const first_names = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), first_names.len);
    try std.testing.expectEqualStrings("John", first_names[0]);
    try std.testing.expectEqualStrings("Jack", first_names[1]);

    // Check Age column (int64 as written by parquet-cpp-arrow)
    const ages = result.columns[2].int64s orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), ages.len);
    try std.testing.expectEqual(@as(i64, 52), ages[0]); // John Doe
    try std.testing.expectEqual(@as(i64, 23), ages[1]); // Jack McGinnis
    try std.testing.expectEqual(@as(i64, 38), ages[2]); // John "Da Man" Repici
    try std.testing.expectEqual(@as(i64, 96), ages[3]); // Stephen Tyler

    // Check City column (byte_array)
    const cities = result.columns[4].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualStrings("Riverside", cities[0]);
    try std.testing.expectEqualStrings("Phila", cities[1]);
}

test "readParquet: addresses_snappy.parquet (snappy compressed)" {
    const allocator = std.testing.allocator;

    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses_snappy.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // Same data as uncompressed
    try std.testing.expectEqual(@as(usize, 7), result.num_rows);
    try std.testing.expectEqual(@as(usize, 7), result.columns.len);

    // Check first column
    try std.testing.expectEqualStrings("First Name", result.columns[0].name);
    const first_names = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualStrings("John", first_names[0]);

    // Check Age column (int64)
    const ages = result.columns[2].int64s orelse return error.MissingData;
    try std.testing.expectEqual(@as(i64, 52), ages[0]);
}

test "resolveSchema: flat schema" {
    const allocator = std.testing.allocator;
    // Root + 2 leaf columns
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "col_a", .type_ = .int32, .repetition_type = .required },
        .{ .name = "col_b", .type_ = .byte_array, .repetition_type = .optional },
    };
    const leaves = try resolveSchema(allocator, &schema);
    defer allocator.free(leaves);

    try std.testing.expectEqual(@as(usize, 2), leaves.len);
    try std.testing.expectEqualStrings("col_a", leaves[0].schema_element.name);
    try std.testing.expectEqual(@as(u8, 0), leaves[0].max_def_level);
    try std.testing.expectEqualStrings("col_b", leaves[1].schema_element.name);
    try std.testing.expectEqual(@as(u8, 1), leaves[1].max_def_level);
}

test "resolveSchema: empty schema returns error" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{};
    try std.testing.expectError(error.EmptySchema, resolveSchema(allocator, &schema));
}

test "resolveSchema: all required columns" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "id", .type_ = .int64, .repetition_type = .required },
        .{ .name = "value", .type_ = .double, .repetition_type = .required },
        .{ .name = "flag", .type_ = .boolean, .repetition_type = .required },
    };
    const leaves = try resolveSchema(allocator, &schema);
    defer allocator.free(leaves);

    try std.testing.expectEqual(@as(usize, 3), leaves.len);
    for (leaves) |leaf| {
        try std.testing.expectEqual(@as(u8, 0), leaf.max_def_level);
    }
    try std.testing.expectEqual(@as(usize, 0), leaves[0].column_index);
    try std.testing.expectEqual(@as(usize, 1), leaves[1].column_index);
    try std.testing.expectEqual(@as(usize, 2), leaves[2].column_index);
}

test "resolveSchema: all optional columns" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "a", .type_ = .int32, .repetition_type = .optional },
        .{ .name = "b", .type_ = .byte_array, .repetition_type = .optional },
    };
    const leaves = try resolveSchema(allocator, &schema);
    defer allocator.free(leaves);

    try std.testing.expectEqual(@as(usize, 2), leaves.len);
    try std.testing.expectEqual(@as(u8, 1), leaves[0].max_def_level);
    try std.testing.expectEqual(@as(u8, 1), leaves[1].max_def_level);
}

test "resolveSchema: single column" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "only_col", .type_ = .float, .repetition_type = .required },
    };
    const leaves = try resolveSchema(allocator, &schema);
    defer allocator.free(leaves);

    try std.testing.expectEqual(@as(usize, 1), leaves.len);
    try std.testing.expectEqualStrings("only_col", leaves[0].schema_element.name);
}

test "readParquet: invalid magic returns error" {
    const allocator = std.testing.allocator;
    // Valid size (12 bytes) but wrong magic at start
    const data = [_]u8{ 'N', 'O', 'P', 'E', 0x00, 0x00, 0x00, 0x00, 'P', 'A', 'R', '1' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: wrong trailing magic returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 'P', 'A', 'R', '1', 0x00, 0x00, 0x00, 0x00, 'N', 'O', 'P', 'E' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: file too small returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 'P', 'A', 'R', '1' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: addresses.parquet all column types" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // Verify all string columns have correct physical type
    for ([_]usize{ 0, 1, 3, 4, 5 }) |col_idx| {
        try std.testing.expectEqual(types.PhysicalType.byte_array, result.columns[col_idx].physical_type);
        try std.testing.expect(result.columns[col_idx].byte_arrays != null);
    }

    // Age is int64
    try std.testing.expectEqual(types.PhysicalType.int64, result.columns[2].physical_type);
    try std.testing.expect(result.columns[2].int64s != null);

    // Zip is int64
    try std.testing.expectEqual(types.PhysicalType.int64, result.columns[6].physical_type);
    try std.testing.expect(result.columns[6].int64s != null);

    // Check all age values
    const ages = result.columns[2].int64s.?;
    try std.testing.expectEqual(@as(i64, 52), ages[0]);
    try std.testing.expectEqual(@as(i64, 23), ages[1]);
    try std.testing.expectEqual(@as(i64, 38), ages[2]);
    try std.testing.expectEqual(@as(i64, 96), ages[3]);
    try std.testing.expectEqual(@as(i64, 14), ages[4]);
    try std.testing.expectEqual(@as(i64, 56), ages[5]);
    try std.testing.expectEqual(@as(i64, 14), ages[6]);

    // Check all last names
    const last_names = result.columns[1].byte_arrays.?;
    try std.testing.expectEqualStrings("Doe", last_names[0]);
    try std.testing.expectEqualStrings("McGinnis", last_names[1]);
    try std.testing.expectEqualStrings("Repici", last_names[2]);
    try std.testing.expectEqualStrings("Tyler", last_names[3]);
    try std.testing.expectEqualStrings("Blankman", last_names[4]);
    try std.testing.expectEqualStrings("Jet", last_names[5]);
    try std.testing.expectEqualStrings("Blankman", last_names[6]);

    // Check special characters in address
    const addresses = result.columns[3].byte_arrays.?;
    try std.testing.expectEqualStrings("120 jefferson st.", addresses[0]);
    try std.testing.expectEqualStrings("220 hobo Av.", addresses[1]);
}

test "readParquet: snappy and uncompressed produce same data" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();

    const uncompressed_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(uncompressed_data);
    var result_uc = try readParquet(allocator, uncompressed_data);
    defer result_uc.deinit();

    const snappy_data = try cwd.readFileAlloc(io, "data/addresses_snappy.parquet", allocator, .unlimited);
    defer allocator.free(snappy_data);
    var result_sn = try readParquet(allocator, snappy_data);
    defer result_sn.deinit();

    // Same dimensions
    try std.testing.expectEqual(result_uc.num_rows, result_sn.num_rows);
    try std.testing.expectEqual(result_uc.columns.len, result_sn.columns.len);

    // Same column names and types
    for (0..result_uc.columns.len) |i| {
        try std.testing.expectEqualStrings(result_uc.columns[i].name, result_sn.columns[i].name);
        try std.testing.expectEqual(result_uc.columns[i].physical_type, result_sn.columns[i].physical_type);
    }

    // Same age values
    const ages_uc = result_uc.columns[2].int64s.?;
    const ages_sn = result_sn.columns[2].int64s.?;
    for (0..ages_uc.len) |i| {
        try std.testing.expectEqual(ages_uc[i], ages_sn[i]);
    }

    // Same first names
    const names_uc = result_uc.columns[0].byte_arrays.?;
    const names_sn = result_sn.columns[0].byte_arrays.?;
    for (0..names_uc.len) |i| {
        try std.testing.expectEqualStrings(names_uc[i], names_sn[i]);
    }
}
