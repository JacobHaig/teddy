const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const encoding_writer = @import("encoding_writer.zig");
const PlainEncoder = encoding_writer.PlainEncoder;
const encodeRleLevels = encoding_writer.encodeRleLevels;
const snappy = @import("snappy.zig");

// ============================================================
// Column Chunk Writer
// ============================================================

pub const ColumnWriteResult = struct {
    data: []u8,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    num_values: i64,
    allocator: Allocator,

    pub fn deinit(self: *ColumnWriteResult) void {
        self.allocator.free(self.data);
    }
};

pub const ColumnData = struct {
    name: []const u8,
    physical_type: types.PhysicalType,
    converted_type: ?types.ConvertedType = null,
    logical_type: ?types.LogicalType = null,
    type_length: ?i32 = null,
    // Legacy DECIMAL fields 7/8; propagated to schema element on write.
    scale: ?i32 = null,
    precision: ?i32 = null,
    int32s: ?[]const i32 = null,
    int64s: ?[]const i64 = null,
    floats: ?[]const f32 = null,
    doubles: ?[]const f64 = null,
    byte_arrays: ?[]const []const u8 = null,
    booleans: ?[]const bool = null,
    num_values: usize = 0,
    /// Optional validity bitmap (true = present, false = null), one entry per
    /// row. When non-null the column is OPTIONAL: the page gets a definition-
    /// level section and only present values are encoded. Same convention as
    /// ParquetColumn.validity and Series.validity. Length must equal num_values.
    validity: ?[]const bool = null,
};

/// Index i is present (true) when there is no validity bitmap or validity[i].
inline fn isValid(col: ColumnData, i: usize) bool {
    if (col.validity) |v| return v[i];
    return true;
}

/// Write a single column as a complete Parquet column chunk (page header + page data).
pub fn writeColumn(allocator: Allocator, col: ColumnData, codec: types.CompressionCodec) !ColumnWriteResult {
    // Step 1: Encode values with PlainEncoder
    var encoder = PlainEncoder.init(allocator);
    defer encoder.deinit();

    // For OPTIONAL columns, null indices are SKIPPED before any width
    // validation: null placeholders (e.g. empty FLBA/INT96 slices) must not
    // trip FixedLengthMismatch. Only present values are PLAIN-encoded.
    switch (col.physical_type) {
        .int32 => {
            if (col.int32s) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    try encoder.writeInt32(v);
                }
            }
        },
        .int64 => {
            if (col.int64s) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    try encoder.writeInt64(v);
                }
            }
        },
        .float => {
            if (col.floats) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    try encoder.writeFloat(v);
                }
            }
        },
        .double => {
            if (col.doubles) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    try encoder.writeDouble(v);
                }
            }
        },
        .byte_array => {
            if (col.byte_arrays) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    try encoder.writeByteArray(v);
                }
            }
        },
        .boolean => {
            if (col.booleans) |vals| {
                if (col.validity == null) {
                    try encoder.writeBooleans(vals);
                } else {
                    // writeBooleans takes a whole slice; build a present-only
                    // bool slice (freed locally — the encoder copies the bits).
                    var present = try std.ArrayList(bool).initCapacity(allocator, vals.len);
                    defer present.deinit(allocator);
                    for (vals, 0..) |v, i| {
                        if (!isValid(col, i)) continue;
                        present.appendAssumeCapacity(v);
                    }
                    try encoder.writeBooleans(present.items);
                }
            }
        },
        .fixed_len_byte_array => {
            // MissingTypeLength = caller forgot the width entirely;
            // InvalidTypeLength = width present but non-positive (matches reader).
            const tl = col.type_length orelse return error.MissingTypeLength;
            if (tl <= 0) return error.InvalidTypeLength;
            const width: usize = @intCast(tl);
            if (col.byte_arrays) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    if (v.len != width) return error.FixedLengthMismatch;
                    try encoder.writeFixedByteArray(v);
                }
            }
        },
        .int96 => {
            if (col.byte_arrays) |vals| {
                for (vals, 0..) |v, i| {
                    if (!isValid(col, i)) continue;
                    if (v.len != 12) return error.FixedLengthMismatch;
                    try encoder.writeFixedByteArray(v);
                }
            }
        },
    }

    // Build the uncompressed page body. For OPTIONAL columns prepend the
    // definition-level section: [4-byte LE byte length][RLE-encoded levels],
    // levels[i] = 1 present / 0 null (max_def_level == 1). REQUIRED columns
    // (validity == null) emit no def-level section (reader skips when
    // max_def_level == 0). The whole body — def levels + values — is what gets
    // compressed for v1 data pages (the reader decompresses the entire page
    // and only then reads the def-level length prefix).
    var body = std.ArrayList(u8).empty;
    defer body.deinit(allocator);

    if (col.validity) |valid| {
        var levels = try allocator.alloc(u1, valid.len);
        defer allocator.free(levels);
        for (valid, 0..) |present, i| levels[i] = @intFromBool(present);

        var level_bytes = std.ArrayList(u8).empty;
        defer level_bytes.deinit(allocator);
        try encodeRleLevels(allocator, levels, &level_bytes);

        var len_prefix: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_prefix, @intCast(level_bytes.items.len), .little);
        try body.appendSlice(allocator, &len_prefix);
        try body.appendSlice(allocator, level_bytes.items);
    }
    try body.appendSlice(allocator, encoder.written());

    const uncompressed_data = body.items;
    const uncompressed_size: i32 = @intCast(uncompressed_data.len);

    // Step 2: Optionally compress
    var compressed_data: ?[]u8 = null;
    defer if (compressed_data) |cd| allocator.free(cd);

    const page_data: []const u8 = switch (codec) {
        .snappy => blk: {
            compressed_data = try snappy.compress(allocator, uncompressed_data);
            break :blk compressed_data.?;
        },
        .uncompressed => uncompressed_data,
        else => return error.UnsupportedCodec,
    };
    const compressed_size: i32 = @intCast(page_data.len);

    // Step 3: Build page header
    const page_header = metadata.PageHeader{
        .page_type = .data_page,
        .uncompressed_page_size = uncompressed_size,
        .compressed_page_size = compressed_size,
        .data_page_header = .{
            .num_values = @intCast(col.num_values),
            .encoding = .plain,
            .definition_level_encoding = .rle,
            .repetition_level_encoding = .rle,
        },
    };

    // Step 4: Serialize page header
    var header_writer = ThriftWriter.init(allocator);
    defer header_writer.deinit();
    try page_header.encode(&header_writer);
    const header_bytes = header_writer.written();

    // Step 5: Concatenate header + page data
    const total_len = header_bytes.len + page_data.len;
    const result = try allocator.alloc(u8, total_len);
    @memcpy(result[0..header_bytes.len], header_bytes);
    @memcpy(result[header_bytes.len..], page_data);

    return .{
        .data = result,
        .total_compressed_size = @intCast(total_len),
        .total_uncompressed_size = @as(i64, @intCast(header_bytes.len)) + @as(i64, uncompressed_size),
        .num_values = @intCast(col.num_values),
        .allocator = allocator,
    };
}

