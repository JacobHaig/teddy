const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const PlainEncoder = @import("encoding_writer.zig").PlainEncoder;
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
    int32s: ?[]const i32 = null,
    int64s: ?[]const i64 = null,
    floats: ?[]const f32 = null,
    doubles: ?[]const f64 = null,
    byte_arrays: ?[]const []const u8 = null,
    booleans: ?[]const bool = null,
    num_values: usize = 0,
};

/// Write a single column as a complete Parquet column chunk (page header + page data).
pub fn writeColumn(allocator: Allocator, col: ColumnData, codec: types.CompressionCodec) !ColumnWriteResult {
    // Step 1: Encode values with PlainEncoder
    var encoder = PlainEncoder.init(allocator);
    defer encoder.deinit();

    switch (col.physical_type) {
        .int32 => {
            if (col.int32s) |vals| {
                for (vals) |v| try encoder.writeInt32(v);
            }
        },
        .int64 => {
            if (col.int64s) |vals| {
                for (vals) |v| try encoder.writeInt64(v);
            }
        },
        .float => {
            if (col.floats) |vals| {
                for (vals) |v| try encoder.writeFloat(v);
            }
        },
        .double => {
            if (col.doubles) |vals| {
                for (vals) |v| try encoder.writeDouble(v);
            }
        },
        .byte_array => {
            if (col.byte_arrays) |vals| {
                for (vals) |v| try encoder.writeByteArray(v);
            }
        },
        .boolean => {
            if (col.booleans) |vals| {
                try encoder.writeBooleans(vals);
            }
        },
        else => return error.UnsupportedType,
    }

    const uncompressed_data = encoder.written();
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

