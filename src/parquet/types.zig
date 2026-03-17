const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================
// Parquet Physical Types (from parquet.thrift)
// ============================================================

pub const PhysicalType = enum(u8) {
    boolean = 0,
    int32 = 1,
    int64 = 2,
    int96 = 3,
    float = 4,
    double = 5,
    byte_array = 6,
    fixed_len_byte_array = 7,
};

pub const ConvertedType = enum(u8) {
    utf8 = 0,
    map = 1,
    map_key_value = 2,
    list = 3,
    @"enum" = 4,
    decimal = 5,
    date = 6,
    time_millis = 7,
    time_micros = 8,
    timestamp_millis = 9,
    timestamp_micros = 10,
    uint_8 = 11,
    uint_16 = 12,
    uint_32 = 13,
    uint_64 = 14,
    int_8 = 15,
    int_16 = 16,
    int_32 = 17,
    int_64 = 18,
    json = 19,
    bson = 20,
    interval = 21,
};

pub const FieldRepetitionType = enum(u8) {
    required = 0,
    optional = 1,
    repeated = 2,
};

pub const Encoding = enum(u8) {
    plain = 0,
    plain_dictionary = 2,
    rle = 3,
    bit_packed = 4,
    delta_binary_packed = 5,
    delta_length_byte_array = 6,
    delta_byte_array = 7,
    rle_dictionary = 8,
    byte_stream_split = 9,
};

pub const CompressionCodec = enum(u8) {
    uncompressed = 0,
    snappy = 1,
    gzip = 2,
    lzo = 3,
    brotli = 4,
    lz4 = 5,
    zstd = 6,
    lz4_raw = 7,
};

pub const PageType = enum(u8) {
    data_page = 0,
    index_page = 1,
    dictionary_page = 2,
    data_page_v2 = 3,
};

pub const BoundaryOrder = enum(u8) {
    unordered = 0,
    ascending = 1,
    descending = 2,
};

// ============================================================
// Output Types — what the Parquet reader produces
// ============================================================

/// A single column of decoded Parquet data.
/// Exactly one of the typed arrays (booleans, int32s, ...) is populated
/// based on physical_type. For OPTIONAL columns, `validity` tracks which
/// rows have values (true = present, false = null).
pub const ParquetColumn = struct {
    name: []const u8, // allocator-owned
    physical_type: PhysicalType,
    converted_type: ?ConvertedType,
    is_optional: bool,

    booleans: ?[]bool,
    int32s: ?[]i32,
    int64s: ?[]i64,
    floats: ?[]f32,
    doubles: ?[]f64,
    byte_arrays: ?[][]const u8, // each element is allocator-owned

    validity: ?[]bool, // null if column is REQUIRED
    num_rows: usize,
    allocator: Allocator,

    pub fn deinit(self: *ParquetColumn) void {
        self.allocator.free(self.name);

        if (self.booleans) |b| self.allocator.free(b);
        if (self.int32s) |b| self.allocator.free(b);
        if (self.int64s) |b| self.allocator.free(b);
        if (self.floats) |b| self.allocator.free(b);
        if (self.doubles) |b| self.allocator.free(b);
        if (self.byte_arrays) |arrays| {
            for (arrays) |arr| {
                self.allocator.free(arr);
            }
            self.allocator.free(arrays);
        }
        if (self.validity) |v| self.allocator.free(v);
    }

    pub fn initEmpty(allocator: Allocator) ParquetColumn {
        return .{
            .name = &.{},
            .physical_type = .boolean,
            .converted_type = null,
            .is_optional = false,
            .booleans = null,
            .int32s = null,
            .int64s = null,
            .floats = null,
            .doubles = null,
            .byte_arrays = null,
            .validity = null,
            .num_rows = 0,
            .allocator = allocator,
        };
    }
};

/// Result of reading a complete Parquet file.
pub const ParquetResult = struct {
    columns: []ParquetColumn, // allocator-owned array
    num_rows: usize,
    allocator: Allocator,

    pub fn deinit(self: *ParquetResult) void {
        for (self.columns) |*col| {
            col.deinit();
        }
        self.allocator.free(self.columns);
    }
};

// ============================================================
// Tests
// ============================================================

test "PhysicalType enum values match Thrift IDL" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(PhysicalType.boolean));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(PhysicalType.int32));
    try std.testing.expectEqual(@as(u8, 6), @intFromEnum(PhysicalType.byte_array));
    try std.testing.expectEqual(@as(u8, 7), @intFromEnum(PhysicalType.fixed_len_byte_array));
}

test "CompressionCodec enum values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(CompressionCodec.uncompressed));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(CompressionCodec.snappy));
}

test "ParquetColumn: initEmpty and deinit" {
    const allocator = std.testing.allocator;
    var col = ParquetColumn.initEmpty(allocator);
    col.deinit(); // should not crash
}

test "ParquetColumn: deinit with populated int32s" {
    const allocator = std.testing.allocator;
    var col = ParquetColumn.initEmpty(allocator);
    const name = try allocator.alloc(u8, 3);
    @memcpy(name, "age");
    col.name = name;
    col.physical_type = .int32;

    const data = try allocator.alloc(i32, 3);
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    col.int32s = data;
    col.num_rows = 3;
    col.deinit(); // should free name + int32s
}

test "ParquetColumn: deinit with byte_arrays frees all strings" {
    const allocator = std.testing.allocator;
    var col = ParquetColumn.initEmpty(allocator);
    const name = try allocator.alloc(u8, 4);
    @memcpy(name, "text");
    col.name = name;
    col.physical_type = .byte_array;

    const arrays = try allocator.alloc([]const u8, 2);
    arrays[0] = try allocator.dupe(u8, "hello");
    arrays[1] = try allocator.dupe(u8, "world");
    col.byte_arrays = arrays;
    col.num_rows = 2;
    col.deinit(); // should free each string + arrays + name
}

test "ParquetColumn: deinit with validity" {
    const allocator = std.testing.allocator;
    var col = ParquetColumn.initEmpty(allocator);
    const name = try allocator.alloc(u8, 3);
    @memcpy(name, "opt");
    col.name = name;
    col.is_optional = true;

    const validity = try allocator.alloc(bool, 3);
    validity[0] = true;
    validity[1] = false;
    validity[2] = true;
    col.validity = validity;

    const data = try allocator.alloc(f64, 3);
    data[0] = 1.0;
    data[1] = 0.0;
    data[2] = 3.0;
    col.doubles = data;
    col.num_rows = 3;
    col.deinit();
}

test "ParquetResult: deinit frees all columns" {
    const allocator = std.testing.allocator;
    const columns = try allocator.alloc(ParquetColumn, 2);

    // Column 0: int32
    columns[0] = ParquetColumn.initEmpty(allocator);
    const name0 = try allocator.alloc(u8, 1);
    name0[0] = 'a';
    columns[0].name = name0;
    columns[0].int32s = try allocator.alloc(i32, 1);
    columns[0].int32s.?[0] = 42;

    // Column 1: bool
    columns[1] = ParquetColumn.initEmpty(allocator);
    const name1 = try allocator.alloc(u8, 1);
    name1[0] = 'b';
    columns[1].name = name1;
    columns[1].booleans = try allocator.alloc(bool, 1);
    columns[1].booleans.?[0] = true;

    var result = ParquetResult{
        .columns = columns,
        .num_rows = 1,
        .allocator = allocator,
    };
    result.deinit(); // should free everything without leaks
}

test "Encoding enum values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(Encoding.plain));
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(Encoding.plain_dictionary));
    try std.testing.expectEqual(@as(u8, 3), @intFromEnum(Encoding.rle));
    try std.testing.expectEqual(@as(u8, 8), @intFromEnum(Encoding.rle_dictionary));
}

test "PageType enum values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(PageType.data_page));
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(PageType.dictionary_page));
    try std.testing.expectEqual(@as(u8, 3), @intFromEnum(PageType.data_page_v2));
}

test "ConvertedType enum values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(ConvertedType.utf8));
    try std.testing.expectEqual(@as(u8, 6), @intFromEnum(ConvertedType.date));
    try std.testing.expectEqual(@as(u8, 15), @intFromEnum(ConvertedType.int_8));
    try std.testing.expectEqual(@as(u8, 11), @intFromEnum(ConvertedType.uint_8));
}

test "FieldRepetitionType enum values" {
    try std.testing.expectEqual(@as(u8, 0), @intFromEnum(FieldRepetitionType.required));
    try std.testing.expectEqual(@as(u8, 1), @intFromEnum(FieldRepetitionType.optional));
    try std.testing.expectEqual(@as(u8, 2), @intFromEnum(FieldRepetitionType.repeated));
}
