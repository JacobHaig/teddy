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

