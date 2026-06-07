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

// ============================================================
// Modern Logical Types (parquet.thrift LogicalType union, field 10
// of SchemaElement). ConvertedType above is the legacy 1:1 enum.
// ============================================================

pub const TimeUnit = enum { millis, micros, nanos };

pub const DecimalParams = struct { scale: i32, precision: i32 };
pub const TimeParams = struct { is_adjusted_to_utc: bool, unit: TimeUnit };
pub const TimestampParams = struct { is_adjusted_to_utc: bool, unit: TimeUnit };
pub const IntParams = struct { bit_width: i8, is_signed: bool };

/// One variant per parquet.thrift LogicalType union field. Parameterless
/// variants are empty thrift structs on the wire. VARIANT/GEOMETRY/GEOGRAPHY
/// carry optional fields we don't model yet — they decode by skipping the
/// payload and re-encode as empty structs (all their fields are optional).
pub const LogicalType = union(enum) {
    string, // 1
    map, // 2
    list, // 3
    @"enum", // 4
    decimal: DecimalParams, // 5
    date, // 6
    time: TimeParams, // 7
    timestamp: TimestampParams, // 8
    integer: IntParams, // 10
    unknown, // 11 (NullType)
    json, // 12
    bson, // 13
    uuid, // 14
    float16, // 15
    variant, // 16
    geometry, // 17
    geography, // 18
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
// Owned Parquet Schema Tree (nested columns)
// ============================================================

/// Owned parquet schema subtree (nested columns). Allocated by
/// buildSchemaTree; freed via deinit. Names are owned copies.
pub const SchemaNode = struct {
    name: []u8,
    /// required/optional/repeated of THIS node.
    repetition: FieldRepetitionType,
    /// Leaf payload (null for group nodes).
    physical: ?PhysicalType = null,
    converted: ?ConvertedType = null,
    logical: ?LogicalType = null,
    type_length: ?i32 = null,
    scale: ?i32 = null,
    precision: ?i32 = null,
    /// For leaves: cumulative levels (ancestor walk).
    max_def: u8 = 0,
    max_rep: u8 = 0,
    /// For leaves: index into the row group's column-chunk order.
    leaf_index: ?usize = null,
    children: []SchemaNode = &.{},

    pub fn deinit(self: *SchemaNode, allocator: Allocator) void {
        for (self.children) |*child| child.deinit(allocator);
        if (self.children.len > 0) allocator.free(self.children);
        allocator.free(self.name);
    }

    pub fn isLeaf(self: *const SchemaNode) bool {
        return self.children.len == 0 and self.physical != null;
    }

    /// Index of the immediate child named `name`, or null. Used by slice .2 /
    /// accessors to resolve struct field names positionally.
    pub fn fieldIndex(self: *const SchemaNode, name: []const u8) ?usize {
        for (self.children, 0..) |*child, i| {
            if (std.mem.eql(u8, child.name, name)) return i;
        }
        return null;
    }
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
    logical_type: ?LogicalType,
    type_length: ?i32, // non-null only for FIXED_LEN_BYTE_ARRAY
    // Legacy DECIMAL fields 7/8; modern files also carry them in logical_type.
    scale: ?i32,
    precision: ?i32,
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

    /// True for leaf columns derived from a nested subtree (LIST/MAP/STRUCT).
    /// Such columns are surfaced via ParquetResult.nested_columns, never in
    /// `columns`, and carry raw level streams for record assembly (slice .2).
    nested: bool = false,
    /// Which top-level child of the schema root this leaf descends from. Lets
    /// slice .2 group sibling leaves back into one nested root. 0 for flat
    /// columns whose direct parent is the root.
    root_child_index: usize = 0,
    /// Raw per-leaf level streams, populated only for nested leaves
    /// (max_rep > 0 or def depth > 1). Lengths equal the leaf's total value
    /// count (incl. nulls); values arrays hold only present values.
    def_levels: ?[]u16 = null,
    rep_levels: ?[]u16 = null,

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
        if (self.def_levels) |d| self.allocator.free(d);
        if (self.rep_levels) |r| self.allocator.free(r);
    }

    pub fn initEmpty(allocator: Allocator) ParquetColumn {
        return .{
            .name = &.{},
            .physical_type = .boolean,
            .converted_type = null,
            .logical_type = null,
            .type_length = null,
            .scale = null,
            .precision = null,
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
            .nested = false,
            .root_child_index = 0,
            .def_levels = null,
            .rep_levels = null,
        };
    }
};

/// Result of reading a complete Parquet file.
pub const ParquetResult = struct {
    columns: []ParquetColumn, // allocator-owned array; FLAT leaves only
    num_rows: usize,
    allocator: Allocator,

    /// Owned schema tree (root node + descendants). Always set on success.
    /// Slice .2 walks this to assemble nested records.
    schema_tree: ?SchemaNode = null,
    /// Leaf columns derived from nested subtrees (LIST/MAP/STRUCT), excluded
    /// from `columns` so the flat dataframe adapter never sees them. Each
    /// carries raw def/rep streams. Owned; freed here.
    nested_columns: []ParquetColumn = &.{},

    pub fn deinit(self: *ParquetResult) void {
        for (self.columns) |*col| {
            col.deinit();
        }
        self.allocator.free(self.columns);
        for (self.nested_columns) |*col| {
            col.deinit();
        }
        if (self.nested_columns.len > 0) self.allocator.free(self.nested_columns);
        if (self.schema_tree) |*tree| tree.deinit(self.allocator);
    }
};

