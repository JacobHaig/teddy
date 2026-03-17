const std = @import("std");
const Allocator = std.mem.Allocator;
const ThriftReader = @import("thrift.zig").ThriftReader;
const CompactType = @import("thrift.zig").CompactType;
const types = @import("types.zig");

// ============================================================
// Parquet Metadata Structures (decoded from Thrift)
// ============================================================

pub const Statistics = struct {
    max: ?[]const u8 = null,
    min: ?[]const u8 = null,
    null_count: ?i64 = null,
    distinct_count: ?i64 = null,
    max_value: ?[]const u8 = null,
    min_value: ?[]const u8 = null,
    is_max_value_exact: ?bool = null,
    is_min_value_exact: ?bool = null,

    pub fn decode(reader: *ThriftReader) !Statistics {
        var result = Statistics{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.max = try reader.readBinary(),
                2 => result.min = try reader.readBinary(),
                3 => result.null_count = try reader.readZigZagI64(),
                4 => result.distinct_count = try reader.readZigZagI64(),
                5 => result.max_value = try reader.readBinary(),
                6 => result.min_value = try reader.readBinary(),
                7 => result.is_max_value_exact = fh.field_type == .boolean_true,
                8 => result.is_min_value_exact = fh.field_type == .boolean_true,
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const SchemaElement = struct {
    type_: ?types.PhysicalType = null,
    type_length: ?i32 = null,
    repetition_type: ?types.FieldRepetitionType = null,
    name: []const u8 = "",
    num_children: ?i32 = null,
    converted_type: ?types.ConvertedType = null,
    scale: ?i32 = null,
    precision: ?i32 = null,
    field_id: ?i32 = null,
    // LogicalType is a union — we skip it for now (converted_type covers most cases)

    pub fn decode(reader: *ThriftReader) !SchemaElement {
        var result = SchemaElement{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.type_ = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                2 => result.type_length = try reader.readZigZagI32(),
                3 => result.repetition_type = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                4 => result.name = try reader.readString(),
                5 => result.num_children = try reader.readZigZagI32(),
                6 => result.converted_type = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                7 => result.scale = try reader.readZigZagI32(),
                8 => result.precision = try reader.readZigZagI32(),
                9 => result.field_id = try reader.readZigZagI32(),
                10 => try reader.skip(fh.field_type), // LogicalType (union/struct) — skip
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const DataPageHeader = struct {
    num_values: i32 = 0,
    encoding: types.Encoding = .plain,
    definition_level_encoding: types.Encoding = .rle,
    repetition_level_encoding: types.Encoding = .rle,
    statistics: ?Statistics = null,

    pub fn decode(reader: *ThriftReader) !DataPageHeader {
        var result = DataPageHeader{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.encoding = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                3 => result.definition_level_encoding = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                4 => result.repetition_level_encoding = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                5 => result.statistics = try Statistics.decode(reader),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const DictionaryPageHeader = struct {
    num_values: i32 = 0,
    encoding: types.Encoding = .plain,
    is_sorted: ?bool = null,

    pub fn decode(reader: *ThriftReader) !DictionaryPageHeader {
        var result = DictionaryPageHeader{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.encoding = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                3 => result.is_sorted = fh.field_type == .boolean_true,
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const DataPageHeaderV2 = struct {
    num_values: i32 = 0,
    num_nulls: i32 = 0,
    num_rows: i32 = 0,
    encoding: types.Encoding = .plain,
    definition_levels_byte_length: i32 = 0,
    repetition_levels_byte_length: i32 = 0,
    is_compressed: ?bool = null, // default true
    statistics: ?Statistics = null,

    pub fn decode(reader: *ThriftReader) !DataPageHeaderV2 {
        var result = DataPageHeaderV2{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.num_nulls = try reader.readZigZagI32(),
                3 => result.num_rows = try reader.readZigZagI32(),
                4 => result.encoding = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                5 => result.definition_levels_byte_length = try reader.readZigZagI32(),
                6 => result.repetition_levels_byte_length = try reader.readZigZagI32(),
                7 => result.is_compressed = fh.field_type == .boolean_true,
                8 => result.statistics = try Statistics.decode(reader),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const PageHeader = struct {
    page_type: types.PageType = .data_page,
    uncompressed_page_size: i32 = 0,
    compressed_page_size: i32 = 0,
    crc: ?i32 = null,
    data_page_header: ?DataPageHeader = null,
    index_page_header: bool = false, // placeholder
    dictionary_page_header: ?DictionaryPageHeader = null,
    data_page_header_v2: ?DataPageHeaderV2 = null,

    pub fn decode(reader: *ThriftReader) !PageHeader {
        var result = PageHeader{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.page_type = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                2 => result.uncompressed_page_size = try reader.readZigZagI32(),
                3 => result.compressed_page_size = try reader.readZigZagI32(),
                4 => result.crc = try reader.readZigZagI32(),
                5 => result.data_page_header = try DataPageHeader.decode(reader),
                6 => try reader.skip(fh.field_type), // IndexPageHeader — skip
                7 => result.dictionary_page_header = try DictionaryPageHeader.decode(reader),
                8 => result.data_page_header_v2 = try DataPageHeaderV2.decode(reader),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const KeyValue = struct {
    key: []const u8 = "",
    value: ?[]const u8 = null,

    pub fn decode(reader: *ThriftReader) !KeyValue {
        var result = KeyValue{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.key = try reader.readString(),
                2 => result.value = try reader.readString(),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }
};

pub const ColumnMetaData = struct {
    type_: types.PhysicalType = .boolean,
    encodings: []types.Encoding = &.{},
    path_in_schema: [][]const u8 = &.{},
    codec: types.CompressionCodec = .uncompressed,
    num_values: i64 = 0,
    total_uncompressed_size: i64 = 0,
    total_compressed_size: i64 = 0,
    key_value_metadata: ?[]KeyValue = null,
    data_page_offset: i64 = 0,
    index_page_offset: ?i64 = null,
    dictionary_page_offset: ?i64 = null,
    statistics: ?Statistics = null,

    // Whether we allocated the arrays
    _encodings_owned: bool = false,
    _paths_owned: bool = false,

    pub fn decode(reader: *ThriftReader, allocator: Allocator) !ColumnMetaData {
        var result = ColumnMetaData{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.type_ = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                2 => {
                    // list<Encoding>
                    const list_hdr = try reader.readListHeader();
                    const encs = try allocator.alloc(types.Encoding, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        encs[i] = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32())));
                    }
                    result.encodings = encs;
                    result._encodings_owned = true;
                },
                3 => {
                    // list<string>
                    const list_hdr = try reader.readListHeader();
                    const paths = try allocator.alloc([]const u8, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        paths[i] = try reader.readString();
                    }
                    result.path_in_schema = paths;
                    result._paths_owned = true;
                },
                4 => result.codec = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32()))),
                5 => result.num_values = try reader.readZigZagI64(),
                6 => result.total_uncompressed_size = try reader.readZigZagI64(),
                7 => result.total_compressed_size = try reader.readZigZagI64(),
                8 => {
                    // list<KeyValue> key_value_metadata — skip for now
                    try reader.skip(fh.field_type);
                },
                9 => result.data_page_offset = try reader.readZigZagI64(),
                10 => result.index_page_offset = try reader.readZigZagI64(),
                11 => result.dictionary_page_offset = try reader.readZigZagI64(),
                12 => result.statistics = try Statistics.decode(reader),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn deinit(self: *ColumnMetaData, allocator: Allocator) void {
        if (self._encodings_owned) allocator.free(self.encodings);
        if (self._paths_owned) allocator.free(self.path_in_schema);
    }
};

pub const ColumnChunk = struct {
    file_path: ?[]const u8 = null,
    file_offset: i64 = 0,
    meta_data: ?ColumnMetaData = null,

    pub fn decode(reader: *ThriftReader, allocator: Allocator) !ColumnChunk {
        var result = ColumnChunk{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.file_path = try reader.readString(),
                2 => result.file_offset = try reader.readZigZagI64(),
                3 => result.meta_data = try ColumnMetaData.decode(reader, allocator),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn deinit(self: *ColumnChunk, allocator: Allocator) void {
        if (self.meta_data) |*md| md.deinit(allocator);
    }
};

pub const RowGroup = struct {
    columns: []ColumnChunk = &.{},
    total_byte_size: i64 = 0,
    num_rows: i64 = 0,
    sorting_columns: bool = false, // placeholder
    file_offset: ?i64 = null,
    total_compressed_size: ?i64 = null,
    ordinal: ?i16 = null,

    _columns_owned: bool = false,

    pub fn decode(reader: *ThriftReader, allocator: Allocator) !RowGroup {
        var result = RowGroup{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => {
                    // list<ColumnChunk>
                    const list_hdr = try reader.readListHeader();
                    const cols = try allocator.alloc(ColumnChunk, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        cols[i] = try ColumnChunk.decode(reader, allocator);
                    }
                    result.columns = cols;
                    result._columns_owned = true;
                },
                2 => result.total_byte_size = try reader.readZigZagI64(),
                3 => result.num_rows = try reader.readZigZagI64(),
                4 => try reader.skip(fh.field_type), // sorting_columns
                5 => result.file_offset = try reader.readZigZagI64(),
                6 => result.total_compressed_size = try reader.readZigZagI64(),
                7 => result.ordinal = try reader.readZigZagI16(),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn deinit(self: *RowGroup, allocator: Allocator) void {
        if (self._columns_owned) {
            for (self.columns) |*col| col.deinit(allocator);
            allocator.free(self.columns);
        }
    }
};

pub const FileMetaData = struct {
    version: i32 = 0,
    schema: []SchemaElement = &.{},
    num_rows: i64 = 0,
    row_groups: []RowGroup = &.{},
    key_value_metadata: ?[]KeyValue = null,
    created_by: ?[]const u8 = null,

    _schema_owned: bool = false,
    _row_groups_owned: bool = false,
    _kv_owned: bool = false,

    pub fn decode(reader: *ThriftReader, allocator: Allocator) !FileMetaData {
        var result = FileMetaData{};
        reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.version = try reader.readZigZagI32(),
                2 => {
                    // list<SchemaElement>
                    const list_hdr = try reader.readListHeader();
                    const elems = try allocator.alloc(SchemaElement, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        elems[i] = try SchemaElement.decode(reader);
                    }
                    result.schema = elems;
                    result._schema_owned = true;
                },
                3 => result.num_rows = try reader.readZigZagI64(),
                4 => {
                    // list<RowGroup>
                    const list_hdr = try reader.readListHeader();
                    const rgs = try allocator.alloc(RowGroup, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        rgs[i] = try RowGroup.decode(reader, allocator);
                    }
                    result.row_groups = rgs;
                    result._row_groups_owned = true;
                },
                5 => {
                    // list<KeyValue>
                    const list_hdr = try reader.readListHeader();
                    const kvs = try allocator.alloc(KeyValue, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        kvs[i] = try KeyValue.decode(reader);
                    }
                    result.key_value_metadata = kvs;
                    result._kv_owned = true;
                },
                6 => result.created_by = try reader.readString(),
                7 => try reader.skip(fh.field_type), // column_orders
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn deinit(self: *FileMetaData, allocator: Allocator) void {
        if (self._schema_owned) allocator.free(self.schema);
        if (self._row_groups_owned) {
            for (self.row_groups) |*rg| rg.deinit(allocator);
            allocator.free(self.row_groups);
        }
        if (self._kv_owned) {
            if (self.key_value_metadata) |kvs| allocator.free(kvs);
        }
    }
};

// ============================================================
// Tests
// ============================================================

test "SchemaElement decode: simple field" {
    // Build a Thrift-encoded SchemaElement: name="age", type=INT64(2), repetition=OPTIONAL(1)
    // Field 1 (type): delta=1, type=i32 → 0x15, value=zigzag(2)=4 → 0x04
    // Field 3 (repetition): delta=2, type=i32 → 0x25, value=zigzag(1)=2 → 0x02
    // Field 4 (name): delta=1, type=binary → 0x18, length=3 "age"
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x04, // field 1 (type=INT64, zigzag(2)=4)
        0x25, 0x02, // field 3 (repetition=OPTIONAL, zigzag(1)=2)
        0x18, 0x03, 'a', 'g', 'e', // field 4 (name="age")
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqual(types.PhysicalType.int64, elem.type_.?);
    try std.testing.expectEqual(types.FieldRepetitionType.optional, elem.repetition_type.?);
    try std.testing.expectEqualStrings("age", elem.name);
}

test "PageHeader decode: data page" {
    // PageHeader: page_type=DATA_PAGE(0), uncompressed=100, compressed=80
    // Field 1 (page_type): delta=1, i32 → 0x15, zigzag(0)=0 → 0x00
    // Field 2 (uncompressed): delta=1, i32 → 0x15, zigzag(100)=200 → [0xC8, 0x01]
    // Field 3 (compressed): delta=1, i32 → 0x15, zigzag(80)=160 → [0xA0, 0x01]
    // Field 5 (data_page_header): delta=2, struct → 0x2C
    //   DataPageHeader: num_values=10, encoding=PLAIN(0), def=RLE(3), rep=RLE(3)
    //   Field 1: 0x15, zigzag(10)=20=0x14
    //   Field 2: 0x15, zigzag(0)=0=0x00
    //   Field 3: 0x15, zigzag(3)=6=0x06
    //   Field 4: 0x15, zigzag(3)=6=0x06
    //   Stop: 0x00
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x00, // page_type = DATA_PAGE
        0x15, 0xC8, 0x01, // uncompressed = 100
        0x15, 0xA0, 0x01, // compressed = 80
        0x2C, // field 5 = data_page_header (struct)
        0x15, 0x14, // num_values = 10
        0x15, 0x00, // encoding = PLAIN
        0x15, 0x06, // def_level_encoding = RLE
        0x15, 0x06, // rep_level_encoding = RLE
        0x00, // stop (DataPageHeader)
        0x00, // stop (PageHeader)
    };
    var reader = ThriftReader.init(&data);
    const ph = try PageHeader.decode(&reader);
    try std.testing.expectEqual(types.PageType.data_page, ph.page_type);
    try std.testing.expectEqual(@as(i32, 100), ph.uncompressed_page_size);
    try std.testing.expectEqual(@as(i32, 80), ph.compressed_page_size);
    try std.testing.expect(ph.data_page_header != null);
    try std.testing.expectEqual(@as(i32, 10), ph.data_page_header.?.num_values);
    try std.testing.expectEqual(types.Encoding.plain, ph.data_page_header.?.encoding);
}

test "DictionaryPageHeader decode" {
    // Field 1 (num_values): delta=1, i32 → 0x15, zigzag(50)=100 → 0x64
    // Field 2 (encoding): delta=1, i32 → 0x15, zigzag(0)=0 → 0x00 (PLAIN)
    // Field 3 (is_sorted): delta=1, boolean_true → 0x11
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x64, // num_values = 50
        0x15, 0x00, // encoding = PLAIN
        0x11, // is_sorted = true
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const dph = try DictionaryPageHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 50), dph.num_values);
    try std.testing.expectEqual(types.Encoding.plain, dph.encoding);
    try std.testing.expectEqual(true, dph.is_sorted.?);
}

test "DictionaryPageHeader decode: unsorted" {
    // Field 1: num_values = 5
    // Field 3: is_sorted = false (boolean_false = type nibble 2)
    const data = [_]u8{
        0x15, 0x0A, // num_values = 5 (zigzag(5)=10)
        0x25, 0x00, // field 3 encoding = PLAIN (skip field 2 to test delta=2)
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const dph = try DictionaryPageHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 5), dph.num_values);
}

test "DataPageHeaderV2 decode" {
    // Field 1 (num_values): delta=1, i32 → 0x15, zigzag(100)=200 → [0xC8, 0x01]
    // Field 2 (num_nulls): delta=1, i32 → 0x15, zigzag(5)=10 → 0x0A
    // Field 3 (num_rows): delta=1, i32 → 0x15, zigzag(100)=200 → [0xC8, 0x01]
    // Field 4 (encoding): delta=1, i32 → 0x15, zigzag(8)=16 → 0x10 (RLE_DICTIONARY)
    // Field 5 (def_levels_byte_length): delta=1, i32 → 0x15, zigzag(20)=40 → 0x28
    // Field 6 (rep_levels_byte_length): delta=1, i32 → 0x15, zigzag(0)=0 → 0x00
    // Field 7 (is_compressed): delta=1, boolean_true → 0x11
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0xC8, 0x01, // num_values = 100
        0x15, 0x0A, // num_nulls = 5
        0x15, 0xC8, 0x01, // num_rows = 100
        0x15, 0x10, // encoding = RLE_DICTIONARY (8)
        0x15, 0x28, // def_levels_byte_length = 20
        0x15, 0x00, // rep_levels_byte_length = 0
        0x11, // is_compressed = true
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const v2 = try DataPageHeaderV2.decode(&reader);
    try std.testing.expectEqual(@as(i32, 100), v2.num_values);
    try std.testing.expectEqual(@as(i32, 5), v2.num_nulls);
    try std.testing.expectEqual(@as(i32, 100), v2.num_rows);
    try std.testing.expectEqual(types.Encoding.rle_dictionary, v2.encoding);
    try std.testing.expectEqual(@as(i32, 20), v2.definition_levels_byte_length);
    try std.testing.expectEqual(@as(i32, 0), v2.repetition_levels_byte_length);
    try std.testing.expectEqual(true, v2.is_compressed.?);
}

test "KeyValue decode" {
    // Field 1 (key): delta=1, binary → 0x18, length=5, "hello"
    // Field 2 (value): delta=1, binary → 0x18, length=5, "world"
    // Stop: 0x00
    const data = [_]u8{
        0x18, 0x05, 'h', 'e', 'l', 'l', 'o', // key = "hello"
        0x18, 0x05, 'w', 'o', 'r', 'l', 'd', // value = "world"
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const kv = try KeyValue.decode(&reader);
    try std.testing.expectEqualStrings("hello", kv.key);
    try std.testing.expectEqualStrings("world", kv.value.?);
}

test "ColumnMetaData decode" {
    const allocator = std.testing.allocator;
    // Field 1 (type): delta=1, i32 → 0x15, zigzag(6)=12 → 0x0C (BYTE_ARRAY)
    // Field 2 (encodings): delta=1, list → 0x19
    //   list header: 2 elements, type i32 → 0x25
    //   zigzag(0)=0 (PLAIN), zigzag(8)=16 → 0x10 (RLE_DICTIONARY)
    // Field 4 (codec): delta=2, i32 → 0x25, zigzag(1)=2 → 0x02 (SNAPPY)
    // Field 5 (num_values): delta=1, i64 → 0x16, zigzag(100)=200 → [0xC8, 0x01]
    // Field 6 (total_uncompressed): delta=1, i64 → 0x16, zigzag(500)=1000 → [0xE8, 0x07]
    // Field 7 (total_compressed): delta=1, i64 → 0x16, zigzag(400)=800 → [0xA0, 0x06]
    // Field 9 (data_page_offset): delta=2, i64 → 0x26, zigzag(4)=8 → 0x08
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x0C, // type = BYTE_ARRAY (6)
        0x19, // field 2: list
        0x25, 0x00, 0x10, // list(2, i32): PLAIN, RLE_DICTIONARY
        0x25, 0x02, // field 4: codec = SNAPPY (1)
        0x16, 0xC8, 0x01, // field 5: num_values = 100
        0x16, 0xE8, 0x07, // field 6: total_uncompressed = 500
        0x16, 0xA0, 0x06, // field 7: total_compressed = 400
        0x26, 0x08, // field 9: data_page_offset = 4
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    var cmd = try ColumnMetaData.decode(&reader, allocator);
    defer cmd.deinit(allocator);

    try std.testing.expectEqual(types.PhysicalType.byte_array, cmd.type_);
    try std.testing.expectEqual(@as(usize, 2), cmd.encodings.len);
    try std.testing.expectEqual(types.Encoding.plain, cmd.encodings[0]);
    try std.testing.expectEqual(types.Encoding.rle_dictionary, cmd.encodings[1]);
    try std.testing.expectEqual(types.CompressionCodec.snappy, cmd.codec);
    try std.testing.expectEqual(@as(i64, 100), cmd.num_values);
    try std.testing.expectEqual(@as(i64, 500), cmd.total_uncompressed_size);
    try std.testing.expectEqual(@as(i64, 400), cmd.total_compressed_size);
    try std.testing.expectEqual(@as(i64, 4), cmd.data_page_offset);
}

test "Statistics decode: null_count and distinct_count" {
    // Field 3 (null_count): delta=3, i64 → 0x36, zigzag(5)=10 → 0x0A
    // Field 4 (distinct_count): delta=1, i64 → 0x16, zigzag(42)=84 → 0x54
    // Stop: 0x00
    const data = [_]u8{
        0x36, 0x0A, // null_count = 5
        0x16, 0x54, // distinct_count = 42
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const stats = try Statistics.decode(&reader);
    try std.testing.expectEqual(@as(i64, 5), stats.null_count.?);
    try std.testing.expectEqual(@as(i64, 42), stats.distinct_count.?);
    try std.testing.expect(stats.max == null);
    try std.testing.expect(stats.min == null);
}

test "PageHeader decode: dictionary page" {
    // Field 1 (page_type): delta=1, i32 → 0x15, zigzag(2)=4 → 0x04 (DICTIONARY_PAGE)
    // Field 2 (uncompressed): delta=1, i32 → 0x15, zigzag(200)=400 → [0x90, 0x03]
    // Field 3 (compressed): delta=1, i32 → 0x15, zigzag(150)=300 → [0xAC, 0x02]
    // Field 7 (dict_page_header): delta=4, struct → 0x4C
    //   num_values=25: 0x15, zigzag(25)=50 → 0x32
    //   encoding=PLAIN: 0x15, 0x00
    //   stop: 0x00
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x04, // page_type = DICTIONARY_PAGE (2)
        0x15, 0x90, 0x03, // uncompressed = 200
        0x15, 0xAC, 0x02, // compressed = 150
        0x4C, // field 7 = dict_page_header (struct, delta=4)
        0x15, 0x32, // num_values = 25
        0x15, 0x00, // encoding = PLAIN
        0x00, // stop (DictionaryPageHeader)
        0x00, // stop (PageHeader)
    };
    var reader = ThriftReader.init(&data);
    const ph = try PageHeader.decode(&reader);
    try std.testing.expectEqual(types.PageType.dictionary_page, ph.page_type);
    try std.testing.expectEqual(@as(i32, 200), ph.uncompressed_page_size);
    try std.testing.expectEqual(@as(i32, 150), ph.compressed_page_size);
    try std.testing.expect(ph.dictionary_page_header != null);
    try std.testing.expectEqual(@as(i32, 25), ph.dictionary_page_header.?.num_values);
    try std.testing.expectEqual(types.Encoding.plain, ph.dictionary_page_header.?.encoding);
    try std.testing.expect(ph.data_page_header == null);
}

test "SchemaElement decode: root node with num_children" {
    // Field 4 (name): delta=4, binary → 0x48, length=6, "schema"
    // Field 5 (num_children): delta=1, i32 → 0x15, zigzag(3)=6 → 0x06
    // Stop: 0x00
    const data = [_]u8{
        0x48, 0x06, 's', 'c', 'h', 'e', 'm', 'a', // name = "schema"
        0x15, 0x06, // num_children = 3
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqualStrings("schema", elem.name);
    try std.testing.expectEqual(@as(i32, 3), elem.num_children.?);
    try std.testing.expect(elem.type_ == null); // root node has no type
    try std.testing.expect(elem.repetition_type == null);
}

test "SchemaElement decode: field with converted_type" {
    // Field 1 (type): delta=1, i32 → 0x15, zigzag(6)=12 → 0x0C (BYTE_ARRAY)
    // Field 3 (repetition): delta=2, i32 → 0x25, zigzag(1)=2 → 0x02 (OPTIONAL)
    // Field 4 (name): delta=1, binary → 0x18, length=4, "name"
    // Field 6 (converted_type): delta=2, i32 → 0x25, zigzag(0)=0 → 0x00 (UTF8)
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x0C, // type = BYTE_ARRAY
        0x25, 0x02, // repetition = OPTIONAL
        0x18, 0x04, 'n', 'a', 'm', 'e', // name = "name"
        0x25, 0x00, // converted_type = UTF8
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqual(types.PhysicalType.byte_array, elem.type_.?);
    try std.testing.expectEqual(types.FieldRepetitionType.optional, elem.repetition_type.?);
    try std.testing.expectEqualStrings("name", elem.name);
    try std.testing.expectEqual(types.ConvertedType.utf8, elem.converted_type.?);
}

test "ColumnChunk decode" {
    const allocator = std.testing.allocator;
    // Field 2 (file_offset): delta=2, i64 → 0x26, zigzag(100)=200 → [0xC8, 0x01]
    // Field 3 (meta_data): delta=1, struct → 0x1C
    //   Field 1 (type): 0x15, zigzag(1)=2 → 0x02 (INT32)
    //   Field 4 (codec): delta=3, i32 → 0x35, zigzag(0)=0 → 0x00 (UNCOMPRESSED)
    //   Field 5 (num_values): delta=1, i64 → 0x16, zigzag(7)=14 → 0x0E
    //   Field 6: delta=1, i64 → 0x16, zigzag(28)=56 → 0x38
    //   Field 7: delta=1, i64 → 0x16, zigzag(28)=56 → 0x38
    //   Field 9 (data_page_offset): delta=2, i64 → 0x26, zigzag(4)=8 → 0x08
    //   Stop: 0x00
    // Stop: 0x00
    const data = [_]u8{
        0x26, 0xC8, 0x01, // file_offset = 100
        0x1C, // meta_data (struct)
        0x15, 0x02, //   type = INT32 (1)
        0x35, 0x00, //   field 4: codec = UNCOMPRESSED
        0x16, 0x0E, //   num_values = 7
        0x16, 0x38, //   total_uncompressed = 28
        0x16, 0x38, //   total_compressed = 28
        0x26, 0x08, //   data_page_offset = 4
        0x00, // stop (ColumnMetaData)
        0x00, // stop (ColumnChunk)
    };
    var reader = ThriftReader.init(&data);
    var cc = try ColumnChunk.decode(&reader, allocator);
    defer cc.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 100), cc.file_offset);
    try std.testing.expect(cc.meta_data != null);
    try std.testing.expectEqual(types.PhysicalType.int32, cc.meta_data.?.type_);
    try std.testing.expectEqual(types.CompressionCodec.uncompressed, cc.meta_data.?.codec);
    try std.testing.expectEqual(@as(i64, 7), cc.meta_data.?.num_values);
    try std.testing.expectEqual(@as(i64, 4), cc.meta_data.?.data_page_offset);
}

test "RowGroup decode: single column" {
    const allocator = std.testing.allocator;
    // Field 1 (columns): delta=1, list → 0x19
    //   list header: 1 element of struct → 0x1C
    //   ColumnChunk:
    //     Field 2 (file_offset): 0x26, zigzag(0)=0 → 0x00
    //     Stop: 0x00
    // Field 2 (total_byte_size): delta=1, i64 → 0x16, zigzag(500)=1000 → [0xE8, 0x07]
    // Field 3 (num_rows): delta=1, i64 → 0x16, zigzag(7)=14 → 0x0E
    // Stop: 0x00
    const data = [_]u8{
        0x19, // field 1: list
        0x1C, // list(1, struct)
        0x26, 0x00, // ColumnChunk: file_offset = 0
        0x00, // stop (ColumnChunk)
        0x16, 0xE8, 0x07, // total_byte_size = 500
        0x16, 0x0E, // num_rows = 7
        0x00, // stop (RowGroup)
    };
    var reader = ThriftReader.init(&data);
    var rg = try RowGroup.decode(&reader, allocator);
    defer rg.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), rg.columns.len);
    try std.testing.expectEqual(@as(i64, 500), rg.total_byte_size);
    try std.testing.expectEqual(@as(i64, 7), rg.num_rows);
}

test "FileMetaData decode: minimal" {
    const allocator = std.testing.allocator;
    // Field 1 (version): delta=1, i32 → 0x15, zigzag(2)=4 → 0x04
    // Field 2 (schema): delta=1, list → 0x19
    //   list header: 1 element of struct → 0x1C
    //   SchemaElement: name="root", num_children=0
    //     Field 4 (name): 0x48, len=4, "root"
    //     Stop: 0x00
    // Field 3 (num_rows): delta=1, i64 → 0x16, zigzag(0)=0 → 0x00
    // Field 4 (row_groups): delta=1, list → 0x19
    //   list header: 0 elements of struct → 0x0C
    // Stop: 0x00
    const data = [_]u8{
        0x15, 0x04, // version = 2
        0x19, // field 2: list (schema)
        0x1C, // list(1, struct)
        0x48, 0x04, 'r', 'o', 'o', 't', // name = "root"
        0x00, // stop (SchemaElement)
        0x16, 0x00, // num_rows = 0
        0x19, // field 4: list (row_groups)
        0x0C, // list(0, struct)
        0x00, // stop (FileMetaData)
    };
    var reader = ThriftReader.init(&data);
    var fmd = try FileMetaData.decode(&reader, allocator);
    defer fmd.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 2), fmd.version);
    try std.testing.expectEqual(@as(usize, 1), fmd.schema.len);
    try std.testing.expectEqualStrings("root", fmd.schema[0].name);
    try std.testing.expectEqual(@as(i64, 0), fmd.num_rows);
    try std.testing.expectEqual(@as(usize, 0), fmd.row_groups.len);
}

test "FileMetaData decode: with created_by" {
    const allocator = std.testing.allocator;
    // Field 1: version=2
    // Field 2: schema list with 1 element
    // Field 3: num_rows=0
    // Field 4: row_groups empty
    // Field 6: created_by = "test"
    const data = [_]u8{
        0x15, 0x04, // version = 2
        0x19, // schema list
        0x1C, // list(1, struct)
        0x48, 0x01, 'x', // SchemaElement: name="x"
        0x00, // stop (SchemaElement)
        0x16, 0x00, // num_rows = 0
        0x19, 0x0C, // row_groups: list(0, struct)
        0x28, 0x04, 't', 'e', 's', 't', // field 6: created_by = "test"
        0x00, // stop
    };
    var reader = ThriftReader.init(&data);
    var fmd = try FileMetaData.decode(&reader, allocator);
    defer fmd.deinit(allocator);

    try std.testing.expectEqualStrings("test", fmd.created_by.?);
}
