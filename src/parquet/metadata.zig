const std = @import("std");
const Allocator = std.mem.Allocator;
const ThriftReader = @import("thrift.zig").ThriftReader;
const CompactType = @import("thrift.zig").CompactType;
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
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
                10 => try reader.skip(fh.field_type),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn encode(self: *const SchemaElement, w: *ThriftWriter) !void {
        w.pushStruct();
        if (self.type_) |t| {
            try w.writeFieldHeader(1, .i32);
            try w.writeZigZagI32(@intCast(@intFromEnum(t)));
        }
        if (self.repetition_type) |r| {
            try w.writeFieldHeader(3, .i32);
            try w.writeZigZagI32(@intCast(@intFromEnum(r)));
        }
        try w.writeFieldHeader(4, .binary);
        try w.writeString(self.name);
        if (self.num_children) |n| {
            try w.writeFieldHeader(5, .i32);
            try w.writeZigZagI32(n);
        }
        if (self.converted_type) |ct| {
            try w.writeFieldHeader(6, .i32);
            try w.writeZigZagI32(@intCast(@intFromEnum(ct)));
        }
        try w.writeFieldStop();
        w.popStruct();
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

    pub fn encode(self: *const DataPageHeader, w: *ThriftWriter) !void {
        w.pushStruct();
        try w.writeFieldHeader(1, .i32);
        try w.writeZigZagI32(self.num_values);
        try w.writeFieldHeader(2, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.encoding)));
        try w.writeFieldHeader(3, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.definition_level_encoding)));
        try w.writeFieldHeader(4, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.repetition_level_encoding)));
        try w.writeFieldStop();
        w.popStruct();
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
    is_compressed: ?bool = null,
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
    index_page_header: bool = false,
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
                6 => try reader.skip(fh.field_type),
                7 => result.dictionary_page_header = try DictionaryPageHeader.decode(reader),
                8 => result.data_page_header_v2 = try DataPageHeaderV2.decode(reader),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn encode(self: *const PageHeader, w: *ThriftWriter) !void {
        w.pushStruct();
        try w.writeFieldHeader(1, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.page_type)));
        try w.writeFieldHeader(2, .i32);
        try w.writeZigZagI32(self.uncompressed_page_size);
        try w.writeFieldHeader(3, .i32);
        try w.writeZigZagI32(self.compressed_page_size);
        if (self.data_page_header) |dph| {
            try w.writeFieldHeader(5, .@"struct");
            try dph.encode(w);
        }
        try w.writeFieldStop();
        w.popStruct();
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
                    const list_hdr = try reader.readListHeader();
                    const encs = try allocator.alloc(types.Encoding, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        encs[i] = @enumFromInt(@as(u8, @intCast(try reader.readZigZagI32())));
                    }
                    result.encodings = encs;
                    result._encodings_owned = true;
                },
                3 => {
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
                8 => try reader.skip(fh.field_type),
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

    pub fn encode(self: *const ColumnMetaData, w: *ThriftWriter) !void {
        w.pushStruct();
        // Field 1: type
        try w.writeFieldHeader(1, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.type_)));
        // Field 2: encodings list
        try w.writeFieldHeader(2, .list);
        try w.writeListHeader(.i32, @intCast(self.encodings.len));
        for (self.encodings) |enc| {
            try w.writeZigZagI32(@intCast(@intFromEnum(enc)));
        }
        // Field 3: path_in_schema list
        try w.writeFieldHeader(3, .list);
        try w.writeListHeader(.binary, @intCast(self.path_in_schema.len));
        for (self.path_in_schema) |path| {
            try w.writeString(path);
        }
        // Field 4: codec
        try w.writeFieldHeader(4, .i32);
        try w.writeZigZagI32(@intCast(@intFromEnum(self.codec)));
        // Field 5: num_values
        try w.writeFieldHeader(5, .i64);
        try w.writeZigZagI64(self.num_values);
        // Field 6: total_uncompressed_size
        try w.writeFieldHeader(6, .i64);
        try w.writeZigZagI64(self.total_uncompressed_size);
        // Field 7: total_compressed_size
        try w.writeFieldHeader(7, .i64);
        try w.writeZigZagI64(self.total_compressed_size);
        // Field 9: data_page_offset
        try w.writeFieldHeader(9, .i64);
        try w.writeZigZagI64(self.data_page_offset);
        try w.writeFieldStop();
        w.popStruct();
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

    pub fn encode(self: *const ColumnChunk, w: *ThriftWriter) !void {
        w.pushStruct();
        try w.writeFieldHeader(2, .i64);
        try w.writeZigZagI64(self.file_offset);
        if (self.meta_data) |md| {
            try w.writeFieldHeader(3, .@"struct");
            try md.encode(w);
        }
        try w.writeFieldStop();
        w.popStruct();
    }

    pub fn deinit(self: *ColumnChunk, allocator: Allocator) void {
        if (self.meta_data) |*md| md.deinit(allocator);
    }
};

pub const RowGroup = struct {
    columns: []ColumnChunk = &.{},
    total_byte_size: i64 = 0,
    num_rows: i64 = 0,
    sorting_columns: bool = false,
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
                4 => try reader.skip(fh.field_type),
                5 => result.file_offset = try reader.readZigZagI64(),
                6 => result.total_compressed_size = try reader.readZigZagI64(),
                7 => result.ordinal = try reader.readZigZagI16(),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn encode(self: *const RowGroup, w: *ThriftWriter) !void {
        w.pushStruct();
        // Field 1: columns list
        try w.writeFieldHeader(1, .list);
        try w.writeListHeader(.@"struct", @intCast(self.columns.len));
        for (self.columns) |col| {
            try col.encode(w);
        }
        // Field 2: total_byte_size
        try w.writeFieldHeader(2, .i64);
        try w.writeZigZagI64(self.total_byte_size);
        // Field 3: num_rows
        try w.writeFieldHeader(3, .i64);
        try w.writeZigZagI64(self.num_rows);
        try w.writeFieldStop();
        w.popStruct();
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
                    const list_hdr = try reader.readListHeader();
                    const rgs = try allocator.alloc(RowGroup, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        rgs[i] = try RowGroup.decode(reader, allocator);
                    }
                    result.row_groups = rgs;
                    result._row_groups_owned = true;
                },
                5 => {
                    const list_hdr = try reader.readListHeader();
                    const kvs = try allocator.alloc(KeyValue, list_hdr.size);
                    for (0..list_hdr.size) |i| {
                        kvs[i] = try KeyValue.decode(reader);
                    }
                    result.key_value_metadata = kvs;
                    result._kv_owned = true;
                },
                6 => result.created_by = try reader.readString(),
                7 => try reader.skip(fh.field_type),
                else => try reader.skip(fh.field_type),
            }
        }
        reader.popStruct();
        return result;
    }

    pub fn encode(self: *const FileMetaData, w: *ThriftWriter) !void {
        w.pushStruct();
        // Field 1: version
        try w.writeFieldHeader(1, .i32);
        try w.writeZigZagI32(self.version);
        // Field 2: schema list
        try w.writeFieldHeader(2, .list);
        try w.writeListHeader(.@"struct", @intCast(self.schema.len));
        for (self.schema) |elem| {
            try elem.encode(w);
        }
        // Field 3: num_rows
        try w.writeFieldHeader(3, .i64);
        try w.writeZigZagI64(self.num_rows);
        // Field 4: row_groups list
        try w.writeFieldHeader(4, .list);
        try w.writeListHeader(.@"struct", @intCast(self.row_groups.len));
        for (self.row_groups) |rg| {
            try rg.encode(w);
        }
        // Field 6: created_by
        if (self.created_by) |cb| {
            try w.writeFieldHeader(6, .binary);
            try w.writeString(cb);
        }
        try w.writeFieldStop();
        w.popStruct();
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

test "SchemaElement encode/decode round-trip" {
    const allocator = std.testing.allocator;
    const orig = SchemaElement{
        .type_ = .int64,
        .repetition_type = .required,
        .name = "age",
        .converted_type = .int_64,
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try SchemaElement.decode(&r);
    try std.testing.expectEqual(orig.type_.?, decoded.type_.?);
    try std.testing.expectEqual(orig.repetition_type.?, decoded.repetition_type.?);
    try std.testing.expectEqualStrings(orig.name, decoded.name);
    try std.testing.expectEqual(orig.converted_type.?, decoded.converted_type.?);
}

test "SchemaElement encode/decode round-trip: root node" {
    const allocator = std.testing.allocator;
    const orig = SchemaElement{ .name = "schema", .num_children = 3 };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try SchemaElement.decode(&r);
    try std.testing.expectEqualStrings("schema", decoded.name);
    try std.testing.expectEqual(@as(i32, 3), decoded.num_children.?);
    try std.testing.expect(decoded.type_ == null);
}

test "DataPageHeader encode/decode round-trip" {
    const allocator = std.testing.allocator;
    const orig = DataPageHeader{ .num_values = 100, .encoding = .plain, .definition_level_encoding = .rle, .repetition_level_encoding = .rle };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try DataPageHeader.decode(&r);
    try std.testing.expectEqual(orig.num_values, decoded.num_values);
    try std.testing.expectEqual(orig.encoding, decoded.encoding);
}

test "PageHeader encode/decode round-trip" {
    const allocator = std.testing.allocator;
    const orig = PageHeader{
        .page_type = .data_page,
        .uncompressed_page_size = 200,
        .compressed_page_size = 150,
        .data_page_header = .{ .num_values = 50, .encoding = .plain },
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try PageHeader.decode(&r);
    try std.testing.expectEqual(orig.page_type, decoded.page_type);
    try std.testing.expectEqual(orig.uncompressed_page_size, decoded.uncompressed_page_size);
    try std.testing.expectEqual(orig.compressed_page_size, decoded.compressed_page_size);
    try std.testing.expectEqual(orig.data_page_header.?.num_values, decoded.data_page_header.?.num_values);
}

test "PageHeader decode: data page" {
    const data = [_]u8{
        0x15, 0x00, 0x15, 0xC8, 0x01, 0x15, 0xA0, 0x01,
        0x2C, 0x15, 0x14, 0x15, 0x00, 0x15, 0x06, 0x15, 0x06, 0x00, 0x00,
    };
    var reader = ThriftReader.init(&data);
    const ph = try PageHeader.decode(&reader);
    try std.testing.expectEqual(types.PageType.data_page, ph.page_type);
    try std.testing.expectEqual(@as(i32, 100), ph.uncompressed_page_size);
    try std.testing.expectEqual(@as(i32, 80), ph.compressed_page_size);
    try std.testing.expect(ph.data_page_header != null);
    try std.testing.expectEqual(@as(i32, 10), ph.data_page_header.?.num_values);
}

test "DictionaryPageHeader decode" {
    const data = [_]u8{ 0x15, 0x64, 0x15, 0x00, 0x11, 0x00 };
    var reader = ThriftReader.init(&data);
    const dph = try DictionaryPageHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 50), dph.num_values);
    try std.testing.expectEqual(types.Encoding.plain, dph.encoding);
    try std.testing.expectEqual(true, dph.is_sorted.?);
}

test "DictionaryPageHeader decode: unsorted" {
    const data = [_]u8{ 0x15, 0x0A, 0x25, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const dph = try DictionaryPageHeader.decode(&reader);
    try std.testing.expectEqual(@as(i32, 5), dph.num_values);
}

test "DataPageHeaderV2 decode" {
    const data = [_]u8{
        0x15, 0xC8, 0x01, 0x15, 0x0A, 0x15, 0xC8, 0x01,
        0x15, 0x10, 0x15, 0x28, 0x15, 0x00, 0x11, 0x00,
    };
    var reader = ThriftReader.init(&data);
    const v2 = try DataPageHeaderV2.decode(&reader);
    try std.testing.expectEqual(@as(i32, 100), v2.num_values);
    try std.testing.expectEqual(@as(i32, 5), v2.num_nulls);
    try std.testing.expectEqual(types.Encoding.rle_dictionary, v2.encoding);
    try std.testing.expectEqual(true, v2.is_compressed.?);
}

test "KeyValue decode" {
    const data = [_]u8{
        0x18, 0x05, 'h', 'e', 'l', 'l', 'o',
        0x18, 0x05, 'w', 'o', 'r', 'l', 'd',
        0x00,
    };
    var reader = ThriftReader.init(&data);
    const kv = try KeyValue.decode(&reader);
    try std.testing.expectEqualStrings("hello", kv.key);
    try std.testing.expectEqualStrings("world", kv.value.?);
}

test "ColumnMetaData decode" {
    const allocator = std.testing.allocator;
    const data = [_]u8{
        0x15, 0x0C, 0x19, 0x25, 0x00, 0x10, 0x25, 0x02,
        0x16, 0xC8, 0x01, 0x16, 0xE8, 0x07, 0x16, 0xA0, 0x06,
        0x26, 0x08, 0x00,
    };
    var reader = ThriftReader.init(&data);
    var cmd = try ColumnMetaData.decode(&reader, allocator);
    defer cmd.deinit(allocator);
    try std.testing.expectEqual(types.PhysicalType.byte_array, cmd.type_);
    try std.testing.expectEqual(@as(usize, 2), cmd.encodings.len);
    try std.testing.expectEqual(types.CompressionCodec.snappy, cmd.codec);
    try std.testing.expectEqual(@as(i64, 100), cmd.num_values);
}

test "ColumnMetaData encode/decode round-trip" {
    const allocator = std.testing.allocator;
    var encs = [_]types.Encoding{ .plain, .rle };
    var paths = [_][]const u8{"col1"};
    const orig = ColumnMetaData{
        .type_ = .int64,
        .encodings = &encs,
        .path_in_schema = &paths,
        .codec = .uncompressed,
        .num_values = 42,
        .total_uncompressed_size = 336,
        .total_compressed_size = 336,
        .data_page_offset = 4,
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    var decoded = try ColumnMetaData.decode(&r, allocator);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(orig.type_, decoded.type_);
    try std.testing.expectEqual(orig.codec, decoded.codec);
    try std.testing.expectEqual(orig.num_values, decoded.num_values);
    try std.testing.expectEqual(orig.data_page_offset, decoded.data_page_offset);
}

test "Statistics decode: null_count and distinct_count" {
    const data = [_]u8{ 0x36, 0x0A, 0x16, 0x54, 0x00 };
    var reader = ThriftReader.init(&data);
    const stats = try Statistics.decode(&reader);
    try std.testing.expectEqual(@as(i64, 5), stats.null_count.?);
    try std.testing.expectEqual(@as(i64, 42), stats.distinct_count.?);
}

test "PageHeader decode: dictionary page" {
    const data = [_]u8{
        0x15, 0x04, 0x15, 0x90, 0x03, 0x15, 0xAC, 0x02,
        0x4C, 0x15, 0x32, 0x15, 0x00, 0x00, 0x00,
    };
    var reader = ThriftReader.init(&data);
    const ph = try PageHeader.decode(&reader);
    try std.testing.expectEqual(types.PageType.dictionary_page, ph.page_type);
    try std.testing.expectEqual(@as(i32, 200), ph.uncompressed_page_size);
    try std.testing.expect(ph.dictionary_page_header != null);
}

test "SchemaElement decode: root node with num_children" {
    const data = [_]u8{ 0x48, 0x06, 's', 'c', 'h', 'e', 'm', 'a', 0x15, 0x06, 0x00 };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqualStrings("schema", elem.name);
    try std.testing.expectEqual(@as(i32, 3), elem.num_children.?);
}

test "SchemaElement decode: field with converted_type" {
    const data = [_]u8{
        0x15, 0x0C, 0x25, 0x02, 0x18, 0x04, 'n', 'a', 'm', 'e', 0x25, 0x00, 0x00,
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqual(types.PhysicalType.byte_array, elem.type_.?);
    try std.testing.expectEqual(types.ConvertedType.utf8, elem.converted_type.?);
}

test "ColumnChunk decode" {
    const allocator = std.testing.allocator;
    const data = [_]u8{
        0x26, 0xC8, 0x01, 0x1C, 0x15, 0x02, 0x35, 0x00,
        0x16, 0x0E, 0x16, 0x38, 0x16, 0x38, 0x26, 0x08, 0x00, 0x00,
    };
    var reader = ThriftReader.init(&data);
    var cc = try ColumnChunk.decode(&reader, allocator);
    defer cc.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 100), cc.file_offset);
    try std.testing.expect(cc.meta_data != null);
}

test "RowGroup decode: single column" {
    const allocator = std.testing.allocator;
    const data = [_]u8{
        0x19, 0x1C, 0x26, 0x00, 0x00, 0x16, 0xE8, 0x07, 0x16, 0x0E, 0x00,
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
    const data = [_]u8{
        0x15, 0x04, 0x19, 0x1C, 0x48, 0x04, 'r', 'o', 'o', 't', 0x00,
        0x16, 0x00, 0x19, 0x0C, 0x00,
    };
    var reader = ThriftReader.init(&data);
    var fmd = try FileMetaData.decode(&reader, allocator);
    defer fmd.deinit(allocator);
    try std.testing.expectEqual(@as(i32, 2), fmd.version);
    try std.testing.expectEqualStrings("root", fmd.schema[0].name);
}

test "FileMetaData decode: with created_by" {
    const allocator = std.testing.allocator;
    const data = [_]u8{
        0x15, 0x04, 0x19, 0x1C, 0x48, 0x01, 'x', 0x00,
        0x16, 0x00, 0x19, 0x0C, 0x28, 0x04, 't', 'e', 's', 't', 0x00,
    };
    var reader = ThriftReader.init(&data);
    var fmd = try FileMetaData.decode(&reader, allocator);
    defer fmd.deinit(allocator);
    try std.testing.expectEqualStrings("test", fmd.created_by.?);
}

test "FileMetaData encode/decode round-trip" {
    const allocator = std.testing.allocator;
    var schema_elems = [_]SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .type_ = .int32, .repetition_type = .required, .name = "id" },
    };
    const orig = FileMetaData{
        .version = 2,
        .schema = &schema_elems,
        .num_rows = 100,
        .row_groups = &.{},
        .created_by = "teddy (Zig)",
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    var decoded = try FileMetaData.decode(&r, allocator);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(@as(i32, 2), decoded.version);
    try std.testing.expectEqual(@as(usize, 2), decoded.schema.len);
    try std.testing.expectEqualStrings("root", decoded.schema[0].name);
    try std.testing.expectEqualStrings("id", decoded.schema[1].name);
    try std.testing.expectEqual(@as(i64, 100), decoded.num_rows);
    try std.testing.expectEqualStrings("teddy (Zig)", decoded.created_by.?);
}
