const std = @import("std");
const Allocator = std.mem.Allocator;
const ThriftReader = @import("thrift_reader.zig").ThriftReader;
const CompactType = @import("thrift_reader.zig").CompactType;
const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const types = @import("types.zig");

/// Checked wire-to-enum conversion: unknown or out-of-range values from the
/// file become an error instead of an @enumFromInt panic (review P3 — wire
/// enums are validated here at the boundary and trusted everywhere else).
fn enumFromWire(comptime E: type, raw: i32) !E {
    const Tag = @typeInfo(E).@"enum".tag_type;
    const tag = std.math.cast(Tag, raw) orelse return error.InvalidEnumValue;
    inline for (@typeInfo(E).@"enum".fields) |field| {
        if (field.value == tag) return @field(E, field.name);
    }
    return error.InvalidEnumValue;
}

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
        try reader.pushStruct();
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

// ============================================================
// LogicalType (SchemaElement field 10) — thrift union decode/encode
// ============================================================

fn decodeTimeUnit(reader: *ThriftReader) !types.TimeUnit {
    var result: ?types.TimeUnit = null;
    try reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => {
                try reader.skip(fh.field_type);
                result = .millis;
            },
            2 => {
                try reader.skip(fh.field_type);
                result = .micros;
            },
            3 => {
                try reader.skip(fh.field_type);
                result = .nanos;
            },
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result orelse error.InvalidLogicalType;
}

fn decodeDecimalType(reader: *ThriftReader) !types.DecimalParams {
    var result = types.DecimalParams{ .scale = 0, .precision = 0 };
    try reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.scale = try reader.readZigZagI32(),
            2 => result.precision = try reader.readZigZagI32(),
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

fn decodeTimeType(reader: *ThriftReader) !types.TimeParams {
    var result = types.TimeParams{ .is_adjusted_to_utc = false, .unit = .millis };
    try reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.is_adjusted_to_utc = fh.field_type == .boolean_true,
            2 => result.unit = try decodeTimeUnit(reader),
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

fn decodeTimestampType(reader: *ThriftReader) !types.TimestampParams {
    const t = try decodeTimeType(reader); // identical wire layout
    return .{ .is_adjusted_to_utc = t.is_adjusted_to_utc, .unit = t.unit };
}

fn decodeIntType(reader: *ThriftReader) !types.IntParams {
    var result = types.IntParams{ .bit_width = 0, .is_signed = false };
    try reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.bit_width = @bitCast(try reader.readByte()),
            2 => result.is_signed = fh.field_type == .boolean_true,
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

/// Decode the LogicalType thrift union: a struct with one field set (if a
/// malformed file sets several, the last recognized one wins).
/// Returns null for union fields we don't recognize (future spec additions),
/// so callers fall back to converted_type / physical type.
pub fn decodeLogicalType(reader: *ThriftReader) !?types.LogicalType {
    var result: ?types.LogicalType = null;
    try reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => {
                try reader.skip(fh.field_type);
                result = .string;
            },
            2 => {
                try reader.skip(fh.field_type);
                result = .map;
            },
            3 => {
                try reader.skip(fh.field_type);
                result = .list;
            },
            4 => {
                try reader.skip(fh.field_type);
                result = .@"enum";
            },
            5 => result = .{ .decimal = try decodeDecimalType(reader) },
            6 => {
                try reader.skip(fh.field_type);
                result = .date;
            },
            7 => result = .{ .time = try decodeTimeType(reader) },
            8 => result = .{ .timestamp = try decodeTimestampType(reader) },
            10 => result = .{ .integer = try decodeIntType(reader) },
            11 => {
                try reader.skip(fh.field_type);
                result = .unknown;
            },
            12 => {
                try reader.skip(fh.field_type);
                result = .json;
            },
            13 => {
                try reader.skip(fh.field_type);
                result = .bson;
            },
            14 => {
                try reader.skip(fh.field_type);
                result = .uuid;
            },
            15 => {
                try reader.skip(fh.field_type);
                result = .float16;
            },
            16 => {
                try reader.skip(fh.field_type);
                result = .variant;
            },
            17 => {
                try reader.skip(fh.field_type);
                result = .geometry;
            },
            18 => {
                try reader.skip(fh.field_type);
                result = .geography;
            },
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

fn encodeEmptyVariant(w: *ThriftWriter, field_id: i16) !void {
    try w.writeFieldHeader(field_id, .@"struct");
    w.pushStruct();
    try w.writeFieldStop();
    w.popStruct();
}

fn encodeTimeUnit(w: *ThriftWriter, unit: types.TimeUnit) !void {
    w.pushStruct();
    const field_id: i16 = switch (unit) {
        .millis => 1,
        .micros => 2,
        .nanos => 3,
    };
    try encodeEmptyVariant(w, field_id);
    try w.writeFieldStop();
    w.popStruct();
}

fn encodeTimeFields(w: *ThriftWriter, is_utc: bool, unit: types.TimeUnit) !void {
    w.pushStruct();
    try w.writeBoolField(1, is_utc);
    try w.writeFieldHeader(2, .@"struct");
    try encodeTimeUnit(w, unit);
    try w.writeFieldStop();
    w.popStruct();
}

/// Encode the LogicalType thrift union (one field set, then stop).
pub fn encodeLogicalType(w: *ThriftWriter, lt: types.LogicalType) !void {
    w.pushStruct();
    switch (lt) {
        .string => try encodeEmptyVariant(w, 1),
        .map => try encodeEmptyVariant(w, 2),
        .list => try encodeEmptyVariant(w, 3),
        .@"enum" => try encodeEmptyVariant(w, 4),
        .decimal => |d| {
            try w.writeFieldHeader(5, .@"struct");
            w.pushStruct();
            try w.writeFieldHeader(1, .i32);
            try w.writeZigZagI32(d.scale);
            try w.writeFieldHeader(2, .i32);
            try w.writeZigZagI32(d.precision);
            try w.writeFieldStop();
            w.popStruct();
        },
        .date => try encodeEmptyVariant(w, 6),
        .time => |t| {
            try w.writeFieldHeader(7, .@"struct");
            try encodeTimeFields(w, t.is_adjusted_to_utc, t.unit);
        },
        .timestamp => |t| {
            try w.writeFieldHeader(8, .@"struct");
            try encodeTimeFields(w, t.is_adjusted_to_utc, t.unit);
        },
        .integer => |i| {
            try w.writeFieldHeader(10, .@"struct");
            w.pushStruct();
            try w.writeFieldHeader(1, .i8);
            try w.writeByte(@bitCast(i.bit_width));
            try w.writeBoolField(2, i.is_signed);
            try w.writeFieldStop();
            w.popStruct();
        },
        .unknown => try encodeEmptyVariant(w, 11),
        .json => try encodeEmptyVariant(w, 12),
        .bson => try encodeEmptyVariant(w, 13),
        .uuid => try encodeEmptyVariant(w, 14),
        .float16 => try encodeEmptyVariant(w, 15),
        .variant => try encodeEmptyVariant(w, 16),
        .geometry => try encodeEmptyVariant(w, 17),
        .geography => try encodeEmptyVariant(w, 18),
    }
    try w.writeFieldStop();
    w.popStruct();
}

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
    logical_type: ?types.LogicalType = null,

    pub fn decode(reader: *ThriftReader) !SchemaElement {
        var result = SchemaElement{};
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.type_ = try enumFromWire(types.PhysicalType, try reader.readZigZagI32()),
                2 => result.type_length = try reader.readZigZagI32(),
                3 => result.repetition_type = try enumFromWire(types.FieldRepetitionType, try reader.readZigZagI32()),
                4 => result.name = try reader.readString(),
                5 => result.num_children = try reader.readZigZagI32(),
                6 => result.converted_type = try enumFromWire(types.ConvertedType, try reader.readZigZagI32()),
                7 => result.scale = try reader.readZigZagI32(),
                8 => result.precision = try reader.readZigZagI32(),
                9 => result.field_id = try reader.readZigZagI32(),
                10 => result.logical_type = try decodeLogicalType(reader),
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
        if (self.type_length) |tl| {
            try w.writeFieldHeader(2, .i32);
            try w.writeZigZagI32(tl);
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
        if (self.scale) |sc| {
            try w.writeFieldHeader(7, .i32);
            try w.writeZigZagI32(sc);
        }
        if (self.precision) |p| {
            try w.writeFieldHeader(8, .i32);
            try w.writeZigZagI32(p);
        }
        if (self.logical_type) |lt| {
            try w.writeFieldHeader(10, .@"struct");
            try encodeLogicalType(w, lt);
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
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.encoding = try enumFromWire(types.Encoding, try reader.readZigZagI32()),
                3 => result.definition_level_encoding = try enumFromWire(types.Encoding, try reader.readZigZagI32()),
                4 => result.repetition_level_encoding = try enumFromWire(types.Encoding, try reader.readZigZagI32()),
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
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.encoding = try enumFromWire(types.Encoding, try reader.readZigZagI32()),
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
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.num_values = try reader.readZigZagI32(),
                2 => result.num_nulls = try reader.readZigZagI32(),
                3 => result.num_rows = try reader.readZigZagI32(),
                4 => result.encoding = try enumFromWire(types.Encoding, try reader.readZigZagI32()),
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
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.page_type = try enumFromWire(types.PageType, try reader.readZigZagI32()),
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
        try reader.pushStruct();
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
        // Free any already-owned allocations if a later field fails to decode.
        errdefer result.deinit(allocator);
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.type_ = try enumFromWire(types.PhysicalType, try reader.readZigZagI32()),
                2 => {
                    const list_hdr = try reader.readListHeader();
                    const encs = try allocator.alloc(types.Encoding, list_hdr.size);
                    errdefer allocator.free(encs); // free if decode fails mid-loop
                    for (0..list_hdr.size) |i| {
                        encs[i] = try enumFromWire(types.Encoding, try reader.readZigZagI32());
                    }
                    result.encodings = encs;
                    result._encodings_owned = true;
                },
                3 => {
                    const list_hdr = try reader.readListHeader();
                    const paths = try allocator.alloc([]const u8, list_hdr.size);
                    errdefer allocator.free(paths); // free if decode fails mid-loop
                    for (0..list_hdr.size) |i| {
                        paths[i] = try reader.readString();
                    }
                    result.path_in_schema = paths;
                    result._paths_owned = true;
                },
                4 => result.codec = try enumFromWire(types.CompressionCodec, try reader.readZigZagI32()),
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
        // Free meta_data if a later field or the stop-byte read fails.
        errdefer result.deinit(allocator);
        try reader.pushStruct();
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
        // Free any already-owned columns if a later field fails to decode.
        errdefer result.deinit(allocator);
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => {
                    const list_hdr = try reader.readListHeader();
                    const cols = try allocator.alloc(ColumnChunk, list_hdr.size);
                    var cols_init: usize = 0;
                    errdefer {
                        // Free only the ColumnChunks that were fully decoded.
                        for (cols[0..cols_init]) |*c| c.deinit(allocator);
                        allocator.free(cols);
                    }
                    for (0..list_hdr.size) |i| {
                        cols[i] = try ColumnChunk.decode(reader, allocator);
                        cols_init += 1;
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
        // Free any already-owned allocations if a later field fails to decode.
        errdefer result.deinit(allocator);
        try reader.pushStruct();
        while (true) {
            const fh = try reader.readFieldHeader();
            if (fh.field_type == .stop) break;
            switch (fh.field_id) {
                1 => result.version = try reader.readZigZagI32(),
                2 => {
                    // Free any previously-decoded schema (duplicate field in corrupt data).
                    if (result._schema_owned) {
                        allocator.free(result.schema);
                        result.schema = &.{};
                        result._schema_owned = false;
                    }
                    const list_hdr = try reader.readListHeader();
                    const elems = try allocator.alloc(SchemaElement, list_hdr.size);
                    errdefer allocator.free(elems); // SchemaElements are POD; free if decode fails
                    for (0..list_hdr.size) |i| {
                        elems[i] = try SchemaElement.decode(reader);
                    }
                    result.schema = elems;
                    result._schema_owned = true;
                },
                3 => result.num_rows = try reader.readZigZagI64(),
                4 => {
                    // Free any previously-decoded row groups (duplicate field in corrupt data).
                    if (result._row_groups_owned) {
                        for (result.row_groups) |*rg| rg.deinit(allocator);
                        allocator.free(result.row_groups);
                        result.row_groups = &.{};
                        result._row_groups_owned = false;
                    }
                    const list_hdr = try reader.readListHeader();
                    const rgs = try allocator.alloc(RowGroup, list_hdr.size);
                    var rgs_init: usize = 0;
                    errdefer {
                        // Free only the RowGroups that were fully decoded.
                        for (rgs[0..rgs_init]) |*rg| rg.deinit(allocator);
                        allocator.free(rgs);
                    }
                    for (0..list_hdr.size) |i| {
                        rgs[i] = try RowGroup.decode(reader, allocator);
                        rgs_init += 1;
                    }
                    result.row_groups = rgs;
                    result._row_groups_owned = true;
                },
                5 => {
                    // Free any previously-decoded kv metadata (duplicate field in corrupt data).
                    if (result._kv_owned) {
                        if (result.key_value_metadata) |kvs| allocator.free(kvs);
                        result.key_value_metadata = null;
                        result._kv_owned = false;
                    }
                    const list_hdr = try reader.readListHeader();
                    const kvs = try allocator.alloc(KeyValue, list_hdr.size);
                    errdefer allocator.free(kvs); // KeyValues are POD; free if decode fails
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

test "SchemaElement decode: field 10 logical_type (DATE)" {
    const data = [_]u8{
        0x15, 0x02, // field 1: type = INT32 (zigzag(1)=2)
        0x25, 0x00, // field 3: repetition = required
        0x18, 0x01, 'd', // field 4: name = "d"
        0x6C, // field 10 header (delta 6 from field 4, struct)
        0x6C, 0x00, 0x00, // LogicalType union: DATE variant + stop
        0x00, // SchemaElement stop
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqualStrings("d", elem.name);
    try std.testing.expect(elem.logical_type.? == .date);
}

test "SchemaElement encode/decode round-trip: scale + precision + logical decimal" {
    const allocator = std.testing.allocator;
    const orig = SchemaElement{
        .type_ = .int32,
        .repetition_type = .required,
        .name = "amount",
        .converted_type = .decimal,
        .scale = 2,
        .precision = 9,
        .logical_type = .{ .decimal = .{ .scale = 2, .precision = 9 } },
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try SchemaElement.decode(&r);
    try std.testing.expectEqual(orig.converted_type.?, decoded.converted_type.?);
    try std.testing.expectEqual(@as(?i32, 2), decoded.scale);
    try std.testing.expectEqual(@as(?i32, 9), decoded.precision);
    try std.testing.expect(decoded.logical_type.? == .decimal);
    try std.testing.expectEqual(@as(i32, 2), decoded.logical_type.?.decimal.scale);
    try std.testing.expectEqual(@as(i32, 9), decoded.logical_type.?.decimal.precision);
    try std.testing.expectEqualStrings("amount", decoded.name);
}

test "SchemaElement encode/decode round-trip: logical_type + type_length" {
    const allocator = std.testing.allocator;
    const orig = SchemaElement{
        .type_ = .fixed_len_byte_array,
        .type_length = 16,
        .repetition_type = .required,
        .name = "u",
        .logical_type = .uuid,
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try SchemaElement.decode(&r);
    try std.testing.expectEqual(@as(i32, 16), decoded.type_length.?);
    try std.testing.expect(decoded.logical_type.? == .uuid);
    try std.testing.expectEqual(orig.type_.?, decoded.type_.?);
}

test "LogicalType encode/decode round-trip: every variant" {
    const allocator = std.testing.allocator;
    const cases = [_]types.LogicalType{
        .string,                                                              .map,
        .list,                                                                .@"enum",
        .{ .decimal = .{ .scale = 2, .precision = 38 } },                     .date,
        .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } },
        .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .nanos } },
        .{ .integer = .{ .bit_width = 64, .is_signed = false } },             .unknown,
        .json,                                                                .bson,
        .uuid,                                                                .float16,
        .variant,                                                             .geometry,
        .geography,
    };
    for (cases) |orig| {
        var w = ThriftWriter.init(allocator);
        defer w.deinit();
        try encodeLogicalType(&w, orig);
        var r = ThriftReader.init(w.written());
        const decoded = (try decodeLogicalType(&r)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualDeep(orig, decoded);
    }
}

test "decodeLogicalType: DATE (empty variant)" {
    // union field 6 (DateType, empty struct), then union stop
    const data = [_]u8{ 0x6C, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expect(lt == .date);
}

test "decodeLogicalType: unknown union field returns null" {
    // field 99 (unknown, empty struct), then union stop
    const data = [_]u8{ 0x0C, 0xC6, 0x01, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectEqual(@as(?types.LogicalType, null), try decodeLogicalType(&reader));
}

test "decodeLogicalType: TIME with missing unit returns error" {
    // field 7 (TimeType): field 1 bool_true, field 2 empty TimeUnit struct (no variant)
    const data = [_]u8{ 0x7C, 0x11, 0x1C, 0x00, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidLogicalType, decodeLogicalType(&reader));
}

test "decodeLogicalType: truncated input returns error" {
    // DECIMAL header then EOF mid-struct
    const data = [_]u8{ 0x5C, 0x15 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.UnexpectedEof, decodeLogicalType(&reader));
}

test "decodeLogicalType: TIMESTAMP(utc=true, micros)" {
    // field 8 struct; inner: field 1 bool_true, field 2 TimeUnit union
    //   TimeUnit: field 2 (MICROS) empty struct
    const data = [_]u8{ 0x8C, 0x11, 0x1C, 0x2C, 0x00, 0x00, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(true, lt.timestamp.is_adjusted_to_utc);
    try std.testing.expectEqual(types.TimeUnit.micros, lt.timestamp.unit);
}

test "decodeLogicalType: DECIMAL(scale=2, precision=10)" {
    // field 5 struct; inner: field 1 scale zigzag(2)=4, field 2 precision zigzag(10)=20
    const data = [_]u8{ 0x5C, 0x15, 0x04, 0x15, 0x14, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(@as(i32, 2), lt.decimal.scale);
    try std.testing.expectEqual(@as(i32, 10), lt.decimal.precision);
}

test "decodeLogicalType: INTEGER(bit_width=16, signed)" {
    // field 10 struct; inner: field 1 i8 (raw byte 16), field 2 bool_true
    const data = [_]u8{ 0xAC, 0x13, 0x10, 0x11, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(@as(i8, 16), lt.integer.bit_width);
    try std.testing.expectEqual(true, lt.integer.is_signed);
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

// ============================================================
// Hardening Tests (Phase 11 Unit A)
// ============================================================

test "SchemaElement decode: unknown type_ value 99 → error.InvalidEnumValue" {
    // field 1 (type_), i32, delta=1 → byte 0x15
    // zigzag(99) = 198 = varint [0xC6, 0x01]
    // then stop byte
    const data = [_]u8{ 0x15, 0xC6, 0x01, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidEnumValue, SchemaElement.decode(&reader));
}

test "PageHeader decode: unknown page_type 99 → error.InvalidEnumValue" {
    // field 1 (page_type), i32, delta=1 → byte 0x15
    // zigzag(99) = 198 = varint [0xC6, 0x01]
    // then stop
    const data = [_]u8{ 0x15, 0xC6, 0x01, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidEnumValue, PageHeader.decode(&reader));
}

test "ColumnMetaData decode: unknown codec 200 → error.InvalidEnumValue" {
    const allocator = std.testing.allocator;
    // We build a minimal ColumnMetaData with field 4 (codec) = 200.
    // field 4 header: delta=4 from 0, type=i32(5) → byte 0x45
    // zigzag(200) = 400 = varint [0x90, 0x03]
    // then stop
    const data = [_]u8{ 0x45, 0x90, 0x03, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidEnumValue, ColumnMetaData.decode(&reader, allocator));
}

test "SchemaElement decode: negative enum value (zigzag(-1)=1) → error.InvalidEnumValue" {
    // field 1 (type_), i32, value zigzag(-1)=1 → varint [0x01]
    // std.math.cast(u8, -1) fails → error.InvalidEnumValue
    const data = [_]u8{ 0x15, 0x01, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidEnumValue, SchemaElement.decode(&reader));
}

test "DataPageHeader decode: unknown encoding value 99 → error.InvalidEnumValue" {
    // field 2 (encoding), delta=2, type=i32(5) → byte 0x25
    // zigzag(99) = 198 = varint [0xC6, 0x01]
    // We skip field 1 by jumping straight to field 2 with absolute id if needed.
    // Actually: field 1 first (num_values), then field 2 (encoding).
    // Skip field 1 with delta=1, type=i32, value=0: 0x15, 0x00
    // Then field 2 encoding=99: delta=1, type=i32(5) → 0x15, then zigzag(99)=[0xC6,0x01]
    const data = [_]u8{ 0x15, 0x00, 0x15, 0xC6, 0x01, 0x00 };
    var reader = ThriftReader.init(&data);
    try std.testing.expectError(error.InvalidEnumValue, DataPageHeader.decode(&reader));
}
