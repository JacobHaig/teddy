const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const encoding = @import("encoding_reader.zig");
const snappy = @import("snappy.zig");
const ThriftReader = @import("thrift_reader.zig").ThriftReader;

// ============================================================
// Column Chunk Reader
// ============================================================

/// Information about a leaf column resolved from the schema.
pub const LeafColumn = struct {
    schema_element: metadata.SchemaElement,
    max_def_level: u8,
    max_rep_level: u8,
    column_index: usize,
};

/// Read all pages for a single column chunk and produce a ParquetColumn.
pub fn readColumnChunk(
    allocator: Allocator,
    file_data: []const u8,
    column_chunk: metadata.ColumnChunk,
    leaf: LeafColumn,
    num_rows: usize,
) !types.ParquetColumn {
    const col_meta = column_chunk.meta_data orelse return error.MissingColumnMetadata;
    const physical_type = col_meta.type_;

    // Determine starting offset in the file
    const start_offset: usize = blk: {
        if (col_meta.dictionary_page_offset) |dpo| {
            break :blk @intCast(dpo);
        }
        break :blk @intCast(col_meta.data_page_offset);
    };

    // Total bytes for this column chunk
    const total_size: usize = @intCast(col_meta.total_compressed_size);
    const end_offset = start_offset + total_size;

    if (end_offset > file_data.len) return error.ColumnDataOutOfBounds;

    // Accumulated values across pages
    var all_values = ValueAccumulator.init(allocator, physical_type);
    defer all_values.deinit();

    var all_def_levels = std.ArrayList(u32).empty;
    defer all_def_levels.deinit(allocator);

    // Dictionary (populated if we encounter a dictionary page)
    var dict: ?DictionaryStore = null;
    defer if (dict) |*d| d.deinit();

    var pos = start_offset;
    var values_read: usize = 0;
    const total_values: usize = @intCast(col_meta.num_values);

    while (pos < end_offset and values_read < total_values) {
        // Decode page header (Thrift)
        var thrift_reader = ThriftReader.init(file_data[pos..end_offset]);
        const page_header = try metadata.PageHeader.decode(&thrift_reader);
        pos += thrift_reader.pos; // advance past the Thrift header

        const compressed_size: usize = @intCast(page_header.compressed_page_size);
        const uncompressed_size: usize = @intCast(page_header.uncompressed_page_size);

        if (pos + compressed_size > file_data.len) return error.PageDataOutOfBounds;
        const page_data = file_data[pos .. pos + compressed_size];
        pos += compressed_size;

        switch (page_header.page_type) {
            .dictionary_page => {
                const dict_header = page_header.dictionary_page_header orelse return error.MissingDictionaryHeader;
                const decompressed = try decompressPage(allocator, page_data, col_meta.codec, uncompressed_size);
                defer if (decompressed.ptr != page_data.ptr) allocator.free(decompressed);

                dict = try DictionaryStore.init(allocator, decompressed, physical_type, @intCast(dict_header.num_values));
            },
            .data_page => {
                const dp_header = page_header.data_page_header orelse return error.MissingDataPageHeader;
                const num_values_in_page: usize = @intCast(dp_header.num_values);

                const decompressed = try decompressPage(allocator, page_data, col_meta.codec, uncompressed_size);
                defer if (decompressed.ptr != page_data.ptr) allocator.free(decompressed);

                var data_pos: usize = 0;

                // Read repetition levels (skip for flat schema: max_rep_level == 0)
                if (leaf.max_rep_level > 0) {
                    const rep_len = std.mem.readInt(u32, decompressed[data_pos..][0..4], .little);
                    data_pos += 4 + rep_len;
                }

                // Read definition levels
                if (leaf.max_def_level > 0) {
                    const def_len_bytes = std.mem.readInt(u32, decompressed[data_pos..][0..4], .little);
                    const def_data = decompressed[data_pos + 4 .. data_pos + 4 + def_len_bytes];
                    data_pos += 4 + def_len_bytes;

                    var rle_dec = encoding.RleBitPackedDecoder.init(def_data, leaf.max_def_level);
                    const levels = try rle_dec.readBatch(num_values_in_page, allocator);
                    defer allocator.free(levels);
                    try all_def_levels.appendSlice(allocator, levels);

                    // Count non-null values for decoding
                    var non_null: usize = 0;
                    for (levels) |l| {
                        if (l == leaf.max_def_level) non_null += 1;
                    }

                    // Decode values (only non-null count)
                    try decodeValues(&all_values, decompressed[data_pos..], dp_header.encoding, physical_type, non_null, &dict, allocator);
                } else {
                    // All values are non-null
                    try decodeValues(&all_values, decompressed[data_pos..], dp_header.encoding, physical_type, num_values_in_page, &dict, allocator);
                }

                values_read += num_values_in_page;
            },
            .data_page_v2 => {
                const dp2 = page_header.data_page_header_v2 orelse return error.MissingDataPageV2Header;
                const num_values_in_page: usize = @intCast(dp2.num_values);
                const rep_len: usize = @intCast(dp2.repetition_levels_byte_length);
                const def_len: usize = @intCast(dp2.definition_levels_byte_length);

                // In v2, rep/def levels are NOT compressed
                var data_pos: usize = 0;

                // Skip repetition levels
                data_pos += rep_len;

                // Read definition levels
                if (def_len > 0 and leaf.max_def_level > 0) {
                    const def_data = page_data[data_pos .. data_pos + def_len];
                    data_pos += def_len;

                    var rle_dec = encoding.RleBitPackedDecoder.init(def_data, leaf.max_def_level);
                    const levels = try rle_dec.readBatch(num_values_in_page, allocator);
                    defer allocator.free(levels);
                    try all_def_levels.appendSlice(allocator, levels);

                    var non_null: usize = 0;
                    for (levels) |l| {
                        if (l == leaf.max_def_level) non_null += 1;
                    }

                    // The rest of the page is compressed (unless is_compressed=false)
                    const is_compressed = dp2.is_compressed orelse true;
                    const values_data = page_data[data_pos..];
                    const values_uncompressed_size = uncompressed_size - rep_len - def_len;

                    const decompressed_vals = if (is_compressed)
                        try decompressPage(allocator, values_data, col_meta.codec, values_uncompressed_size)
                    else
                        values_data;
                    defer if (is_compressed and decompressed_vals.ptr != values_data.ptr) allocator.free(decompressed_vals);

                    try decodeValues(&all_values, decompressed_vals, dp2.encoding, physical_type, non_null, &dict, allocator);
                } else {
                    data_pos += def_len;
                    const is_compressed = dp2.is_compressed orelse true;
                    const values_data = page_data[data_pos..];
                    const values_uncompressed_size = uncompressed_size - rep_len - def_len;

                    const decompressed_vals = if (is_compressed)
                        try decompressPage(allocator, values_data, col_meta.codec, values_uncompressed_size)
                    else
                        values_data;
                    defer if (is_compressed and decompressed_vals.ptr != values_data.ptr) allocator.free(decompressed_vals);

                    try decodeValues(&all_values, decompressed_vals, dp2.encoding, physical_type, num_values_in_page, &dict, allocator);
                }

                values_read += num_values_in_page;
            },
            else => {
                // Skip unknown page types
            },
        }
    }

    // Build the ParquetColumn
    return buildColumn(allocator, &all_values, all_def_levels.items, leaf, num_rows);
}

// ============================================================
// Helpers
// ============================================================

fn decompressPage(allocator: Allocator, data: []const u8, codec: types.CompressionCodec, uncompressed_size: usize) ![]const u8 {
    return switch (codec) {
        .uncompressed => data, // return same pointer, caller checks before freeing
        .snappy => try snappy.decompress(allocator, data),
        else => {
            _ = uncompressed_size;
            return error.UnsupportedCompression;
        },
    };
}

fn decodeValues(
    acc: *ValueAccumulator,
    data: []const u8,
    enc: types.Encoding,
    physical_type: types.PhysicalType,
    count: usize,
    dict: *?DictionaryStore,
    allocator: Allocator,
) !void {
    switch (enc) {
        .plain => {
            var dec = encoding.PlainDecoder.init(data);
            try acc.decodePlain(&dec, count, allocator);
        },
        .rle_dictionary, .plain_dictionary => {
            const d = dict.* orelse return error.MissingDictionary;
            if (data.len == 0) return;
            const bit_width = data[0];
            var rle_dec = encoding.RleBitPackedDecoder.init(data[1..], bit_width);
            try acc.decodeDictionary(&rle_dec, &d, count, physical_type, allocator);
        },
        else => return error.UnsupportedEncoding,
    }
}

/// Build the final ParquetColumn from accumulated values + definition levels.
fn buildColumn(
    allocator: Allocator,
    acc: *ValueAccumulator,
    def_levels: []const u32,
    leaf: LeafColumn,
    num_rows: usize,
) !types.ParquetColumn {
    var col = types.ParquetColumn.initEmpty(allocator);
    errdefer col.deinit();

    // Copy the column name
    const name_copy = try allocator.alloc(u8, leaf.schema_element.name.len);
    @memcpy(name_copy, leaf.schema_element.name);
    col.name = name_copy;

    col.physical_type = leaf.schema_element.type_ orelse .byte_array;
    col.converted_type = leaf.schema_element.converted_type;
    col.is_optional = leaf.max_def_level > 0;
    col.num_rows = num_rows;

    if (leaf.max_def_level > 0) {
        // Build validity array and expand values with nulls
        const validity = try allocator.alloc(bool, num_rows);
        errdefer allocator.free(validity);

        var value_idx: usize = 0;
        for (0..num_rows) |i| {
            if (i < def_levels.len) {
                validity[i] = def_levels[i] == leaf.max_def_level;
            } else {
                validity[i] = false;
            }
        }
        col.validity = validity;

        // Expand values: insert defaults where null
        try acc.expandWithNulls(allocator, validity, num_rows, &value_idx, &col);
    } else {
        // All values are present, just move the arrays
        acc.moveInto(&col);
    }

    return col;
}

// ============================================================
// Value Accumulator — collects decoded values across pages
// ============================================================

pub const ValueAccumulator = struct {
    allocator: Allocator,
    physical_type: types.PhysicalType,
    int32s: std.ArrayList(i32),
    int64s: std.ArrayList(i64),
    floats: std.ArrayList(f32),
    doubles: std.ArrayList(f64),
    booleans: std.ArrayList(bool),
    byte_arrays: std.ArrayList([]const u8),

    pub fn init(allocator: Allocator, physical_type: types.PhysicalType) ValueAccumulator {
        return .{
            .allocator = allocator,
            .physical_type = physical_type,
            .int32s = std.ArrayList(i32).empty,
            .int64s = std.ArrayList(i64).empty,
            .floats = std.ArrayList(f32).empty,
            .doubles = std.ArrayList(f64).empty,
            .booleans = std.ArrayList(bool).empty,
            .byte_arrays = std.ArrayList([]const u8).empty,
        };
    }

    pub fn deinit(self: *ValueAccumulator) void {
        self.int32s.deinit(self.allocator);
        self.int64s.deinit(self.allocator);
        self.floats.deinit(self.allocator);
        self.doubles.deinit(self.allocator);
        self.booleans.deinit(self.allocator);
        for (self.byte_arrays.items) |arr| {
            self.allocator.free(arr);
        }
        self.byte_arrays.deinit(self.allocator);
    }

    pub fn decodePlain(self: *ValueAccumulator, dec: *encoding.PlainDecoder, count: usize, allocator: Allocator) !void {
        switch (self.physical_type) {
            .int32 => {
                for (0..count) |_| {
                    try self.int32s.append(allocator, try dec.readInt32());
                }
            },
            .int64 => {
                for (0..count) |_| {
                    try self.int64s.append(allocator, try dec.readInt64());
                }
            },
            .float => {
                for (0..count) |_| {
                    try self.floats.append(allocator, try dec.readFloat());
                }
            },
            .double => {
                for (0..count) |_| {
                    try self.doubles.append(allocator, try dec.readDouble());
                }
            },
            .boolean => {
                const bools = try dec.readBooleans(count, allocator);
                defer allocator.free(bools);
                try self.booleans.appendSlice(allocator, bools);
            },
            .byte_array => {
                for (0..count) |_| {
                    const src = try dec.readByteArray();
                    const copy = try allocator.alloc(u8, src.len);
                    @memcpy(copy, src);
                    try self.byte_arrays.append(allocator, copy);
                }
            },
            .fixed_len_byte_array => {
                // Would need type_length from schema — for now treat like byte_array
                return error.UnsupportedEncoding;
            },
            .int96 => return error.UnsupportedEncoding,
        }
    }

    pub fn decodeDictionary(self: *ValueAccumulator, rle_dec: *encoding.RleBitPackedDecoder, dict: *const DictionaryStore, count: usize, physical_type: types.PhysicalType, allocator: Allocator) !void {
        for (0..count) |_| {
            const idx = try rle_dec.next();
            switch (physical_type) {
                .int32 => try self.int32s.append(allocator, dict.int32s.items[idx]),
                .int64 => try self.int64s.append(allocator, dict.int64s.items[idx]),
                .float => try self.floats.append(allocator, dict.floats.items[idx]),
                .double => try self.doubles.append(allocator, dict.doubles.items[idx]),
                .byte_array => {
                    const src = dict.byte_arrays.items[idx];
                    const copy = try allocator.alloc(u8, src.len);
                    @memcpy(copy, src);
                    try self.byte_arrays.append(allocator, copy);
                },
                else => return error.UnsupportedEncoding,
            }
        }
    }

    pub fn moveInto(self: *ValueAccumulator, col: *types.ParquetColumn) void {
        switch (self.physical_type) {
            .int32 => {
                col.int32s = self.int32s.toOwnedSlice(self.allocator) catch null;
                self.int32s = std.ArrayList(i32).empty;
            },
            .int64 => {
                col.int64s = self.int64s.toOwnedSlice(self.allocator) catch null;
                self.int64s = std.ArrayList(i64).empty;
            },
            .float => {
                col.floats = self.floats.toOwnedSlice(self.allocator) catch null;
                self.floats = std.ArrayList(f32).empty;
            },
            .double => {
                col.doubles = self.doubles.toOwnedSlice(self.allocator) catch null;
                self.doubles = std.ArrayList(f64).empty;
            },
            .boolean => {
                col.booleans = self.booleans.toOwnedSlice(self.allocator) catch null;
                self.booleans = std.ArrayList(bool).empty;
            },
            .byte_array, .fixed_len_byte_array => {
                col.byte_arrays = self.byte_arrays.toOwnedSlice(self.allocator) catch null;
                self.byte_arrays = std.ArrayList([]const u8).empty;
            },
            else => {},
        }
    }

    pub fn expandWithNulls(self: *ValueAccumulator, allocator: Allocator, validity: []const bool, num_rows: usize, value_idx: *usize, col: *types.ParquetColumn) !void {
        _ = value_idx;
        switch (self.physical_type) {
            .int32 => {
                const result = try allocator.alloc(i32, num_rows);
                var vi: usize = 0;
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.int32s.items.len) {
                        result[i] = self.int32s.items[vi];
                        vi += 1;
                    } else {
                        result[i] = 0;
                    }
                }
                col.int32s = result;
            },
            .int64 => {
                const result = try allocator.alloc(i64, num_rows);
                var vi: usize = 0;
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.int64s.items.len) {
                        result[i] = self.int64s.items[vi];
                        vi += 1;
                    } else {
                        result[i] = 0;
                    }
                }
                col.int64s = result;
            },
            .float => {
                const result = try allocator.alloc(f32, num_rows);
                var vi: usize = 0;
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.floats.items.len) {
                        result[i] = self.floats.items[vi];
                        vi += 1;
                    } else {
                        result[i] = 0;
                    }
                }
                col.floats = result;
            },
            .double => {
                const result = try allocator.alloc(f64, num_rows);
                var vi: usize = 0;
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.doubles.items.len) {
                        result[i] = self.doubles.items[vi];
                        vi += 1;
                    } else {
                        result[i] = 0;
                    }
                }
                col.doubles = result;
            },
            .boolean => {
                const result = try allocator.alloc(bool, num_rows);
                var vi: usize = 0;
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.booleans.items.len) {
                        result[i] = self.booleans.items[vi];
                        vi += 1;
                    } else {
                        result[i] = false;
                    }
                }
                col.booleans = result;
            },
            .byte_array, .fixed_len_byte_array => {
                const result = try allocator.alloc([]const u8, num_rows);
                var vi: usize = 0;
                var initialized: usize = 0;
                errdefer {
                    for (0..initialized) |i| allocator.free(result[i]);
                    allocator.free(result);
                }
                for (0..num_rows) |i| {
                    if (validity[i] and vi < self.byte_arrays.items.len) {
                        // Transfer ownership
                        result[i] = self.byte_arrays.items[vi];
                        vi += 1;
                    } else {
                        result[i] = try allocator.alloc(u8, 0);
                    }
                    initialized += 1;
                }
                // Mark all elements as transferred so deinit won't double-free them
                self.byte_arrays.clearRetainingCapacity();
                col.byte_arrays = result;
            },
            else => {},
        }
    }
};

// ============================================================
// Dictionary Store
// ============================================================

pub const DictionaryStore = struct {
    allocator: Allocator,
    int32s: std.ArrayList(i32),
    int64s: std.ArrayList(i64),
    floats: std.ArrayList(f32),
    doubles: std.ArrayList(f64),
    byte_arrays: std.ArrayList([]const u8),

    pub fn init(allocator: Allocator, data: []const u8, physical_type: types.PhysicalType, num_values: usize) !DictionaryStore {
        var store = DictionaryStore{
            .allocator = allocator,
            .int32s = std.ArrayList(i32).empty,
            .int64s = std.ArrayList(i64).empty,
            .floats = std.ArrayList(f32).empty,
            .doubles = std.ArrayList(f64).empty,
            .byte_arrays = std.ArrayList([]const u8).empty,
        };
        errdefer store.deinit();

        var dec = encoding.PlainDecoder.init(data);
        switch (physical_type) {
            .int32 => {
                for (0..num_values) |_| {
                    try store.int32s.append(allocator, try dec.readInt32());
                }
            },
            .int64 => {
                for (0..num_values) |_| {
                    try store.int64s.append(allocator, try dec.readInt64());
                }
            },
            .float => {
                for (0..num_values) |_| {
                    try store.floats.append(allocator, try dec.readFloat());
                }
            },
            .double => {
                for (0..num_values) |_| {
                    try store.doubles.append(allocator, try dec.readDouble());
                }
            },
            .byte_array => {
                for (0..num_values) |_| {
                    const src = try dec.readByteArray();
                    const copy = try allocator.alloc(u8, src.len);
                    @memcpy(copy, src);
                    try store.byte_arrays.append(allocator, copy);
                }
            },
            else => return error.UnsupportedEncoding,
        }

        return store;
    }

    pub fn deinit(self: *DictionaryStore) void {
        self.int32s.deinit(self.allocator);
        self.int64s.deinit(self.allocator);
        self.floats.deinit(self.allocator);
        self.doubles.deinit(self.allocator);
        for (self.byte_arrays.items) |arr| {
            self.allocator.free(arr);
        }
        self.byte_arrays.deinit(self.allocator);
    }
};

