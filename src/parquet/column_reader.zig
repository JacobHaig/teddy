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

/// RLE/bit-packed bit width for a level stream whose maximum value is
/// `max_level`: ceil(log2(max_level + 1)). For max_level 0 → 0, 1 → 1
/// (so flat columns are byte-for-byte unchanged), 2/3 → 2, etc. The parquet
/// spec uses exactly this width for def/rep level encoding.
fn levelBitWidth(max_level: u8) u8 {
    if (max_level == 0) return 0;
    return @intCast(@as(u8, 8) - @as(u8, @clz(max_level)));
}

/// Information about a leaf column resolved from the schema.
pub const LeafColumn = struct {
    schema_element: metadata.SchemaElement,
    max_def_level: u8,
    max_rep_level: u8,
    column_index: usize,
    /// True if this leaf descends from a nested subtree (LIST/MAP/STRUCT):
    /// either it is repeated (max_rep > 0) or its definition depth exceeds the
    /// single optional level a genuinely-flat column would have. When set,
    /// readColumnChunk preserves raw def/rep streams instead of expanding
    /// placeholder nulls (which assumes one value slot per row — wrong when
    /// values can repeat).
    nested: bool = false,
    /// Which top-level child of the schema root this leaf descends from.
    root_child_index: usize = 0,
    /// Dotted schema path (e.g. "l.list.element"), allocator-owned by the
    /// caller; used as the surfaced column name for nested leaves. When null,
    /// the leaf's own schema_element.name is used (flat columns).
    dotted_path: ?[]const u8 = null,
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

    // P5: file-controlled i64 offsets. A negative value (cast fails) or a value
    // past EOF would otherwise produce an out-of-bounds slice panic, so convert
    // through std.math.cast and validate both ends against the buffer length.
    const start_offset: usize = blk: {
        if (col_meta.dictionary_page_offset) |dpo| {
            break :blk std.math.cast(usize, dpo) orelse return error.CorruptFile;
        }
        break :blk std.math.cast(usize, col_meta.data_page_offset) orelse return error.CorruptFile;
    };

    // Total bytes for this column chunk (file-controlled i64; negative → error).
    const total_size: usize = std.math.cast(usize, col_meta.total_compressed_size) orelse return error.CorruptFile;
    // Checked add: start_offset + total_size could overflow usize on a crafted file.
    const end_offset = std.math.add(usize, start_offset, total_size) catch return error.CorruptFile;

    // Validate BOTH ends: start_offset must itself be in-bounds (the slice
    // file_data[pos..end_offset] below would otherwise panic) and end_offset
    // must not run past EOF.
    if (start_offset > file_data.len) return error.ColumnDataOutOfBounds;
    if (end_offset > file_data.len) return error.ColumnDataOutOfBounds;

    // Accumulated values across pages
    var all_values = ValueAccumulator.init(allocator, physical_type, leaf.schema_element.type_length orelse 0);
    defer all_values.deinit();

    var all_def_levels = std.ArrayList(u32).empty;
    defer all_def_levels.deinit(allocator);

    // Repetition levels are only decoded+retained for nested leaves
    // (max_rep_level > 0). Flat columns never populate this.
    var all_rep_levels = std.ArrayList(u32).empty;
    defer all_rep_levels.deinit(allocator);

    // Dictionary (populated if we encounter a dictionary page)
    var dict: ?DictionaryStore = null;
    defer if (dict) |*d| d.deinit();

    var pos = start_offset;
    var values_read: usize = 0;
    // P4: num_values is a file-controlled i64; a negative value (cast fails) is
    // corrupt rather than "read nothing".
    const total_values: usize = std.math.cast(usize, col_meta.num_values) orelse return error.CorruptFile;

    while (pos < end_offset and values_read < total_values) {
        // Decode page header (Thrift)
        var thrift_reader = ThriftReader.init(file_data[pos..end_offset]);
        const page_header = try metadata.PageHeader.decode(&thrift_reader);
        pos += thrift_reader.pos; // advance past the Thrift header

        // P4: page sizes are file-controlled i32s; negative (cast fails) → error.
        const compressed_size: usize = std.math.cast(usize, page_header.compressed_page_size) orelse return error.CorruptPageData;
        const uncompressed_size: usize = std.math.cast(usize, page_header.uncompressed_page_size) orelse return error.CorruptPageData;

        // Checked add: pos + compressed_size could overflow usize on a crafted file.
        const page_end = std.math.add(usize, pos, compressed_size) catch return error.PageDataOutOfBounds;
        if (page_end > file_data.len) return error.PageDataOutOfBounds;
        const page_data = file_data[pos..page_end];
        pos = page_end;

        switch (page_header.page_type) {
            .dictionary_page => {
                const dict_header = page_header.dictionary_page_header orelse return error.MissingDictionaryHeader;
                const decompressed = try decompressPage(allocator, page_data, col_meta.codec, uncompressed_size);
                defer if (decompressed.ptr != page_data.ptr) allocator.free(decompressed);

                // P4: dictionary entry count is a file-controlled i32; negative → error.
                const dict_num_values: usize = std.math.cast(usize, dict_header.num_values) orelse return error.CorruptPageData;
                dict = try DictionaryStore.init(allocator, decompressed, physical_type, leaf.schema_element.type_length orelse 0, dict_num_values);
            },
            .data_page => {
                const dp_header = page_header.data_page_header orelse return error.MissingDataPageHeader;
                // P4: file-controlled i32; negative → error.
                const num_values_in_page: usize = std.math.cast(usize, dp_header.num_values) orelse return error.CorruptPageData;

                const decompressed = try decompressPage(allocator, page_data, col_meta.codec, uncompressed_size);
                defer if (decompressed.ptr != page_data.ptr) allocator.free(decompressed);

                var data_pos: usize = 0;

                // Read repetition levels. For flat schema (max_rep_level == 0)
                // there is no rep-level region at all. For nested leaves we now
                // DECODE them (was previously skipped in the flat-only reader).
                // P2: the 4-byte length prefix and the levels region it describes are
                // file-controlled; validate both before slicing or a malformed page
                // would panic on an out-of-bounds slice.
                if (leaf.max_rep_level > 0) {
                    if (decompressed.len < data_pos + 4) return error.CorruptPageData;
                    const rep_len: usize = std.mem.readInt(u32, decompressed[data_pos..][0..4], .little);
                    const rep_start = data_pos + 4;
                    // data_pos + 4 + rep_len could overflow / exceed the page.
                    const rep_end = std.math.add(usize, rep_start, rep_len) catch return error.CorruptPageData;
                    if (rep_end > decompressed.len) return error.CorruptPageData;
                    const rep_data = decompressed[rep_start..rep_end];
                    data_pos = rep_end;

                    var rep_dec = encoding.RleBitPackedDecoder.init(rep_data, levelBitWidth(leaf.max_rep_level));
                    const rep_levels = try rep_dec.readBatch(num_values_in_page, allocator);
                    defer allocator.free(rep_levels);
                    try all_rep_levels.appendSlice(allocator, rep_levels);
                }

                // Read definition levels (P2: same length-prefix validation).
                if (leaf.max_def_level > 0) {
                    if (decompressed.len < data_pos + 4) return error.CorruptPageData;
                    const def_len_bytes: usize = std.mem.readInt(u32, decompressed[data_pos..][0..4], .little);
                    const def_start = data_pos + 4;
                    const def_end = std.math.add(usize, def_start, def_len_bytes) catch return error.CorruptPageData;
                    if (def_end > decompressed.len) return error.CorruptPageData;
                    const def_data = decompressed[def_start..def_end];
                    data_pos = def_end;

                    var rle_dec = encoding.RleBitPackedDecoder.init(def_data, levelBitWidth(leaf.max_def_level));
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
                // P4: file-controlled i32s; negative (cast fails) → error.
                const num_values_in_page: usize = std.math.cast(usize, dp2.num_values) orelse return error.CorruptPageData;
                const rep_len: usize = std.math.cast(usize, dp2.repetition_levels_byte_length) orelse return error.CorruptPageData;
                const def_len: usize = std.math.cast(usize, dp2.definition_levels_byte_length) orelse return error.CorruptPageData;

                // P2: rep_len + def_len is subtracted from uncompressed_size below;
                // if the header lies that sum can exceed uncompressed_size and the
                // subtraction would underflow usize. It is also used to slice
                // page_data. Validate the combined prefix against BOTH bounds up
                // front so every downstream slice/subtraction is safe.
                const levels_total = std.math.add(usize, rep_len, def_len) catch return error.CorruptPageData;
                if (levels_total > uncompressed_size) return error.CorruptPageData;
                if (levels_total > page_data.len) return error.CorruptPageData;
                const values_uncompressed_size = uncompressed_size - levels_total;

                // In v2, rep/def levels are NOT compressed
                var data_pos: usize = 0;

                // Skip repetition levels
                data_pos += rep_len;

                // Read definition levels
                if (def_len > 0 and leaf.max_def_level > 0) {
                    const def_data = page_data[data_pos .. data_pos + def_len];
                    data_pos += def_len;

                    var rle_dec = encoding.RleBitPackedDecoder.init(def_data, levelBitWidth(leaf.max_def_level));
                    const levels = try rle_dec.readBatch(num_values_in_page, allocator);
                    defer allocator.free(levels);
                    try all_def_levels.appendSlice(allocator, levels);

                    var non_null: usize = 0;
                    for (levels) |l| {
                        if (l == leaf.max_def_level) non_null += 1;
                    }

                    // The rest of the page is compressed (unless is_compressed=false).
                    // values_uncompressed_size was validated above (no underflow);
                    // data_pos == rep_len + def_len <= page_data.len so the slice is safe.
                    const is_compressed = dp2.is_compressed orelse true;
                    const values_data = page_data[data_pos..];

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
    return buildColumn(allocator, &all_values, all_def_levels.items, all_rep_levels.items, leaf, num_rows);
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

/// Build the final ParquetColumn from accumulated values + def/rep levels.
fn buildColumn(
    allocator: Allocator,
    acc: *ValueAccumulator,
    def_levels: []const u32,
    rep_levels: []const u32,
    leaf: LeafColumn,
    num_rows: usize,
) !types.ParquetColumn {
    var col = types.ParquetColumn.initEmpty(allocator);
    errdefer col.deinit();

    // Copy the column name. Nested leaves use the dotted schema path so flat
    // consumers can at least identify them; flat leaves use the bare name.
    const src_name = leaf.dotted_path orelse leaf.schema_element.name;
    const name_copy = try allocator.alloc(u8, src_name.len);
    @memcpy(name_copy, src_name);
    col.name = name_copy;

    col.physical_type = leaf.schema_element.type_ orelse .byte_array;
    col.converted_type = leaf.schema_element.converted_type;
    col.logical_type = leaf.schema_element.logical_type;
    col.type_length = leaf.schema_element.type_length;
    col.scale = leaf.schema_element.scale;
    col.precision = leaf.schema_element.precision;
    col.is_optional = leaf.max_def_level > 0;
    col.num_rows = num_rows;
    col.nested = leaf.nested;
    col.root_child_index = leaf.root_child_index;

    if (leaf.nested) {
        // Nested leaf: do NOT expand to one value-per-row (values can repeat
        // or be absent). Move the present values verbatim and attach the raw
        // def/rep streams; slice .2's Dremel pass assembles records from these.
        // num_rows stays the ROW count from metadata.
        acc.moveInto(&col);

        const def_copy = try allocator.alloc(u16, def_levels.len);
        errdefer allocator.free(def_copy);
        for (def_levels, 0..) |d, i| def_copy[i] = @intCast(d);
        col.def_levels = def_copy;

        const rep_copy = try allocator.alloc(u16, rep_levels.len);
        errdefer allocator.free(rep_copy);
        for (rep_levels, 0..) |r, i| rep_copy[i] = @intCast(r);
        col.rep_levels = rep_copy;

        return col;
    }

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
    /// Width in bytes for FIXED_LEN_BYTE_ARRAY values (from the schema). Unused
    /// for other physical types; INT96 is always 12 bytes.
    type_length: i32,
    int32s: std.ArrayList(i32),
    int64s: std.ArrayList(i64),
    floats: std.ArrayList(f32),
    doubles: std.ArrayList(f64),
    booleans: std.ArrayList(bool),
    byte_arrays: std.ArrayList([]const u8),

    pub fn init(allocator: Allocator, physical_type: types.PhysicalType, type_length: i32) ValueAccumulator {
        return .{
            .allocator = allocator,
            .physical_type = physical_type,
            .type_length = type_length,
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
                if (self.type_length <= 0) return error.InvalidTypeLength;
                // Safe @intCast: the `<= 0` guard above proves type_length is positive.
                const n: usize = @intCast(self.type_length);
                for (0..count) |_| {
                    const src = try dec.readFixedByteArray(n);
                    const copy = try allocator.alloc(u8, src.len);
                    @memcpy(copy, src);
                    try self.byte_arrays.append(allocator, copy);
                }
            },
            .int96 => {
                // INT96 is a fixed 12-byte value (legacy timestamp). Preserve the
                // raw bytes; semantic decoding to a timestamp happens in the
                // logical-type layer (Phase 6d).
                for (0..count) |_| {
                    const src = try dec.readFixedByteArray(12);
                    const copy = try allocator.alloc(u8, 12);
                    @memcpy(copy, src);
                    try self.byte_arrays.append(allocator, copy);
                }
            },
        }
    }

    pub fn decodeDictionary(self: *ValueAccumulator, rle_dec: *encoding.RleBitPackedDecoder, dict: *const DictionaryStore, count: usize, physical_type: types.PhysicalType, allocator: Allocator) !void {
        for (0..count) |_| {
            const idx = try rle_dec.next();
            // P1: idx is decoded from the file's RLE index stream and is entirely
            // attacker-controlled. Bounds-check against the matching dictionary
            // array for this physical type before indexing, or a malformed stream
            // panics on an out-of-bounds access. (byte_array/flba/int96 all share
            // the byte_arrays store.)
            switch (physical_type) {
                .int32 => {
                    if (idx >= dict.int32s.items.len) return error.InvalidDictionaryIndex;
                    try self.int32s.append(allocator, dict.int32s.items[idx]);
                },
                .int64 => {
                    if (idx >= dict.int64s.items.len) return error.InvalidDictionaryIndex;
                    try self.int64s.append(allocator, dict.int64s.items[idx]);
                },
                .float => {
                    if (idx >= dict.floats.items.len) return error.InvalidDictionaryIndex;
                    try self.floats.append(allocator, dict.floats.items[idx]);
                },
                .double => {
                    if (idx >= dict.doubles.items.len) return error.InvalidDictionaryIndex;
                    try self.doubles.append(allocator, dict.doubles.items[idx]);
                },
                .byte_array, .fixed_len_byte_array, .int96 => {
                    if (idx >= dict.byte_arrays.items.len) return error.InvalidDictionaryIndex;
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
            .byte_array, .fixed_len_byte_array, .int96 => {
                col.byte_arrays = self.byte_arrays.toOwnedSlice(self.allocator) catch null;
                self.byte_arrays = std.ArrayList([]const u8).empty;
            },
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
            .byte_array, .fixed_len_byte_array, .int96 => {
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
                // P6: when the def-levels imply fewer values than were actually
                // decoded (a malformed file), `vi` stops short of the accumulator
                // length and the tail entries items[vi..] were never moved into
                // `result`. clearRetainingCapacity() would drop those slices
                // without freeing them → leak. Free the untransferred tail first.
                for (self.byte_arrays.items[vi..]) |arr| allocator.free(arr);
                // Mark all elements as transferred so deinit won't double-free them.
                self.byte_arrays.clearRetainingCapacity();
                col.byte_arrays = result;
            },
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

    pub fn init(allocator: Allocator, data: []const u8, physical_type: types.PhysicalType, type_length: i32, num_values: usize) !DictionaryStore {
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
            .fixed_len_byte_array => {
                if (type_length <= 0) return error.InvalidTypeLength;
                // Safe @intCast: the `<= 0` guard above proves type_length is positive.
                const n: usize = @intCast(type_length);
                for (0..num_values) |_| {
                    const src = try dec.readFixedByteArray(n);
                    const copy = try allocator.alloc(u8, src.len);
                    @memcpy(copy, src);
                    try store.byte_arrays.append(allocator, copy);
                }
            },
            .int96 => {
                for (0..num_values) |_| {
                    const src = try dec.readFixedByteArray(12);
                    const copy = try allocator.alloc(u8, 12);
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

