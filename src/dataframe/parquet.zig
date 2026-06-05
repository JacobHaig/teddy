const std = @import("std");
const Allocator = std.mem.Allocator;
const dataframe = @import("dataframe.zig");
const series_mod = @import("series.zig");
const String = @import("strings.zig").String;
const Raw = @import("raw.zig").Raw;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const parquet = @import("parquet");

// ============================================================
// Adapter: ParquetColumn → Dataframe
// ============================================================

/// Convert a ParquetResult into a Dataframe.
/// Caller owns the returned Dataframe. The ParquetResult can be deinited
/// independently after this call (data is copied).
pub fn toDataframe(allocator: Allocator, result: *parquet.ParquetResult) !*dataframe.Dataframe {
    const df = try dataframe.Dataframe.init(allocator);
    errdefer df.deinit();

    for (result.columns) |*col| {
        try addColumn(allocator, df, col);
    }

    return df;
}

/// Dataframe-side type a parquet column resolves to.
pub const ResolvedKind = enum {
    boolean,
    int8_,
    int16_,
    int32_,
    int64_,
    uint8_,
    uint16_,
    uint32_,
    uint64_,
    float32_,
    float64_,
    string,
    raw,
};

/// Resolution precedence: modern logical_type (field 10) → legacy
/// converted_type (field 6) → bare physical type. Logical annotations not yet
/// surfaced as dataframe types (date/time/timestamp/decimal/uuid/float16/...)
/// fall through to the physical default; slices 6d-2a.1–.5 flip them one at a
/// time. Deferred types (VARIANT/GEOMETRY/GEOGRAPHY) and INT96 resolve to Raw.
pub fn resolveKind(col: *const parquet.ParquetColumn) ResolvedKind {
    if (col.logical_type) |lt| switch (lt) {
        .integer => |it| {
            if (it.is_signed) {
                switch (it.bit_width) {
                    8 => return .int8_,
                    16 => return .int16_,
                    32 => return .int32_,
                    64 => return .int64_,
                    else => {},
                }
            } else {
                switch (it.bit_width) {
                    8 => return .uint8_,
                    16 => return .uint16_,
                    32 => return .uint32_,
                    64 => return .uint64_,
                    else => {},
                }
            }
        },
        .string, .@"enum", .json => return .string,
        .variant, .geometry, .geography => return .raw,
        else => {},
    };
    if (col.converted_type) |ct| switch (ct) {
        .int_8 => return .int8_,
        .int_16 => return .int16_,
        .uint_8 => return .uint8_,
        .uint_16 => return .uint16_,
        .uint_32 => return .uint32_,
        .uint_64 => return .uint64_,
        .utf8 => return .string,
        else => {},
    };
    return switch (col.physical_type) {
        .boolean => .boolean,
        .int32 => .int32_,
        .int64 => .int64_,
        .float => .float32_,
        .double => .float64_,
        .byte_array, .fixed_len_byte_array => .string,
        .int96 => .raw,
    };
}

fn addColumn(allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    switch (resolveKind(col)) {
        .boolean => {
            var s = try df.createSeries(bool);
            try s.rename(col.name);
            const vals = col.booleans orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else false);
            }
        },
        .int8_ => return addNarrowIntColumn(i8, allocator, df, col),
        .int16_ => return addNarrowIntColumn(i16, allocator, df, col),
        .uint8_ => return addNarrowIntColumn(u8, allocator, df, col),
        .uint16_ => return addNarrowIntColumn(u16, allocator, df, col),
        // uint_32/uint_64 span the full unsigned range, so the signed bit
        // pattern must be reinterpreted (bitcast), not range-cast.
        .uint32_ => return addUintColumn(u32, i32, df, col, col.int32s),
        .uint64_ => return addUintColumn(u64, i64, df, col, col.int64s),
        .int32_ => {
            var s = try df.createSeries(i32);
            try s.rename(col.name);
            const vals = col.int32s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .int64_ => {
            var s = try df.createSeries(i64);
            try s.rename(col.name);
            const vals = col.int64s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .float32_ => {
            var s = try df.createSeries(f32);
            try s.rename(col.name);
            const vals = col.floats orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .float64_ => {
            var s = try df.createSeries(f64);
            try s.rename(col.name);
            const vals = col.doubles orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .string => {
            var s = try df.createSeries(String);
            try s.rename(col.name);
            // .string requires a byte-array-backed physical; a string/json/enum
            // annotation over another physical is malformed — error rather than
            // silently producing a 0-row column (width/height desync).
            const vals = col.byte_arrays orelse return error.UnexpectedPhysicalType;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                if (valid and i < vals.len) {
                    try s.append(try String.fromSlice(allocator, vals[i]));
                } else {
                    try s.append(try String.fromSlice(allocator, ""));
                }
            }
        },
        .raw => return addRawColumn(allocator, df, col),
    }
}

/// Fallback: preserve undecoded bytes + column metadata so the column can be
/// re-emitted bit-faithfully. Mirrors the String arm's null handling (invalid
/// rows become empty values — the adapter does not carry validity yet).
fn addRawColumn(allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    var s = try df.createSeries(Raw);
    try s.rename(col.name);
    s.meta = .{
        .physical_type = col.physical_type,
        .converted_type = col.converted_type,
        .logical_type = col.logical_type,
        .type_length = col.type_length,
    };
    // Raw columns are byte-array-backed (INT96/FLBA/BYTE_ARRAY); anything else
    // reaching here is malformed — error rather than a silent 0-row column.
    const vals = col.byte_arrays orelse return error.UnexpectedPhysicalType;
    for (0..col.num_rows) |i| {
        const valid = if (col.validity) |v| v[i] else true;
        if (valid and i < vals.len) {
            try s.append(try Raw.fromSlice(allocator, vals[i]));
        } else {
            try s.append(try Raw.fromSlice(allocator, ""));
        }
    }
}

// ============================================================
// Adapter: Dataframe → ColumnData (for Parquet writer)
// ============================================================

pub const ColumnData = parquet.ColumnData;

pub const DataframeColumns = struct {
    columns: []ColumnData,
    /// Allocated string slices that need to be freed
    string_bufs: [][]const []const u8,
    allocator: Allocator,

    pub fn deinit(self: *DataframeColumns) void {
        for (self.string_bufs) |buf| {
            self.allocator.free(buf);
        }
        self.allocator.free(self.string_bufs);
        self.allocator.free(self.columns);
    }
};

/// Convert a Dataframe's columns to ColumnData slices for the Parquet writer.
pub fn fromDataframe(allocator: Allocator, df: *dataframe.Dataframe) !DataframeColumns {
    const width = df.width();
    const cols = try allocator.alloc(ColumnData, width);
    errdefer allocator.free(cols);

    // Track string buffers that need cleanup
    var string_bufs: std.ArrayList([]const []const u8) = .empty;
    defer string_bufs.deinit(allocator);

    for (df.series.items, 0..) |*boxed, i| {
        cols[i] = try boxedToColumnData(allocator, boxed, &string_bufs);
    }

    const bufs = try string_bufs.toOwnedSlice(allocator);
    return .{ .columns = cols, .string_bufs = bufs, .allocator = allocator };
}

// Convert a BoxedSeries to ColumnData for Parquet writing. Allocates string slices for string columns.
fn boxedToColumnData(allocator: Allocator, boxed: *BoxedSeries, string_bufs: *std.ArrayList([]const []const u8)) !ColumnData {
    return switch (boxed.*) {
        .int32 => |s| .{
            .name = s.name.toSlice(),
            .physical_type = .int32,
            .int32s = s.values.items,
            .num_values = s.values.items.len,
        },
        .int64 => |s| .{
            .name = s.name.toSlice(),
            .physical_type = .int64,
            .int64s = s.values.items,
            .num_values = s.values.items.len,
        },
        .float32 => |s| .{
            .name = s.name.toSlice(),
            .physical_type = .float,
            .floats = s.values.items,
            .num_values = s.values.items.len,
        },
        .float64 => |s| .{
            .name = s.name.toSlice(),
            .physical_type = .double,
            .doubles = s.values.items,
            .num_values = s.values.items.len,
        },
        .bool => |s| .{
            .name = s.name.toSlice(),
            .physical_type = .boolean,
            .booleans = s.values.items,
            .num_values = s.values.items.len,
        },
        .string => |s| blk: {
            // Convert String values to []const u8 slices
            const slices = try allocator.alloc([]const u8, s.values.items.len);
            for (s.values.items, 0..) |*str, j| {
                slices[j] = str.toSlice();
            }
            try string_bufs.append(allocator, slices);
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = .byte_array,
                .converted_type = .utf8,
                .byte_arrays = slices,
                .num_values = s.values.items.len,
            };
        },
        .raw => |s| blk: {
            // Re-emit the preserved physical type + annotations bit-faithfully.
            const slices = try allocator.alloc([]const u8, s.values.items.len);
            for (s.values.items, 0..) |*r, j| {
                slices[j] = r.toSlice();
            }
            try string_bufs.append(allocator, slices);
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = s.meta.physical_type,
                .converted_type = s.meta.converted_type,
                .logical_type = s.meta.logical_type,
                .type_length = s.meta.type_length,
                .byte_arrays = slices,
                .num_values = s.values.items.len,
            };
        },
        .isize => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .int8 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .int16 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .uint8 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .uint16 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .uint32 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .uint64 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .uint128 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .int128 => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
        .usize => |s| blk: {
            _ = s;
            break :blk error.UnsupportedType;
        },
    };
}

fn addNarrowIntColumn(comptime T: type, allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    _ = allocator;
    var s = try df.createSeries(T);
    try s.rename(col.name);
    const vals = col.int32s orelse return;
    for (0..col.num_rows) |i| {
        const valid = if (col.validity) |v| v[i] else true;
        const val: T = if (valid and i < vals.len) @intCast(vals[i]) else 0;
        try s.append(val);
    }
}

/// Build an unsigned column from a same-width signed physical column by
/// reinterpreting the bit pattern (UINT_32 over INT32, UINT_64 over INT64).
/// `@bitCast` is required because the unsigned value range overflows the signed
/// physical type — a plain cast would trap on "negative" stored values.
fn addUintColumn(comptime U: type, comptime I: type, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn, source: ?[]const I) !void {
    var s = try df.createSeries(U);
    try s.rename(col.name);
    const vals = source orelse return;
    for (0..col.num_rows) |i| {
        const valid = if (col.validity) |v| v[i] else true;
        const val: U = if (valid and i < vals.len) @bitCast(vals[i]) else 0;
        try s.append(val);
    }
}
