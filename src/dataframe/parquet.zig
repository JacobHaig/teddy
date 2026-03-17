const std = @import("std");
const Allocator = std.mem.Allocator;
const dataframe = @import("dataframe.zig");
const series_mod = @import("series.zig");
const String = @import("strings.zig").String;
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

fn addColumn(allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    switch (col.physical_type) {
        .boolean => {
            var s = try df.createSeries(bool);
            try s.rename(col.name);
            const vals = col.booleans orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else false);
            }
        },
        .int32 => {
            // Check converted type for narrower integer types
            if (col.converted_type) |ct| {
                switch (ct) {
                    .int_8 => return addNarrowIntColumn(i8, allocator, df, col),
                    .int_16 => return addNarrowIntColumn(i16, allocator, df, col),
                    .uint_8 => return addNarrowIntColumn(u8, allocator, df, col),
                    .uint_16 => return addNarrowIntColumn(u16, allocator, df, col),
                    else => {},
                }
            }
            var s = try df.createSeries(i32);
            try s.rename(col.name);
            const vals = col.int32s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .int64 => {
            var s = try df.createSeries(i64);
            try s.rename(col.name);
            const vals = col.int64s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .float => {
            var s = try df.createSeries(f32);
            try s.rename(col.name);
            const vals = col.floats orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .double => {
            var s = try df.createSeries(f64);
            try s.rename(col.name);
            const vals = col.doubles orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .byte_array, .fixed_len_byte_array => {
            var s = try df.createSeries(String);
            try s.rename(col.name);
            const vals = col.byte_arrays orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                if (valid and i < vals.len) {
                    try s.append(try String.fromSlice(allocator, vals[i]));
                } else {
                    try s.append(try String.fromSlice(allocator, ""));
                }
            }
        },
        else => return error.UnsupportedParquetType,
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
    var string_bufs: std.ArrayList([]const []const u8) = .{};
    defer string_bufs.deinit(allocator);

    for (df.series.items, 0..) |*boxed, i| {
        cols[i] = try boxedToColumnData(allocator, boxed, &string_bufs);
    }

    const bufs = try string_bufs.toOwnedSlice(allocator);
    return .{ .columns = cols, .string_bufs = bufs, .allocator = allocator };
}

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
