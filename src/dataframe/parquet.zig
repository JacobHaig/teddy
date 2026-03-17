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
