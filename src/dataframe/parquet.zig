const std = @import("std");
const Allocator = std.mem.Allocator;
const dataframe = @import("dataframe.zig");
const series_mod = @import("series.zig");
const String = @import("strings.zig").String;
const Raw = @import("raw.zig").Raw;
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
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
    date_,
    time_,
    timestamp_,
};

/// Resolution precedence: modern logical_type (field 10) → legacy
/// converted_type (field 6) → bare physical type. Logical annotations not yet
/// surfaced as dataframe types (decimal/uuid/float16/...)
/// fall through to the physical default; slices 6d-2a.2–.5 flip them one at a
/// time. Deferred types (VARIANT/GEOMETRY/GEOGRAPHY) resolve to Raw.
/// INT96 physical resolves to timestamp_ (legacy format decoded via fromInt96Bytes).
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
        .date => return .date_,
        .time => return .time_,
        .timestamp => return .timestamp_,
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
        .date => return .date_,
        .time_millis, .time_micros => return .time_,
        .timestamp_millis, .timestamp_micros => return .timestamp_,
        else => {},
    };
    return switch (col.physical_type) {
        .boolean => .boolean,
        .int32 => .int32_,
        .int64 => .int64_,
        .float => .float32_,
        .double => .float64_,
        .byte_array, .fixed_len_byte_array => .string,
        .int96 => .timestamp_,
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
        .date_ => {
            var s = try df.createSeries(Date);
            try s.rename(col.name);
            // DATE is INT32-backed (days since epoch).
            const vals = col.int32s orelse return error.UnexpectedPhysicalType;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(.{ .days = if (valid and i < vals.len) vals[i] else 0 });
            }
        },
        .time_ => {
            var s = try df.createSeries(Time);
            try s.rename(col.name);
            // unit/utc: modern annotation wins; legacy time_millis/time_micros
            // imply utc=true per LogicalTypes.md.
            var unit: parquet.TimeUnit = .millis;
            var utc = true;
            if (col.logical_type) |lt| {
                unit = lt.time.unit;
                utc = lt.time.is_adjusted_to_utc;
            } else if (col.converted_type) |ct| {
                unit = if (ct == .time_micros) .micros else .millis;
            }
            if (col.int32s) |vals| { // TIME(MILLIS) is INT32-backed
                for (0..col.num_rows) |i| {
                    const valid = if (col.validity) |v| v[i] else true;
                    try s.append(.{ .value = if (valid and i < vals.len) vals[i] else 0, .unit = unit, .utc = utc });
                }
            } else if (col.int64s) |vals| {
                for (0..col.num_rows) |i| {
                    const valid = if (col.validity) |v| v[i] else true;
                    try s.append(.{ .value = if (valid and i < vals.len) vals[i] else 0, .unit = unit, .utc = utc });
                }
            } else return error.UnexpectedPhysicalType;
        },
        .timestamp_ => {
            var s = try df.createSeries(Timestamp);
            try s.rename(col.name);
            if (col.physical_type == .int96) {
                // Legacy INT96: 12-byte julian-day timestamps -> nanos.
                const vals = col.byte_arrays orelse return error.UnexpectedPhysicalType;
                for (0..col.num_rows) |i| {
                    const valid = if (col.validity) |v| v[i] else true;
                    if (valid and i < vals.len) {
                        if (vals[i].len != 12) return error.InvalidInt96;
                        const bytes_ptr: *const [12]u8 = vals[i][0..12];
                        try s.append(try Timestamp.fromInt96Bytes(bytes_ptr));
                    } else {
                        try s.append(.{ .value = 0, .unit = .nanos, .utc = false, .origin = .int96 });
                    }
                }
            } else {
                var unit: parquet.TimeUnit = .millis;
                var utc = true;
                if (col.logical_type) |lt| {
                    unit = lt.timestamp.unit;
                    utc = lt.timestamp.is_adjusted_to_utc;
                } else if (col.converted_type) |ct| {
                    unit = if (ct == .timestamp_micros) .micros else .millis;
                }
                const vals = col.int64s orelse return error.UnexpectedPhysicalType;
                for (0..col.num_rows) |i| {
                    const valid = if (col.validity) |v| v[i] else true;
                    try s.append(.{ .value = if (valid and i < vals.len) vals[i] else 0, .unit = unit, .utc = utc, .origin = .int64 });
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
    /// Owns every scratch allocation made while converting (slice arrays,
    /// numeric buffers, encoded byte values). Inner slices borrowed from
    /// Series values (String/Raw bytes) are NOT arena-owned and are not freed.
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *DataframeColumns) void {
        self.arena.deinit();
    }
};

/// Options for the dataframe → Parquet adapter.
pub const AdapterWriteOptions = struct {
    /// When true AND every value in a Timestamp column carries origin=int96,
    /// the column is re-emitted as INT96 (bit-faithful). Default: modern INT64
    /// TIMESTAMP.
    emit_int96: bool = false,
};

/// Convert a Dataframe's columns to ColumnData slices for the Parquet writer.
pub fn fromDataframe(allocator: Allocator, df: *dataframe.Dataframe, opts: AdapterWriteOptions) !DataframeColumns {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const scratch = arena.allocator();
    const cols = try scratch.alloc(ColumnData, df.width());
    for (df.series.items, 0..) |*boxed, i| {
        cols[i] = try boxedToColumnData(scratch, boxed, opts);
    }
    return .{ .columns = cols, .arena = arena };
}

// Convert a BoxedSeries to ColumnData for Parquet writing. All scratch
// allocations (slice arrays, numeric buffers) come from the passed arena
// allocator; the arena owns them and frees them on DataframeColumns.deinit().
fn boxedToColumnData(scratch: Allocator, boxed: *BoxedSeries, opts: AdapterWriteOptions) !ColumnData {
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
            // Convert String values to []const u8 slices; arena owns the array.
            const slices = try scratch.alloc([]const u8, s.values.items.len);
            for (s.values.items, 0..) |*str, j| {
                slices[j] = str.toSlice();
            }
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
            const slices = try scratch.alloc([]const u8, s.values.items.len);
            for (s.values.items, 0..) |*r, j| {
                slices[j] = r.toSlice();
            }
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
        .date => |s| blk: {
            // DATE re-emits as INT32 + both annotations (modern field 10 and
            // legacy converted_type), matching what pyarrow writes.
            const days = try scratch.alloc(i32, s.values.items.len);
            for (s.values.items, 0..) |d, j| {
                days[j] = d.days;
            }
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = .int32,
                .converted_type = .date,
                .logical_type = .date,
                .int32s = days,
                .num_values = s.values.items.len,
            };
        },
        .time => |s| blk: {
            const n = s.values.items.len;
            // TIME(MILLIS) is INT32-backed; micros/nanos are INT64. Legacy
            // converted_type exists only for millis/micros.
            // Column metadata contract (first-value-wins): unit/utc come from
            // values[0] — uniform by construction when read from parquet. For
            // hand-built mixed columns: values that re-express losslessly in
            // values[0]'s unit are silently normalized; precision loss errors
            // (LossyTimeUnit); a mixed utc flag is silently coerced to
            // values[0]'s. Revisit if hand-built mixing becomes a use case.
            const unit: parquet.TimeUnit = if (n > 0) s.values.items[0].unit else .millis;
            const utc: bool = if (n > 0) s.values.items[0].utc else true;
            const lt: parquet.LogicalType = .{ .time = .{ .is_adjusted_to_utc = utc, .unit = unit } };
            if (unit == .millis) {
                const buf = try scratch.alloc(i32, n);
                for (s.values.items, 0..) |t, j| {
                    const nanos = t.toNanos();
                    const per: i128 = 1_000_000;
                    if (@rem(nanos, per) != 0) return error.LossyTimeUnit;
                    buf[j] = std.math.cast(i32, @divTrunc(nanos, per)) orelse return error.Overflow;
                }
                break :blk .{
                    .name = s.name.toSlice(),
                    .physical_type = .int32,
                    .converted_type = .time_millis,
                    .logical_type = lt,
                    .int32s = buf,
                    .num_values = n,
                };
            }
            const buf = try scratch.alloc(i64, n);
            const per: i128 = if (unit == .micros) 1_000 else 1;
            for (s.values.items, 0..) |t, j| {
                const nanos = t.toNanos();
                if (@rem(nanos, per) != 0) return error.LossyTimeUnit;
                buf[j] = std.math.cast(i64, @divTrunc(nanos, per)) orelse return error.Overflow;
            }
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = .int64,
                .converted_type = if (unit == .micros) .time_micros else null,
                .logical_type = lt,
                .int64s = buf,
                .num_values = n,
            };
        },
        .timestamp => |s| blk: {
            const n = s.values.items.len;
            // Same first-value-wins unit/utc contract as the .time arm above.
            const unit: parquet.TimeUnit = if (n > 0) s.values.items[0].unit else .millis;
            const utc: bool = if (n > 0) s.values.items[0].utc else true;
            // INT96 re-emit: opt-in AND every value must carry int96 origin —
            // any non-int96 value (or an empty column) forces the modern
            // INT64 path.
            var all_int96 = n > 0;
            for (s.values.items) |t| {
                if (t.origin != .int96) {
                    all_int96 = false;
                    break;
                }
            }
            if (opts.emit_int96 and all_int96) {
                const slices = try scratch.alloc([]const u8, n);
                for (s.values.items, 0..) |t, j| {
                    const bytes = try scratch.alloc(u8, 12);
                    const encoded = try t.toInt96Bytes();
                    @memcpy(bytes, &encoded);
                    slices[j] = bytes;
                }
                break :blk .{
                    .name = s.name.toSlice(),
                    .physical_type = .int96,
                    .byte_arrays = slices,
                    .num_values = n,
                };
            }
            // Modern default: INT64 + TIMESTAMP(unit, utc); legacy converted
            // only for utc millis/micros (matches pyarrow's emission).
            const buf = try scratch.alloc(i64, n);
            const per: i128 = switch (unit) {
                .millis => 1_000_000,
                .micros => 1_000,
                .nanos => 1,
            };
            for (s.values.items, 0..) |t, j| {
                const nanos = t.toNanos();
                if (@rem(nanos, per) != 0) return error.LossyTimeUnit;
                buf[j] = std.math.cast(i64, @divTrunc(nanos, per)) orelse return error.Overflow;
            }
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = .int64,
                .converted_type = if (!utc) null else switch (unit) {
                    .millis => .timestamp_millis,
                    .micros => .timestamp_micros,
                    .nanos => null,
                },
                .logical_type = .{ .timestamp = .{ .is_adjusted_to_utc = utc, .unit = unit } },
                .int64s = buf,
                .num_values = n,
            };
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
