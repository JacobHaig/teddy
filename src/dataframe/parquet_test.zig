const std = @import("std");
const parquet = @import("parquet");
const adapter = @import("parquet.zig");
const Raw = @import("raw.zig").Raw;
const Binary = @import("binary.zig").Binary;
const FixedBytes = @import("fixed_bytes.zig").FixedBytes;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Uuid = @import("uuid.zig").Uuid;
const Interval = @import("interval.zig").Interval;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;

test "resolveKind: precedence logical -> converted -> physical" {
    // NOTE: `col` is mutated across cases and fields CARRY OVER — when adding
    // a case, reset physical_type/converted_type/logical_type explicitly.
    var col = parquet.ParquetColumn.initEmpty(std.testing.allocator);

    // Bare physical
    col.physical_type = .int32;
    try std.testing.expectEqual(adapter.ResolvedKind.int32_, adapter.resolveKind(&col));

    // Legacy converted type wins over bare physical
    col.converted_type = .uint_32;
    try std.testing.expectEqual(adapter.ResolvedKind.uint32_, adapter.resolveKind(&col));

    // Modern logical type wins over converted
    col.logical_type = .{ .integer = .{ .bit_width = 8, .is_signed = true } };
    try std.testing.expectEqual(adapter.ResolvedKind.int8_, adapter.resolveKind(&col));

    // date logical annotation now surfaces as date_ (6d-2a.1)
    col.converted_type = null;
    col.logical_type = .date;
    try std.testing.expectEqual(adapter.ResolvedKind.date_, adapter.resolveKind(&col));

    // time logical annotation -> time_ (6d-2a.2)
    col.converted_type = null;
    col.physical_type = .int64;
    col.logical_type = .{ .time = .{ .unit = .micros, .is_adjusted_to_utc = false } };
    try std.testing.expectEqual(adapter.ResolvedKind.time_, adapter.resolveKind(&col));

    // timestamp logical annotation -> timestamp_ (6d-2a.2)
    col.converted_type = null;
    col.physical_type = .int64;
    col.logical_type = .{ .timestamp = .{ .unit = .micros, .is_adjusted_to_utc = true } };
    try std.testing.expectEqual(adapter.ResolvedKind.timestamp_, adapter.resolveKind(&col));

    // Legacy converted time_millis -> time_ (6d-2a.2)
    col.logical_type = null;
    col.physical_type = .int32;
    col.converted_type = .time_millis;
    try std.testing.expectEqual(adapter.ResolvedKind.time_, adapter.resolveKind(&col));

    // Legacy converted timestamp_micros -> timestamp_ (6d-2a.2)
    col.logical_type = null;
    col.physical_type = .int64;
    col.converted_type = .timestamp_micros;
    try std.testing.expectEqual(adapter.ResolvedKind.timestamp_, adapter.resolveKind(&col));

    // Deferred logical types -> raw
    col.converted_type = null;
    col.physical_type = .byte_array;
    col.logical_type = .variant;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));
    col.logical_type = .geometry;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // INT96 physical -> timestamp_ (6d-2a.2: decoded via fromInt96Bytes)
    col.logical_type = null;
    col.physical_type = .int96;
    col.converted_type = null;
    try std.testing.expectEqual(adapter.ResolvedKind.timestamp_, adapter.resolveKind(&col));

    // byte_array + utf8 -> string (unchanged: annotation wins over physical default)
    col.logical_type = null;
    col.physical_type = .byte_array;
    col.converted_type = .utf8;
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));

    // bare byte_array (no annotation) -> binary_ (6d-2a.4)
    col.converted_type = null;
    col.logical_type = null;
    col.physical_type = .byte_array;
    try std.testing.expectEqual(adapter.ResolvedKind.binary_, adapter.resolveKind(&col));

    // bare fixed_len_byte_array (no annotation) -> fixedbytes_ (6d-2a.4)
    col.converted_type = null;
    col.logical_type = null;
    col.physical_type = .fixed_len_byte_array;
    try std.testing.expectEqual(adapter.ResolvedKind.fixedbytes_, adapter.resolveKind(&col));

    // converted .bson -> binary_
    col.logical_type = null;
    col.physical_type = .byte_array;
    col.converted_type = .bson;
    try std.testing.expectEqual(adapter.ResolvedKind.binary_, adapter.resolveKind(&col));

    // logical .bson -> binary_
    col.converted_type = null;
    col.physical_type = .byte_array;
    col.logical_type = .bson;
    try std.testing.expectEqual(adapter.ResolvedKind.binary_, adapter.resolveKind(&col));

    // converted .@"enum" -> string (legacy ENUM annotation preserved as String)
    col.logical_type = null;
    col.physical_type = .byte_array;
    col.converted_type = .@"enum";
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));

    // converted .json -> string (legacy JSON annotation preserved as String)
    col.converted_type = .json;
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));

    // logical decimal precision 10 -> decimal_
    col.converted_type = null;
    col.logical_type = .{ .decimal = .{ .precision = 10, .scale = 2 } };
    col.physical_type = .fixed_len_byte_array;
    col.precision = null;
    try std.testing.expectEqual(adapter.ResolvedKind.decimal_, adapter.resolveKind(&col));

    // logical decimal precision 77 -> raw (exceeds 76-digit cap)
    col.logical_type = .{ .decimal = .{ .precision = 77, .scale = 0 } };
    col.precision = null;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // converted .decimal with col.precision=9 -> decimal_
    col.logical_type = null;
    col.physical_type = .int32;
    col.converted_type = .decimal;
    col.precision = 9;
    try std.testing.expectEqual(adapter.ResolvedKind.decimal_, adapter.resolveKind(&col));

    // converted .decimal with col.precision=null -> raw (defensive: no field 8)
    col.logical_type = null;
    col.physical_type = .int32;
    col.converted_type = .decimal;
    col.precision = null;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // logical .uuid on FLBA -> uuid_ (6d-2a.5)
    col.converted_type = null;
    col.physical_type = .fixed_len_byte_array;
    col.logical_type = .uuid;
    try std.testing.expectEqual(adapter.ResolvedKind.uuid_, adapter.resolveKind(&col));

    // bare FLBA(16) WITHOUT uuid annotation stays fixedbytes_ (regression pin)
    col.logical_type = null;
    col.converted_type = null;
    col.physical_type = .fixed_len_byte_array;
    try std.testing.expectEqual(adapter.ResolvedKind.fixedbytes_, adapter.resolveKind(&col));

    // logical .float16 on FLBA -> float16_ (6d-2a.5)
    col.converted_type = null;
    col.physical_type = .fixed_len_byte_array;
    col.logical_type = .float16;
    try std.testing.expectEqual(adapter.ResolvedKind.float16_, adapter.resolveKind(&col));

    // converted .interval -> interval_ (6d-2a.5)
    col.logical_type = null;
    col.physical_type = .fixed_len_byte_array;
    col.converted_type = .interval;
    try std.testing.expectEqual(adapter.ResolvedKind.interval_, adapter.resolveKind(&col));
}

test "adapter: INT96 column decodes to Timestamp" {
    // INT96 now resolves to Timestamp (origin=int96, unit=nanos, utc=false).
    // The fixture was written with datetimes: 2021-01-01, 2021-06-15 12:30, 2022-03-03 03:03:03.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    const boxed = &df.series.items[0];
    try std.testing.expectEqualStrings("Timestamp", boxed.typeName());
    const s = boxed.timestamp;
    try std.testing.expectEqual(@as(usize, 3), s.len());

    // Row 0: 2021-01-01T00:00:00 (no time component)
    const ts0 = s.values.items[0];
    try std.testing.expectEqual(Timestamp.Origin.int96, ts0.origin);
    try std.testing.expect(!ts0.utc);
    try std.testing.expectEqual(parquet.TimeUnit.nanos, ts0.unit);
    const d0 = ts0.toDate().toCivil();
    try std.testing.expectEqual(@as(i32, 2021), d0.year);
    try std.testing.expectEqual(@as(u8, 1), d0.month);
    try std.testing.expectEqual(@as(u8, 1), d0.day);

    // Row 1: 2021-06-15T12:30:00 — verify hour
    const ts1 = s.values.items[1];
    try std.testing.expectEqual(@as(u8, 12), ts1.timeOfDay().hour());
}

test "adapter: logical_annotations.parquet still reads end-to-end" {
    // date -> Date (6d-2a.1); time -> Time (6d-2a.2); timestamp -> Timestamp (6d-2a.2);
    // decimal FLBA -> Decimal (6d-2a.3).
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 4), df.width());

    // Column 0: date32 -> Date (6d-2a.1)
    try std.testing.expectEqualStrings("Date", df.series.items[0].typeName());
    const d0 = df.series.items[0].date.values.items[0].toCivil();
    try std.testing.expectEqual(@as(i32, 2020), d0.year);
    try std.testing.expectEqual(@as(u8, 1), d0.month);
    try std.testing.expectEqual(@as(u8, 1), d0.day);
    const d1 = df.series.items[0].date.values.items[1].toCivil();
    try std.testing.expectEqual(@as(i32, 2021), d1.year);
    try std.testing.expectEqual(@as(u8, 6), d1.month);
    try std.testing.expectEqual(@as(u8, 15), d1.day);

    // Column 1: time64(us) -> Time (6d-2a.2); fixture: time(1,2,3) / time(4,5,6)
    // pyarrow writes time without tz -> is_adjusted_to_utc=false
    try std.testing.expectEqualStrings("Time", df.series.items[1].typeName());
    const t0 = df.series.items[1].time.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.micros, t0.unit);
    try std.testing.expect(!t0.utc);
    try std.testing.expectEqual(@as(u8, 1), t0.hour());
    try std.testing.expectEqual(@as(u8, 2), t0.minute());
    try std.testing.expectEqual(@as(u8, 3), t0.second());

    // Column 2: timestamp(us, tz="UTC") -> Timestamp (6d-2a.2)
    try std.testing.expectEqualStrings("Timestamp", df.series.items[2].typeName());
    const ts0 = df.series.items[2].timestamp.values.items[0];
    try std.testing.expect(ts0.utc);
    try std.testing.expectEqual(parquet.TimeUnit.micros, ts0.unit);
    const ts_date0 = ts0.toDate().toCivil();
    try std.testing.expectEqual(@as(i32, 2020), ts_date0.year);
    try std.testing.expectEqual(@as(u8, 1), ts_date0.month);
    try std.testing.expectEqual(@as(u8, 1), ts_date0.day);
    try std.testing.expectEqual(@as(u8, 12), ts0.timeOfDay().hour());

    // Column 3: decimal(10,2) FLBA -> Decimal (6d-2a.3)
    try std.testing.expectEqualStrings("Decimal", df.series.items[3].typeName());
    const dec0 = df.series.items[3].decimal.values.items[0];
    try std.testing.expectEqual(@as(u8, 10), dec0.precision);
    try std.testing.expectEqual(@as(i8, 2), dec0.scale);
    try std.testing.expectEqual(@as(i256, 1234567890), dec0.unscaled); // 12345678.90 * 100
    // Check format "12345678.90"
    var fmtbuf: [64]u8 = undefined;
    const fmted = try std.fmt.bufPrint(&fmtbuf, "{f}", .{dec0});
    try std.testing.expectEqualStrings("12345678.90", fmted);
}

test "adapter: Date column round-trips losslessly with both annotations" {
    // Full logical_annotations df (Date + Time + Timestamp + decimal-as-String)
    // written and read back; assert column 0 (Date) annotations and values.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // Column 0: Date annotations preserved on the wire
    try std.testing.expect(result2.columns[0].logical_type.? == .date);
    try std.testing.expectEqual(parquet.ConvertedType.date, result2.columns[0].converted_type.?);
    // Days preserved
    try std.testing.expectEqual(@as(i32, 18262), result2.columns[0].int32s.?[0]);
    try std.testing.expectEqual(@as(i32, 18793), result2.columns[0].int32s.?[1]);

    // Re-resolves at the dataframe layer — decimal column now writes as Decimal
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqual(@as(usize, 4), df2.width());
    try std.testing.expectEqualStrings("Date", df2.series.items[0].typeName());
    // Column 3 re-resolves to Decimal (no longer String)
    try std.testing.expectEqualStrings("Decimal", df2.series.items[3].typeName());
}

test "adapter: Time and Timestamp round-trip losslessly with annotations" {
    // logical_annotations → df → write (default opts) → read → assert wire
    // types + values + unit/utc for Time (col 1) and Timestamp (col 2).
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();

    // Capture original wire values before converting to df
    const orig_time_vals = result.columns[1].int64s.?;
    const orig_ts_vals = result.columns[2].int64s.?;
    const n_rows = result.columns[1].num_rows;

    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // --- Column 1: Time (time64 micros, utc=false) ---
    const tc = &result2.columns[1];
    try std.testing.expectEqual(parquet.PhysicalType.int64, tc.physical_type);
    const tlt = tc.logical_type orelse return error.MissingLogicalType;
    try std.testing.expectEqual(parquet.TimeUnit.micros, tlt.time.unit);
    try std.testing.expect(!tlt.time.is_adjusted_to_utc);
    try std.testing.expectEqual(parquet.ConvertedType.time_micros, tc.converted_type.?);
    // Wire int64 values identical to original
    for (0..n_rows) |i| {
        try std.testing.expectEqual(orig_time_vals[i], tc.int64s.?[i]);
    }

    // --- Column 2: Timestamp (timestamp micros, utc=true) ---
    const tsc = &result2.columns[2];
    try std.testing.expectEqual(parquet.PhysicalType.int64, tsc.physical_type);
    const tslt = tsc.logical_type orelse return error.MissingLogicalType;
    try std.testing.expectEqual(parquet.TimeUnit.micros, tslt.timestamp.unit);
    try std.testing.expect(tslt.timestamp.is_adjusted_to_utc);
    try std.testing.expectEqual(parquet.ConvertedType.timestamp_micros, tsc.converted_type.?);
    for (0..n_rows) |i| {
        try std.testing.expectEqual(orig_ts_vals[i], tsc.int64s.?[i]);
    }

    // Re-resolves to Time/Timestamp at the dataframe layer with same unit/utc
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Time", df2.series.items[1].typeName());
    const t0 = df2.series.items[1].time.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.micros, t0.unit);
    try std.testing.expect(!t0.utc);

    try std.testing.expectEqualStrings("Timestamp", df2.series.items[2].typeName());
    const ts0 = df2.series.items[2].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.micros, ts0.unit);
    try std.testing.expect(ts0.utc);
}

test "adapter: INT96 default write modernizes to INT64 TIMESTAMP(nanos)" {
    // int96.parquet → df → fromDataframe(.{}) → write → read:
    // physical int64, logical .timestamp{utc=false, nanos}, converted null;
    // values equal the Timestamps' nanos; re-resolves with origin=int64.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    // Capture nanos values from the Timestamp series
    const ts_series = df.series.items[0].timestamp;
    const n = ts_series.len();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    const col = &result2.columns[0];
    // Physical must be int64 (default modernization)
    try std.testing.expectEqual(parquet.PhysicalType.int64, col.physical_type);
    // Logical: TIMESTAMP(utc=false, nanos)
    const lt = col.logical_type orelse return error.MissingLogicalType;
    try std.testing.expectEqual(parquet.TimeUnit.nanos, lt.timestamp.unit);
    try std.testing.expect(!lt.timestamp.is_adjusted_to_utc);
    // No legacy converted type (non-utc nanos → logical only)
    try std.testing.expectEqual(@as(?parquet.ConvertedType, null), col.converted_type);
    // Values equal the timestamps' nanos values
    for (0..n) |i| {
        try std.testing.expectEqual(ts_series.values.items[i].value, col.int64s.?[i]);
    }

    // Re-reads with origin=int64
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Timestamp", df2.series.items[0].typeName());
    try std.testing.expectEqual(Timestamp.Origin.int64, df2.series.items[0].timestamp.values.items[0].origin);
}

test "adapter: emit_int96 re-emits INT96 bit-faithfully" {
    // int96.parquet → df → fromDataframe(.{.emit_int96=true}) → write → read:
    // physical int96; byte_arrays identical to original file's byte_arrays.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    // Read original to capture raw 12-byte arrays
    var orig = try parquet.readParquet(allocator, file_data);
    defer orig.deinit();
    const orig_arrays = orig.columns[0].byte_arrays orelse return error.NoByteArrays;
    const n = orig.columns[0].num_rows;

    var df = try adapter.toDataframe(allocator, &orig);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{ .emit_int96 = true });
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    const col = &result2.columns[0];
    try std.testing.expectEqual(parquet.PhysicalType.int96, col.physical_type);
    const arrays2 = col.byte_arrays orelse return error.NoByteArrays;
    try std.testing.expectEqual(n, arrays2.len);
    for (0..n) |i| {
        try std.testing.expectEqualSlices(u8, orig_arrays[i], arrays2[i]);
    }

    // Re-read df has origin=int96
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Timestamp", df2.series.items[0].typeName());
    try std.testing.expectEqual(Timestamp.Origin.int96, df2.series.items[0].timestamp.values.items[0].origin);
}

test "adapter: time_units.parquet full round-trip" {
    // read → df → write(default) → read → df2: all four columns re-resolve
    // with identical unit/utc/typeName; t_ms goes back out as INT32+time_millis.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/time_units.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // t_ms: INT32 + time_millis physical (millis -> int32 path)
    try std.testing.expectEqual(parquet.PhysicalType.int32, result2.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.time_millis, result2.columns[0].converted_type.?);

    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqual(@as(usize, 4), df2.width());

    // All four columns re-resolve to same typeName
    try std.testing.expectEqualStrings("Time", df2.series.items[0].typeName());
    try std.testing.expectEqualStrings("Timestamp", df2.series.items[1].typeName());
    try std.testing.expectEqualStrings("Timestamp", df2.series.items[2].typeName());
    try std.testing.expectEqualStrings("Timestamp", df2.series.items[3].typeName());

    // unit/utc preserved
    const t_ms2 = df2.series.items[0].time.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.millis, t_ms2.unit);
    try std.testing.expect(!t_ms2.utc);

    const ts_ms2 = df2.series.items[1].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.millis, ts_ms2.unit);
    try std.testing.expect(ts_ms2.utc);

    const ts_ns2 = df2.series.items[2].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.nanos, ts_ns2.unit);
    try std.testing.expect(ts_ns2.utc);

    const ts_local2 = df2.series.items[3].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.micros, ts_local2.unit);
    try std.testing.expect(!ts_local2.utc);
}

test "adapter: time_units.parquet unit/utc breadth" {
    // Fixture has: t_ms (time32 ms, INT32-backed), ts_ms (timestamp ms UTC),
    // ts_ns (timestamp ns UTC), ts_local (timestamp us, no tz -> utc=false).
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/time_units.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 4), df.width());

    // t_ms: time32(ms) -> Time, unit=millis (INT32-backed path), utc=false
    try std.testing.expectEqualStrings("Time", df.series.items[0].typeName());
    const t_ms = df.series.items[0].time.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.millis, t_ms.unit);
    try std.testing.expect(!t_ms.utc);
    try std.testing.expectEqual(@as(u8, 1), t_ms.hour()); // time(1,2,3)

    // ts_ms: timestamp("ms", tz="UTC") -> Timestamp, unit=millis, utc=true
    try std.testing.expectEqualStrings("Timestamp", df.series.items[1].typeName());
    const ts_ms = df.series.items[1].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.millis, ts_ms.unit);
    try std.testing.expect(ts_ms.utc);

    // ts_ns: timestamp("ns", tz="UTC") -> Timestamp, unit=nanos, utc=true
    try std.testing.expectEqualStrings("Timestamp", df.series.items[2].typeName());
    const ts_ns = df.series.items[2].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.nanos, ts_ns.unit);
    try std.testing.expect(ts_ns.utc);

    // ts_local: timestamp("us") no tz -> Timestamp, unit=micros, utc=false
    try std.testing.expectEqualStrings("Timestamp", df.series.items[3].typeName());
    const ts_local = df.series.items[3].timestamp.values.items[0];
    try std.testing.expectEqual(parquet.TimeUnit.micros, ts_local.unit);
    try std.testing.expect(!ts_local.utc);
}

test "adapter: decimals.parquet all three physicals" {
    // decimals.parquet: d9 (INT32, p=9, s=2), d18 (INT64, p=18, s=4), d38 (FLBA(16), p=38, s=10).
    // Both rows per column: positive + negative value.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/decimals.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 3), df.width());

    // d9: precision=9, scale=2; INT32-backed; 1234567.89 -> unscaled 123456789
    try std.testing.expectEqualStrings("Decimal", df.series.items[0].typeName());
    const d9_0 = df.series.items[0].decimal.values.items[0];
    try std.testing.expectEqual(@as(u8, 9), d9_0.precision);
    try std.testing.expectEqual(@as(i8, 2), d9_0.scale);
    try std.testing.expectEqual(@as(i256, 123456789), d9_0.unscaled);
    var buf9: [32]u8 = undefined;
    const fmt9 = try std.fmt.bufPrint(&buf9, "{f}", .{d9_0});
    try std.testing.expectEqualStrings("1234567.89", fmt9);
    // row 1: -0.01 -> unscaled -1
    const d9_1 = df.series.items[0].decimal.values.items[1];
    try std.testing.expectEqual(@as(i256, -1), d9_1.unscaled);
    var buf9b: [32]u8 = undefined;
    const fmt9b = try std.fmt.bufPrint(&buf9b, "{f}", .{d9_1});
    try std.testing.expectEqualStrings("-0.01", fmt9b);

    // d18: precision=18, scale=4; INT64-backed; 12345678901234.5678 -> unscaled 123456789012345678
    try std.testing.expectEqualStrings("Decimal", df.series.items[1].typeName());
    const d18_0 = df.series.items[1].decimal.values.items[0];
    try std.testing.expectEqual(@as(u8, 18), d18_0.precision);
    try std.testing.expectEqual(@as(i8, 4), d18_0.scale);
    try std.testing.expectEqual(@as(i256, 123456789012345678), d18_0.unscaled);
    // row 1: -1.0001 -> unscaled -10001
    const d18_1 = df.series.items[1].decimal.values.items[1];
    try std.testing.expectEqual(@as(i256, -10001), d18_1.unscaled);
    var buf18b: [32]u8 = undefined;
    const fmt18b = try std.fmt.bufPrint(&buf18b, "{f}", .{d18_1});
    try std.testing.expectEqualStrings("-1.0001", fmt18b);

    // d38: precision=38, scale=10; FLBA(16)-backed
    try std.testing.expectEqualStrings("Decimal", df.series.items[2].typeName());
    const d38_0 = df.series.items[2].decimal.values.items[0];
    try std.testing.expectEqual(@as(u8, 38), d38_0.precision);
    try std.testing.expectEqual(@as(i8, 10), d38_0.scale);
    try std.testing.expectEqual(@as(i256, 12345678901234567890123456780123456789), d38_0.unscaled);
    // row 1: -0.0000000001 -> unscaled -1
    const d38_1 = df.series.items[2].decimal.values.items[1];
    try std.testing.expectEqual(@as(i256, -1), d38_1.unscaled);
    var buf38b: [64]u8 = undefined;
    const fmt38b = try std.fmt.bufPrint(&buf38b, "{f}", .{d38_1});
    try std.testing.expectEqualStrings("-0.0000000001", fmt38b);
}

test "adapter: Decimal round-trips losslessly across physicals" {
    // decimals.parquet -> df -> write -> read: physicals int32/int64/FLBA(16)
    // respectively; unscaled values identical; scale/precision fields 7/8 +
    // logical annotation present on re-read; re-resolves to Decimal.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/decimals.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // d9: INT32 on wire, decimal annotation present, scale/precision fields
    try std.testing.expectEqual(parquet.PhysicalType.int32, result2.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.decimal, result2.columns[0].converted_type.?);
    try std.testing.expect(result2.columns[0].logical_type.? == .decimal);
    try std.testing.expectEqual(@as(i32, 2), result2.columns[0].scale.?);
    try std.testing.expectEqual(@as(i32, 9), result2.columns[0].precision.?);
    try std.testing.expectEqual(@as(i32, 123456789), result2.columns[0].int32s.?[0]);
    try std.testing.expectEqual(@as(i32, -1), result2.columns[0].int32s.?[1]);

    // d18: INT64 on wire
    try std.testing.expectEqual(parquet.PhysicalType.int64, result2.columns[1].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.decimal, result2.columns[1].converted_type.?);
    try std.testing.expect(result2.columns[1].logical_type.? == .decimal);
    try std.testing.expectEqual(@as(i32, 4), result2.columns[1].scale.?);
    try std.testing.expectEqual(@as(i32, 18), result2.columns[1].precision.?);
    try std.testing.expectEqual(@as(i64, 123456789012345678), result2.columns[1].int64s.?[0]);
    try std.testing.expectEqual(@as(i64, -10001), result2.columns[1].int64s.?[1]);

    // d38: FLBA(16) on wire
    try std.testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, result2.columns[2].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.decimal, result2.columns[2].converted_type.?);
    try std.testing.expect(result2.columns[2].logical_type.? == .decimal);
    try std.testing.expectEqual(@as(i32, 10), result2.columns[2].scale.?);
    try std.testing.expectEqual(@as(i32, 38), result2.columns[2].precision.?);
    try std.testing.expectEqual(@as(?i32, 16), result2.columns[2].type_length); // minBytes(38)=16

    // Re-resolve to Decimal with same precision/scale
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Decimal", df2.series.items[0].typeName());
    try std.testing.expectEqualStrings("Decimal", df2.series.items[1].typeName());
    try std.testing.expectEqualStrings("Decimal", df2.series.items[2].typeName());

    const d9_rt = df2.series.items[0].decimal.values.items[0];
    try std.testing.expectEqual(@as(i256, 123456789), d9_rt.unscaled);
    try std.testing.expectEqual(@as(u8, 9), d9_rt.precision);
    try std.testing.expectEqual(@as(i8, 2), d9_rt.scale);

    const d38_rt = df2.series.items[2].decimal.values.items[0];
    try std.testing.expectEqual(@as(i256, 12345678901234567890123456780123456789), d38_rt.unscaled);
    try std.testing.expectEqual(@as(u8, 38), d38_rt.precision);
    try std.testing.expectEqual(@as(i8, 10), d38_rt.scale);
}

test "adapter: binary_kinds.parquet reads as Binary" {
    // Unannotated BYTE_ARRAY -> Binary (6d-2a.4). Fixture written by gen_fixtures.py
    // with pa.binary(): rows b"\x00\x01\xff" and b"\xde\xad\xbe\xef".
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/binary_kinds.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 1), df.width());
    const boxed = &df.series.items[0];
    try std.testing.expectEqualStrings("Binary", boxed.typeName());

    const s = boxed.binary;
    try std.testing.expectEqual(@as(usize, 2), s.len());

    // Row 0: b"\x00\x01\xff"
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x01, 0xff }, s.values.items[0].toSlice());
    // Row 1: b"\xde\xad\xbe\xef"
    try std.testing.expectEqualSlices(u8, &.{ 0xde, 0xad, 0xbe, 0xef }, s.values.items[1].toSlice());

    // Plain binary: no annotations on meta
    try std.testing.expect(s.meta.converted_type == null);
    try std.testing.expect(s.meta.logical_type == null);
}

test "adapter: flba.parquet reads as FixedBytes with width 4" {
    // Unannotated FIXED_LEN_BYTE_ARRAY(4) -> FixedBytes (6d-2a.4).
    // Fixture: "fb" column with "abcd"/"efgh"/"ijkl".
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/flba.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 1), df.width());
    const boxed = &df.series.items[0];
    try std.testing.expectEqualStrings("FixedBytes", boxed.typeName());

    const s = boxed.fixed_bytes;
    try std.testing.expectEqual(@as(usize, 3), s.len());
    // Width captured from type_length on the parquet column
    try std.testing.expectEqual(@as(?i32, 4), s.meta.width);
    // Values
    try std.testing.expectEqualStrings("abcd", s.values.items[0].toSlice());
    try std.testing.expectEqualStrings("efgh", s.values.items[1].toSlice());
    try std.testing.expectEqualStrings("ijkl", s.values.items[2].toSlice());
}

test "adapter: binary_kinds round-trip (no annotations, bytes identical)" {
    // binary_kinds.parquet -> df -> write -> read: physical BYTE_ARRAY,
    // no converted/logical annotations, bytes identical; re-resolves Binary.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/binary_kinds.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // Physical BYTE_ARRAY, no annotations
    try std.testing.expectEqual(parquet.PhysicalType.byte_array, result2.columns[0].physical_type);
    try std.testing.expectEqual(@as(?parquet.ConvertedType, null), result2.columns[0].converted_type);
    try std.testing.expectEqual(@as(?parquet.LogicalType, null), result2.columns[0].logical_type);

    // Bytes preserved
    const vals = result2.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualSlices(u8, &.{ 0x00, 0x01, 0xff }, vals[0]);
    try std.testing.expectEqualSlices(u8, &.{ 0xde, 0xad, 0xbe, 0xef }, vals[1]);

    // Re-resolves to Binary
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Binary", df2.series.items[0].typeName());
}

test "adapter: flba round-trip (FLBA type_length 4, bytes identical)" {
    // flba.parquet -> df (FixedBytes width=4) -> write -> read: FLBA(4), bytes
    // identical; re-resolves FixedBytes with width 4.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/flba.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // Physical FIXED_LEN_BYTE_ARRAY with type_length=4
    try std.testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, result2.columns[0].physical_type);
    try std.testing.expectEqual(@as(?i32, 4), result2.columns[0].type_length);

    // Bytes preserved
    const vals = result2.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualStrings("abcd", vals[0]);
    try std.testing.expectEqualStrings("efgh", vals[1]);
    try std.testing.expectEqualStrings("ijkl", vals[2]);

    // Re-resolves to FixedBytes with width 4
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("FixedBytes", df2.series.items[0].typeName());
    try std.testing.expectEqual(@as(?i32, 4), df2.series.items[0].fixed_bytes.meta.width);
}

test "adapter: hand-built Binary(bson) round-trips annotations" {
    // Hand-built Binary column with BSON meta -> write -> read:
    // re-read carries converted_type=.bson AND logical_type=.bson AND resolves binary_.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(Binary);
    try s.rename("bson_col");
    s.meta = .{ .converted_type = .bson, .logical_type = .bson };
    try s.append(try Binary.fromSlice(allocator, &.{ 0x01, 0x02, 0x03 }));
    try s.append(try Binary.fromSlice(allocator, &.{ 0xAA, 0xBB }));

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // Annotations preserved on the wire
    const col = &result2.columns[0];
    try std.testing.expectEqual(parquet.PhysicalType.byte_array, col.physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.bson, col.converted_type.?);
    try std.testing.expect(col.logical_type.? == .bson);

    // Re-resolves to binary_
    try std.testing.expectEqual(adapter.ResolvedKind.binary_, adapter.resolveKind(col));

    // Re-read df carries the annotations
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Binary", df2.series.items[0].typeName());
    const s2 = df2.series.items[0].binary;
    try std.testing.expectEqual(parquet.ConvertedType.bson, s2.meta.converted_type.?);
    try std.testing.expect(s2.meta.logical_type.? == .bson);
}

test "adapter: uuid_f16.parquet reads typed columns" {
    // uuid_f16.parquet: col 0 "u" = FLBA(16)+UUID -> Uuid series;
    //                   col 1 "h" = FLBA(2)+FLOAT16 -> f16 series.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/uuid_f16.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());

    // Column 0: Uuid
    const u_boxed = &df.series.items[0];
    try std.testing.expectEqualStrings("Uuid", u_boxed.typeName());
    const u_series = u_boxed.uuid;
    try std.testing.expectEqual(@as(usize, 2), u_series.len());

    // Row 0: "01234567-89ab-cdef-0123-456789abcdef"
    var buf: [40]u8 = undefined;
    const row0_str = try std.fmt.bufPrint(&buf, "{f}", .{u_series.values.items[0]});
    try std.testing.expectEqualStrings("01234567-89ab-cdef-0123-456789abcdef", row0_str);

    // Column 1: f16
    const h_boxed = &df.series.items[1];
    try std.testing.expectEqualStrings("f16", h_boxed.typeName());
    const h_series = h_boxed.float16;
    try std.testing.expectEqual(@as(usize, 2), h_series.len());

    // Values: 1.5 and -0.25 are exactly representable in f16.
    try std.testing.expectEqual(@as(f16, 1.5), h_series.values.items[0]);
    try std.testing.expectEqual(@as(f16, -0.25), h_series.values.items[1]);
}

test "adapter: uuid_f16 round-trip lossless (bytes + annotation)" {
    // uuid_f16.parquet -> df -> write -> read:
    // col 0: FLBA(16) + logical uuid preserved; bytes identical; re-resolves Uuid.
    // col 1: FLBA(2) + logical float16 preserved; values bit-exact; re-resolves f16.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/uuid_f16.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();

    // Capture original uuid bytes for comparison
    const orig_uuid_arrays = result.columns[0].byte_arrays orelse return error.NoByteArrays;
    const orig_h_arrays = result.columns[1].byte_arrays orelse return error.NoByteArrays;

    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // --- UUID column (col 0) ---
    const uc = &result2.columns[0];
    try std.testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, uc.physical_type);
    try std.testing.expectEqual(@as(?i32, 16), uc.type_length);
    try std.testing.expect(uc.logical_type != null);
    try std.testing.expect(uc.logical_type.? == .uuid);
    // Bytes identical
    const u_arrays2 = uc.byte_arrays orelse return error.NoByteArrays;
    for (0..2) |i| {
        try std.testing.expectEqualSlices(u8, orig_uuid_arrays[i], u_arrays2[i]);
    }
    // Re-resolves to uuid_
    try std.testing.expectEqual(adapter.ResolvedKind.uuid_, adapter.resolveKind(uc));

    // --- float16 column (col 1) ---
    const hc = &result2.columns[1];
    try std.testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, hc.physical_type);
    try std.testing.expectEqual(@as(?i32, 2), hc.type_length);
    try std.testing.expect(hc.logical_type != null);
    try std.testing.expect(hc.logical_type.? == .float16);
    // Bytes bit-exact
    const h_arrays2 = hc.byte_arrays orelse return error.NoByteArrays;
    for (0..2) |i| {
        try std.testing.expectEqualSlices(u8, orig_h_arrays[i], h_arrays2[i]);
    }
    // Re-resolves to float16_
    try std.testing.expectEqual(adapter.ResolvedKind.float16_, adapter.resolveKind(hc));

    // Re-read df has typed columns
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Uuid", df2.series.items[0].typeName());
    try std.testing.expectEqualStrings("f16", df2.series.items[1].typeName());

    // f16 values identical
    try std.testing.expectEqual(@as(f16, 1.5), df2.series.items[1].float16.values.items[0]);
    try std.testing.expectEqual(@as(f16, -0.25), df2.series.items[1].float16.values.items[1]);
}

test "adapter: f16 NaN payload round-trips bit-exactly" {
    // The f16 wire codec is a pure bitcast, so even non-canonical NaN payloads
    // must survive write -> read unchanged (pins against any future
    // canonicalization sneaking into the path).
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");
    const nan_payload: u16 = 0x7E01; // qNaN with a non-zero payload bit

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();
    var s = try df.createSeries(f16);
    try s.rename("h");
    try s.append(@bitCast(nan_payload));
    try s.append(1.5);

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();

    const v0: u16 = @bitCast(df2.series.items[0].float16.values.items[0]);
    try std.testing.expectEqual(nan_payload, v0);
    try std.testing.expectEqual(@as(f16, 1.5), df2.series.items[0].float16.values.items[1]);
}

test "adapter: Interval hand-built teddy-to-teddy round-trip" {
    // Hand-build a df with Series(Interval), write, read back: FLBA(12) +
    // converted_type=.interval on wire; components identical after re-read;
    // re-resolves Interval.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(Interval);
    try s.rename("dur");
    try s.append(.{ .months = 1, .days = 2, .millis = 3000 });
    try s.append(.{ .months = 13, .days = 0, .millis = 1 });

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();

    // Wire: FLBA(12) + converted_type=.interval; NO logical type (none exists).
    const col = &result2.columns[0];
    try std.testing.expectEqual(parquet.PhysicalType.fixed_len_byte_array, col.physical_type);
    try std.testing.expectEqual(@as(?i32, 12), col.type_length);
    try std.testing.expectEqual(parquet.ConvertedType.interval, col.converted_type.?);
    try std.testing.expectEqual(@as(?parquet.LogicalType, null), col.logical_type);

    // Re-resolves to interval_
    try std.testing.expectEqual(adapter.ResolvedKind.interval_, adapter.resolveKind(col));

    // Re-read df: components identical
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Interval", df2.series.items[0].typeName());
    const iv0 = df2.series.items[0].interval.values.items[0];
    try std.testing.expectEqual(@as(u32, 1), iv0.months);
    try std.testing.expectEqual(@as(u32, 2), iv0.days);
    try std.testing.expectEqual(@as(u32, 3000), iv0.millis);
    const iv1 = df2.series.items[0].interval.values.items[1];
    try std.testing.expectEqual(@as(u32, 13), iv1.months);
    try std.testing.expectEqual(@as(u32, 0), iv1.days);
    try std.testing.expectEqual(@as(u32, 1), iv1.millis);
}

test "adapter: multi_rowgroup OPTIONAL column reads nulls (Phase 10a Unit B)" {
    // The "opt" column (int64, OPTIONAL) has nulls at rows 1,3,6. Before 10a
    // the reader materialized those slots as 0; now validity[i]==false ->
    // appendNull, so isNull reflects the def levels and non-null values are
    // exact (10/30/50/60).
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/multi_rowgroup.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    const opt = df.getSeries("opt") orelse return error.ColumnNotFound;
    try std.testing.expect(opt.* == .int64);
    try std.testing.expectEqual(@as(usize, 7), opt.len());

    // isNull pattern {f,t,f,t,f,f,t}
    const expected_null = [_]bool{ false, true, false, true, false, false, true };
    for (expected_null, 0..) |want, i| {
        try std.testing.expectEqual(want, opt.isNull(i));
    }
    // Non-null values are exact.
    try std.testing.expectEqual(@as(i64, 10), opt.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 30), opt.int64.values.items[2]);
    try std.testing.expectEqual(@as(i64, 50), opt.int64.values.items[4]);
    try std.testing.expectEqual(@as(i64, 60), opt.int64.values.items[5]);
}

test "nulls: df round-trips through parquet with definition levels (10b)" {
    // Six column types (incl. an owning type, two scratch-buffer types, and
    // a Timestamp) with a null in row 1 — write must emit OPTIONAL columns
    // with RLE definition levels, read must restore the exact isNull pattern.
    const dataframe_mod = @import("dataframe.zig");
    const Date = @import("date.zig").Date;
    const allocator = std.testing.allocator;

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var c_i64 = try df.createSeries(i64);
    try c_i64.rename("n");
    try c_i64.append(1);
    try c_i64.appendNull();

    var c_str = try df.createSeries(@import("strings.zig").String);
    try c_str.rename("s");
    try c_str.append(try @import("strings.zig").String.fromSlice(allocator, "a"));
    try c_str.appendNull();

    var c_date = try df.createSeries(Date);
    try c_date.rename("d");
    try c_date.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));
    try c_date.appendNull();

    var c_dec = try df.createSeries(Decimal);
    try c_dec.rename("dec");
    try c_dec.append(.{ .unscaled = 123, .precision = 9, .scale = 2 });
    try c_dec.appendNull();

    var c_bin = try df.createSeries(Binary);
    try c_bin.rename("b");
    try c_bin.append(try Binary.fromSlice(allocator, "ab"));
    try c_bin.appendNull();

    var c_ts = try df.createSeries(Timestamp);
    try c_ts.rename("ts");
    try c_ts.append(.{ .value = 1_000_000, .unit = .micros, .utc = true, .origin = .int64 });
    try c_ts.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();

    const names = [_][]const u8{ "n", "s", "d", "dec", "b", "ts" };
    const types = [_][]const u8{ "i64", "String", "Date", "Decimal", "Binary", "Timestamp" };
    for (names, types, 0..) |name, tn, k| {
        const col = &df2.series.items[k];
        try std.testing.expectEqualStrings(name, col.name());
        try std.testing.expectEqualStrings(tn, col.typeName());
        try std.testing.expect(!col.isNull(0));
        try std.testing.expect(col.isNull(1));
    }
    // Spot-check the non-null values survived.
    try std.testing.expectEqual(@as(i64, 1), df2.series.items[0].int64.values.items[0]);
    try std.testing.expectEqualStrings("a", df2.series.items[1].string.values.items[0].toSlice());
    try std.testing.expectEqual(@as(i32, 18262), df2.series.items[2].date.values.items[0].days);
    try std.testing.expectEqual(@as(i256, 123), df2.series.items[3].decimal.values.items[0].unscaled);
    try std.testing.expectEqualStrings("ab", df2.series.items[4].binary.values.items[0].toSlice());
    try std.testing.expectEqual(@as(i64, 1_000_000), df2.series.items[5].timestamp.values.items[0].value);
}

test "nulls: all-null column round-trips through parquet (10b)" {
    // Degenerate OPTIONAL column: every def level is 0, zero values follow.
    const dataframe_mod = @import("dataframe.zig");
    const allocator = std.testing.allocator;

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();
    var c = try df.createSeries(i64);
    try c.rename("all_null");
    try c.appendNull();
    try c.appendNull();
    try c.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();

    const col = &df2.series.items[0];
    try std.testing.expectEqual(@as(usize, 3), col.len());
    for (0..3) |i| {
        try std.testing.expect(col.isNull(i));
    }
}

test "nulls: multi_rowgroup opt column write round-trip preserves nulls (10b)" {
    // Fixture nulls -> df -> teddy write -> re-read: the isNull pattern must
    // survive the full circle now that definition levels are written.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/multi_rowgroup.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();

    const opt = df2.getSeries("opt") orelse return error.ColumnNotFound;
    const expected_null = [_]bool{ false, true, false, true, false, false, true };
    for (expected_null, 0..) |want, i| {
        try std.testing.expectEqual(want, opt.isNull(i));
    }
    try std.testing.expectEqual(@as(i64, 10), opt.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 60), opt.int64.values.items[5]);
}

// ============================================================
// Nested columns (Phase 6d-2b.2): df-level surfacing + assembly.
// ============================================================

/// Render row `i` of a nested column to a stack buffer for assertion.
fn nestedRowString(boxed: *BoxedSeries, i: usize, buf: []u8) ![]u8 {
    var s = try boxed.asStringAt(i);
    defer s.deinit();
    @memcpy(buf[0..s.toSlice().len], s.toSlice());
    return buf[0..s.toSlice().len];
}

test "nested: nested_smoke.parquet surfaces l + s as Nested columns" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_smoke.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    // 4 columns: flat_before, flat_after (flat), then l, s (nested, appended).
    try std.testing.expectEqual(@as(usize, 4), df.width());

    const l = df.getSeries("l") orelse return error.ColumnNotFound;
    const s = df.getSeries("s") orelse return error.ColumnNotFound;

    var buf: [128]u8 = undefined;
    // l: [[1,2], None, []]
    try std.testing.expectEqualStrings("[1, 2]", try nestedRowString(l, 0, &buf));
    try std.testing.expect(l.isNull(1)); // null list -> appendNull
    try std.testing.expectEqualStrings("[]", try nestedRowString(l, 2, &buf));
    // s: [{1,"x"}, {2,null}, None]
    try std.testing.expectEqualStrings("{1, \"x\"}", try nestedRowString(s, 0, &buf));
    try std.testing.expectEqualStrings("{2, null}", try nestedRowString(s, 1, &buf));
    try std.testing.expect(s.isNull(2)); // null struct -> appendNull
}

test "nested: nested_kinds.parquet per-row rendering + accessors" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_kinds.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    // 5 nested columns: l, ls, sl, ll, m.
    try std.testing.expectEqual(@as(usize, 5), df.width());

    var buf: [256]u8 = undefined;

    // l  list<int64>: [[1,2], None, [], [3]]
    const l = df.getSeries("l") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("[1, 2]", try nestedRowString(l, 0, &buf));
    try std.testing.expect(l.isNull(1));
    try std.testing.expectEqualStrings("[]", try nestedRowString(l, 2, &buf));
    try std.testing.expectEqualStrings("[3]", try nestedRowString(l, 3, &buf));

    // ls list<struct{a,b}>: [[{1,"x"},{2,None}], [], None, [{3,"z"}]]
    const ls = df.getSeries("ls") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("[{1, \"x\"}, {2, null}]", try nestedRowString(ls, 0, &buf));
    try std.testing.expectEqualStrings("[]", try nestedRowString(ls, 1, &buf));
    try std.testing.expect(ls.isNull(2));
    try std.testing.expectEqualStrings("[{3, \"z\"}]", try nestedRowString(ls, 3, &buf));
    // accessors: row0 listLen 2, structAt(0).a == 1
    const ls0 = &ls.nested.values.items[0];
    try std.testing.expectEqual(@as(usize, 2), try ls0.listLen());
    const ls0_s0 = try ls0.listAt(0);
    try std.testing.expectEqual(@as(i64, 1), (try ls0_s0.structAt(0)).int);

    // sl struct{v:list<int64>, w:string}: [{[1],"p"}, {None,"q"}, {[],"r"}, None]
    const sl = df.getSeries("sl") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("{[1], \"p\"}", try nestedRowString(sl, 0, &buf));
    try std.testing.expectEqualStrings("{null, \"q\"}", try nestedRowString(sl, 1, &buf));
    try std.testing.expectEqualStrings("{[], \"r\"}", try nestedRowString(sl, 2, &buf));
    try std.testing.expect(sl.isNull(3));

    // ll list<list<int64>>: [[[1],[2,3]], [[]], None, [[4]]]
    const ll = df.getSeries("ll") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("[[1], [2, 3]]", try nestedRowString(ll, 0, &buf));
    try std.testing.expectEqualStrings("[[]]", try nestedRowString(ll, 1, &buf));
    try std.testing.expect(ll.isNull(2));
    try std.testing.expectEqualStrings("[[4]]", try nestedRowString(ll, 3, &buf));
    // accessors: row0 listAt(1).listLen == 2
    const ll0 = &ll.nested.values.items[0];
    const ll0_1 = try ll0.listAt(1);
    try std.testing.expectEqual(@as(usize, 2), try ll0_1.listLen());

    // m  map<string,int64>: [{"a":1,"b":2}, {}, None, {"c":None}]
    const m = df.getSeries("m") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("{\"a\": 1, \"b\": 2}", try nestedRowString(m, 0, &buf));
    try std.testing.expectEqualStrings("{}", try nestedRowString(m, 1, &buf));
    try std.testing.expect(m.isNull(2));
    try std.testing.expectEqualStrings("{\"c\": null}", try nestedRowString(m, 3, &buf));
    // accessors: row0 mapLen 2, mapAt(0) key "a" value 1
    const m0 = &m.nested.values.items[0];
    try std.testing.expectEqual(@as(usize, 2), try m0.mapLen());
    const e0 = try m0.mapAt(0);
    try std.testing.expectEqualStrings("a", e0.key.string.toSlice());
    try std.testing.expectEqual(@as(i64, 1), e0.value.int);
}

// ============================================================
// Nested WRITE round-trips (Phase 13.1): parquet → df → parquet → df.
// ============================================================

/// Read a parquet file → df → write → read → df2, asserting each named nested
/// column round-trips identically (typeName, num_rows, per-row isNull +
/// asStringAt). The HEADLINE proof that shredding inverts assembly on the wire.
fn assertNestedRoundTrip(allocator: std.mem.Allocator, path: []const u8, names: []const []const u8) !void {
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, path, allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();

    for (names) |name| {
        const a = df.getSeries(name) orelse return error.ColumnNotFound;
        const b = df2.getSeries(name) orelse return error.ColumnNotFound;
        try std.testing.expectEqualStrings(a.typeName(), b.typeName());
        try std.testing.expectEqualStrings("Nested", b.typeName());
        try std.testing.expectEqual(a.len(), b.len());
        for (0..a.len()) |i| {
            try std.testing.expectEqual(a.isNull(i), b.isNull(i));
            var sa = try a.asStringAt(i);
            defer sa.deinit();
            var sb = try b.asStringAt(i);
            defer sb.deinit();
            try std.testing.expectEqualStrings(sa.toSlice(), sb.toSlice());
        }
    }
}

test "nested write: nested_kinds.parquet round-trips identically" {
    try assertNestedRoundTrip(std.testing.allocator, "data/nested_kinds.parquet", &.{ "l", "ls", "sl", "ll", "m" });
}

test "nested write: nested_smoke.parquet (mixed flat+nested) round-trips" {
    // Proves flat + nested leaves coexist in one written file.
    const allocator = std.testing.allocator;
    try assertNestedRoundTrip(allocator, "data/nested_smoke.parquet", &.{ "l", "s" });

    // And the flat columns survive too.
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_smoke.parquet", allocator, .unlimited);
    defer allocator.free(file_data);
    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();
    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);
    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    const fb = df2.getSeries("flat_before") orelse return error.ColumnNotFound;
    const fa = df2.getSeries("flat_after") orelse return error.ColumnNotFound;
    try std.testing.expectEqual(@as(i64, 1), fb.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 3), fb.int64.values.items[2]);
    var s0 = try fa.asStringAt(0);
    defer s0.deinit();
    try std.testing.expectEqualStrings("p", s0.toSlice());
}

test "nested write: LIST group carries converted_type .list (conformance)" {
    // After writing, re-read the schema and verify the written LIST group is
    // spec-annotated (the read-side group-annotation fix → flatten emits it).
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_kinds.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();
    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    const tree = result2.schema_tree orelse return error.NoSchemaTree;
    // Find the top-level "l" child (list<int64>) and assert its group annotation.
    var found = false;
    for (tree.children) |*child| {
        if (std.mem.eql(u8, child.name, "l")) {
            const is_list = (child.converted == .list) or
                (child.logical != null and child.logical.? == .list);
            try std.testing.expect(is_list);
            found = true;
        }
    }
    try std.testing.expect(found);
}

test "nested write: a Series(Nested) without meta.schema errors NestedWriteRequiresSchema" {
    const allocator = std.testing.allocator;
    const dataframe = @import("dataframe.zig");
    const Nested = @import("nested.zig").Nested;

    var df = try dataframe.Dataframe.init(allocator);
    defer df.deinit();
    var s = try df.createSeries(Nested);
    try s.rename("bare");
    // A hand-built list value, but NO meta.schema set.
    var items = try allocator.alloc(Nested, 1);
    items[0] = .{ .int = 7 };
    try s.append(.{ .list = .{ .allocator = allocator, .items = items } });

    try std.testing.expectError(error.NestedWriteRequiresSchema, adapter.fromDataframe(allocator, df, .{}));
}

/// Read → toDataframe on possibly-corrupt bytes; both outcomes are acceptable
/// (clean error or a valid df). Must never panic, never leak. The malformed
/// battery in src/parquet/ is reader-only; this extends coverage through the
/// dataframe adapter (nested assembly) for the nested fixtures, which the
/// parquet module cannot import (dataframe → parquet, not the reverse).
fn expectNoPanicNested(allocator: std.mem.Allocator, data: []const u8) void {
    var result = parquet.readParquet(allocator, data) catch return;
    defer result.deinit();
    var df = adapter.toDataframe(allocator, &result) catch return;
    df.deinit();
}

// ============================================================
// Narrow integer round-trip tests (Phase 6d-2a write impl).
//
// For each type T:
//   build a hand-rolled Series(T) with boundary values + one null,
//   write to parquet, read back, convert to dataframe, assert:
//     - typeName() == expected type name
//     - values are identical (including boundary / full-range values)
//     - isNull pattern matches
//
// Critical assertions: u32 with a value > i32::MAX, u64 with a value
// > i64::MAX — these prove the bitcast reinterpretation round-trips.
// ============================================================

test "narrow int roundtrip: i8 (boundary values + null)" {
    // i8: -128, 127, null → INT32 physical + INT_8/integer{8,true} annotations.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(i8);
    try s.rename("c_i8");
    try s.append(-128);
    try s.append(127);
    try s.appendNull();
    try s.append(0);

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    // Wire: INT32 physical with both annotations
    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int32, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.int_8, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expectEqual(@as(i8, 8), lt.integer.bit_width);
    try std.testing.expect(lt.integer.is_signed);

    // Re-read as i8 dataframe column
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_i8") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("i8", col.typeName());
    try std.testing.expectEqual(@as(usize, 4), col.len());

    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(!col.isNull(1));
    try std.testing.expect(col.isNull(2));
    try std.testing.expect(!col.isNull(3));

    try std.testing.expectEqual(@as(i8, -128), col.int8.values.items[0]);
    try std.testing.expectEqual(@as(i8, 127), col.int8.values.items[1]);
    try std.testing.expectEqual(@as(i8, 0), col.int8.values.items[3]);
}

test "narrow int roundtrip: i16 (boundary values + null)" {
    // i16: -32768, 32767, null → INT32 + INT_16/integer{16,true}.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(i16);
    try s.rename("c_i16");
    try s.append(-32768);
    try s.append(32767);
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int32, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.int_16, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expectEqual(@as(i8, 16), lt.integer.bit_width);
    try std.testing.expect(lt.integer.is_signed);

    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_i16") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("i16", col.typeName());
    try std.testing.expectEqual(@as(i16, -32768), col.int16.values.items[0]);
    try std.testing.expectEqual(@as(i16, 32767), col.int16.values.items[1]);
    try std.testing.expect(col.isNull(2));
}

test "narrow int roundtrip: u8 (boundary values + null)" {
    // u8: 0, 255, null → INT32 + UINT_8/integer{8,false}.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(u8);
    try s.rename("c_u8");
    try s.append(0);
    try s.append(255);
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int32, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.uint_8, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expect(!lt.integer.is_signed);

    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_u8") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("u8", col.typeName());
    try std.testing.expectEqual(@as(u8, 0), col.uint8.values.items[0]);
    try std.testing.expectEqual(@as(u8, 255), col.uint8.values.items[1]);
    try std.testing.expect(col.isNull(2));
}

test "narrow int roundtrip: u16 (boundary values + null)" {
    // u16: 0, 65535, null → INT32 + UINT_16/integer{16,false}.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(u16);
    try s.rename("c_u16");
    try s.append(0);
    try s.append(65535);
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int32, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.uint_16, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expect(!lt.integer.is_signed);

    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_u16") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("u16", col.typeName());
    try std.testing.expectEqual(@as(u16, 0), col.uint16.values.items[0]);
    try std.testing.expectEqual(@as(u16, 65535), col.uint16.values.items[1]);
    try std.testing.expect(col.isNull(2));
}

test "narrow int roundtrip: u32 full-range (value > i32::MAX)" {
    // CRITICAL: 4_000_000_000 > i32::MAX (2_147_483_647). The bitcast path
    // must preserve the bit pattern so it round-trips intact. Without @bitCast
    // this would trap or wrap. Also tests a null.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(u32);
    try s.rename("c_u32");
    try s.append(0);
    try s.append(4_000_000_000); // > i32::MAX — the critical assertion
    try s.append(2_147_483_647); // i32::MAX — valid in both signed/unsigned
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int32, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.uint_32, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expectEqual(@as(i8, 32), lt.integer.bit_width);
    try std.testing.expect(!lt.integer.is_signed);

    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_u32") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("u32", col.typeName());
    try std.testing.expectEqual(@as(usize, 4), col.len());

    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(!col.isNull(1));
    try std.testing.expect(!col.isNull(2));
    try std.testing.expect(col.isNull(3));

    try std.testing.expectEqual(@as(u32, 0), col.uint32.values.items[0]);
    try std.testing.expectEqual(@as(u32, 4_000_000_000), col.uint32.values.items[1]); // critical
    try std.testing.expectEqual(@as(u32, 2_147_483_647), col.uint32.values.items[2]);
}

test "narrow int roundtrip: u64 full-range (value > i64::MAX)" {
    // CRITICAL: 18_000_000_000_000_000_000 > i64::MAX (9_223_372_036_854_775_807).
    // Bitcast path must preserve the bit pattern.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(u64);
    try s.rename("c_u64");
    try s.append(0);
    try s.append(18_000_000_000_000_000_000); // > i64::MAX — the critical assertion
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int64, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.uint_64, result.columns[0].converted_type.?);
    const lt = result.columns[0].logical_type orelse return error.MissingLogicalType;
    try std.testing.expect(lt == .integer);
    try std.testing.expectEqual(@as(i8, 64), lt.integer.bit_width);
    try std.testing.expect(!lt.integer.is_signed);

    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_u64") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("u64", col.typeName());
    try std.testing.expectEqual(@as(usize, 3), col.len());

    try std.testing.expect(!col.isNull(0));
    try std.testing.expect(!col.isNull(1));
    try std.testing.expect(col.isNull(2));

    try std.testing.expectEqual(@as(u64, 0), col.uint64.values.items[0]);
    try std.testing.expectEqual(@as(u64, 18_000_000_000_000_000_000), col.uint64.values.items[1]); // critical
}

test "narrow int roundtrip: isize → i64 on wire (no annotation)" {
    // isize is teddy-internal; maps to plain INT64 (no annotation).
    // On re-read comes back as i64. isize→i64 narrowing is documented.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(isize);
    try s.rename("c_isize");
    try s.append(-100);
    try s.append(9_000_000_000);
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    // Plain INT64 with no annotation (isize has no parquet identity)
    try std.testing.expectEqual(parquet.PhysicalType.int64, result.columns[0].physical_type);
    try std.testing.expectEqual(@as(?parquet.ConvertedType, null), result.columns[0].converted_type);
    try std.testing.expectEqual(@as(?parquet.LogicalType, null), result.columns[0].logical_type);

    // Re-reads as i64 (isize narrows to i64 on write)
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_isize") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("i64", col.typeName());
    try std.testing.expectEqual(@as(i64, -100), col.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 9_000_000_000), col.int64.values.items[1]);
    try std.testing.expect(col.isNull(2));
}

test "narrow int roundtrip: usize → u64 on wire (UINT_64 annotation)" {
    // usize maps to INT64 + UINT_64/integer{64,false}. On re-read comes back
    // as u64 (value-preserving on 64-bit targets). usize→u64 conversion is documented.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(usize);
    try s.rename("c_usize");
    try s.append(0);
    try s.append(18_000_000_000_000_000_000); // > i64::MAX — exercises bitcast
    try s.appendNull();

    var cols = try adapter.fromDataframe(allocator, df, .{});
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int64, result.columns[0].physical_type);
    try std.testing.expectEqual(parquet.ConvertedType.uint_64, result.columns[0].converted_type.?);

    // Re-reads as u64 (usize round-trips as u64)
    var df2 = try adapter.toDataframe(allocator, &result);
    defer df2.deinit();
    const col = df2.getSeries("c_usize") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("u64", col.typeName());
    try std.testing.expectEqual(@as(u64, 0), col.uint64.values.items[0]);
    try std.testing.expectEqual(@as(u64, 18_000_000_000_000_000_000), col.uint64.values.items[1]);
    try std.testing.expect(col.isNull(2));
}

test "narrow int roundtrip: i128 still returns UnsupportedType" {
    // Parquet has no 128-bit integer physical type. Confirm UnsupportedType.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(i128);
    try s.rename("c_i128");
    try s.append(42);

    try std.testing.expectError(error.UnsupportedType, adapter.fromDataframe(allocator, df, .{}));
}

test "narrow int roundtrip: u128 still returns UnsupportedType" {
    // Parquet has no 128-bit integer physical type. Confirm UnsupportedType.
    const allocator = std.testing.allocator;
    const dataframe_mod = @import("dataframe.zig");

    var df = try dataframe_mod.Dataframe.init(allocator);
    defer df.deinit();

    var s = try df.createSeries(u128);
    try s.rename("c_u128");
    try s.append(42);

    try std.testing.expectError(error.UnsupportedType, adapter.fromDataframe(allocator, df, .{}));
}

test "malformed: nested assembly never panics over corrupted nested fixtures" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const paths = [_][]const u8{ "data/nested_smoke.parquet", "data/nested_kinds.parquet" };
    for (paths) |path| {
        const data = try cwd.readFileAlloc(io, path, allocator, .unlimited);
        defer allocator.free(data);

        // Truncation sweep: ~16 strides.
        const stride = @max(1, data.len / 16);
        var n: usize = 0;
        while (n < data.len) : (n += stride) {
            expectNoPanicNested(allocator, data[0..n]);
        }

        // Single-byte bit-flip sweep: ~16 strides.
        const mutable = try allocator.dupe(u8, data);
        defer allocator.free(mutable);
        var off: usize = 0;
        while (off < data.len) : (off += stride) {
            mutable[off] ^= 0xFF;
            expectNoPanicNested(allocator, mutable);
            mutable[off] = data[off]; // restore
        }
    }
}
