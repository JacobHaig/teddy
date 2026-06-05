const std = @import("std");
const parquet = @import("parquet");
const adapter = @import("parquet.zig");
const Raw = @import("raw.zig").Raw;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;

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

    // byte_array + utf8 -> string (unchanged behavior)
    col.physical_type = .byte_array;
    col.converted_type = .utf8;
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));
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
    // decimal FLBA still resolves to String.
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

    // Column 3: decimal FLBA -> String (for now)
    try std.testing.expectEqualStrings("String", df.series.items[3].typeName());
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

    // Re-resolves at the dataframe layer
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqual(@as(usize, 4), df2.width());
    try std.testing.expectEqualStrings("Date", df2.series.items[0].typeName());
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
