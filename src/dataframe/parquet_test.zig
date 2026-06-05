const std = @import("std");
const parquet = @import("parquet");
const adapter = @import("parquet.zig");
const Raw = @import("raw.zig").Raw;

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

    // Not-yet-surfaced logical annotation falls through to the physical default
    col.converted_type = null;
    col.logical_type = .date;
    try std.testing.expectEqual(adapter.ResolvedKind.int32_, adapter.resolveKind(&col));

    // Deferred logical types -> raw
    col.physical_type = .byte_array;
    col.logical_type = .variant;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));
    col.logical_type = .geometry;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // INT96 physical -> raw
    col.logical_type = null;
    col.physical_type = .int96;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // byte_array + utf8 -> string (unchanged behavior)
    col.physical_type = .byte_array;
    col.converted_type = .utf8;
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));
}

test "adapter: INT96 column loads as Raw and round-trips bit-faithfully" {
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
    try std.testing.expectEqualStrings("Raw", boxed.typeName());
    const s = boxed.raw;
    try std.testing.expectEqual(parquet.PhysicalType.int96, s.meta.physical_type);
    try std.testing.expectEqual(@as(usize, 3), s.len());
    for (s.values.items) |r| {
        try std.testing.expectEqual(@as(usize, 12), r.bytes.len);
    }

    // teddy write -> teddy read -> identical bytes and physical type
    var cols = try adapter.fromDataframe(allocator, df);
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int96, result2.columns[0].physical_type);
    // Full circle: the re-read column must resolve to Raw at the dataframe
    // layer too (pins resolveKind, not just the parquet library).
    var df2 = try adapter.toDataframe(allocator, &result2);
    defer df2.deinit();
    try std.testing.expectEqualStrings("Raw", df2.series.items[0].typeName());
    const orig = result.columns[0].byte_arrays.?;
    const rt = result2.columns[0].byte_arrays.?;
    try std.testing.expectEqual(orig.len, rt.len);
    for (orig, rt) |a, b| {
        try std.testing.expectEqualSlices(u8, a, b);
    }
}

test "adapter: logical_annotations.parquet still reads end-to-end" {
    // date/time/timestamp/decimal aren't surfaced yet (slices .1-.5); this
    // pins that they keep resolving to their physical defaults, not Raw.
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
    try std.testing.expectEqualStrings("i32", df.series.items[0].typeName()); // date32 -> i32
    try std.testing.expectEqualStrings("i64", df.series.items[1].typeName()); // time64(us) -> i64
    try std.testing.expectEqualStrings("i64", df.series.items[2].typeName()); // timestamp(us) -> i64
    try std.testing.expectEqualStrings("String", df.series.items[3].typeName()); // decimal FLBA -> String (for now)
}
