const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const Writer = @import("writer.zig").Writer;
const parquet = @import("parquet");

test "writer: CSV toString" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.csv);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expect(output.len > 0);
}

test "writer: JSON toString rows" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.rows);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("[{\"x\":1}]", output);
}

test "writer: JSON toString columns" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.columns);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("{\"x\":[1,2]}", output);
}

test "writer: Parquet toString and read back" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i64).init(allocator);
    try col.rename("val");
    try col.append(10);
    try col.append(20);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.parquet);

    const output = try w.toString(df);
    defer allocator.free(output);

    // Read it back
    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.num_rows);
    try std.testing.expectEqual(@as(i64, 10), result.columns[0].int64s.?[0]);
    try std.testing.expectEqual(@as(i64, 20), result.columns[0].int64s.?[1]);
}

test "writer: builder pattern chaining" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.csv).withDelimiter(';').withHeader(false);
    try std.testing.expectEqual(@as(u8, ';'), w.delimiter);
    try std.testing.expectEqual(false, w.include_header);
}
