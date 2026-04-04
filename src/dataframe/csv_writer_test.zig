const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const csv_writer = @import("csv_writer.zig");
const csv = @import("csv_reader.zig");
const series_mod = @import("series.zig");

test "csv_writer: round-trip integers" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col1 = try series_mod.Series(i64).init(allocator);
    try col1.rename("A");
    try col1.append(1);
    try col1.append(3);
    try df.addSeries(col1.toBoxedSeries());

    var col2 = try series_mod.Series(i64).init(allocator);
    try col2.rename("B");
    try col2.append(2);
    try col2.append(4);
    try df.addSeries(col2.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{});
    defer allocator.free(output);

    var df2 = try csv.parse(allocator, output, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "csv_writer: round-trip floats" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(f64).init(allocator);
    try col.rename("v");
    try col.append(-1.5);
    try col.append(0.0);
    try col.append(3.14);
    try df.addSeries(col.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{});
    defer allocator.free(output);

    var df2 = try csv.parse(allocator, output, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "csv_writer: round-trip strings with special characters" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello, world");
    try col.tryAppend("say \"hi\"");
    try col.tryAppend("normal");
    try df.addSeries(col.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{});
    defer allocator.free(output);

    var df2 = try csv.parse(allocator, output, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "csv_writer: round-trip multi-column mixed types" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var ids = try series_mod.Series(i64).init(allocator);
    try ids.rename("id");
    try ids.append(1);
    try ids.append(2);
    try ids.append(3);
    try df.addSeries(ids.toBoxedSeries());

    var scores = try series_mod.Series(f64).init(allocator);
    try scores.rename("score");
    try scores.append(9.5);
    try scores.append(7.0);
    try scores.append(8.25);
    try df.addSeries(scores.toBoxedSeries());

    var names = try series_mod.Series(String).init(allocator);
    try names.rename("name");
    try names.tryAppend("alice");
    try names.tryAppend("bob");
    try names.tryAppend("carol");
    try df.addSeries(names.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{});
    defer allocator.free(output);

    var df2 = try csv.parse(allocator, output, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "csv_writer: round-trip tab delimiter" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i64).init(allocator);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try df.addSeries(col.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{ .delimiter = '\t' });
    defer allocator.free(output);

    var df2 = try csv.parse(allocator, output, .{ .delimiter = '\t' });
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "csv_writer: quoting fields with commas" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello, world");
    try col.tryAppend("normal");
    try df.addSeries(col.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{});
    defer allocator.free(output);

    try std.testing.expect(std.mem.indexOf(u8, output, "\"hello, world\"") != null);
}

test "csv_writer: no header option" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i32).init(allocator);
    try col.rename("x");
    try col.append(42);
    try df.addSeries(col.toBoxedSeries());

    const output = try csv_writer.writeToString(allocator, df, .{ .include_header = false });
    defer allocator.free(output);

    try std.testing.expectEqualStrings("42\n", output);
}
