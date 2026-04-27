const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const json_writer = @import("json_writer.zig");
const json_reader = @import("json_reader.zig");
const series_mod = @import("series.zig");

test "json_writer: rows format" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    const output = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("[{\"x\":1},{\"x\":2}]", output);
}

test "json_writer: columns format" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    const output = try json_writer.writeToString(allocator, df, .columns);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("{\"x\":[1,2]}", output);
}

test "json_writer: string values are quoted" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello");
    try df.addSeries(col.toBoxedSeries());

    const output = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("[{\"name\":\"hello\"}]", output);
}

test "json_writer: empty dataframe" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const rows_output = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(rows_output);
    try std.testing.expectEqualStrings("[]", rows_output);

    const cols_output = try json_writer.writeToString(allocator, df, .columns);
    defer allocator.free(cols_output);
    try std.testing.expectEqualStrings("{}", cols_output);
}

test "json_writer: round-trip rows integers" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i64).init(allocator);
    try col.rename("n");
    try col.append(-10);
    try col.append(0);
    try col.append(42);
    try df.addSeries(col.toBoxedSeries());

    const json = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: round-trip columns integers" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i64).init(allocator);
    try col.rename("n");
    try col.append(-10);
    try col.append(0);
    try col.append(42);
    try df.addSeries(col.toBoxedSeries());

    const json = try json_writer.writeToString(allocator, df, .columns);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: round-trip rows floats" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(f64).init(allocator);
    try col.rename("v");
    try col.append(-1.5);
    try col.append(0.0);
    try col.append(3.14);
    try df.addSeries(col.toBoxedSeries());

    const json = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: round-trip rows strings" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello");
    try col.tryAppend("world");
    try col.tryAppend("with \"quotes\"");
    try df.addSeries(col.toBoxedSeries());

    const json = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: round-trip multi-column rows" {
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

    const json = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: ndjson basic" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    const output = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("{\"x\":1}\n{\"x\":2}", output);
}

test "json_writer: ndjson empty dataframe" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const output = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("", output);
}

test "json_writer: ndjson round-trip integers" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(i64).init(allocator);
    try col.rename("n");
    try col.append(-10);
    try col.append(0);
    try col.append(42);
    try df.addSeries(col.toBoxedSeries());

    const out = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(out);

    var df2 = try json_reader.parse(allocator, out, .{ .format = .ndjson });
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: ndjson round-trip floats" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(f64).init(allocator);
    try col.rename("v");
    try col.append(-1.5);
    try col.append(0.0);
    try col.append(3.14);
    try df.addSeries(col.toBoxedSeries());

    const out = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(out);

    var df2 = try json_reader.parse(allocator, out, .{ .format = .ndjson });
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: ndjson round-trip strings" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello");
    try col.tryAppend("world");
    try col.tryAppend("with \"quotes\"");
    try df.addSeries(col.toBoxedSeries());

    const out = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(out);

    var df2 = try json_reader.parse(allocator, out, .{ .format = .ndjson });
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: ndjson round-trip multi-column" {
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

    const out = try json_writer.writeToString(allocator, df, .ndjson);
    defer allocator.free(out);

    var df2 = try json_reader.parse(allocator, out, .{ .format = .ndjson });
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}

test "json_writer: round-trip multi-column columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var ids = try series_mod.Series(i64).init(allocator);
    try ids.rename("id");
    try ids.append(1);
    try ids.append(2);
    try df.addSeries(ids.toBoxedSeries());

    var values = try series_mod.Series(f64).init(allocator);
    try values.rename("val");
    try values.append(1.1);
    try values.append(2.2);
    try df.addSeries(values.toBoxedSeries());

    const json = try json_writer.writeToString(allocator, df, .columns);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}
