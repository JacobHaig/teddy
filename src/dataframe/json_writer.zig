const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;

pub const JsonFormat = enum { rows, columns };

/// Write a Dataframe to JSON format, returns an owned slice.
/// Caller must free the returned slice with allocator.free().
pub fn writeToString(allocator: Allocator, df: *Dataframe, format: JsonFormat) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);

    switch (format) {
        .rows => try writeRows(&buf, allocator, df),
        .columns => try writeColumns(&buf, allocator, df),
    }

    return buf.toOwnedSlice(allocator);
}

fn writeRows(buf: *std.ArrayList(u8), allocator: Allocator, df: *Dataframe) !void {
    const w = df.width();
    const h = df.height();

    try buf.append(allocator, '[');
    for (0..h) |row| {
        if (row > 0) try buf.append(allocator, ',');
        try buf.append(allocator, '{');
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, ',');
            // Write key
            try appendJsonString(buf, allocator, df.series.items[col].name());
            try buf.append(allocator, ':');
            // Write value
            try appendJsonValue(buf, allocator, &df.series.items[col], row);
        }
        try buf.append(allocator, '}');
    }
    try buf.append(allocator, ']');
}

fn writeColumns(buf: *std.ArrayList(u8), allocator: Allocator, df: *Dataframe) !void {
    const w = df.width();
    const h = df.height();

    try buf.append(allocator, '{');
    for (0..w) |col| {
        if (col > 0) try buf.append(allocator, ',');
        try appendJsonString(buf, allocator, df.series.items[col].name());
        try buf.append(allocator, ':');
        try buf.append(allocator, '[');
        for (0..h) |row| {
            if (row > 0) try buf.append(allocator, ',');
            try appendJsonValue(buf, allocator, &df.series.items[col], row);
        }
        try buf.append(allocator, ']');
    }
    try buf.append(allocator, '}');
}

fn appendJsonValue(buf: *std.ArrayList(u8), allocator: Allocator, series: *BoxedSeries, row: usize) !void {
    if (isStringSeries(series)) {
        var str = try series.asStringAt(row);
        defer str.deinit();
        try appendJsonString(buf, allocator, str.toSlice());
    } else {
        var str = try series.asStringAt(row);
        defer str.deinit();
        try buf.appendSlice(allocator, str.toSlice());
    }
}

fn isStringSeries(series: *BoxedSeries) bool {
    return switch (series.*) {
        .string => true,
        .bool => true,
        else => false,
    };
}

fn appendJsonString(buf: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => try buf.append(allocator, c),
        }
    }
    try buf.append(allocator, '"');
}

// --- Tests ---

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

    const output = try writeToString(allocator, df, .rows);
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

    const output = try writeToString(allocator, df, .columns);
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

    const output = try writeToString(allocator, df, .rows);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("[{\"name\":\"hello\"}]", output);
}

test "json_writer: empty dataframe" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const rows_output = try writeToString(allocator, df, .rows);
    defer allocator.free(rows_output);
    try std.testing.expectEqualStrings("[]", rows_output);

    const cols_output = try writeToString(allocator, df, .columns);
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

    const json = try writeToString(allocator, df, .rows);
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

    const json = try writeToString(allocator, df, .columns);
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

    const json = try writeToString(allocator, df, .rows);
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

    const json = try writeToString(allocator, df, .rows);
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

    const json = try writeToString(allocator, df, .rows);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
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

    const json = try writeToString(allocator, df, .columns);
    defer allocator.free(json);

    var df2 = try json_reader.parse(allocator, json, .{});
    defer df2.deinit();

    try std.testing.expect(try df.compareDataframe(df2));
}
