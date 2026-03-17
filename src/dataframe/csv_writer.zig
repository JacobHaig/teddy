const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;

pub const WriteOptions = struct {
    delimiter: u8 = ',',
    include_header: bool = true,
};

/// Write a Dataframe to CSV format, returns an owned slice.
/// Caller must free the returned slice with allocator.free().
pub fn writeToString(allocator: Allocator, df: *Dataframe, options: WriteOptions) ![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);

    const w = df.width();
    const h = df.height();

    if (options.include_header) {
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, options.delimiter);
            try appendCsvField(&buf, allocator, df.series.items[col].name(), options.delimiter);
        }
        try buf.append(allocator, '\n');
    }

    for (0..h) |row| {
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, options.delimiter);
            var str = try df.series.items[col].asStringAt(row);
            defer str.deinit();
            try appendCsvField(&buf, allocator, str.toSlice(), options.delimiter);
        }
        try buf.append(allocator, '\n');
    }

    return buf.toOwnedSlice(allocator);
}

fn appendCsvField(buf: *std.ArrayList(u8), allocator: Allocator, field: []const u8, delimiter: u8) !void {
    var needs_quoting = false;
    for (field) |c| {
        if (c == delimiter or c == '"' or c == '\n' or c == '\r') {
            needs_quoting = true;
            break;
        }
    }

    if (needs_quoting) {
        try buf.append(allocator, '"');
        for (field) |c| {
            if (c == '"') try buf.append(allocator, '"');
            try buf.append(allocator, c);
        }
        try buf.append(allocator, '"');
    } else {
        try buf.appendSlice(allocator, field);
    }
}

// --- Tests ---

const csv = @import("csv.zig");

test "csv_writer: round-trip basic CSV" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const series_mod = @import("series.zig");
    var col1 = try series_mod.Series(i32).init(allocator);
    try col1.rename("A");
    try col1.append(1);
    try col1.append(3);
    try df.addSeries(col1.toBoxedSeries());

    var col2 = try series_mod.Series(i32).init(allocator);
    try col2.rename("B");
    try col2.append(2);
    try col2.append(4);
    try df.addSeries(col2.toBoxedSeries());

    const output = try writeToString(allocator, df, .{});
    defer allocator.free(output);

    // Parse it back
    var df2 = try csv.parse(allocator, output, .{});
    defer df2.deinit();

    try std.testing.expectEqual(@as(usize, 2), df2.width());
    try std.testing.expectEqual(@as(usize, 2), df2.height());
}

test "csv_writer: quoting fields with commas" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const series_mod = @import("series.zig");
    var col = try series_mod.Series(String).init(allocator);
    try col.rename("name");
    try col.tryAppend("hello, world");
    try col.tryAppend("normal");
    try df.addSeries(col.toBoxedSeries());

    const output = try writeToString(allocator, df, .{});
    defer allocator.free(output);

    // Should contain quoted field
    try std.testing.expect(std.mem.indexOf(u8, output, "\"hello, world\"") != null);
}

test "csv_writer: no header option" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    const series_mod = @import("series.zig");
    var col = try series_mod.Series(i32).init(allocator);
    try col.rename("x");
    try col.append(42);
    try df.addSeries(col.toBoxedSeries());

    const output = try writeToString(allocator, df, .{ .include_header = false });
    defer allocator.free(output);

    try std.testing.expectEqualStrings("42\n", output);
}
