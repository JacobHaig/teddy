const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const writeNestedJson = @import("nested_json.zig").writeNestedJson;

pub const JsonFormat = enum { rows, columns, ndjson };

/// Write a Dataframe to JSON format, returns an owned slice.
/// Caller must free the returned slice with allocator.free().
pub fn writeToString(allocator: Allocator, df: *Dataframe, format: JsonFormat) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);

    switch (format) {
        .rows => try writeRows(&buf, allocator, df),
        .columns => try writeColumns(&buf, allocator, df),
        .ndjson => try writeNdjson(&buf, allocator, df),
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

fn writeNdjson(buf: *std.ArrayList(u8), allocator: Allocator, df: *Dataframe) !void {
    const w = df.width();
    const h = df.height();
    for (0..h) |row| {
        if (row > 0) try buf.append(allocator, '\n');
        try buf.append(allocator, '{');
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, ',');
            try appendJsonString(buf, allocator, df.series.items[col].name());
            try buf.append(allocator, ':');
            try appendJsonValue(buf, allocator, &df.series.items[col], row);
        }
        try buf.append(allocator, '}');
    }
}

fn appendJsonValue(buf: *std.ArrayList(u8), allocator: Allocator, series: *BoxedSeries, row: usize) !void {
    if (series.isNull(row)) {
        // JSON null is bare for every column type — never the quoted string "null".
        try buf.appendSlice(allocator, "null");
        return;
    }
    // Nested columns render via the dedicated valid-JSON walker, which names
    // struct fields from the column's SchemaNode (Nested.format is positional
    // — `{1,"x"}` — and not valid JSON, so it must NOT be used here).
    if (series.* == .nested) {
        const s = series.nested;
        const node: ?*const @import("parquet").types.SchemaNode = s.meta.schema;
        try writeNestedJson(buf, allocator, &s.values.items[row], node);
        return;
    }
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
        .raw => true,
        .date => true,
        .time => true,
        .timestamp => true,
        // Decimal strings stay quoted: documents exactness and avoids JSON
        // float mangling of the fixed-point value.
        .decimal => true,
        // Binary/FixedBytes render as hex strings — must be quoted.
        .binary => true,
        .fixed_bytes => true,
        // Uuid renders as canonical hyphenated hex — must be quoted.
        .uuid => true,
        // Interval renders as human-readable text — must be quoted.
        .interval => true,
        // Nested is handled by appendJsonValue's dedicated writeNestedJson
        // branch BEFORE this function is consulted; it never reaches here.
        .nested => false,
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

