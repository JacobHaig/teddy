const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;

// ============================================================
// JSON Reader — parses JSON into a Dataframe
// ============================================================

pub const JsonFormat = enum { auto, rows, columns };

pub const ParseOptions = struct {
    format: JsonFormat = .auto,
};

/// Parse JSON content into a Dataframe.
pub fn parse(allocator: Allocator, content: []const u8, options: ParseOptions) !*Dataframe {
    const trimmed = std.mem.trim(u8, content, " \t\n\r");
    if (trimmed.len == 0) return Dataframe.init(allocator);

    const format: JsonFormat = switch (options.format) {
        .auto => detectFormat(trimmed),
        else => options.format,
    };

    return switch (format) {
        .rows => parseRows(allocator, trimmed),
        .columns => parseColumns(allocator, trimmed),
        .auto => unreachable,
    };
}

fn detectFormat(content: []const u8) JsonFormat {
    if (content[0] == '[') return .rows;
    return .columns;
}

// ============================================================
// Row-oriented: [{"col": val, ...}, ...]
// ============================================================

fn parseRows(allocator: Allocator, content: []const u8) !*Dataframe {
    // Phase 1: Parse all objects into intermediate storage
    var col_names = std.ArrayList([]const u8).empty;
    defer col_names.deinit(allocator);

    var all_values = std.ArrayList(std.ArrayList(JsonValue)).empty;
    defer {
        for (all_values.items) |*col_vals| {
            for (col_vals.items) |*v| v.deinit(allocator);
            col_vals.deinit(allocator);
        }
        all_values.deinit(allocator);
    }

    var pos: usize = 0;
    skipWhitespace(content, &pos);
    if (pos >= content.len or content[pos] != '[') return error.InvalidJson;
    pos += 1;

    var row_count: usize = 0;
    while (pos < content.len) {
        skipWhitespace(content, &pos);
        if (pos >= content.len) break;
        if (content[pos] == ']') break;
        if (content[pos] == ',') { pos += 1; continue; }
        if (content[pos] != '{') return error.InvalidJson;
        pos += 1;

        // Parse object fields
        while (pos < content.len) {
            skipWhitespace(content, &pos);
            if (pos >= content.len) break;
            if (content[pos] == '}') { pos += 1; break; }
            if (content[pos] == ',') { pos += 1; continue; }

            // Parse key
            const key = try parseString(content, &pos);
            skipWhitespace(content, &pos);
            if (pos >= content.len or content[pos] != ':') return error.InvalidJson;
            pos += 1;

            // Parse value
            skipWhitespace(content, &pos);
            const val = try parseValue(allocator, content, &pos);

            // Find or create column
            const col_idx = findColIndex(col_names.items, key) orelse blk: {
                const idx = col_names.items.len;
                try col_names.append(allocator, key);
                var new_col: std.ArrayList(JsonValue) = .empty;
                for (0..row_count) |_| {
                    try new_col.append(allocator, .null);
                }
                try all_values.append(allocator, new_col);
                break :blk idx;
            };

            try all_values.items[col_idx].append(allocator, val);
        }

        // Fill missing columns for this row with nulls
        for (all_values.items) |*col_vals| {
            if (col_vals.items.len <= row_count) {
                try col_vals.append(allocator, .null);
            }
        }
        row_count += 1;
    }

    // Phase 2: Infer types and build DataFrame
    return buildDataframe(allocator, col_names.items, all_values.items);
}

// ============================================================
// Column-oriented: {"col": [val, ...], ...}
// ============================================================

fn parseColumns(allocator: Allocator, content: []const u8) !*Dataframe {
    var col_names = std.ArrayList([]const u8).empty;
    defer col_names.deinit(allocator);

    var all_values = std.ArrayList(std.ArrayList(JsonValue)).empty;
    defer {
        for (all_values.items) |*col_vals| {
            for (col_vals.items) |*v| v.deinit(allocator);
            col_vals.deinit(allocator);
        }
        all_values.deinit(allocator);
    }

    var pos: usize = 0;
    skipWhitespace(content, &pos);
    if (pos >= content.len or content[pos] != '{') return error.InvalidJson;
    pos += 1;

    while (pos < content.len) {
        skipWhitespace(content, &pos);
        if (pos >= content.len) break;
        if (content[pos] == '}') break;
        if (content[pos] == ',') { pos += 1; continue; }

        // Parse key
        const key = try parseString(content, &pos);
        try col_names.append(allocator, key);
        skipWhitespace(content, &pos);
        if (pos >= content.len or content[pos] != ':') return error.InvalidJson;
        pos += 1;
        skipWhitespace(content, &pos);

        // Parse array of values
        if (pos >= content.len or content[pos] != '[') return error.InvalidJson;
        pos += 1;

        var col_vals: std.ArrayList(JsonValue) = .empty;
        while (pos < content.len) {
            skipWhitespace(content, &pos);
            if (pos >= content.len) break;
            if (content[pos] == ']') { pos += 1; break; }
            if (content[pos] == ',') { pos += 1; continue; }
            const val = try parseValue(allocator, content, &pos);
            try col_vals.append(allocator, val);
        }
        try all_values.append(allocator, col_vals);
    }

    return buildDataframe(allocator, col_names.items, all_values.items);
}

// ============================================================
// Type inference and DataFrame construction
// ============================================================

const JsonValue = union(enum) {
    integer: i64,
    float: f64,
    string: []const u8,
    boolean: bool,
    null,

    fn deinit(self: *JsonValue, allocator: Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            else => {},
        }
    }
};

const ColumnType = enum { integer, float, string, boolean };

fn inferColumnType(values: []const JsonValue) ColumnType {
    var has_int = false;
    var has_float = false;
    var has_bool = false;
    var has_string = false;

    for (values) |v| {
        switch (v) {
            .integer => has_int = true,
            .float => has_float = true,
            .boolean => has_bool = true,
            .string => has_string = true,
            .null => {},
        }
    }

    if (has_string) return .string;
    if (has_float or (has_int and has_float)) return .float;
    if (has_int and has_float) return .float;
    if (has_bool and !has_int and !has_float) return .boolean;
    if (has_int) return .integer;
    return .string; // fallback
}

fn buildDataframe(allocator: Allocator, col_names: []const []const u8, all_values: []std.ArrayList(JsonValue)) !*Dataframe {
    const df = try Dataframe.init(allocator);
    errdefer df.deinit();

    for (col_names, 0..) |name, i| {
        const values = all_values[i].items;
        const col_type = inferColumnType(values);

        switch (col_type) {
            .integer => {
                var s = try Series(i64).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    try s.append(switch (v) {
                        .integer => |n| n,
                        .float => |f| @as(i64, @intFromFloat(f)),
                        else => 0,
                    });
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .float => {
                var s = try Series(f64).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    try s.append(switch (v) {
                        .float => |f| f,
                        .integer => |n| @as(f64, @floatFromInt(n)),
                        else => 0.0,
                    });
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .boolean => {
                var s = try Series(bool).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    try s.append(switch (v) {
                        .boolean => |b| b,
                        else => false,
                    });
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .string => {
                var s = try Series(String).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    const slice: []const u8 = switch (v) {
                        .string => |str| str,
                        .integer => |n| blk: {
                            _ = n;
                            break :blk "";
                        },
                        else => "",
                    };
                    try s.append(try String.fromSlice(allocator, slice));
                }
                try df.addSeries(s.toBoxedSeries());
            },
        }
    }

    return df;
}

// ============================================================
// JSON Tokenizer Helpers
// ============================================================

fn findColIndex(names: []const []const u8, key: []const u8) ?usize {
    for (names, 0..) |name, i| {
        if (std.mem.eql(u8, name, key)) return i;
    }
    return null;
}

fn skipWhitespace(content: []const u8, pos: *usize) void {
    while (pos.* < content.len and (content[pos.*] == ' ' or content[pos.*] == '\t' or content[pos.*] == '\n' or content[pos.*] == '\r')) {
        pos.* += 1;
    }
}

/// Parse a JSON string, returning a slice into the original content (for unescaped strings)
/// or an error if the string is malformed. Returns the content between quotes.
fn parseString(content: []const u8, pos: *usize) ![]const u8 {
    if (pos.* >= content.len or content[pos.*] != '"') return error.InvalidJson;
    pos.* += 1;
    const start = pos.*;
    var has_escape = false;
    while (pos.* < content.len and content[pos.*] != '"') {
        if (content[pos.*] == '\\') {
            has_escape = true;
            pos.* += 2; // skip escape sequence
        } else {
            pos.* += 1;
        }
    }
    if (pos.* >= content.len) return error.InvalidJson;
    const slice = content[start..pos.*];
    pos.* += 1; // skip closing quote
    if (has_escape) return slice; // caller will get raw escaped form — good enough for keys
    return slice;
}

/// Parse a JSON string, allocating a new buffer if escapes need to be unescaped.
fn parseStringAlloc(allocator: Allocator, content: []const u8, pos: *usize) ![]u8 {
    if (pos.* >= content.len or content[pos.*] != '"') return error.InvalidJson;
    pos.* += 1;

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    while (pos.* < content.len and content[pos.*] != '"') {
        if (content[pos.*] == '\\') {
            pos.* += 1;
            if (pos.* >= content.len) return error.InvalidJson;
            switch (content[pos.*]) {
                '"' => try buf.append(allocator, '"'),
                '\\' => try buf.append(allocator, '\\'),
                'n' => try buf.append(allocator, '\n'),
                'r' => try buf.append(allocator, '\r'),
                't' => try buf.append(allocator, '\t'),
                '/' => try buf.append(allocator, '/'),
                else => {
                    try buf.append(allocator, '\\');
                    try buf.append(allocator, content[pos.*]);
                },
            }
        } else {
            try buf.append(allocator, content[pos.*]);
        }
        pos.* += 1;
    }
    if (pos.* >= content.len) return error.InvalidJson;
    pos.* += 1; // skip closing quote

    return buf.toOwnedSlice(allocator);
}

fn parseValue(allocator: Allocator, content: []const u8, pos: *usize) !JsonValue {
    skipWhitespace(content, pos);
    if (pos.* >= content.len) return error.InvalidJson;

    const c = content[pos.*];
    if (c == '"') {
        const s = try parseStringAlloc(allocator, content, pos);
        return .{ .string = s };
    }
    if (c == 't') {
        if (pos.* + 4 <= content.len and std.mem.eql(u8, content[pos.* .. pos.* + 4], "true")) {
            pos.* += 4;
            return .{ .boolean = true };
        }
        return error.InvalidJson;
    }
    if (c == 'f') {
        if (pos.* + 5 <= content.len and std.mem.eql(u8, content[pos.* .. pos.* + 5], "false")) {
            pos.* += 5;
            return .{ .boolean = false };
        }
        return error.InvalidJson;
    }
    if (c == 'n') {
        if (pos.* + 4 <= content.len and std.mem.eql(u8, content[pos.* .. pos.* + 4], "null")) {
            pos.* += 4;
            return .null;
        }
        return error.InvalidJson;
    }
    // Number
    return parseNumber(content, pos);
}

fn parseNumber(content: []const u8, pos: *usize) !JsonValue {
    const start = pos.*;
    var is_float = false;

    if (pos.* < content.len and content[pos.*] == '-') pos.* += 1;
    while (pos.* < content.len and content[pos.*] >= '0' and content[pos.*] <= '9') pos.* += 1;
    if (pos.* < content.len and content[pos.*] == '.') {
        is_float = true;
        pos.* += 1;
        while (pos.* < content.len and content[pos.*] >= '0' and content[pos.*] <= '9') pos.* += 1;
    }
    // Handle exponent
    if (pos.* < content.len and (content[pos.*] == 'e' or content[pos.*] == 'E')) {
        is_float = true;
        pos.* += 1;
        if (pos.* < content.len and (content[pos.*] == '+' or content[pos.*] == '-')) pos.* += 1;
        while (pos.* < content.len and content[pos.*] >= '0' and content[pos.*] <= '9') pos.* += 1;
    }

    const num_str = content[start..pos.*];
    if (num_str.len == 0) return error.InvalidJson;

    if (is_float) {
        const f = std.fmt.parseFloat(f64, num_str) catch return error.InvalidJson;
        return .{ .float = f };
    } else {
        const n = std.fmt.parseInt(i64, num_str, 10) catch return error.InvalidJson;
        return .{ .integer = n };
    }
}

