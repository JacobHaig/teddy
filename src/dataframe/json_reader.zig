const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;

// ============================================================
// JSON Reader — parses JSON into a Dataframe
// ============================================================

pub const JsonFormat = enum { auto, rows, columns, ndjson };

/// The concrete format actually used to parse, after `.auto` resolution.
/// Has no `.auto` member, so the dispatch switch is exhaustive by construction
/// (no `unreachable` arm).
const DetectedFormat = enum { rows, columns, ndjson };

pub const ParseOptions = struct {
    format: JsonFormat = .auto,
};

/// Parse JSON content into a Dataframe.
pub fn parse(allocator: Allocator, content: []const u8, options: ParseOptions) !*Dataframe {
    const trimmed = std.mem.trim(u8, content, " \t\n\r");
    if (trimmed.len == 0) return Dataframe.init(allocator);

    const detected: DetectedFormat = switch (options.format) {
        .auto => detectFormat(trimmed),
        .rows => .rows,
        .columns => .columns,
        .ndjson => .ndjson,
    };

    return switch (detected) {
        .rows => parseRows(allocator, trimmed),
        .columns => parseColumns(allocator, trimmed),
        .ndjson => parseNdjson(allocator, trimmed),
    };
}

fn detectFormat(content: []const u8) DetectedFormat {
    // content is already trimmed and non-empty.
    if (content[0] == '[') return .rows;
    if (content[0] == '{') {
        // NDJSON iff there are >=2 non-empty lines that each start with '{'
        // (after trimming). A single object — even pretty-printed across many
        // lines — is columns. Callers can force any format via ParseOptions.
        var brace_lines: usize = 0;
        var it = std.mem.splitScalar(u8, content, '\n');
        while (it.next()) |raw| {
            const line = std.mem.trim(u8, raw, " \t\r");
            if (line.len > 0 and line[0] == '{') brace_lines += 1;
            if (brace_lines >= 2) return .ndjson;
        }
        return .columns;
    }
    return .columns; // fallback (unusual input)
}

// ============================================================
// Row-oriented: [{"col": val, ...}, ...]
// ============================================================

fn parseObjectFields(
    allocator: Allocator,
    content: []const u8,
    pos: *usize,
    col_names: *std.ArrayList([]u8),
    all_values: *std.ArrayList(std.ArrayList(JsonValue)),
    row_count: usize,
) !void {
    while (pos.* < content.len) {
        skipWhitespace(content, pos);
        if (pos.* >= content.len) break;
        if (content[pos.*] == '}') { pos.* += 1; break; }
        if (content[pos.*] == ',') { pos.* += 1; continue; }

        const key = try parseStringAlloc(allocator, content, pos);
        skipWhitespace(content, pos);
        if (pos.* >= content.len or content[pos.*] != ':') {
            allocator.free(key);
            return error.InvalidJson;
        }
        pos.* += 1;

        skipWhitespace(content, pos);
        const val = parseValue(allocator, content, pos) catch |e| {
            allocator.free(key);
            return e;
        };

        const col_idx = if (findColIndex(col_names.items, key)) |idx| blk: {
            // Column already exists; the freshly-allocated key is redundant.
            allocator.free(key);
            break :blk idx;
        } else blk: {
            const idx = col_names.items.len;
            // Once appended, `key` is owned by col_names (its outer defer frees
            // it). The only gap is the append itself failing — free only there.
            col_names.append(allocator, key) catch |e| {
                allocator.free(key);
                return e;
            };
            var new_col: std.ArrayList(JsonValue) = .empty;
            for (0..row_count) |_| {
                try new_col.append(allocator, .null);
            }
            try all_values.append(allocator, new_col);
            break :blk idx;
        };

        try all_values.items[col_idx].append(allocator, val);
    }
}

fn parseRows(allocator: Allocator, content: []const u8) !*Dataframe {
    var col_names = std.ArrayList([]u8).empty;
    defer {
        for (col_names.items) |k| allocator.free(k);
        col_names.deinit(allocator);
    }

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

        try parseObjectFields(allocator, content, &pos, &col_names, &all_values, row_count);

        for (all_values.items) |*col_vals| {
            if (col_vals.items.len <= row_count) {
                try col_vals.append(allocator, .null);
            }
        }
        row_count += 1;
    }

    return buildDataframe(allocator, col_names.items, all_values.items);
}

// ============================================================
// NDJSON / JSONL: one JSON object per line
// ============================================================

fn parseNdjson(allocator: Allocator, content: []const u8) !*Dataframe {
    var col_names = std.ArrayList([]u8).empty;
    defer {
        for (col_names.items) |k| allocator.free(k);
        col_names.deinit(allocator);
    }

    var all_values = std.ArrayList(std.ArrayList(JsonValue)).empty;
    defer {
        for (all_values.items) |*col_vals| {
            for (col_vals.items) |*v| v.deinit(allocator);
            col_vals.deinit(allocator);
        }
        all_values.deinit(allocator);
    }

    var row_count: usize = 0;
    var line_iter = std.mem.splitScalar(u8, content, '\n');
    while (line_iter.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        if (line[0] != '{') return error.InvalidJson;
        var pos: usize = 1;
        try parseObjectFields(allocator, line, &pos, &col_names, &all_values, row_count);
        for (all_values.items) |*col_vals| {
            if (col_vals.items.len <= row_count) {
                try col_vals.append(allocator, .null);
            }
        }
        row_count += 1;
    }

    return buildDataframe(allocator, col_names.items, all_values.items);
}

// ============================================================
// Column-oriented: {"col": [val, ...], ...}
// ============================================================

fn parseColumns(allocator: Allocator, content: []const u8) !*Dataframe {
    var col_names = std.ArrayList([]u8).empty;
    defer {
        for (col_names.items) |k| allocator.free(k);
        col_names.deinit(allocator);
    }

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
        const key = try parseStringAlloc(allocator, content, &pos);
        // Once appended, `key` is owned by col_names (its outer defer frees it);
        // free only if the append itself fails.
        col_names.append(allocator, key) catch |e| {
            allocator.free(key);
            return e;
        };
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
    // Original had two branches: `has_float` and the redundant `has_int and has_float`
    // (subsumed by the first). The duplicate is removed; the logic is identical.
    if (has_float) return .float;
    if (has_bool and !has_int) return .boolean;
    if (has_int) return .integer;
    return .string; // fallback
}

fn buildDataframe(allocator: Allocator, col_names: []const []u8, all_values: []std.ArrayList(JsonValue)) !*Dataframe {
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
                    switch (v) {
                        .null => try s.appendNull(),
                        .integer => |n| try s.append(n),
                        // Defensive: with current inference a column containing any
                        // .float value is promoted to .float before this arm is
                        // reached, so this branch is dead in practice. Kept correct
                        // anyway: @trunc produces f64, so @intFromFloat is required.
                        .float => |f| try s.append(@as(i64, @intFromFloat(@trunc(f)))),
                        // Bools that end up in an integer column (e.g. mixed bool+int
                        // inference): encode as 1/0 rather than always 0.
                        .boolean => |b| try s.append(@intFromBool(b)),
                        .string => try s.append(0),
                    }
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .float => {
                var s = try Series(f64).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    switch (v) {
                        .null => try s.appendNull(),
                        .float => |f| try s.append(f),
                        .integer => |n| try s.append(@as(f64, @floatFromInt(n))),
                        else => try s.append(0.0),
                    }
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .boolean => {
                var s = try Series(bool).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    switch (v) {
                        .null => try s.appendNull(),
                        .boolean => |b| try s.append(b),
                        else => try s.append(false),
                    }
                }
                try df.addSeries(s.toBoxedSeries());
            },
            .string => {
                var s = try Series(String).init(allocator);
                try s.rename(name);
                for (values) |v| {
                    switch (v) {
                        .null => try s.appendNull(),
                        .string => |str| try s.append(try String.fromSlice(allocator, str)),
                        // B2b: non-string scalars in a mixed column are stringified
                        // rather than silently dropped as "". allocPrint produces an
                        // owned buffer; String.fromSlice copies it, so we free the
                        // intermediate buffer immediately after.
                        .integer => |n| {
                            const buf = try std.fmt.allocPrint(allocator, "{d}", .{n});
                            defer allocator.free(buf);
                            try s.append(try String.fromSlice(allocator, buf));
                        },
                        .float => |f| {
                            const buf = try std.fmt.allocPrint(allocator, "{d}", .{f});
                            defer allocator.free(buf);
                            try s.append(try String.fromSlice(allocator, buf));
                        },
                        .boolean => |b| {
                            try s.append(try String.fromSlice(allocator, if (b) "true" else "false"));
                        },
                    }
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
                'b' => try buf.append(allocator, 0x08),
                'f' => try buf.append(allocator, 0x0C),
                else => {
                    // `\uXXXX` unicode escapes are not yet decoded (needs code-point
                    // -> UTF-8 + surrogate-pair handling); emitted raw for now.
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
        const n = std.fmt.parseInt(i64, num_str, 10) catch |e| switch (e) {
            // A valid JSON integer beyond i64 range: fall back to f64 (lossy but
            // valid — JSON numbers are conceptually arbitrary-precision).
            error.Overflow => {
                const f = std.fmt.parseFloat(f64, num_str) catch return error.InvalidJson;
                return .{ .float = f };
            },
            else => return error.InvalidJson,
        };
        return .{ .integer = n };
    }
}

