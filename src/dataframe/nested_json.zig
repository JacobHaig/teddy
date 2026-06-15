//! Phase 9.2 — valid-JSON renderer for Nested column values.
//!
//! `Nested.format` renders structs POSITIONALLY (`{1, "x"}` — not valid JSON;
//! field names live in the column SchemaNode, not the value tree). CSV/print/
//! asStringAt still want that positional form, so we DO NOT change format.
//! Instead this dedicated renderer walks the value tree IN PARALLEL with the
//! column's SchemaNode subtree and emits real JSON:
//!   - struct  -> {"name": val, ...}  (names from the struct group's children)
//!   - list    -> [val, ...]
//!   - map     -> {"key": val, ...}   (key rendered as a JSON string)
//!   - scalars -> JSON strings (string/date/time/etc.) or bare (number/bool/null)
//!
//! When `node` is null OR its shape does not line up with the value (e.g. a
//! hand-built struct column with no schema), we FALL BACK to positional arrays
//! for structs (`[1, "x"]`) — still valid JSON, never `{1, "x"}`.
//!
//! Schema navigation mirrors nested_assembly.zig's shape classification.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Nested = @import("nested.zig").Nested;
const parquet = @import("parquet");

const SchemaNode = parquet.types.SchemaNode;

const Shape = enum { leaf, list3, list2, map, strukt };

/// Classify a SchemaNode's structural shape (mirrors nested_assembly.shapeOf).
fn shapeOf(node: *const SchemaNode) Shape {
    if (node.isLeaf()) {
        if (node.repetition == .repeated) return .list2;
        return .leaf;
    }
    if (node.children.len == 1 and node.children[0].repetition == .repeated) {
        const mid = &node.children[0];
        if (mid.children.len == 2) return .map;
        if (mid.children.len == 1) return .list3;
        return .list2;
    }
    if (node.repetition == .repeated) return .list2;
    return .strukt;
}

/// The element subtree of a LIST node, or null if `node` is not a list-shaped
/// group whose element can be resolved.
fn listElement(node: *const SchemaNode) ?*const SchemaNode {
    return switch (shapeOf(node)) {
        // 3-level: node -> repeated middle -> element child.
        .list3 => &node.children[0].children[0],
        // 2-level legacy: the repeated node itself carries the element content.
        .list2 => node,
        else => null,
    };
}

/// The key/value subtrees of a MAP node, or null if not map-shaped.
fn mapKeyValue(node: *const SchemaNode) ?struct { key: *const SchemaNode, value: *const SchemaNode } {
    if (shapeOf(node) != .map) return null;
    const kv = &node.children[0];
    if (kv.children.len != 2) return null;
    return .{ .key = &kv.children[0], .value = &kv.children[1] };
}

/// Write `value` to `buf` as valid JSON, using `node` (when present and
/// shape-compatible) to name struct fields. Falls back to positional arrays
/// for structs when schema is absent or mismatched.
pub fn writeNestedJson(
    buf: *std.ArrayList(u8),
    allocator: Allocator,
    value: *const Nested,
    node: ?*const SchemaNode,
) Allocator.Error!void {
    switch (value.*) {
        .null_ => try buf.appendSlice(allocator, "null"),
        .boolean => |v| try buf.appendSlice(allocator, if (v) "true" else "false"),
        .int => |v| try appendFmt(buf, allocator, "{d}", .{v}),
        .uint => |v| try appendFmt(buf, allocator, "{d}", .{v}),
        .float => |v| try appendFmt(buf, allocator, "{d}", .{v}),
        // Temporal / decimal / uuid / interval render to text -> JSON strings.
        .date => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .time => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .timestamp => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .decimal => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .uuid => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .interval => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .string => |*v| try appendJsonString(buf, allocator, v.toSlice()),
        .bytes => |*v| try appendHexString(buf, allocator, v.toSlice()),
        .list => |*l| {
            const elem_node = if (node) |n| listElement(n) else null;
            try buf.append(allocator, '[');
            for (l.items, 0..) |*item, i| {
                if (i > 0) try buf.append(allocator, ',');
                try writeNestedJson(buf, allocator, item, elem_node);
            }
            try buf.append(allocator, ']');
        },
        .strukt => |*st| {
            // Use field names from the struct group's children when the schema
            // is present AND lines up (same field count, struct shape).
            const named = if (node) |n|
                (shapeOf(n) == .strukt and n.children.len == st.fields.len)
            else
                false;

            if (named) {
                const n = node.?;
                try buf.append(allocator, '{');
                for (st.fields, 0..) |*f, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try appendJsonString(buf, allocator, n.children[i].name);
                    try buf.append(allocator, ':');
                    try writeNestedJson(buf, allocator, f, &n.children[i]);
                }
                try buf.append(allocator, '}');
            } else {
                // Fallback: positional JSON array (valid JSON, never `{1,"x"}`).
                try buf.append(allocator, '[');
                for (st.fields, 0..) |*f, i| {
                    if (i > 0) try buf.append(allocator, ',');
                    try writeNestedJson(buf, allocator, f, null);
                }
                try buf.append(allocator, ']');
            }
        },
        .map => |*m| {
            const kv = if (node) |n| mapKeyValue(n) else null;
            try buf.append(allocator, '{');
            for (m.entries, 0..) |*e, i| {
                if (i > 0) try buf.append(allocator, ',');
                try writeMapKey(buf, allocator, &e.key);
                try buf.append(allocator, ':');
                try writeNestedJson(buf, allocator, &e.value, if (kv) |x| x.value else null);
            }
            try buf.append(allocator, '}');
        },
    }
}

/// A JSON object key must be a string. Scalar keys render to their text form
/// then get quoted; container keys (unusual) fall back to a quoted positional
/// rendering so the output is still parseable.
fn writeMapKey(buf: *std.ArrayList(u8), allocator: Allocator, key: *const Nested) Allocator.Error!void {
    switch (key.*) {
        .string => |*v| try appendJsonString(buf, allocator, v.toSlice()),
        .null_ => try appendJsonString(buf, allocator, "null"),
        .boolean => |v| try appendJsonString(buf, allocator, if (v) "true" else "false"),
        .int => |v| try appendQuotedFmt(buf, allocator, "{d}", .{v}),
        .uint => |v| try appendQuotedFmt(buf, allocator, "{d}", .{v}),
        .float => |v| try appendQuotedFmt(buf, allocator, "{d}", .{v}),
        .date => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .time => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .timestamp => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .decimal => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .uuid => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .interval => |v| try appendQuotedFmt(buf, allocator, "{f}", .{v}),
        .bytes => |*v| try appendHexString(buf, allocator, v.toSlice()),
        // Container keys: render value-form into a temp then quote it.
        .list, .strukt, .map => {
            var tmp = std.ArrayList(u8).empty;
            defer tmp.deinit(allocator);
            try writeNestedJson(&tmp, allocator, key, null);
            try appendJsonString(buf, allocator, tmp.items);
        },
    }
}

fn appendFmt(buf: *std.ArrayList(u8), allocator: Allocator, comptime fmt: []const u8, args: anytype) !void {
    const s = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(s);
    try buf.appendSlice(allocator, s);
}

fn appendQuotedFmt(buf: *std.ArrayList(u8), allocator: Allocator, comptime fmt: []const u8, args: anytype) !void {
    const s = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(s);
    try appendJsonString(buf, allocator, s);
}

fn appendHexString(buf: *std.ArrayList(u8), allocator: Allocator, bytes: []const u8) !void {
    try buf.append(allocator, '"');
    for (bytes) |b| try appendFmt(buf, allocator, "{x:0>2}", .{b});
    try buf.append(allocator, '"');
}

/// Quote + escape (mirrors json_writer.appendJsonString).
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

// ===========================================================================
// Tests
// ===========================================================================

const strings = @import("strings.zig");
const testing = std.testing;

/// Build a SchemaNode tree for struct{a: int64, b: string} on the heap-free
/// stack. Names are owned by the caller's arena via the supplied allocator.
fn structSchema(allocator: Allocator, names: []const []const u8) ![]SchemaNode {
    const children = try allocator.alloc(SchemaNode, names.len);
    for (names, 0..) |nm, i| {
        children[i] = .{
            .name = try allocator.dupe(u8, nm),
            .repetition = .optional,
            .physical = .int64,
            .max_def = 1,
            .max_rep = 0,
            .leaf_index = i,
        };
    }
    return children;
}

test "nested_json: struct WITH schema renders named object" {
    const allocator = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const children = try structSchema(arena, &.{ "a", "b" });
    const node = SchemaNode{
        .name = try arena.dupe(u8, "s"),
        .repetition = .optional,
        .children = children,
    };

    var fields = try allocator.alloc(Nested, 2);
    fields[0] = .{ .int = 1 };
    fields[1] = .{ .string = try strings.String.fromSlice(allocator, "x") };
    var st: Nested = .{ .strukt = .{ .allocator = allocator, .fields = fields } };
    defer st.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    try writeNestedJson(&buf, allocator, &st, &node);
    try testing.expectEqualStrings("{\"a\":1,\"b\":\"x\"}", buf.items);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, buf.items, .{});
    defer parsed.deinit();
}

test "nested_json: struct WITHOUT schema falls back to positional array" {
    const allocator = testing.allocator;

    var fields = try allocator.alloc(Nested, 2);
    fields[0] = .{ .int = 1 };
    fields[1] = .{ .string = try strings.String.fromSlice(allocator, "x") };
    var st: Nested = .{ .strukt = .{ .allocator = allocator, .fields = fields } };
    defer st.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    try writeNestedJson(&buf, allocator, &st, null);
    try testing.expectEqualStrings("[1,\"x\"]", buf.items);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, buf.items, .{});
    defer parsed.deinit();
}

test "nested_json: list renders as JSON array" {
    const allocator = testing.allocator;

    var items = try allocator.alloc(Nested, 2);
    items[0] = .{ .int = 1 };
    items[1] = .{ .int = 2 };
    var l: Nested = .{ .list = .{ .allocator = allocator, .items = items } };
    defer l.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    try writeNestedJson(&buf, allocator, &l, null);
    try testing.expectEqualStrings("[1,2]", buf.items);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, buf.items, .{});
    defer parsed.deinit();
}

test "nested_json: map renders as JSON object with string keys" {
    const allocator = testing.allocator;

    var entries = try allocator.alloc(Nested.MapEntry, 1);
    entries[0] = .{
        .key = .{ .string = try strings.String.fromSlice(allocator, "a") },
        .value = .{ .int = 1 },
    };
    var m: Nested = .{ .map = .{ .allocator = allocator, .entries = entries } };
    defer m.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    try writeNestedJson(&buf, allocator, &m, null);
    try testing.expectEqualStrings("{\"a\":1}", buf.items);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, buf.items, .{});
    defer parsed.deinit();
}

test "nested_json: schema field-count mismatch falls back to positional" {
    const allocator = testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Schema says 3 fields, value has 2 -> mismatch -> positional fallback.
    const children = try structSchema(arena, &.{ "a", "b", "c" });
    const node = SchemaNode{
        .name = try arena.dupe(u8, "s"),
        .repetition = .optional,
        .children = children,
    };

    var fields = try allocator.alloc(Nested, 2);
    fields[0] = .{ .int = 1 };
    fields[1] = .{ .int = 2 };
    var st: Nested = .{ .strukt = .{ .allocator = allocator, .fields = fields } };
    defer st.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    try writeNestedJson(&buf, allocator, &st, &node);
    try testing.expectEqualStrings("[1,2]", buf.items);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, buf.items, .{});
    defer parsed.deinit();
}
