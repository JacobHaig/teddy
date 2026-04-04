const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const json_reader = @import("json_reader.zig");

test "json_reader: rows format basic" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"x\":1,\"y\":2},{\"x\":3,\"y\":4}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
}

test "json_reader: columns format basic" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"x\":[1,3],\"y\":[2,4]}", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
}

test "json_reader: auto-detect rows" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"a\":1}]", .{ .format = .auto });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.height());
}

test "json_reader: auto-detect columns" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":[1]}", .{ .format = .auto });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.height());
}

test "json_reader: string values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"name\":\"hello\"},{\"name\":\"world\"}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: boolean values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"flag\":true},{\"flag\":false}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: float values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"v\":1.5},{\"v\":2.5}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: null values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"x\":1},{\"x\":null}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: empty array" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 0), df.height());
}

test "json_reader: empty object" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{}", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 0), df.height());
}

test "json_reader: empty string" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 0), df.height());
}

test "json_reader: mixed int/float becomes float" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"v\":1},{\"v\":2.5}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: string escapes" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"name\":\"hello\\nworld\"}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.height());
}

test "json_reader: negative numbers" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"x\":-42},{\"x\":-7}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}
