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
    // Phase 10a Unit B: JSON null now reads as a real null (was placeholder 0).
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"x\":1},{\"x\":null}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());

    const x = df.getSeries("x") orelse return error.ColumnNotFound;
    try std.testing.expect(x.* == .int64);
    try std.testing.expect(!x.isNull(0));
    try std.testing.expect(x.isNull(1));
    try std.testing.expectEqual(@as(i64, 1), x.int64.values.items[0]);
}

test "json_reader: round-trip preserves null as bare null" {
    // Phase 10a Unit B: null -> appendNull -> writes back as bare `null`.
    const allocator = std.testing.allocator;
    const json_writer = @import("json_writer.zig");
    const input = "[{\"x\":1},{\"x\":null}]";
    var df = try json_reader.parse(allocator, input, .{});
    defer df.deinit();

    const x = df.getSeries("x") orelse return error.ColumnNotFound;
    try std.testing.expect(x.isNull(1));

    const out = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(out);
    try std.testing.expectEqualStrings(input, out);
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

test "json_reader: ndjson basic integers" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"x\":1,\"y\":2}\n{\"x\":3,\"y\":4}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
}

test "json_reader: ndjson string values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"name\":\"alice\"}\n{\"name\":\"bob\"}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 1), df.width());
}

test "json_reader: ndjson float values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"v\":1.5}\n{\"v\":2.5}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: ndjson boolean values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"flag\":true}\n{\"flag\":false}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: ndjson null values" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"x\":1}\n{\"x\":null}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: ndjson auto-detect" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":1}\n{\"a\":2}", .{ .format = .auto });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: ndjson single line explicit" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":42,\"b\":99}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
}

test "json_reader: ndjson trailing newline" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"x\":1}\n{\"x\":2}\n", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "json_reader: ndjson empty content" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 0), df.height());
}

test "json_reader: ndjson missing columns become null" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"x\":1,\"y\":2}\n{\"x\":3}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
}

test "json_reader: ndjson mixed int and float promotes to float" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"v\":1}\n{\"v\":2.5}", .{ .format = .ndjson });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

// ---- B2 regression tests (Phase 12) ----

test "json_reader B2b: mixed int+string column stringifies the integer" {
    // [1, "a"] must produce a String column with values "1" and "a",
    // not "" and "a" (the previous silent data loss).
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"v\":1},{\"v\":\"a\"}]", .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.height());
    const col = df.getSeries("v") orelse return error.ColumnNotFound;
    // Inferred as string because one value is a string.
    try std.testing.expect(col.* == .string);
    try std.testing.expectEqualStrings("1", col.string.values.items[0].toSlice());
    try std.testing.expectEqualStrings("a", col.string.values.items[1].toSlice());
}

test "json_reader B2b: mixed bool+string column stringifies the bool" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"v\":true},{\"v\":\"x\"}]", .{});
    defer df.deinit();

    const col = df.getSeries("v") orelse return error.ColumnNotFound;
    try std.testing.expect(col.* == .string);
    try std.testing.expectEqualStrings("true", col.string.values.items[0].toSlice());
    try std.testing.expectEqualStrings("x", col.string.values.items[1].toSlice());
}

test "json_reader B2b: mixed float+string column stringifies the float" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"v\":3.14},{\"v\":\"pi\"}]", .{});
    defer df.deinit();

    const col = df.getSeries("v") orelse return error.ColumnNotFound;
    try std.testing.expect(col.* == .string);
    // The stringified float must not be empty.
    try std.testing.expect(col.string.values.items[0].toSlice().len > 0);
    try std.testing.expectEqualStrings("pi", col.string.values.items[1].toSlice());
}

test "json_reader B2 int arm: bool values in integer column encode as 1/0" {
    // A column that has bool + int values is inferred as .integer.
    // The bool arm must produce @intFromBool(b) (1/0), not a constant 0.
    // We verify true→1 and false→0 distinctly.
    const allocator = std.testing.allocator;
    // Force inference: bool + integer = integer column.
    var df = try json_reader.parse(allocator, "[{\"v\":true},{\"v\":false},{\"v\":2}]", .{});
    defer df.deinit();

    const col = df.getSeries("v") orelse return error.ColumnNotFound;
    // bool+int → integer column
    try std.testing.expect(col.* == .int64);
    try std.testing.expectEqual(@as(i64, 1), col.int64.values.items[0]); // true → 1
    try std.testing.expectEqual(@as(i64, 0), col.int64.values.items[1]); // false → 0
    try std.testing.expectEqual(@as(i64, 2), col.int64.values.items[2]); // int stays
}

// ---- Phase 7 (JSON reader fixes) ----

test "json_reader P7: auto-detect rows from leading bracket" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"a\":1}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.height());
    try std.testing.expectEqual(@as(usize, 1), df.width());
    try std.testing.expect(df.getSeries("a") != null);
}

test "json_reader P7: auto-detect columns from single object" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":[1,2]}", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 1), df.width());
}

test "json_reader P7: auto-detect ndjson from two brace lines" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":1}\n{\"a\":2}", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 1), df.width());
}

test "json_reader P7: force columns format on single object" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\"a\":[1,2,3]}", .{ .format = .columns });
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 3), df.height());
    try std.testing.expectEqual(@as(usize, 1), df.width());
}

test "json_reader P7: pretty-printed columns object is not misdetected as ndjson" {
    // A single columns object split across lines, but only ONE leading '{'.
    // Must detect columns, not ndjson.
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "{\n  \"a\": [1, 2],\n  \"b\": [3, 4]\n}", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.height());
    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expect(df.getSeries("a") != null);
    try std.testing.expect(df.getSeries("b") != null);
}

test "json_reader P7: escaped object key becomes correct column name" {
    // {"a\"b": 1} -> column named exactly a"b (3 bytes: a, doublequote, b).
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"a\\\"b\": 1}]", .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 1), df.width());
    const col = df.getSeries("a\"b") orelse return error.ColumnNotFound;
    try std.testing.expect(col.* == .int64);
    try std.testing.expectEqual(@as(i64, 1), col.int64.values.items[0]);
}

test "json_reader P7: integer beyond i64 range falls back to float64" {
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"n\": 123456789012345678901234567890}]", .{});
    defer df.deinit();
    const col = df.getSeries("n") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("f64", col.typeName());
    const v = col.float64.values.items[0];
    try std.testing.expect(std.math.isFinite(v));
    try std.testing.expect(v > 1e29);
}

test "json_reader P7: backspace and formfeed escapes unescape" {
    // "a\bc\fd" -> bytes a, 0x08, c, 0x0C, d.
    const allocator = std.testing.allocator;
    var df = try json_reader.parse(allocator, "[{\"s\": \"a\\bc\\fd\"}]", .{});
    defer df.deinit();
    const col = df.getSeries("s") orelse return error.ColumnNotFound;
    try std.testing.expect(col.* == .string);
    const bytes = col.string.values.items[0].toSlice();
    try std.testing.expectEqualSlices(u8, &[_]u8{ 'a', 0x08, 'c', 0x0C, 'd' }, bytes);
}
