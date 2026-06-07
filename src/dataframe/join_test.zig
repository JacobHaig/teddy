const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const join = @import("join.zig").join;

fn createLeftDf(allocator: Allocator) !*Dataframe {
    var df = try Dataframe.init(allocator);
    errdefer df.deinit();

    var id = try df.createSeries(i32);
    try id.rename("id");
    try id.append(1);
    try id.append(2);
    try id.append(3);

    var name = try df.createSeries(String);
    try name.rename("name");
    try name.tryAppend("Alice");
    try name.tryAppend("Bob");
    try name.tryAppend("Carol");

    return df;
}

fn createRightDf(allocator: Allocator) !*Dataframe {
    var df = try Dataframe.init(allocator);
    errdefer df.deinit();

    var id = try df.createSeries(i32);
    try id.rename("id");
    try id.append(2);
    try id.append(3);
    try id.append(4);

    var score = try df.createSeries(i64);
    try score.rename("score");
    try score.append(80);
    try score.append(90);
    try score.append(70);

    return df;
}

test "join: inner join" {
    const allocator = std.testing.allocator;
    var left = try createLeftDf(allocator);
    defer left.deinit();
    var right = try createRightDf(allocator);
    defer right.deinit();

    var result = try join(allocator, left, right, "id", .inner);
    defer result.deinit();

    // Only ids 2 and 3 match
    try std.testing.expectEqual(@as(usize, 2), result.height());
    try std.testing.expect(result.getSeries("name") != null);
    try std.testing.expect(result.getSeries("score") != null);
}

test "join: left join keeps unmatched left rows" {
    const allocator = std.testing.allocator;
    var left = try createLeftDf(allocator);
    defer left.deinit();
    var right = try createRightDf(allocator);
    defer right.deinit();

    var result = try join(allocator, left, right, "id", .left);
    defer result.deinit();

    // All 3 left rows present; id=1 has no right match
    try std.testing.expectEqual(@as(usize, 3), result.height());
}

test "join: right join keeps unmatched right rows" {
    const allocator = std.testing.allocator;
    var left = try createLeftDf(allocator);
    defer left.deinit();
    var right = try createRightDf(allocator);
    defer right.deinit();

    var result = try join(allocator, left, right, "id", .right);
    defer result.deinit();

    // All 3 right rows present; id=4 has no left match
    try std.testing.expectEqual(@as(usize, 3), result.height());
}

test "join: outer join keeps all rows" {
    const allocator = std.testing.allocator;
    var left = try createLeftDf(allocator);
    defer left.deinit();
    var right = try createRightDf(allocator);
    defer right.deinit();

    var result = try join(allocator, left, right, "id", .outer);
    defer result.deinit();

    // ids 1,2,3,4 — 4 rows total
    try std.testing.expectEqual(@as(usize, 4), result.height());
}

test "join: missing column returns error" {
    const allocator = std.testing.allocator;
    var left = try createLeftDf(allocator);
    defer left.deinit();
    var right = try createRightDf(allocator);
    defer right.deinit();

    try std.testing.expectError(error.ColumnNotFound, join(allocator, left, right, "nope", .inner));
}

test "join: Raw value column is deep-copied (no double-free)" {
    // Regression: addJoinedColumn's matched-row arm must clone owning value
    // types via the capability convention. A shallow copy of Raw.bytes would
    // double-free on deinit — the testing allocator turns that into a failure.
    const Raw = @import("raw.zig").Raw;
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var k = try left.createSeries(i32);
    try k.rename("k");
    try k.append(1);
    try k.append(2);
    var payload = try left.createSeries(Raw);
    try payload.rename("payload");
    try payload.append(try Raw.fromSlice(allocator, "ab"));
    try payload.append(try Raw.fromSlice(allocator, "cd"));

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    try rk.append(2);
    var v = try right.createSeries(i64);
    try v.rename("v");
    try v.append(10);
    try v.append(20);

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
    const joined = result.getSeries("payload") orelse return error.TestUnexpectedResult;
    const joined_raw = joined.raw;
    try std.testing.expectEqualStrings("ab", joined_raw.values.items[0].toSlice());
    try std.testing.expectEqualStrings("cd", joined_raw.values.items[1].toSlice());
    // Deep copy: joined values must not alias the source buffers.
    try std.testing.expect(joined_raw.values.items[0].bytes.ptr != payload.values.items[0].bytes.ptr);
}

test "join: Date value column compiles and unmatched row is null" {
    // Left df: key=i32, right df: key=i32 + when=Date.
    // Left join on key — row k=2 (left) has no right match, so the right's
    // Date cell must be a real null (not a fabricated epoch placeholder).
    const Date = @import("date.zig").Date;
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    try lk.append(2);

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    var when = try right.createSeries(Date);
    try when.rename("when");
    try when.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));

    var result = try join(allocator, left, right, "k", .left);
    defer result.deinit();

    // Both left rows present
    try std.testing.expectEqual(@as(usize, 2), result.height());

    const when_col = result.getSeries("when") orelse return error.TestUnexpectedResult;
    const when_s = when_col.date;

    // Row 0 (k=1): matched → 2020-01-01
    const c0 = when_s.values.items[0].toCivil();
    try std.testing.expectEqual(@as(i32, 2020), c0.year);
    try std.testing.expectEqual(@as(u8, 1), c0.month);
    try std.testing.expectEqual(@as(u8, 1), c0.day);

    // Row 1 (k=2): no right match → real null (not epoch placeholder)
    try std.testing.expect(when_s.isNull(1));
}

test "join: null keys never match" {
    // left k={1, null}, right k={null, 1} v={100, 200}
    // INNER join: only k=1 matches; the two nulls do NOT pair.
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    try lk.appendNull();

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.appendNull();
    try rk.append(1);
    var v = try right.createSeries(i64);
    try v.rename("v");
    try v.append(100);
    try v.append(200);

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    // Only k=1 matches → height 1
    try std.testing.expectEqual(@as(usize, 1), result.height());
    const v_col = result.getSeries("v") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 200), v_col.int64.values.items[0]);
}

test "join: null key kept as unmatched in left join" {
    // left k={1, null} name={"a","b"}, right k={1} v={10}
    // LEFT join → height 2; null-key row has v isNull; name "b" survives.
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    try lk.appendNull();
    var lname = try left.createSeries(String);
    try lname.rename("name");
    try lname.tryAppend("a");
    try lname.tryAppend("b");

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    var rv = try right.createSeries(i64);
    try rv.rename("v");
    try rv.append(10);

    var result = try join(allocator, left, right, "k", .left);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());

    // Row 1: null-key row — v must be null
    const v_col = result.getSeries("v") orelse return error.TestUnexpectedResult;
    try std.testing.expect(v_col.int64.isNull(1));

    // name "b" must survive in the result
    const name_col = result.getSeries("name") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("b", name_col.string.values.items[1].toSlice());
}

test "join: null never matches placeholder zero" {
    // left k={null}, right k={0} v={7}
    // INNER join → height 0 (null must not match the 0 placeholder)
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.appendNull();

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(0);
    var rv = try right.createSeries(i64);
    try rv.rename("v");
    try rv.append(7);

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), result.height());
}

test "join: matched row with null value cell stays null" {
    // left k={1,2}, right k={1,2} v={10, null}
    // INNER join → v[0]==10, v[1] isNull
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    try lk.append(2);

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    try rk.append(2);
    var rv = try right.createSeries(i64);
    try rv.rename("v");
    try rv.append(10);
    try rv.appendNull();

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
    const v_col = result.getSeries("v") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 10), v_col.int64.values.items[0]);
    try std.testing.expect(v_col.int64.isNull(1));
}

// ---- B6 regression tests (Phase 12) ----

test "join B6: duplicate non-key column renamed to _right" {
    // left {k, v}, right {k, v} — v collides.
    // Result must have "v" (left values) and "v_right" (right values).
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    try lk.append(2);
    var lv = try left.createSeries(i64);
    try lv.rename("v");
    try lv.append(10);
    try lv.append(20);

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    try rk.append(2);
    var rv = try right.createSeries(i64);
    try rv.rename("v");
    try rv.append(100);
    try rv.append(200);

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());

    // "v" must be present and contain LEFT values.
    const v_col = result.getSeries("v") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 10), v_col.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 20), v_col.int64.values.items[1]);

    // "v_right" must be present and contain RIGHT values.
    const v_right = result.getSeries("v_right") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 100), v_right.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 200), v_right.int64.values.items[1]);
}

test "join B6: no collision — names unchanged" {
    // left {k, a}, right {k, b} — no collision; both names unchanged.
    const allocator = std.testing.allocator;

    var left = try Dataframe.init(allocator);
    defer left.deinit();
    var lk = try left.createSeries(i32);
    try lk.rename("k");
    try lk.append(1);
    var la = try left.createSeries(i64);
    try la.rename("a");
    try la.append(10);

    var right = try Dataframe.init(allocator);
    defer right.deinit();
    var rk = try right.createSeries(i32);
    try rk.rename("k");
    try rk.append(1);
    var rb = try right.createSeries(i64);
    try rb.rename("b");
    try rb.append(99);

    var result = try join(allocator, left, right, "k", .inner);
    defer result.deinit();

    try std.testing.expect(result.getSeries("a") != null);
    try std.testing.expect(result.getSeries("b") != null);
    try std.testing.expect(result.getSeries("b_right") == null);
}
