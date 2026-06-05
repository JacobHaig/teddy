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

test "join: Date value column compiles and unmatched row gets epoch (days=0)" {
    // Left df: key=i32, right df: key=i32 + when=Date.
    // Left join on key — row k=2 (left) has no right match, so the right's
    // Date cell must be synthesized as zeroes(Date) = {.days=0} = epoch.
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

    // Row 1 (k=2): no right match → synthesized epoch (days=0)
    try std.testing.expectEqual(@as(i32, 0), when_s.values.items[1].days);
}
