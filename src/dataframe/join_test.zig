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
