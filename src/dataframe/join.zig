const std = @import("std");
const Allocator = std.mem.Allocator;

const Dataframe = @import("dataframe.zig").Dataframe;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;
const GroupByContext = @import("group.zig").GroupByContext;

pub const JoinType = enum { inner, left, right, outer };

const IndexPair = struct { left: ?usize, right: ?usize };

/// Join two dataframes on a key column.
/// Returns a new Dataframe. Caller owns the returned pointer.
pub fn join(
    allocator: Allocator,
    left_df: *Dataframe,
    right_df: *Dataframe,
    on: []const u8,
    join_type: JoinType,
) !*Dataframe {
    const left_key = left_df.getSeries(on) orelse return error.ColumnNotFound;
    const right_key = right_df.getSeries(on) orelse return error.ColumnNotFound;

    // Dispatch to typed implementation based on left key type
    // Both keys must be the same type (checked by tag comparison)
    if (std.meta.activeTag(left_key.*) != std.meta.activeTag(right_key.*)) return error.TypeMismatch;

    switch (left_key.*) {
        inline else => |left_s, tag| {
            const right_s = @field(right_key.*, @tagName(tag));
            return joinTyped(@TypeOf(left_s.values.items[0]), allocator, left_df, right_df, left_s, right_s, on, join_type);
        },
    }
}

fn joinTyped(
    comptime T: type,
    allocator: Allocator,
    left_df: *Dataframe,
    right_df: *Dataframe,
    left_key: *Series(T),
    right_key: *Series(T),
    on: []const u8,
    join_type: JoinType,
) !*Dataframe {
    const Ctx = GroupByContext(T);

    // Build hash map: right key value -> list of right row indices
    var right_map = std.HashMap(T, std.ArrayList(usize), Ctx, std.hash_map.default_max_load_percentage).init(allocator);
    defer {
        var it = right_map.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(allocator);
        }
        right_map.deinit();
    }

    for (right_key.values.items, 0..) |key, i| {
        const gop = try right_map.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(usize){};
        }
        try gop.value_ptr.append(allocator, i);
    }

    // Collect index pairs: (left_idx or null, right_idx or null)
    var pairs = std.ArrayList(IndexPair){};
    defer pairs.deinit(allocator);

    // Track which right rows were matched (for right/outer joins)
    var right_matched: ?[]bool = null;
    defer if (right_matched) |rm| allocator.free(rm);

    if (join_type == .right or join_type == .outer) {
        right_matched = try allocator.alloc(bool, right_key.len());
        @memset(right_matched.?, false);
    }

    // Probe left keys against right map
    for (left_key.values.items, 0..) |key, left_idx| {
        if (right_map.get(key)) |right_indices| {
            for (right_indices.items) |right_idx| {
                try pairs.append(allocator, .{ .left = left_idx, .right = right_idx });
                if (right_matched) |rm| rm[right_idx] = true;
            }
        } else {
            // No match on right side
            if (join_type == .left or join_type == .outer) {
                try pairs.append(allocator, .{ .left = left_idx, .right = null });
            }
        }
    }

    // Add unmatched right rows for right/outer joins
    if (right_matched) |rm| {
        for (rm, 0..) |matched, right_idx| {
            if (!matched) {
                try pairs.append(allocator, .{ .left = null, .right = right_idx });
            }
        }
    }

    // Build result dataframe
    const result_df = try Dataframe.init(allocator);
    errdefer result_df.deinit();

    // Add left columns
    for (left_df.series.items) |*ls| {
        try addJoinedColumn(result_df, ls, &pairs, true, allocator);
    }

    // Add right columns (skip the join key to avoid duplication)
    for (right_df.series.items) |*rs| {
        if (std.mem.eql(u8, rs.name(), on)) continue;
        try addJoinedColumn(result_df, rs, &pairs, false, allocator);
    }

    return result_df;
}

fn addJoinedColumn(
    result_df: *Dataframe,
    source: *BoxedSeries,
    pairs: *const std.ArrayList(IndexPair),
    is_left: bool,
    allocator: Allocator,
) !void {
    switch (source.*) {
        inline else => |s| {
            const ValType = @TypeOf(s.values.items[0]);
            var new_series = try Series(ValType).init(allocator);
            errdefer new_series.deinit();
            try new_series.rename(s.name.toSlice());

            for (pairs.items) |pair| {
                const idx = if (is_left) pair.left else pair.right;
                if (idx) |i| {
                    if (comptime ValType == String) {
                        var cloned = try s.values.items[i].clone();
                        errdefer cloned.deinit();
                        try new_series.values.append(allocator, cloned);
                    } else {
                        try new_series.values.append(allocator, s.values.items[i]);
                    }
                } else {
                    // Null/unmatched row: use default value
                    if (comptime ValType == String) {
                        var empty = try String.init(allocator);
                        errdefer empty.deinit();
                        try new_series.values.append(allocator, empty);
                    } else {
                        try new_series.values.append(allocator, @as(ValType, 0));
                    }
                }
            }

            try result_df.series.append(allocator, new_series.toBoxedSeries());
        },
    }
}

// --- Tests ---

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
