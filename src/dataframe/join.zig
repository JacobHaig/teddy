const std = @import("std");
const Allocator = std.mem.Allocator;

const Dataframe = @import("dataframe.zig").Dataframe;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Series = @import("series.zig").Series;
const GroupByContext = @import("group.zig").GroupByContext;
const hasMethod = @import("series.zig").hasMethod;

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
        // SQL semantics: a null key matches nothing (not even another null) —
        // it participates only as an unmatched row for right/outer joins.
        if (right_key.isNull(i)) continue;
        const gop = try right_map.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(usize).empty;
        }
        try gop.value_ptr.append(allocator, i);
    }

    // Collect index pairs: (left_idx or null, right_idx or null)
    var pairs = std.ArrayList(IndexPair).empty;
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
        if (left_key.isNull(left_idx)) {
            // Null left key: unmatched by definition (SQL NULL ≠ anything).
            if (join_type == .left or join_type == .outer) {
                try pairs.append(allocator, .{ .left = left_idx, .right = null });
            }
            continue;
        }
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
        try addJoinedColumn(result_df, ls, &pairs, true, allocator, null);
    }

    // Add right columns (skip the join key to avoid duplication).
    // If a right column's name already exists in the result (added from the
    // left side), suffix it with "_right" so both values are accessible and
    // getSeries on the original name consistently returns the left column.
    for (right_df.series.items) |*rs| {
        const col_name = rs.name();
        if (std.mem.eql(u8, col_name, on)) continue;
        if (result_df.getSeries(col_name) != null) {
            // Collision: rename right column to "{name}_right".
            const renamed = try std.fmt.allocPrint(allocator, "{s}_right", .{col_name});
            defer allocator.free(renamed);
            try addJoinedColumn(result_df, rs, &pairs, false, allocator, renamed);
        } else {
            try addJoinedColumn(result_df, rs, &pairs, false, allocator, null);
        }
    }

    return result_df;
}

fn addJoinedColumn(
    result_df: *Dataframe,
    source: *BoxedSeries,
    pairs: *const std.ArrayList(IndexPair),
    is_left: bool,
    allocator: Allocator,
    /// When non-null, the result column is given this name instead of the
    /// source column's original name (used to resolve duplicate column names
    /// between left and right sides by appending the "_right" suffix).
    override_name: ?[]const u8,
) !void {
    switch (source.*) {
        inline else => |s| {
            const ValType = @TypeOf(s.values.items[0]);
            var new_series = try Series(ValType).init(allocator);
            errdefer new_series.deinit();
            try new_series.rename(override_name orelse s.name.toSlice());

            for (pairs.items) |pair| {
                const idx = if (is_left) pair.left else pair.right;
                if (idx) |i| {
                    if (s.isNull(i)) {
                        try new_series.appendNull();
                    } else if (comptime hasMethod(ValType, "clone")) {
                        var cloned = try s.values.items[i].clone();
                        errdefer cloned.deinit();
                        try new_series.values.append(allocator, cloned);
                    } else {
                        try new_series.values.append(allocator, s.values.items[i]);
                    }
                } else {
                    // Unmatched row: a real null (validity-tracked), not a
                    // fabricated placeholder.
                    try new_series.appendNull();
                }
            }

            try result_df.series.append(allocator, new_series.toBoxedSeries());
        },
    }
}

