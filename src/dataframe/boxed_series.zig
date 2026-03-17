const std = @import("std");
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;
const GroupBy = @import("group.zig").GroupBy;
const BoxedGroupBy = @import("boxed_groupby.zig").BoxedGroupBy;
const Dataframe = @import("dataframe.zig").Dataframe;

pub const CompareOp = enum { eq, neq, lt, lte, gt, gte };

pub const BoxedSeries = union(enum) {
    const Self = @This();

    bool: *Series(bool),
    uint8: *Series(u8),
    uint16: *Series(u16),
    uint32: *Series(u32),
    uint64: *Series(u64),
    uint128: *Series(u128),
    usize: *Series(usize),
    int8: *Series(i8),
    int16: *Series(i16),
    int32: *Series(i32),
    int64: *Series(i64),
    int128: *Series(i128),
    float32: *Series(f32),
    float64: *Series(f64),
    string: *Series(String),

    /// Deallocates the contained Series. After this call, the BoxedSeries is invalid.
    pub fn deinit(self: *Self) void {
        switch (self.*) {
            inline else => |p| p.deinit(),
        }
    }

    pub fn print(self: *const Self) void {
        switch (self.*) {
            inline else => |p| p.print(),
        }
    }

    pub fn len(self: *const Self) usize {
        switch (self.*) {
            inline else => |p| return p.len(),
        }
    }

    pub fn name(self: *const Self) []const u8 {
        switch (self.*) {
            inline else => |p| return p.name.toSlice(),
        }
    }

    pub fn dropRow(self: *Self, index: usize) void {
        switch (self.*) {
            inline else => |p| p.dropRow(index),
        }
    }

    pub fn applyInplace(self: *Self, comptime T: type, comptime func: fn (x: T) T) void {
        switch (self.*) {
            inline else => |s| {
                // std.debug.print("Type: {}\n", .{@TypeOf(s)});
                if (comptime *Series(T) == @TypeOf(s)) {
                    s.*.applyInplace(func);
                }
            },
        }
    }

    pub fn rename(self: *Self, new_name: []const u8) !void {
        switch (self.*) {
            inline else => |s| try s.rename(new_name),
        }
    }

    /// Returns a new BoxedSeries containing a deep-copied Series. Caller must call deinit on the returned BoxedSeries.
    pub fn deepCopy(self: *Self) !Self {
        switch (self.*) {
            inline else => |s| {
                const series = try s.*.deepCopy();
                return series.toBoxedSeries();
            },
        }
    }

    pub fn limit(self: *Self, n_limit: usize) void {
        switch (self.*) {
            inline else => |s| s.*.limit(n_limit),
        }
    }

    pub fn printAt(self: *Self, n: usize) void {
        switch (self.*) {
            inline else => |s| s.*.printAt(n),
        }
    }

    pub fn asStringAt(self: *Self, n: usize) !String {
        switch (self.*) {
            inline else => |s| return s.*.asStringAt(n),
        }
    }

    pub fn getNameOwned(self: *const Self) !String {
        switch (self.*) {
            inline else => |p| return p.getNameOwned(),
        }
    }

    pub fn getTypeAsString(self: *Self) !String {
        switch (self.*) {
            inline else => |s| return s.*.getTypeAsString(),
        }
    }

    pub fn groupBy(self: *Self, allocator: std.mem.Allocator, dataframe: *Dataframe) !BoxedGroupBy {
        switch (self.*) {
            inline else => |s| {
                const gb = try s.*.groupBy(allocator, dataframe);
                return gb.toBoxedGroupBy();
            },
        }
    }

    /// Creates a new BoxedSeries containing only the rows at the given indices.
    pub fn filterByIndices(self: *Self, indices: []const usize) !BoxedSeries {
        switch (self.*) {
            inline else => |s| {
                const new_s = try s.filterByIndices(indices);
                return new_s.toBoxedSeries();
            },
        }
    }

    /// Returns indices that would sort the contained series.
    pub fn argSort(self: *Self, allocator: std.mem.Allocator, ascending: bool) !std.ArrayList(usize) {
        switch (self.*) {
            inline else => |s| return s.argSort(allocator, ascending),
        }
    }

    /// Returns indices of first occurrence of each unique value.
    pub fn uniqueIndices(self: *Self, allocator: std.mem.Allocator) !std.ArrayList(usize) {
        switch (self.*) {
            inline else => |s| return s.uniqueIndices(allocator),
        }
    }

    /// Appends all values from another BoxedSeries of the same type.
    pub fn appendFrom(self: *Self, other: *const Self) !void {
        if (std.meta.activeTag(self.*) != std.meta.activeTag(other.*)) return error.TypeMismatch;
        switch (self.*) {
            inline else => |s, tag| {
                const o = @field(other.*, @tagName(tag));
                for (o.values.items) |*val| {
                    if (comptime @TypeOf(val.*) == String) {
                        var cloned = try val.clone();
                        errdefer cloned.deinit();
                        try s.values.append(s.allocator, cloned);
                    } else {
                        try s.values.append(s.allocator, val.*);
                    }
                }
            },
        }
    }

    /// Returns indices where comparison holds for the given typed value.
    pub fn filterIndices(self: *Self, comptime T: type, allocator: std.mem.Allocator, op: CompareOp, value: T) !std.ArrayList(usize) {
        switch (self.*) {
            inline else => |s| {
                if (comptime *Series(T) == @TypeOf(s)) {
                    var indices = std.ArrayList(usize){};
                    for (s.values.items, 0..) |item, i| {
                        const match = if (comptime T == String) blk: {
                            const ord = std.mem.order(u8, item.toSlice(), value.toSlice());
                            break :blk switch (op) {
                                .eq => ord == .eq,
                                .neq => ord != .eq,
                                .lt => ord == .lt,
                                .lte => ord == .lt or ord == .eq,
                                .gt => ord == .gt,
                                .gte => ord == .gt or ord == .eq,
                            };
                        } else if (comptime T == bool) blk: {
                            break :blk switch (op) {
                                .eq => item == value,
                                .neq => item != value,
                                else => false,
                            };
                        } else blk: {
                            break :blk switch (op) {
                                .eq => item == value,
                                .neq => item != value,
                                .lt => item < value,
                                .lte => item <= value,
                                .gt => item > value,
                                .gte => item >= value,
                            };
                        };
                        if (match) try indices.append(allocator, i);
                    }
                    return indices;
                }
            },
        }
        // If we get here, no branch matched the requested type T
        std.debug.print("filter type mismatch: column \"{s}\" has type {s}, but filter was called with type {s}\n", .{
            self.name(),
            self.typeName(),
            @typeName(T),
        });
        return error.TypeMismatch;
    }

    /// Returns a human-readable name for the contained series type.
    pub fn typeName(self: *const Self) []const u8 {
        return switch (self.*) {
            .bool => "bool",
            .uint8 => "u8",
            .uint16 => "u16",
            .uint32 => "u32",
            .uint64 => "u64",
            .uint128 => "u128",
            .usize => "usize",
            .int8 => "i8",
            .int16 => "i16",
            .int32 => "i32",
            .int64 => "i64",
            .int128 => "i128",
            .float32 => "f32",
            .float64 => "f64",
            .string => "String",
        };
    }

    /// Returns the type of the contained Series
    /// It also can not be used at runtime
    pub fn getType(self: *Self) type {
        switch (self.*) {
            .bool => return bool,
            .uint8 => return u8,
            .uint16 => return u16,
            .uint32 => return u32,
            .uint64 => return u64,
            .uint128 => return u128,
            .usize => return usize,
            .int8 => return i8,
            .int16 => return i16,
            .int32 => return i32,
            .int64 => return i64,
            .int128 => return i128,
            .float32 => return f32,
            .float64 => return f64,
            .string => return String,
        }
    }
};
