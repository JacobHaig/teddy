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
    isize: *Series(isize),
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

    /// Returns true if the value at index i is null/missing.
    pub fn isNull(self: *Self, i: usize) bool {
        switch (self.*) {
            inline else => |s| return s.*.isNull(i),
        }
    }

    /// Returns the number of null values in the series.
    pub fn nullCount(self: *Self) usize {
        switch (self.*) {
            inline else => |s| return s.*.nullCount(),
        }
    }

    /// Returns true if the series contains any null values.
    pub fn hasNulls(self: *Self) bool {
        switch (self.*) {
            inline else => |s| return s.*.hasNulls(),
        }
    }

    /// Appends a null/missing value to the series.
    pub fn appendNull(self: *Self) !void {
        switch (self.*) {
            inline else => |s| return s.*.appendNull(),
        }
    }

    /// Returns the sum of non-null values as f64. Returns null if not a numeric type.
    pub fn sum(self: *Self) ?f64 {
        switch (self.*) {
            inline .int8, .int16, .int32, .int64, .int128, .isize => |s| return @as(f64, @floatFromInt(s.sum())),
            inline .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| return @as(f64, @floatFromInt(s.sum())),
            .float32 => |s| return @as(f64, s.sum()),
            .float64 => |s| return s.sum(),
            else => return null,
        }
    }

    /// Returns the minimum non-null value as f64. Returns null if not numeric or all-null.
    pub fn min(self: *Self) ?f64 {
        switch (self.*) {
            inline .int8, .int16, .int32, .int64, .int128, .isize => |s| return if (s.min()) |v| @as(f64, @floatFromInt(v)) else null,
            inline .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| return if (s.min()) |v| @as(f64, @floatFromInt(v)) else null,
            .float32 => |s| return if (s.min()) |v| @as(f64, v) else null,
            .float64 => |s| return s.min(),
            else => return null,
        }
    }

    /// Returns the maximum non-null value as f64. Returns null if not numeric or all-null.
    pub fn max(self: *Self) ?f64 {
        switch (self.*) {
            inline .int8, .int16, .int32, .int64, .int128, .isize => |s| return if (s.max()) |v| @as(f64, @floatFromInt(v)) else null,
            inline .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| return if (s.max()) |v| @as(f64, @floatFromInt(v)) else null,
            .float32 => |s| return if (s.max()) |v| @as(f64, v) else null,
            .float64 => |s| return s.max(),
            else => return null,
        }
    }

    /// Returns the mean of non-null values as f64. Returns null if not numeric.
    pub fn mean(self: *Self) ?f64 {
        switch (self.*) {
            .string, .bool => return null,
            inline else => |s| return s.*.mean(),
        }
    }

    /// Returns the population stddev of non-null values as f64. Returns null if not numeric.
    pub fn stdDev(self: *Self) ?f64 {
        switch (self.*) {
            .string, .bool => return null,
            inline else => |s| return s.*.stdDev(),
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

    /// Comptime-verified lossless cast. Compile error if the conversion could lose data.
    pub fn castSafe(self: *Self, comptime Target: type) !BoxedSeries {
        switch (self.*) {
            inline else => |s| return (try s.castSafe(Target)).toBoxedSeries(),
        }
    }

    /// Strict cast. Returns error on overflow, non-integer float→int, or String parse failure.
    pub fn cast(self: *Self, comptime Target: type) !BoxedSeries {
        switch (self.*) {
            inline else => |s| return (try s.cast(Target)).toBoxedSeries(),
        }
    }

    /// Permissive cast. Conversion failures become null; float→int truncates.
    pub fn castLossy(self: *Self, comptime Target: type) !BoxedSeries {
        switch (self.*) {
            inline else => |s| return (try s.castLossy(Target)).toBoxedSeries(),
        }
    }

    /// Checked sum as f64. Returns error.Overflow if any integer accumulation overflows.
    pub fn sumChecked(self: *Self) !f64 {
        return switch (self.*) {
            .int8, .int16, .int32, .int64, .int128, .isize => |s| @as(f64, @floatFromInt(try s.sumChecked())),
            .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| @as(f64, @floatFromInt(try s.sumChecked())),
            .float32 => |s| @as(f64, try s.sumChecked()),
            .float64 => |s| try s.sumChecked(),
            else => error.TypeNotNumeric,
        };
    }

    /// Product of non-null values as f64. Returns null for non-numeric types.
    pub fn prod(self: *Self) ?f64 {
        return switch (self.*) {
            .int8, .int16, .int32, .int64, .int128, .isize => |s| @as(f64, @floatFromInt(s.prod())),
            .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| @as(f64, @floatFromInt(s.prod())),
            .float32 => |s| @as(f64, s.prod()),
            .float64 => |s| s.prod(),
            else => null,
        };
    }

    /// First non-null value as f64. Returns null for non-numeric or all-null.
    pub fn first(self: *Self) ?f64 {
        return switch (self.*) {
            .int8, .int16, .int32, .int64, .int128, .isize => |s| if (s.first()) |v| @as(f64, @floatFromInt(v)) else null,
            .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| if (s.first()) |v| @as(f64, @floatFromInt(v)) else null,
            .float32 => |s| if (s.first()) |v| @as(f64, v) else null,
            .float64 => |s| s.first(),
            else => null,
        };
    }

    /// Last non-null value as f64. Returns null for non-numeric or all-null.
    pub fn last(self: *Self) ?f64 {
        return switch (self.*) {
            .int8, .int16, .int32, .int64, .int128, .isize => |s| if (s.last()) |v| @as(f64, @floatFromInt(v)) else null,
            .uint8, .uint16, .uint32, .uint64, .uint128, .usize => |s| if (s.last()) |v| @as(f64, @floatFromInt(v)) else null,
            .float32 => |s| if (s.last()) |v| @as(f64, v) else null,
            .float64 => |s| s.last(),
            else => null,
        };
    }

    /// Median of non-null values as f64. Returns null for non-numeric or all-null.
    pub fn median(self: *Self, allocator: std.mem.Allocator) !?f64 {
        return switch (self.*) {
            .string, .bool => null,
            inline else => |s| s.median(allocator),
        };
    }

    /// Quantile of non-null values. q must be in [0,1]. Returns null for non-numeric.
    pub fn quantile(self: *Self, allocator: std.mem.Allocator, q: f64) !?f64 {
        return switch (self.*) {
            .string, .bool => null,
            inline else => |s| s.quantile(allocator, q),
        };
    }

    /// Count of distinct non-null values.
    pub fn nunique(self: *Self, allocator: std.mem.Allocator) !usize {
        switch (self.*) {
            inline else => |s| return s.nunique(allocator),
        }
    }

    /// Running cumulative sum. Nulls propagate. Numeric columns only.
    pub fn cumSum(self: *Self) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.cumSum()).toBoxedSeries(),
        };
    }

    /// Running cumulative minimum.
    pub fn cumMin(self: *Self) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.cumMin()).toBoxedSeries(),
        };
    }

    /// Running cumulative maximum.
    pub fn cumMax(self: *Self) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.cumMax()).toBoxedSeries(),
        };
    }

    /// Running cumulative product.
    pub fn cumProd(self: *Self) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.cumProd()).toBoxedSeries(),
        };
    }

    /// Shift values by n positions. Works on all column types.
    pub fn shift(self: *Self, n: i64) !BoxedSeries {
        switch (self.*) {
            inline else => |s| return (try s.shift(n)).toBoxedSeries(),
        }
    }

    /// Strict element-wise difference. Numeric only.
    pub fn diff(self: *Self, n: usize) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.diff(n)).toBoxedSeries(),
        };
    }

    /// Permissive element-wise difference. Underflow/overflow → null.
    pub fn diffLossy(self: *Self, n: usize) !BoxedSeries {
        return switch (self.*) {
            .string, .bool => error.TypeNotNumeric,
            inline else => |s| (try s.diffLossy(n)).toBoxedSeries(),
        };
    }

    /// Clamp values to [lower, upper]. Type must match the column.
    pub fn clip(self: *Self, comptime T: type, lower: T, upper: T) !BoxedSeries {
        switch (self.*) {
            inline else => |s| {
                if (comptime *Series(T) == @TypeOf(s)) {
                    return (try s.clip(lower, upper)).toBoxedSeries();
                }
            },
        }
        return error.TypeMismatch;
    }

    /// Replace exact matches. Type must match the column.
    pub fn replace(self: *Self, comptime T: type, old_value: T, new_value: T) !BoxedSeries {
        switch (self.*) {
            inline else => |s| {
                if (comptime *Series(T) == @TypeOf(s)) {
                    return (try s.replace(old_value, new_value)).toBoxedSeries();
                }
            },
        }
        return error.TypeMismatch;
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

    /// Appends all rows from another BoxedSeries of the same type, preserving nulls.
    pub fn appendSeries(self: *Self, other: *const Self) !void {
        if (std.meta.activeTag(self.*) != std.meta.activeTag(other.*)) return error.TypeMismatch;
        switch (self.*) {
            inline else => |s, tag| {
                const o = @field(other.*, @tagName(tag));

                for (0..o.values.items.len) |i| {
                    if (o.isNull(i)) {
                        try s.appendNull();
                    } else if (comptime @TypeOf(o.values.items[i]) == String) {
                        var cloned = try o.values.items[i].clone();
                        errdefer cloned.deinit();
                        try s.append(cloned);
                    } else {
                        try s.append(o.values.items[i]);
                    }
                }
            },
        }
    }

    /// Returns indices where comparison holds for the given typed value.
    pub fn indicesWhere(self: *Self, comptime T: type, allocator: std.mem.Allocator, op: CompareOp, value: T) !std.ArrayList(usize) {
        switch (self.*) {
            inline else => |s| {
                if (comptime *Series(T) == @TypeOf(s)) {
                    var indices = std.ArrayList(usize).empty;
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
            .isize => "isize",
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
            .isize => return isize,
            .float32 => return f32,
            .float64 => return f64,
            .string => return String,
        }
    }
};
