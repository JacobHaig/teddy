const std = @import("std");
const Allocator = @import("std").mem.Allocator;

const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Series = @import("series.zig").Series;
const Dataframe = @import("dataframe.zig").Dataframe;
const BoxedGroupBy = @import("boxed_groupby.zig").BoxedGroupBy;
const String = @import("strings.zig").String;

/// Custom hash map context that supports f32, f64, and String keys
/// in addition to the types supported by AutoHashMap.
fn GroupByContext(comptime T: type) type {
    return struct {
        const Self = @This();

        pub fn hash(_: Self, key: T) u64 {
            if (comptime T == String) {
                return std.hash.Wyhash.hash(0, key.toSlice());
            } else if (comptime T == f32) {
                const bits: u32 = @bitCast(key);
                return std.hash.Wyhash.hash(0, std.mem.asBytes(&bits));
            } else if (comptime T == f64) {
                const bits: u64 = @bitCast(key);
                return std.hash.Wyhash.hash(0, std.mem.asBytes(&bits));
            } else {
                return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            }
        }

        pub fn eql(_: Self, a: T, b: T) bool {
            if (comptime T == String) {
                return std.mem.eql(u8, a.toSlice(), b.toSlice());
            } else if (comptime T == f32 or T == f64) {
                const a_bits: if (T == f32) u32 else u64 = @bitCast(a);
                const b_bits: if (T == f32) u32 else u64 = @bitCast(b);
                return a_bits == b_bits;
            } else {
                return a == b;
            }
        }
    };
}

fn GroupMap(comptime T: type) type {
    return std.HashMap(T, std.ArrayList(usize), GroupByContext(T), std.hash_map.default_max_load_percentage);
}

/// Group struct for grouping DataFrame rows by values in a Series
/// T is the type of the group key (e.g., String, i32, etc.)
/// The Hash map maps from group key (T) to a list of row indices (usize)
///
/// Usage:
/// var group_by = try GroupBy(String).init(allocator, &dataframe);
/// defer group_by.deinit();
///
/// But generally, you would use DataFrame.groupBy method instead of directly using this struct.
pub fn GroupBy(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        dataframe: *Dataframe,
        series: *Series(T),
        groups: GroupMap(T),

        /// Initialize a GroupBy struct
        /// Will group the rows of the dataframe based on the values in the series
        /// Caller owns the returned pointer and must call deinit.
        pub fn init(allocator: Allocator, dataframe: *Dataframe, series: *Series(T)) !*Self {
            const ptr = try allocator.create(Self);
            errdefer allocator.destroy(ptr);

            ptr.allocator = allocator;
            ptr.dataframe = dataframe;
            ptr.series = series;

            ptr.groups = GroupMap(T).init(allocator);
            try ptr.setupGroups();
            return ptr;
        }

        pub fn deinit(self: *Self) void {
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit(self.allocator);
            }
            self.groups.deinit();
            self.allocator.destroy(self);
        }

        fn setupGroups(self: *Self) !void {
            const series_len = self.series.len();
            for (0..series_len) |i| {
                const key = self.series.values.items[i];

                const list_ptr = self.groups.getPtr(key);
                if (list_ptr == null) {
                    var new_list = std.ArrayList(usize){};
                    try new_list.append(self.allocator, i);
                    try self.groups.put(key, new_list);
                } else {
                    try list_ptr.?.append(self.allocator, i);
                }
            }
        }

        pub fn toBoxedGroupBy(self: *Self) BoxedGroupBy {
            return switch (T) {
                bool => BoxedGroupBy{ .bool = self },
                u8 => BoxedGroupBy{ .uint8 = self },
                u16 => BoxedGroupBy{ .uint16 = self },
                u32 => BoxedGroupBy{ .uint32 = self },
                u64 => BoxedGroupBy{ .uint64 = self },
                u128 => BoxedGroupBy{ .uint128 = self },
                usize => BoxedGroupBy{ .usize = self },
                i8 => BoxedGroupBy{ .int8 = self },
                i16 => BoxedGroupBy{ .int16 = self },
                i32 => BoxedGroupBy{ .int32 = self },
                i64 => BoxedGroupBy{ .int64 = self },
                i128 => BoxedGroupBy{ .int128 = self },
                f32 => BoxedGroupBy{ .float32 = self },
                f64 => BoxedGroupBy{ .float64 = self },
                String => BoxedGroupBy{ .string = self },
                else => @compileError("Unsupported type for GroupBy: " ++ @typeName(T)),
            };
        }

        pub fn count(self: *Self) !*Series(usize) {
            const result_series = try Series(usize).init(self.allocator);
            errdefer result_series.deinit();
            try result_series.rename("count");

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try result_series.append(entry.value_ptr.items.len);
            }

            return result_series;
        }

        pub fn sum(self: *Self, column: []const u8) !BoxedSeries {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            return switch (series_opt.?.*) {
                .int8 => |s| blk: {
                    const result = try self.sumTyped(i8, s);
                    break :blk result.toBoxedSeries();
                },
                .int16 => |s| blk: {
                    const result = try self.sumTyped(i16, s);
                    break :blk result.toBoxedSeries();
                },
                .int32 => |s| blk: {
                    const result = try self.sumTyped(i32, s);
                    break :blk result.toBoxedSeries();
                },
                .int64 => |s| blk: {
                    const result = try self.sumTyped(i64, s);
                    break :blk result.toBoxedSeries();
                },
                .uint8 => |s| blk: {
                    const result = try self.sumTyped(u8, s);
                    break :blk result.toBoxedSeries();
                },
                .uint16 => |s| blk: {
                    const result = try self.sumTyped(u16, s);
                    break :blk result.toBoxedSeries();
                },
                .uint32 => |s| blk: {
                    const result = try self.sumTyped(u32, s);
                    break :blk result.toBoxedSeries();
                },
                .uint64 => |s| blk: {
                    const result = try self.sumTyped(u64, s);
                    break :blk result.toBoxedSeries();
                },
                .float32 => |s| blk: {
                    const result = try self.sumTyped(f32, s);
                    break :blk result.toBoxedSeries();
                },
                .float64 => |s| blk: {
                    const result = try self.sumTyped(f64, s);
                    break :blk result.toBoxedSeries();
                },
                else => error.TypeNotSummable,
            };
        }

        fn sumTyped(self: *Self, comptime ValType: type, series: *Series(ValType)) !*Series(ValType) {
            const result_series = try Series(ValType).init(self.allocator);
            errdefer result_series.deinit();
            try result_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                var sum_val: ValType = 0;
                for (entry.value_ptr.items) |idx| {
                    sum_val += series.values.items[idx];
                }
                try result_series.append(sum_val);
            }

            return result_series;
        }

        pub fn mean(self: *Self, column: []const u8) !BoxedSeries {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const boxed_series = series_opt.?.*;
            return switch (boxed_series) {
                .int8 => |s| blk: {
                    const result = try self.meanTyped(i8, s);
                    break :blk result.toBoxedSeries();
                },
                .int16 => |s| blk: {
                    const result = try self.meanTyped(i16, s);
                    break :blk result.toBoxedSeries();
                },
                .int32 => |s| blk: {
                    const result = try self.meanTyped(i32, s);
                    break :blk result.toBoxedSeries();
                },
                .int64 => |s| blk: {
                    const result = try self.meanTyped(i64, s);
                    break :blk result.toBoxedSeries();
                },
                .uint8 => |s| blk: {
                    const result = try self.meanTyped(u8, s);
                    break :blk result.toBoxedSeries();
                },
                .uint16 => |s| blk: {
                    const result = try self.meanTyped(u16, s);
                    break :blk result.toBoxedSeries();
                },
                .uint32 => |s| blk: {
                    const result = try self.meanTyped(u32, s);
                    break :blk result.toBoxedSeries();
                },
                .uint64 => |s| blk: {
                    const result = try self.meanTyped(u64, s);
                    break :blk result.toBoxedSeries();
                },
                .float32 => |s| blk: {
                    const result = try self.meanTyped(f32, s);
                    break :blk result.toBoxedSeries();
                },
                .float64 => |s| blk: {
                    const result = try self.meanTyped(f64, s);
                    break :blk result.toBoxedSeries();
                },
                else => error.TypeNotAverageable,
            };
        }

        fn meanTyped(self: *Self, comptime ValType: type, series: *Series(ValType)) !*Series(f64) {
            const result_series = try Series(f64).init(self.allocator);
            errdefer result_series.deinit();
            try result_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                var sum_val: f64 = 0.0;
                for (entry.value_ptr.items) |idx| {
                    const val = series.values.items[idx];
                    sum_val += @as(f64, if (ValType == f32 or ValType == f64)
                        val
                    else
                        @floatFromInt(val));
                }
                const mean_val = sum_val / @as(f64, @floatFromInt(entry.value_ptr.items.len));
                try result_series.append(mean_val);
            }

            return result_series;
        }
    };
}
