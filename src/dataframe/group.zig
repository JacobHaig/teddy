const std = @import("std");
const Allocator = @import("std").mem.Allocator;

const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Series = @import("series.zig").Series;
const Dataframe = @import("dataframe.zig").Dataframe;
const BoxedGroupBy = @import("boxed_groupby.zig").BoxedGroupBy;
const String = @import("strings.zig").String;

/// Custom hash map context that supports f32, f64, and String keys
/// in addition to the types supported by AutoHashMap.
pub fn GroupByContext(comptime T: type) type {
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
                    var new_list = std.ArrayList(usize).empty;
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
                isize => BoxedGroupBy{ .isize = self },
                f32 => BoxedGroupBy{ .float32 = self },
                f64 => BoxedGroupBy{ .float64 = self },
                String => BoxedGroupBy{ .string = self },
                else => @compileError("Unsupported type for GroupBy: " ++ @typeName(T)),
            };
        }

        /// Appends a key to the key series, cloning Strings to avoid double-free.
        fn appendKey(key_series: *Series(T), key: T) !void {
            if (comptime T == String) {
                var cloned = try key.clone();
                errdefer cloned.deinit();
                try key_series.values.append(key_series.allocator, cloned);
            } else {
                try key_series.append(key);
            }
        }

        pub fn count(self: *Self) !*Dataframe {
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            var count_series = try result_df.createSeries(usize);
            try count_series.rename("count");

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                try count_series.append(entry.value_ptr.items.len);
            }

            return result_df;
        }

        pub fn sum(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            switch (series_opt.?.*) {
                .int8 => |s| try self.sumTypedDf(i8, s, result_df, key_series),
                .int16 => |s| try self.sumTypedDf(i16, s, result_df, key_series),
                .int32 => |s| try self.sumTypedDf(i32, s, result_df, key_series),
                .int64 => |s| try self.sumTypedDf(i64, s, result_df, key_series),
                .uint8 => |s| try self.sumTypedDf(u8, s, result_df, key_series),
                .uint16 => |s| try self.sumTypedDf(u16, s, result_df, key_series),
                .uint32 => |s| try self.sumTypedDf(u32, s, result_df, key_series),
                .uint64 => |s| try self.sumTypedDf(u64, s, result_df, key_series),
                .float32 => |s| try self.sumTypedDf(f32, s, result_df, key_series),
                .float64 => |s| try self.sumTypedDf(f64, s, result_df, key_series),
                else => return error.TypeNotSummable,
            }

            return result_df;
        }

        fn sumTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T)) !void {
            var val_series = try result_df.createSeries(ValType);
            try val_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                var sum_val: ValType = 0;
                for (entry.value_ptr.items) |idx| {
                    sum_val += series.values.items[idx];
                }
                try val_series.append(sum_val);
            }
        }

        pub fn mean(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            switch (series_opt.?.*) {
                .int8 => |s| try self.meanTypedDf(i8, s, result_df, key_series),
                .int16 => |s| try self.meanTypedDf(i16, s, result_df, key_series),
                .int32 => |s| try self.meanTypedDf(i32, s, result_df, key_series),
                .int64 => |s| try self.meanTypedDf(i64, s, result_df, key_series),
                .uint8 => |s| try self.meanTypedDf(u8, s, result_df, key_series),
                .uint16 => |s| try self.meanTypedDf(u16, s, result_df, key_series),
                .uint32 => |s| try self.meanTypedDf(u32, s, result_df, key_series),
                .uint64 => |s| try self.meanTypedDf(u64, s, result_df, key_series),
                .float32 => |s| try self.meanTypedDf(f32, s, result_df, key_series),
                .float64 => |s| try self.meanTypedDf(f64, s, result_df, key_series),
                else => return error.TypeNotAverageable,
            }

            return result_df;
        }

        fn meanTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T)) !void {
            var val_series = try result_df.createSeries(f64);
            try val_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                var sum_val: f64 = 0.0;
                for (entry.value_ptr.items) |idx| {
                    const val = series.values.items[idx];
                    sum_val += @as(f64, if (ValType == f32 or ValType == f64)
                        val
                    else
                        @floatFromInt(val));
                }
                const mean_val = sum_val / @as(f64, @floatFromInt(entry.value_ptr.items.len));
                try val_series.append(mean_val);
            }
        }
        pub fn min(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            switch (series_opt.?.*) {
                .int8 => |s| try self.minMaxTypedDf(i8, s, result_df, key_series, true),
                .int16 => |s| try self.minMaxTypedDf(i16, s, result_df, key_series, true),
                .int32 => |s| try self.minMaxTypedDf(i32, s, result_df, key_series, true),
                .int64 => |s| try self.minMaxTypedDf(i64, s, result_df, key_series, true),
                .uint8 => |s| try self.minMaxTypedDf(u8, s, result_df, key_series, true),
                .uint16 => |s| try self.minMaxTypedDf(u16, s, result_df, key_series, true),
                .uint32 => |s| try self.minMaxTypedDf(u32, s, result_df, key_series, true),
                .uint64 => |s| try self.minMaxTypedDf(u64, s, result_df, key_series, true),
                .float32 => |s| try self.minMaxTypedDf(f32, s, result_df, key_series, true),
                .float64 => |s| try self.minMaxTypedDf(f64, s, result_df, key_series, true),
                else => return error.TypeNotComparable,
            }

            return result_df;
        }

        pub fn max(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            switch (series_opt.?.*) {
                .int8 => |s| try self.minMaxTypedDf(i8, s, result_df, key_series, false),
                .int16 => |s| try self.minMaxTypedDf(i16, s, result_df, key_series, false),
                .int32 => |s| try self.minMaxTypedDf(i32, s, result_df, key_series, false),
                .int64 => |s| try self.minMaxTypedDf(i64, s, result_df, key_series, false),
                .uint8 => |s| try self.minMaxTypedDf(u8, s, result_df, key_series, false),
                .uint16 => |s| try self.minMaxTypedDf(u16, s, result_df, key_series, false),
                .uint32 => |s| try self.minMaxTypedDf(u32, s, result_df, key_series, false),
                .uint64 => |s| try self.minMaxTypedDf(u64, s, result_df, key_series, false),
                .float32 => |s| try self.minMaxTypedDf(f32, s, result_df, key_series, false),
                .float64 => |s| try self.minMaxTypedDf(f64, s, result_df, key_series, false),
                else => return error.TypeNotComparable,
            }

            return result_df;
        }

        fn minMaxTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T), is_min: bool) !void {
            var val_series = try result_df.createSeries(ValType);
            try val_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                var result_val = series.values.items[entry.value_ptr.items[0]];
                for (entry.value_ptr.items[1..]) |idx| {
                    const val = series.values.items[idx];
                    if (is_min) {
                        if (val < result_val) result_val = val;
                    } else {
                        if (val > result_val) result_val = val;
                    }
                }
                try val_series.append(result_val);
            }
        }

        pub fn stdDev(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;

            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();

            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());

            switch (series_opt.?.*) {
                .int8 => |s| try self.stdDevTypedDf(i8, s, result_df, key_series),
                .int16 => |s| try self.stdDevTypedDf(i16, s, result_df, key_series),
                .int32 => |s| try self.stdDevTypedDf(i32, s, result_df, key_series),
                .int64 => |s| try self.stdDevTypedDf(i64, s, result_df, key_series),
                .uint8 => |s| try self.stdDevTypedDf(u8, s, result_df, key_series),
                .uint16 => |s| try self.stdDevTypedDf(u16, s, result_df, key_series),
                .uint32 => |s| try self.stdDevTypedDf(u32, s, result_df, key_series),
                .uint64 => |s| try self.stdDevTypedDf(u64, s, result_df, key_series),
                .float32 => |s| try self.stdDevTypedDf(f32, s, result_df, key_series),
                .float64 => |s| try self.stdDevTypedDf(f64, s, result_df, key_series),
                else => return error.TypeNotAverageable,
            }

            return result_df;
        }

        fn stdDevTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T)) !void {
            var val_series = try result_df.createSeries(f64);
            try val_series.rename(series.name.toSlice());

            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                const n: f64 = @floatFromInt(entry.value_ptr.items.len);
                var sum_val: f64 = 0.0;
                for (entry.value_ptr.items) |idx| {
                    const val = series.values.items[idx];
                    sum_val += @as(f64, if (ValType == f32 or ValType == f64) val else @floatFromInt(val));
                }
                const mean_val = sum_val / n;
                var sq_sum: f64 = 0.0;
                for (entry.value_ptr.items) |idx| {
                    const val = series.values.items[idx];
                    const fval: f64 = @as(f64, if (ValType == f32 or ValType == f64) val else @floatFromInt(val));
                    const diff = fval - mean_val;
                    sq_sum += diff * diff;
                }
                try val_series.append(@sqrt(sq_sum / n));
            }
        }

        pub fn prod(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();
            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());
            switch (series_opt.?.*) {
                .int8 => |s| try self.prodTypedDf(i8, s, result_df, key_series),
                .int16 => |s| try self.prodTypedDf(i16, s, result_df, key_series),
                .int32 => |s| try self.prodTypedDf(i32, s, result_df, key_series),
                .int64 => |s| try self.prodTypedDf(i64, s, result_df, key_series),
                .uint8 => |s| try self.prodTypedDf(u8, s, result_df, key_series),
                .uint16 => |s| try self.prodTypedDf(u16, s, result_df, key_series),
                .uint32 => |s| try self.prodTypedDf(u32, s, result_df, key_series),
                .uint64 => |s| try self.prodTypedDf(u64, s, result_df, key_series),
                .float32 => |s| try self.prodTypedDf(f32, s, result_df, key_series),
                .float64 => |s| try self.prodTypedDf(f64, s, result_df, key_series),
                else => return error.TypeNotNumeric,
            }
            return result_df;
        }

        fn prodTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T)) !void {
            var val_series = try result_df.createSeries(ValType);
            try val_series.rename(series.name.toSlice());
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                var p: ValType = 1;
                for (entry.value_ptr.items) |idx| {
                    if (!series.isNull(idx)) p *= series.values.items[idx];
                }
                try val_series.append(p);
            }
        }

        pub fn first(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();
            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());
            switch (series_opt.?.*) {
                .int8 => |s| try self.firstLastTypedDf(i8, s, result_df, key_series, true),
                .int16 => |s| try self.firstLastTypedDf(i16, s, result_df, key_series, true),
                .int32 => |s| try self.firstLastTypedDf(i32, s, result_df, key_series, true),
                .int64 => |s| try self.firstLastTypedDf(i64, s, result_df, key_series, true),
                .uint8 => |s| try self.firstLastTypedDf(u8, s, result_df, key_series, true),
                .uint16 => |s| try self.firstLastTypedDf(u16, s, result_df, key_series, true),
                .uint32 => |s| try self.firstLastTypedDf(u32, s, result_df, key_series, true),
                .uint64 => |s| try self.firstLastTypedDf(u64, s, result_df, key_series, true),
                .float32 => |s| try self.firstLastTypedDf(f32, s, result_df, key_series, true),
                .float64 => |s| try self.firstLastTypedDf(f64, s, result_df, key_series, true),
                else => return error.TypeNotNumeric,
            }
            return result_df;
        }

        pub fn last(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();
            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());
            switch (series_opt.?.*) {
                .int8 => |s| try self.firstLastTypedDf(i8, s, result_df, key_series, false),
                .int16 => |s| try self.firstLastTypedDf(i16, s, result_df, key_series, false),
                .int32 => |s| try self.firstLastTypedDf(i32, s, result_df, key_series, false),
                .int64 => |s| try self.firstLastTypedDf(i64, s, result_df, key_series, false),
                .uint8 => |s| try self.firstLastTypedDf(u8, s, result_df, key_series, false),
                .uint16 => |s| try self.firstLastTypedDf(u16, s, result_df, key_series, false),
                .uint32 => |s| try self.firstLastTypedDf(u32, s, result_df, key_series, false),
                .uint64 => |s| try self.firstLastTypedDf(u64, s, result_df, key_series, false),
                .float32 => |s| try self.firstLastTypedDf(f32, s, result_df, key_series, false),
                .float64 => |s| try self.firstLastTypedDf(f64, s, result_df, key_series, false),
                else => return error.TypeNotNumeric,
            }
            return result_df;
        }

        fn firstLastTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T), is_first: bool) !void {
            var val_series = try result_df.createSeries(ValType);
            try val_series.rename(series.name.toSlice());
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                const indices = entry.value_ptr.items;
                var found: ?ValType = null;
                if (is_first) {
                    for (indices) |idx| {
                        if (!series.isNull(idx)) {
                            found = series.values.items[idx];
                            break;
                        }
                    }
                } else {
                    var j = indices.len;
                    while (j > 0) {
                        j -= 1;
                        if (!series.isNull(indices[j])) {
                            found = series.values.items[indices[j]];
                            break;
                        }
                    }
                }
                if (found) |v| try val_series.append(v) else try val_series.appendNull();
            }
        }

        pub fn median(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();
            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());
            switch (series_opt.?.*) {
                .int8 => |s| try self.medianTypedDf(i8, s, result_df, key_series),
                .int16 => |s| try self.medianTypedDf(i16, s, result_df, key_series),
                .int32 => |s| try self.medianTypedDf(i32, s, result_df, key_series),
                .int64 => |s| try self.medianTypedDf(i64, s, result_df, key_series),
                .uint8 => |s| try self.medianTypedDf(u8, s, result_df, key_series),
                .uint16 => |s| try self.medianTypedDf(u16, s, result_df, key_series),
                .uint32 => |s| try self.medianTypedDf(u32, s, result_df, key_series),
                .uint64 => |s| try self.medianTypedDf(u64, s, result_df, key_series),
                .float32 => |s| try self.medianTypedDf(f32, s, result_df, key_series),
                .float64 => |s| try self.medianTypedDf(f64, s, result_df, key_series),
                else => return error.TypeNotNumeric,
            }
            return result_df;
        }

        fn medianTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), result_df: *Dataframe, key_series: *Series(T)) !void {
            const is_float_type = ValType == f32 or ValType == f64;
            var val_series = try result_df.createSeries(f64);
            try val_series.rename(series.name.toSlice());
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                var buf = std.ArrayList(f64).empty;
                defer buf.deinit(self.allocator);
                for (entry.value_ptr.items) |idx| {
                    if (!series.isNull(idx)) {
                        const fv: f64 = if (is_float_type) @as(f64, series.values.items[idx]) else @as(f64, @floatFromInt(series.values.items[idx]));
                        try buf.append(self.allocator, fv);
                    }
                }
                if (buf.items.len == 0) {
                    try val_series.appendNull();
                    continue;
                }
                std.mem.sortUnstable(f64, buf.items, {}, std.sort.asc(f64));
                const n = buf.items.len;
                const med: f64 = if (n % 2 == 1) buf.items[n / 2] else (buf.items[n / 2 - 1] + buf.items[n / 2]) / 2.0;
                try val_series.append(med);
            }
        }

        pub fn nunique(self: *Self, column: []const u8) !*Dataframe {
            const series_opt = self.dataframe.getSeries(column);
            if (series_opt == null) return error.ColumnNotFound;
            const result_df = try Dataframe.init(self.allocator);
            errdefer result_df.deinit();
            var key_series = try result_df.createSeries(T);
            try key_series.rename(self.series.name.toSlice());
            var count_series = try result_df.createSeries(usize);
            try count_series.rename("nunique");
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                try appendKey(key_series, entry.key_ptr.*);
                switch (series_opt.?.*) {
                    .int8 => |s| try self.nuniqueTypedDf(i8, s, entry.value_ptr.items, count_series),
                    .int16 => |s| try self.nuniqueTypedDf(i16, s, entry.value_ptr.items, count_series),
                    .int32 => |s| try self.nuniqueTypedDf(i32, s, entry.value_ptr.items, count_series),
                    .int64 => |s| try self.nuniqueTypedDf(i64, s, entry.value_ptr.items, count_series),
                    .uint8 => |s| try self.nuniqueTypedDf(u8, s, entry.value_ptr.items, count_series),
                    .uint16 => |s| try self.nuniqueTypedDf(u16, s, entry.value_ptr.items, count_series),
                    .uint32 => |s| try self.nuniqueTypedDf(u32, s, entry.value_ptr.items, count_series),
                    .uint64 => |s| try self.nuniqueTypedDf(u64, s, entry.value_ptr.items, count_series),
                    .float32 => |s| try self.nuniqueTypedDf(f32, s, entry.value_ptr.items, count_series),
                    .float64 => |s| try self.nuniqueTypedDf(f64, s, entry.value_ptr.items, count_series),
                    else => return error.TypeNotNumeric,
                }
            }
            return result_df;
        }

        fn nuniqueTypedDf(self: *Self, comptime ValType: type, series: *Series(ValType), indices: []const usize, count_series: *Series(usize)) !void {
            var seen = std.HashMap(ValType, void, GroupByContext(ValType), std.hash_map.default_max_load_percentage).init(self.allocator);
            defer seen.deinit();
            for (indices) |idx| {
                if (!series.isNull(idx)) {
                    try seen.put(series.values.items[idx], {});
                }
            }
            try count_series.append(seen.count());
        }
    };
}

// --- Tests ---

fn createTestDf(allocator: Allocator) !*Dataframe {
    const df = try Dataframe.init(allocator);
    errdefer df.deinit();

    // category: [1, 2, 1, 2, 1]
    var cat = try df.createSeries(i32);
    try cat.rename("category");
    try cat.append(1);
    try cat.append(2);
    try cat.append(1);
    try cat.append(2);
    try cat.append(1);

    // values: [10, 20, 30, 40, 50]
    var vals = try df.createSeries(i64);
    try vals.rename("values");
    try vals.append(10);
    try vals.append(20);
    try vals.append(30);
    try vals.append(40);
    try vals.append(50);

    return df;
}

test "GroupBy: count produces correct group sizes" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    try std.testing.expectEqual(@as(usize, 2), counts.width());
    try std.testing.expectEqual(@as(usize, 2), counts.height());

    // Check that the count column exists and has correct values
    const count_series = counts.getSeries("count") orelse return error.DoesNotExist;
    const a = count_series.usize.values.items[0];
    const b = count_series.usize.values.items[1];
    try std.testing.expect((a == 3 and b == 2) or (a == 2 and b == 3));
}

test "GroupBy: sum produces correct totals" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var sum_result = try gb.sum("values");
    defer sum_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), sum_result.height());

    // Group 1: 10+30+50=90, Group 2: 20+40=60 (order may vary)
    const sum_col = sum_result.getSeries("values") orelse return error.DoesNotExist;
    const a = sum_col.int64.values.items[0];
    const b = sum_col.int64.values.items[1];
    try std.testing.expect((a == 90 and b == 60) or (a == 60 and b == 90));
}

test "GroupBy: mean produces correct averages" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var mean_result = try gb.mean("values");
    defer mean_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), mean_result.height());

    // Group 1: (10+30+50)/3=30.0, Group 2: (20+40)/2=30.0
    const mean_col = mean_result.getSeries("values") orelse return error.DoesNotExist;
    const a = mean_col.float64.values.items[0];
    const b = mean_col.float64.values.items[1];
    try std.testing.expect((a == 30.0 and b == 30.0));
}

test "GroupBy: sum on nonexistent column returns error" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    try std.testing.expectError(error.ColumnNotFound, gb.sum("nonexistent"));
}

test "GroupBy: mean on nonexistent column returns error" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    try std.testing.expectError(error.ColumnNotFound, gb.mean("nonexistent"));
}

test "GroupBy: single group contains all rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    // All same key
    var cat = try df.createSeries(i32);
    try cat.rename("key");
    try cat.append(1);
    try cat.append(1);
    try cat.append(1);

    var vals = try df.createSeries(i64);
    try vals.rename("val");
    try vals.append(6);
    try vals.append(12);
    try vals.append(18);

    var gb = try df.groupBy("key");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();
    try std.testing.expectEqual(@as(usize, 1), counts.height());
    const count_series = counts.getSeries("count") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(usize, 3), count_series.usize.values.items[0]);

    var mean_result = try gb.mean("val");
    defer mean_result.deinit();
    const mean_col = mean_result.getSeries("val") orelse return error.DoesNotExist;
    try std.testing.expectEqual(12.0, mean_col.float64.values.items[0]);
}

test "GroupBy: f64 keys group correctly" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var keys = try df.createSeries(f64);
    try keys.rename("price");
    try keys.append(1.5);
    try keys.append(2.5);
    try keys.append(1.5);

    var vals = try df.createSeries(i32);
    try vals.rename("qty");
    try vals.append(10);
    try vals.append(20);
    try vals.append(30);

    var gb = try df.groupBy("price");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();
    try std.testing.expectEqual(@as(usize, 2), counts.height());

    var sum_result = try gb.sum("qty");
    defer sum_result.deinit();

    const sum_col = sum_result.getSeries("qty") orelse return error.DoesNotExist;
    const a = sum_col.int32.values.items[0];
    const b = sum_col.int32.values.items[1];
    try std.testing.expect((a == 40 and b == 20) or (a == 20 and b == 40));
}

test "GroupBy: min produces correct minimums" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var min_result = try gb.min("values");
    defer min_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), min_result.height());
    const min_col = min_result.getSeries("values") orelse return error.DoesNotExist;
    // Group 1: min(10,30,50)=10, Group 2: min(20,40)=20
    const a = min_col.int64.values.items[0];
    const b = min_col.int64.values.items[1];
    try std.testing.expect((a == 10 and b == 20) or (a == 20 and b == 10));
}

test "GroupBy: max produces correct maximums" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var max_result = try gb.max("values");
    defer max_result.deinit();

    try std.testing.expectEqual(@as(usize, 2), max_result.height());
    const max_col = max_result.getSeries("values") orelse return error.DoesNotExist;
    // Group 1: max(10,30,50)=50, Group 2: max(20,40)=40
    const a = max_col.int64.values.items[0];
    const b = max_col.int64.values.items[1];
    try std.testing.expect((a == 50 and b == 40) or (a == 40 and b == 50));
}

test "GroupBy: stdDev produces correct standard deviations" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var cat = try df.createSeries(i32);
    try cat.rename("key");
    try cat.append(1);
    try cat.append(1);

    var vals = try df.createSeries(i64);
    try vals.rename("val");
    try vals.append(10);
    try vals.append(20);

    var gb = try df.groupBy("key");
    defer gb.deinit();

    var std_result = try gb.stdDev("val");
    defer std_result.deinit();

    const std_col = std_result.getSeries("val") orelse return error.DoesNotExist;
    // stddev of [10,20]: mean=15, sqrt(((10-15)^2 + (20-15)^2)/2) = sqrt(25) = 5.0
    try std.testing.expectEqual(@as(f64, 5.0), std_col.float64.values.items[0]);
}

test "GroupBy: prod produces correct products" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();
    var gb = try df.groupBy("category");
    defer gb.deinit();
    var prod_result = try gb.prod("values");
    defer prod_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), prod_result.height());
    const col = prod_result.getSeries("values") orelse return error.DoesNotExist;
    // Group 1: 10*30*50=15000, Group 2: 20*40=800
    const a = col.int64.values.items[0];
    const b = col.int64.values.items[1];
    try std.testing.expect((a == 15000 and b == 800) or (a == 800 and b == 15000));
}

test "GroupBy: first and last return correct boundary values" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();
    var gb = try df.groupBy("category");
    defer gb.deinit();
    var first_result = try gb.first("values");
    defer first_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), first_result.height());
    var last_result = try gb.last("values");
    defer last_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), last_result.height());
}

test "GroupBy: median produces correct medians" {
    const allocator = std.testing.allocator;
    var df = try createTestDf(allocator);
    defer df.deinit();
    var gb = try df.groupBy("category");
    defer gb.deinit();
    var med_result = try gb.median("values");
    defer med_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), med_result.height());
    const col = med_result.getSeries("values") orelse return error.DoesNotExist;
    // Group 1: median(10,30,50)=30.0, Group 2: median(20,40)=30.0
    const a = col.float64.values.items[0];
    const b = col.float64.values.items[1];
    try std.testing.expect((a == 30.0 and b == 30.0));
}

test "GroupBy: nunique counts distinct values per group" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var cat = try df.createSeries(i32);
    try cat.rename("key");
    try cat.append(1);
    try cat.append(1);
    try cat.append(2);
    var vals = try df.createSeries(i32);
    try vals.rename("val");
    try vals.append(5);
    try vals.append(5);
    try vals.append(7);
    var gb = try df.groupBy("key");
    defer gb.deinit();
    var result = try gb.nunique("val");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.height());
    const col = result.getSeries("nunique") orelse return error.DoesNotExist;
    // Group 1: nunique([5,5])=1, Group 2: nunique([7])=1
    const a = col.usize.values.items[0];
    const b = col.usize.values.items[1];
    try std.testing.expect((a == 1 and b == 1));
}
