const std = @import("std");
const strings = @import("strings.zig");

const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const GroupBy = @import("group.zig").GroupBy;
const Dataframe = @import("dataframe.zig").Dataframe;

fn canBeSlice(comptime T: type) bool {
    return @typeInfo(T) == .pointer and
        @typeInfo(T).pointer.is_const and
        @typeInfo(T).pointer.size == .one and
        @typeInfo(@typeInfo(T).pointer.child) == .array and
        @typeInfo(@typeInfo(T).pointer.child).array.child == u8 and
        @typeInfo(@typeInfo(T).pointer.child).array.sentinel_ptr != null;
}

// Series struct for holding a single column of data of type T
//
// Usage:
// var series = try Series(i32).init(allocator);
pub fn Series(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        name: strings.String,
        values: std.ArrayList(T),
        /// Optional validity bitmap. null means all values are valid.
        /// When present, validity[i] == false means the value at index i is null/missing.
        validity: ?std.ArrayList(bool) = null,

        /// Allocates a new Series on the heap. Caller owns the returned pointer and must call deinit.
        pub fn init(allocator: std.mem.Allocator) !*Self {
            const ptr = try allocator.create(Self);
            errdefer allocator.destroy(ptr);

            ptr.allocator = allocator;
            ptr.name = try strings.String.init(allocator);
            ptr.values = try std.ArrayList(T).initCapacity(allocator, 0);
            ptr.validity = null;
            return ptr;
        }

        /// Allocates a new Series with a given capacity. Caller owns the returned pointer and must call deinit.
        pub fn initWithCapacity(allocator: std.mem.Allocator, cap: usize) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);
            series_ptr.allocator = allocator;
            series_ptr.name = try strings.String.init(allocator);
            series_ptr.values = try std.ArrayList(T).initCapacity(allocator, cap);
            series_ptr.validity = null;
            return series_ptr;
        }

        /// Renames the series using a slice of u8. No ownership transfer.
        pub fn rename(self: *Self, new_name: []const u8) !void {
            self.name.clear();
            try self.name.appendSlice(new_name);
        }

        /// Renames the series using an UnmanagedString. Ownership of new_name is transferred to the series; caller must not deinit new_name after this call.
        fn renameOwned(self: *Self, new_name: strings.String) !void {
            if (self.name.len() > 0) {
                self.name.deinit();
            }
            self.name = new_name;
        }

        /// Deallocates all memory owned by this Series, including all values and the name. After this call, the pointer is invalid.
        pub fn deinit(self: *Self) void {
            if (comptime T == strings.String) {
                for (self.values.items) |*item| {
                    item.deinit();
                }
            }
            if (self.validity) |*v| v.deinit(self.allocator);
            self.name.deinit();
            self.values.deinit(self.allocator);
            self.allocator.destroy(self);
        }

        pub fn print(self: *Self) void {
            switch (comptime T) {
                strings.String => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), "String" });
                    for (self.values.items) |value| {
                        std.debug.print("{s}\n", .{value.toSlice()});
                    }
                    std.debug.print("\n", .{});
                },

                f32, f64 => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), "Float" });
                    for (self.values.items) |value| {
                        std.debug.print("{d}\n", .{value});
                    }
                    std.debug.print("\n", .{});
                },
                else => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), @typeName(T) });
                    for (self.values.items) |value| {
                        std.debug.print("{}\n", .{value});
                    }
                    std.debug.print("\n", .{});
                },
            }
        }

        pub fn printAt(self: *Self, n: usize) void {
            // TODO: Check if n is < len

            switch (comptime T) {
                strings.String => std.debug.print("{s}", .{self.values.items[n].toSlice()}),
                []const u8 => std.debug.print("{s}", .{self.values.items[n]}),
                f32, f64 => std.debug.print("{d}", .{self.values.items[n]}),
                else => std.debug.print("{}", .{self.values.items[n]}),
            }
        }

        // asStringAt returns a string representation of the value at index n.
        // Returns "null" if the value at index n is null/missing.
        // The owner of the string is responsible for deallocating it.
        pub fn asStringAt(self: *Self, n: usize) !strings.String {
            var string = try strings.String.init(self.allocator);
            if (self.isNull(n)) {
                try string.appendSlice("null");
                return string;
            }
            var buf: [128]u8 = undefined;
            const slice = switch (comptime T) {
                strings.String => try std.fmt.bufPrint(&buf, "{s}", .{self.values.items[n].toSlice()}),
                []const u8 => try std.fmt.bufPrint(&buf, "{s}", .{self.values.items[n]}),
                f32, f64 => try std.fmt.bufPrint(&buf, "{d}", .{self.values.items[n]}),
                else => try std.fmt.bufPrint(&buf, "{}", .{self.values.items[n]}),
            };
            try string.appendSlice(slice);
            return string;
        }

        /// Returns the value at index i as an optional. Returns null if the slot is null/missing.
        pub fn getAt(self: *Self, i: usize) ?T {
            if (self.isNull(i)) return null;
            return self.values.items[i];
        }

        /// toSlice returns a slice of the values in the series.
        /// The does not transfer ownership; the caller must not deinit the series while using the slice.
        pub fn toSlice(self: *Self) []const T {
            return self.values.items;
        }

        // getNameOwned returns a owned copy of the name string.
        pub fn getNameOwned(self: *Self) !strings.String {
            return try self.name.clone();
        }

        // getTypeToString returns the type of the series as an owned string.
        pub fn getTypeAsString(self: *Self) !strings.String {
            var string = try strings.String.init(self.allocator);

            const value = switch (T) {
                strings.String => "String",
                f32 => "Float32",
                f64 => "Float64",
                bool => "Bool",
                u8 => "UInt8",
                u16 => "UInt16",
                u32 => "UInt32",
                u64 => "UInt64",
                i8 => "Int8",
                i16 => "Int16",
                i32 => "Int32",
                i64 => "Int64",
                else => @typeName(T),
            };

            var buf: [64]u8 = undefined;
            const slice = try std.fmt.bufPrint(&buf, "{s}", .{value});
            try string.appendSlice(slice);

            return string;
        }

        pub fn len(self: *Self) usize {
            return self.values.items.len;
        }

        pub fn append(self: *Self, value: T) !void {
            try self.values.append(self.allocator, value);
            if (self.validity) |*v| {
                try v.append(self.allocator, true);
            }
        }

        pub fn appendSlice(self: *Self, slice: []const T) !void {
            for (slice) |item| {
                try self.values.append(self.allocator, item);
                if (self.validity) |*v| {
                    try v.append(self.allocator, true);
                }
            }
        }

        // tryAppend takes a value of any type and appends it to the series.
        // It checks the type at compile time and ensures it matches the series type.
        // If the type is not compatible, it raises a compile-time error.
        pub fn tryAppend(self: *Self, value: anytype) !void {
            if (comptime T == strings.String and @TypeOf(value) == []const u8) {
                var new_value = try strings.String.init(self.allocator);
                errdefer new_value.deinit();
                try new_value.appendSlice(value);
                try self.values.append(new_value);
            } else if (comptime T == strings.String and canBeSlice(@TypeOf(value))) {
                var new_value = try strings.String.init(self.allocator);
                errdefer new_value.deinit();
                try new_value.appendSlice(value[0..]);
                try self.values.append(self.allocator, new_value);
            } else if (comptime T == strings.String and @TypeOf(value) == strings.String) {
                try self.values.append(self.allocator, value);
            } else {
                @compileError("Type mismatch in tryAppend(), expected String but got " ++ @typeName(@TypeOf(value)));
            }
        }

        pub fn tryAppendSlice(self: *Self, slice: anytype) !void {
            if (comptime T == strings.String and @TypeOf(slice) == []const []const u8) {
                for (slice) |item| {
                    var new_value = try strings.String.init(self.allocator);
                    errdefer new_value.deinit();
                    try new_value.appendSlice(item);
                    try self.values.append(new_value);
                }
            } else {
                @compileError("Type mismatch in tryAppendSlice(), expected slice of String but got " ++ @typeName(@TypeOf(slice)));
            }
        }

        /// Appends a null/missing value. Uses a default placeholder value internally.
        pub fn appendNull(self: *Self) !void {
            // Lazily initialize validity bitmap
            if (self.validity == null) {
                self.validity = try std.ArrayList(bool).initCapacity(self.allocator, self.values.items.len);
                // All existing values are valid
                for (0..self.values.items.len) |_| {
                    try self.validity.?.append(self.allocator, true);
                }
            }
            // Append default value as placeholder (value is irrelevant — validity marks it null).
            if (comptime T == strings.String) {
                var empty = try strings.String.init(self.allocator);
                errdefer empty.deinit();
                try self.values.append(self.allocator, empty);
            } else if (comptime T == bool) {
                try self.values.append(self.allocator, false);
            } else {
                try self.values.append(self.allocator, @as(T, 0));
            }
            try self.validity.?.append(self.allocator, false);
        }

        /// Returns true if the value at index i is null/missing.
        pub fn isNull(self: *Self, i: usize) bool {
            if (self.validity) |v| {
                return !v.items[i];
            }
            return false;
        }

        /// Returns the number of null values.
        pub fn nullCount(self: *Self) usize {
            if (self.validity) |v| {
                var count: usize = 0;
                for (v.items) |valid| {
                    if (!valid) count += 1;
                }
                return count;
            }
            return 0;
        }

        /// Returns true if the series has any null values.
        pub fn hasNulls(self: *Self) bool {
            return self.nullCount() > 0;
        }

        pub fn dropRow(self: *Self, index: usize) void {
            switch (T) {
                strings.String => {
                    var a = self.values.orderedRemove(index);
                    a.deinit();
                },
                inline else => _ = self.values.orderedRemove(index),
            }
            if (self.validity) |*v| {
                _ = v.orderedRemove(index);
            }
        }

        pub fn applyInplace(self: *Self, comptime func: fn (x: T) T) void {
            for (self.values.items) |*value| {
                value.* = func(value.*);
            }
        }

        /// Allocates and returns a deep copy of this Series. Caller owns the returned pointer and must call deinit.
        pub fn deepCopy(self: *Self) !*Self {
            const new_series = try Self.init(self.allocator);
            errdefer new_series.deinit();

            try new_series.rename(self.name.toSlice());
            try new_series.values.ensureTotalCapacity(self.allocator, self.values.items.len);
            // Copy validity bitmap if present
            if (self.validity) |v| {
                new_series.validity = try std.ArrayList(bool).initCapacity(self.allocator, v.items.len);
                for (v.items) |valid| {
                    try new_series.validity.?.append(self.allocator, valid);
                }
            }
            for (self.values.items) |*value| {
                switch (comptime T) {
                    strings.String => {
                        var new_value = try strings.String.init(self.allocator);
                        errdefer new_value.deinit();
                        try new_value.appendSlice(value.toSlice());
                        try new_series.values.append(self.allocator, new_value);
                    },
                    inline else => {
                        try new_series.values.append(self.allocator, value.*);
                    },
                }
            }

            return new_series;
        }

        pub fn limit(self: *Self, n_limit: usize) void {
            if (n_limit >= self.values.items.len) return;

            switch (comptime T) {
                strings.String => {
                    for (n_limit..self.values.items.len) |i| {
                        self.values.items[i].deinit();
                    }
                },
                else => {},
            }

            self.values.shrinkAndFree(self.allocator, n_limit);
            if (self.validity) |*v| {
                v.shrinkAndFree(self.allocator, n_limit);
            }
        }

        pub fn compareSeries(self: *Self, other: *Self) bool {
            if (self.len() != other.len()) return false;

            for (self.values.items, 0..) |value, index| {
                const self_null = self.isNull(index);
                const other_null = other.isNull(index);
                if (self_null != other_null) return false; // one null, one not
                if (self_null) continue; // both null — equal, check next
                if (comptime T == strings.String) {
                    if (!std.mem.eql(u8, value.toSlice(), other.values.items[index].toSlice())) return false;
                } else {
                    if (value != other.values.items[index]) return false;
                }
            }

            return true;
        }

        pub fn toBoxedSeries(self: *Self) BoxedSeries {
            return switch (T) {
                bool => BoxedSeries{ .bool = self },
                u8 => BoxedSeries{ .uint8 = self },
                u16 => BoxedSeries{ .uint16 = self },
                u32 => BoxedSeries{ .uint32 = self },
                u64 => BoxedSeries{ .uint64 = self },
                u128 => BoxedSeries{ .uint128 = self },
                usize => BoxedSeries{ .usize = self },
                i8 => BoxedSeries{ .int8 = self },
                i16 => BoxedSeries{ .int16 = self },
                i32 => BoxedSeries{ .int32 = self },
                i64 => BoxedSeries{ .int64 = self },
                i128 => BoxedSeries{ .int128 = self },
                f32 => BoxedSeries{ .float32 = self },
                f64 => BoxedSeries{ .float64 = self },
                strings.String => BoxedSeries{ .string = self },
                // Add other types as needed
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }

        /// Creates a new Series containing only the rows at the given indices.
        /// Caller owns the returned pointer and must call deinit.
        pub fn filterByIndices(self: *Self, indices: []const usize) !*Self {
            const new_series = try Self.initWithCapacity(self.allocator, indices.len);
            errdefer new_series.deinit();
            try new_series.rename(self.name.toSlice());
            // Copy validity bitmap if present
            if (self.validity != null) {
                new_series.validity = try std.ArrayList(bool).initCapacity(self.allocator, indices.len);
            }
            for (indices) |idx| {
                if (comptime T == strings.String) {
                    var cloned = try self.values.items[idx].clone();
                    errdefer cloned.deinit();
                    try new_series.values.append(self.allocator, cloned);
                } else {
                    try new_series.values.append(self.allocator, self.values.items[idx]);
                }
                if (self.validity) |v| {
                    try new_series.validity.?.append(self.allocator, v.items[idx]);
                }
            }
            return new_series;
        }

        /// Returns indices that would sort this series.
        /// Caller owns the returned ArrayList and must deinit it.
        pub fn argSort(self: *Self, allocator: std.mem.Allocator, ascending: bool) !std.ArrayList(usize) {
            const n = self.values.items.len;
            var indices = try std.ArrayList(usize).initCapacity(allocator, n);
            for (0..n) |i| try indices.append(allocator, i);

            const items = self.values.items;
            const Context = struct {
                items_ptr: []const T,
                asc: bool,

                pub fn lessThan(ctx: @This(), a_idx: usize, b_idx: usize) bool {
                    const a = ctx.items_ptr[a_idx];
                    const b = ctx.items_ptr[b_idx];
                    if (comptime T == strings.String) {
                        const order = std.mem.order(u8, a.toSlice(), b.toSlice());
                        return if (ctx.asc) order == .lt else order == .gt;
                    } else if (comptime T == bool) {
                        const ai: u1 = @intFromBool(a);
                        const bi: u1 = @intFromBool(b);
                        return if (ctx.asc) ai < bi else ai > bi;
                    } else {
                        return if (ctx.asc) a < b else a > b;
                    }
                }
            };
            std.mem.sortUnstable(usize, indices.items, Context{ .items_ptr = items, .asc = ascending }, Context.lessThan);
            return indices;
        }

        /// Returns indices of first occurrence of each unique value.
        /// Caller owns the returned ArrayList.
        pub fn uniqueIndices(self: *Self, allocator: std.mem.Allocator) !std.ArrayList(usize) {
            const GroupByContext = @import("group.zig").GroupByContext(T);
            var seen = std.HashMap(T, void, GroupByContext, std.hash_map.default_max_load_percentage).init(allocator);
            defer seen.deinit();

            var indices = std.ArrayList(usize).empty;
            for (self.values.items, 0..) |val, i| {
                const gop = try seen.getOrPut(val);
                if (!gop.found_existing) {
                    try indices.append(allocator, i);
                }
            }
            return indices;
        }

        const is_numeric = !(T == strings.String or T == bool);
        const is_float = (T == f32 or T == f64);

        /// Returns the sum of all non-null values. Only available for numeric types.
        pub fn sum(self: *Self) T {
            comptime if (!is_numeric) @compileError("sum not supported for " ++ @typeName(T));
            var total: T = 0;
            for (self.values.items, 0..) |v, i| {
                if (!self.isNull(i)) total += v;
            }
            return total;
        }

        /// Returns the minimum non-null value, or null if empty or all-null.
        pub fn min(self: *Self) ?T {
            comptime if (!is_numeric) @compileError("min not supported for " ++ @typeName(T));
            var result: ?T = null;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                if (result == null or v < result.?) result = v;
            }
            return result;
        }

        /// Returns the maximum non-null value, or null if empty or all-null.
        pub fn max(self: *Self) ?T {
            comptime if (!is_numeric) @compileError("max not supported for " ++ @typeName(T));
            var result: ?T = null;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                if (result == null or v > result.?) result = v;
            }
            return result;
        }

        /// Returns the mean of non-null values as f64. Only available for numeric types.
        pub fn mean(self: *Self) f64 {
            comptime if (!is_numeric) @compileError("mean not supported for " ++ @typeName(T));
            var total: f64 = 0.0;
            var count: usize = 0;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                total += if (comptime is_float) @as(f64, v) else @as(f64, @floatFromInt(v));
                count += 1;
            }
            if (count == 0) return 0.0;
            return total / @as(f64, @floatFromInt(count));
        }

        /// Returns the population standard deviation of non-null values as f64.
        pub fn stdDev(self: *Self) f64 {
            comptime if (!is_numeric) @compileError("stddev not supported for " ++ @typeName(T));
            var count: usize = 0;
            for (0..self.values.items.len) |i| {
                if (!self.isNull(i)) count += 1;
            }
            if (count == 0) return 0.0;
            const m = self.mean();
            var sq_sum: f64 = 0.0;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                const fv: f64 = if (comptime is_float) @as(f64, v) else @as(f64, @floatFromInt(v));
                const diff = fv - m;
                sq_sum += diff * diff;
            }
            return @sqrt(sq_sum / @as(f64, @floatFromInt(count)));
        }

        /// Returns a new series with all null slots replaced by `value`.
        /// The result has no nulls. Caller owns the returned pointer.
        pub fn fillNull(self: *Self, value: T) !*Self {
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    if (comptime T == strings.String) {
                        var cloned = try value.clone();
                        errdefer cloned.deinit();
                        try result.values.append(self.allocator, cloned);
                    } else {
                        try result.values.append(self.allocator, value);
                    }
                } else {
                    if (comptime T == strings.String) {
                        var cloned = try v.clone();
                        errdefer cloned.deinit();
                        try result.values.append(self.allocator, cloned);
                    } else {
                        try result.values.append(self.allocator, v);
                    }
                }
            }
            // No validity bitmap — result has no nulls.
            return result;
        }

        /// Returns a new series with null slots filled by carrying the last valid value forward (LOCF).
        /// Leading nulls (before any valid value) remain null. Caller owns the returned pointer.
        pub fn fillNullForward(self: *Self) !*Self {
            const result = try self.deepCopy();
            errdefer result.deinit();
            var last_valid: ?T = null;
            for (result.values.items, 0..) |*v, i| {
                if (!result.isNull(i)) {
                    last_valid = v.*;
                } else if (last_valid) |lv| {
                    if (comptime T == strings.String) {
                        v.deinit();
                        v.* = try lv.clone();
                    } else {
                        v.* = lv;
                    }
                    if (result.validity) |*bm| bm.items[i] = true;
                }
            }
            return result;
        }

        /// Returns a new series with null slots filled by carrying the next valid value backward (NOCB).
        /// Trailing nulls (after the last valid value) remain null. Caller owns the returned pointer.
        pub fn fillNullBackward(self: *Self) !*Self {
            const result = try self.deepCopy();
            errdefer result.deinit();
            var next_valid: ?T = null;
            var i = result.values.items.len;
            while (i > 0) {
                i -= 1;
                if (!result.isNull(i)) {
                    next_valid = result.values.items[i];
                } else if (next_valid) |nv| {
                    if (comptime T == strings.String) {
                        result.values.items[i].deinit();
                        result.values.items[i] = try nv.clone();
                    } else {
                        result.values.items[i] = nv;
                    }
                    if (result.validity) |*bm| bm.items[i] = true;
                }
            }
            return result;
        }

        /// Returns a new series with all null rows removed. Caller owns the returned pointer.
        pub fn dropNulls(self: *Self) !*Self {
            if (!self.hasNulls()) return self.deepCopy();
            var valid_indices = std.ArrayList(usize).empty;
            defer valid_indices.deinit(self.allocator);
            for (0..self.values.items.len) |i| {
                if (!self.isNull(i)) try valid_indices.append(self.allocator, i);
            }
            return self.filterByIndices(valid_indices.items);
        }

        pub fn groupBy(self: *Self, allocator: std.mem.Allocator, dataframe: *Dataframe) !*GroupBy(T) {
            return GroupBy(T).init(allocator, dataframe, self);
        }
    };
}

// --- filterByIndices Tests ---

test "Series: filterByIndices basic subset" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.rename("vals");
    try s.append(10);
    try s.append(20);
    try s.append(30);
    try s.append(40);

    var filtered = try s.filterByIndices(&[_]usize{ 1, 3 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.len());
    try std.testing.expectEqual(@as(i32, 20), filtered.values.items[0]);
    try std.testing.expectEqual(@as(i32, 40), filtered.values.items[1]);
    try std.testing.expectEqualStrings("vals", filtered.name.toSlice());
}

test "Series: filterByIndices empty indices" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);

    var filtered = try s.filterByIndices(&[_]usize{});
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 0), filtered.len());
}

test "Series: filterByIndices with strings deep copies" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    try s.tryAppend("hello");
    try s.tryAppend("world");

    var filtered = try s.filterByIndices(&[_]usize{1});
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 1), filtered.len());
    try std.testing.expectEqualStrings("world", filtered.values.items[0].toSlice());
}

test "Series: filterByIndices out-of-order indices" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);

    var filtered = try s.filterByIndices(&[_]usize{ 2, 0 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(i32, 30), filtered.values.items[0]);
    try std.testing.expectEqual(@as(i32, 10), filtered.values.items[1]);
}

test "Series: sum" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);

    try std.testing.expectEqual(@as(i32, 60), s.sum());
}

test "Series: min and max" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(30);
    try s.append(10);
    try s.append(20);

    try std.testing.expectEqual(@as(i32, 10), s.min().?);
    try std.testing.expectEqual(@as(i32, 30), s.max().?);
}

test "Series: min empty returns null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();

    try std.testing.expect(s.min() == null);
}

test "Series: mean" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);

    try std.testing.expectEqual(@as(f64, 15.0), s.mean());
}

test "Series: stdDev" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);

    // mean=15, stddev = sqrt(((10-15)^2 + (20-15)^2) / 2) = sqrt(25) = 5
    try std.testing.expectEqual(@as(f64, 5.0), s.stdDev());
}

test "Series: float mean" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.0);
    try s.append(3.0);

    try std.testing.expectEqual(@as(f64, 2.0), s.mean());
}

test "Series: appendNull and isNull" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);

    try std.testing.expectEqual(@as(usize, 3), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(s.isNull(1));
    try std.testing.expect(!s.isNull(2));
    try std.testing.expectEqual(@as(usize, 1), s.nullCount());
    try std.testing.expect(s.hasNulls());
}

test "Series: no nulls by default" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    try std.testing.expect(!s.isNull(0));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
    try std.testing.expect(!s.hasNulls());
}

test "Series: appendNull with strings" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    try s.tryAppend("hello");
    try s.appendNull();

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(s.isNull(1));
}

test "Series: filterByIndices preserves validity" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);

    var filtered = try s.filterByIndices(&[_]usize{ 0, 1 });
    defer filtered.deinit();

    try std.testing.expect(!filtered.isNull(0));
    try std.testing.expect(filtered.isNull(1));
}

// --- getAt Tests ---

test "Series: getAt returns value when not null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(42);
    try std.testing.expectEqual(@as(?i32, 42), s.getAt(0));
}

test "Series: getAt returns null for null slot" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 1), s.getAt(0));
    try std.testing.expectEqual(@as(?i32, null), s.getAt(1));
    try std.testing.expectEqual(@as(?i32, 3), s.getAt(2));
}

// --- asStringAt null Tests ---

test "Series: asStringAt returns 'null' for null slot" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();

    var v0 = try s.asStringAt(0);
    defer v0.deinit();
    var v1 = try s.asStringAt(1);
    defer v1.deinit();

    try std.testing.expectEqualStrings("10", v0.toSlice());
    try std.testing.expectEqualStrings("null", v1.toSlice());
}

// --- dropRow validity Tests ---

test "Series: dropRow removes validity entry" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull(); // index 1
    try s.append(3);

    s.dropRow(1); // remove the null

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(!s.isNull(1));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
}

test "Series: dropRow keeping null slot updates indices correctly" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull(); // index 1
    try s.append(3);

    s.dropRow(0); // remove the first valid value

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(s.isNull(0)); // null moved to index 0
    try std.testing.expect(!s.isNull(1));
}

// --- limit validity Tests ---

test "Series: limit truncates validity bitmap" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    s.limit(1);

    try std.testing.expectEqual(@as(usize, 1), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
}

// --- appendSlice validity Tests ---

test "Series: appendSlice marks entries valid in existing bitmap" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull(); // triggers bitmap init
    try s.appendSlice(&[_]i32{ 10, 20 });

    try std.testing.expectEqual(@as(usize, 3), s.len());
    try std.testing.expect(s.isNull(0));
    try std.testing.expect(!s.isNull(1));
    try std.testing.expect(!s.isNull(2));
}

// --- compareSeries null-aware Tests ---

test "Series: compareSeries null == null" {
    const allocator = std.testing.allocator;
    var a = try Series(i32).init(allocator);
    defer a.deinit();
    var b = try Series(i32).init(allocator);
    defer b.deinit();
    try a.appendNull();
    try b.appendNull();
    try std.testing.expect(a.compareSeries(b));
}

test "Series: compareSeries null != value" {
    const allocator = std.testing.allocator;
    var a = try Series(i32).init(allocator);
    defer a.deinit();
    var b = try Series(i32).init(allocator);
    defer b.deinit();
    try a.appendNull();
    try b.append(0); // same placeholder, different nullness
    try std.testing.expect(!a.compareSeries(b));
}

// --- Aggregation null-skipping Tests ---

test "Series: sum skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);
    try std.testing.expectEqual(@as(i32, 40), s.sum());
}

test "Series: min skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(5);
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 3), s.min());
}

test "Series: max skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.appendNull();
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 5), s.max());
}

test "Series: min all nulls returns null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.appendNull();
    try std.testing.expectEqual(@as(?i32, null), s.min());
}

test "Series: mean skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);
    // mean of [10, 30] = 20, not mean of [10, 0, 30] = 13.33
    try std.testing.expectEqual(@as(f64, 20.0), s.mean());
}

test "Series: stdDev skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(20);
    // stddev of [10, 20] = 5
    try std.testing.expectEqual(@as(f64, 5.0), s.stdDev());
}

// --- fillNull Tests ---

test "Series: fillNull replaces nulls with value" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    var filled = try s.fillNull(99);
    defer filled.deinit();

    try std.testing.expectEqual(@as(usize, 3), filled.len());
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 99), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 3), filled.getAt(2));
    try std.testing.expect(!filled.hasNulls());
}

test "Series: fillNull with no nulls is a copy" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    var filled = try s.fillNull(0);
    defer filled.deinit();

    try std.testing.expectEqual(@as(usize, 2), filled.len());
    try std.testing.expect(!filled.hasNulls());
}

// --- fillNullForward Tests ---

test "Series: fillNullForward carries last valid value forward" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.appendNull();
    try s.append(4);

    var filled = try s.fillNullForward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(2));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(3));
}

test "Series: fillNullForward leaves leading nulls alone" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(5);

    var filled = try s.fillNullForward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, null), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 5), filled.getAt(1));
}

// --- fillNullBackward Tests ---

test "Series: fillNullBackward carries next valid value backward" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.appendNull();
    try s.append(4);

    var filled = try s.fillNullBackward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(2));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(3));
}

test "Series: fillNullBackward leaves trailing nulls alone" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.appendNull();

    var filled = try s.fillNullBackward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 5), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, null), filled.getAt(1));
}

// --- dropNulls Tests ---

test "Series: dropNulls removes null rows" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    var dropped = try s.dropNulls();
    defer dropped.deinit();

    try std.testing.expectEqual(@as(usize, 2), dropped.len());
    try std.testing.expectEqual(@as(?i32, 1), dropped.getAt(0));
    try std.testing.expectEqual(@as(?i32, 3), dropped.getAt(1));
    try std.testing.expect(!dropped.hasNulls());
}

test "Series: dropNulls with no nulls returns full copy" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    var dropped = try s.dropNulls();
    defer dropped.deinit();

    try std.testing.expectEqual(@as(usize, 2), dropped.len());
}
