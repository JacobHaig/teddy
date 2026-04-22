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
                isize => BoxedSeries{ .isize = self },
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
                const delta = fv - m;
                sq_sum += delta * delta;
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

        /// Comptime-safe cast. Compile error if `T → Target` could ever lose data.
        /// Nulls are preserved. Only allocation errors can occur at runtime.
        /// Use when you can prove at the call site that the types are compatible.
        pub fn castSafe(self: *Self, comptime Target: type) !*Series(Target) {
            comptime if (!isSafeCast(T, Target)) @compileError(
                "castSafe: cast from " ++ @typeName(T) ++ " to " ++ @typeName(Target) ++
                    " may lose data — use cast() to fail on bad values or castLossy() to null them out",
            );
            return self.castImpl(Target, .strict); // safe casts never hit error paths
        }

        /// Strict runtime cast. Returns error if any value overflows, loses fractional
        /// data (float → int with non-integer value), or fails to parse (String → numeric).
        /// Nulls are preserved. Use when bad data should surface as a hard failure.
        pub fn cast(self: *Self, comptime Target: type) !*Series(Target) {
            return self.castImpl(Target, .strict);
        }

        /// Permissive cast. Values that cannot be represented become null rather than
        /// errors: overflow → null, String parse failure → null, NaN/Inf → null.
        /// float → int truncates (1.9 → 1). Nulls are preserved.
        /// Use for best-effort ETL where corrupt values should be skipped.
        pub fn castLossy(self: *Self, comptime Target: type) !*Series(Target) {
            return self.castImpl(Target, .lossy);
        }

        const CastMode = enum { strict, lossy };

        fn castImpl(self: *Self, comptime Target: type, comptime mode: CastMode) !*Series(Target) {
            const result = try Series(Target).init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    try result.appendNull();
                    continue;
                }
                if (comptime mode == .strict) {
                    if (comptime Target == strings.String) {
                        var converted = try castValueStrict(T, Target, v, self.allocator);
                        errdefer converted.deinit();
                        try result.values.append(self.allocator, converted);
                        if (result.validity) |*bm| try bm.append(self.allocator, true);
                    } else {
                        try result.append(try castValueStrict(T, Target, v, self.allocator));
                    }
                } else {
                    // lossy mode
                    const maybe = try castValueLossy(T, Target, v, self.allocator);
                    if (maybe) |converted| {
                        if (comptime Target == strings.String) {
                            // converted is already allocated; just push it
                            try result.values.append(self.allocator, converted);
                            if (result.validity) |*bm| try bm.append(self.allocator, true);
                        } else {
                            try result.append(converted);
                        }
                    } else {
                        // Deinit any allocated String we won't use
                        if (comptime Target == strings.String) {} // castValueLossy returned null, no alloc
                        try result.appendNull();
                    }
                }
            }
            return result;
        }

        /// Checked sum: errors on integer overflow. Floats use regular addition.
        pub fn sumChecked(self: *Self) !T {
            comptime if (!is_numeric) @compileError("sumChecked not supported for " ++ @typeName(T));
            var total: T = 0;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                if (comptime is_float) {
                    total += v;
                } else {
                    const r, const ov = @addWithOverflow(total, v);
                    if (ov != 0) return error.Overflow;
                    total = r;
                }
            }
            return total;
        }

        /// Product of all non-null values. Wraps on integer overflow.
        pub fn prod(self: *Self) T {
            comptime if (!is_numeric) @compileError("prod not supported for " ++ @typeName(T));
            var result: T = 1;
            for (self.values.items, 0..) |v, i| {
                if (!self.isNull(i)) result *= v;
            }
            return result;
        }

        /// Checked product: errors on integer overflow. Floats use regular multiplication.
        pub fn prodChecked(self: *Self) !T {
            comptime if (!is_numeric) @compileError("prodChecked not supported for " ++ @typeName(T));
            var result: T = 1;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) continue;
                if (comptime is_float) {
                    result *= v;
                } else {
                    const r, const ov = @mulWithOverflow(result, v);
                    if (ov != 0) return error.Overflow;
                    result = r;
                }
            }
            return result;
        }

        /// First non-null value, or null if empty or all-null.
        pub fn first(self: *Self) ?T {
            for (self.values.items, 0..) |v, i| {
                if (!self.isNull(i)) return v;
            }
            return null;
        }

        /// Last non-null value, or null if empty or all-null.
        pub fn last(self: *Self) ?T {
            var i = self.values.items.len;
            while (i > 0) {
                i -= 1;
                if (!self.isNull(i)) return self.values.items[i];
            }
            return null;
        }

        /// Median of non-null values as f64. Sorts a temporary copy of valid values.
        /// Returns null if there are no non-null values. Allocator is for the temp sort buffer.
        pub fn median(self: *Self, allocator: std.mem.Allocator) !?f64 {
            comptime if (!is_numeric) @compileError("median not supported for " ++ @typeName(T));
            var valid = std.ArrayList(f64).empty;
            defer valid.deinit(allocator);
            for (self.values.items, 0..) |v, i| {
                if (!self.isNull(i)) {
                    const fv: f64 = if (comptime is_float) @as(f64, v) else @as(f64, @floatFromInt(v));
                    try valid.append(allocator, fv);
                }
            }
            if (valid.items.len == 0) return null;
            std.mem.sortUnstable(f64, valid.items, {}, std.sort.asc(f64));
            const n = valid.items.len;
            if (n % 2 == 1) return valid.items[n / 2];
            return (valid.items[n / 2 - 1] + valid.items[n / 2]) / 2.0;
        }

        /// Quantile of non-null values via linear interpolation. q must be in [0.0, 1.0].
        /// Returns null if there are no non-null values. Allocator is for the temp sort buffer.
        pub fn quantile(self: *Self, allocator: std.mem.Allocator, q: f64) !?f64 {
            comptime if (!is_numeric) @compileError("quantile not supported for " ++ @typeName(T));
            if (q < 0.0 or q > 1.0) return error.InvalidQuantile;
            var valid = std.ArrayList(f64).empty;
            defer valid.deinit(allocator);
            for (self.values.items, 0..) |v, i| {
                if (!self.isNull(i)) {
                    const fv: f64 = if (comptime is_float) @as(f64, v) else @as(f64, @floatFromInt(v));
                    try valid.append(allocator, fv);
                }
            }
            if (valid.items.len == 0) return null;
            std.mem.sortUnstable(f64, valid.items, {}, std.sort.asc(f64));
            const n = valid.items.len;
            if (n == 1) return valid.items[0];
            const pos = q * @as(f64, @floatFromInt(n - 1));
            const lo: usize = @floor(pos);
            const hi: usize = @min(lo + 1, n - 1);
            const frac = pos - @as(f64, @floatFromInt(lo));
            return valid.items[lo] + frac * (valid.items[hi] - valid.items[lo]);
        }

        /// Count of distinct non-null values. Allocates a temporary hash map.
        pub fn nunique(self: *Self, allocator: std.mem.Allocator) !usize {
            var indices = try self.uniqueIndices(allocator);
            defer indices.deinit(allocator);
            return indices.items.len;
        }

        /// Running cumulative sum. Nulls produce null output; does not skip.
        /// Wraps on integer overflow (debug mode panics).
        pub fn cumSum(self: *Self) !*Self {
            comptime if (!is_numeric) @compileError("cumSum not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            var running: T = 0;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    try result.appendNull();
                } else {
                    running += v;
                    try result.append(running);
                }
            }
            return result;
        }

        /// Running cumulative minimum. Nulls produce null output.
        pub fn cumMin(self: *Self) !*Self {
            comptime if (!is_numeric) @compileError("cumMin not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            var running: ?T = null;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    try result.appendNull();
                } else {
                    running = if (running) |r| @min(r, v) else v;
                    try result.append(running.?);
                }
            }
            return result;
        }

        /// Running cumulative maximum. Nulls produce null output.
        pub fn cumMax(self: *Self) !*Self {
            comptime if (!is_numeric) @compileError("cumMax not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            var running: ?T = null;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    try result.appendNull();
                } else {
                    running = if (running) |r| @max(r, v) else v;
                    try result.append(running.?);
                }
            }
            return result;
        }

        /// Running cumulative product. Nulls produce null output.
        /// Wraps on integer overflow (debug mode panics).
        pub fn cumProd(self: *Self) !*Self {
            comptime if (!is_numeric) @compileError("cumProd not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            var running: T = 1;
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    try result.appendNull();
                } else {
                    running *= v;
                    try result.append(running);
                }
            }
            return result;
        }

        /// Shift values by n positions. Positive n shifts down (prepends nulls),
        /// negative n shifts up (appends nulls). Length is preserved; shifted-off values are dropped.
        pub fn shift(self: *Self, n: i64) !*Self {
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            const slen = self.values.items.len;
            if (n >= 0) {
                const sn: usize = @min(@as(usize, @intCast(n)), slen);
                for (0..sn) |_| try result.appendNull();
                for (0..slen - sn) |i| {
                    if (self.isNull(i)) try result.appendNull() else {
                        if (comptime T == strings.String) {
                            var cloned = try self.values.items[i].clone();
                            errdefer cloned.deinit();
                            try result.values.append(self.allocator, cloned);
                            if (result.validity) |*bm| try bm.append(self.allocator, true);
                        } else {
                            try result.append(self.values.items[i]);
                        }
                    }
                }
            } else {
                const sn: usize = @min(@as(usize, @intCast(-n)), slen);
                for (sn..slen) |i| {
                    if (self.isNull(i)) try result.appendNull() else {
                        if (comptime T == strings.String) {
                            var cloned = try self.values.items[i].clone();
                            errdefer cloned.deinit();
                            try result.values.append(self.allocator, cloned);
                            if (result.validity) |*bm| try bm.append(self.allocator, true);
                        } else {
                            try result.append(self.values.items[i]);
                        }
                    }
                }
                for (0..sn) |_| try result.appendNull();
            }
            return result;
        }

        /// Strict element-wise difference: result[i] = values[i] - values[i-n].
        /// First n rows are null. Either operand null → null output.
        /// For unsigned integers, underflow returns error.Underflow; use diffLossy to null instead.
        pub fn diff(self: *Self, n: usize) !*Self {
            comptime if (!is_numeric) @compileError("diff not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            for (0..self.values.items.len) |i| {
                if (i < n or self.isNull(i) or self.isNull(i - n)) {
                    try result.appendNull();
                } else if (comptime is_float) {
                    try result.append(self.values.items[i] - self.values.items[i - n]);
                } else {
                    const r, const ov = @subWithOverflow(self.values.items[i], self.values.items[i - n]);
                    if (ov != 0) return error.Underflow;
                    try result.append(r);
                }
            }
            return result;
        }

        /// Permissive element-wise difference. Underflow/overflow → null instead of error.
        pub fn diffLossy(self: *Self, n: usize) !*Self {
            comptime if (!is_numeric) @compileError("diffLossy not supported for " ++ @typeName(T));
            const result = try Self.init(self.allocator);
            errdefer result.deinit();
            try result.rename(self.name.toSlice());
            for (0..self.values.items.len) |i| {
                if (i < n or self.isNull(i) or self.isNull(i - n)) {
                    try result.appendNull();
                } else if (comptime is_float) {
                    try result.append(self.values.items[i] - self.values.items[i - n]);
                } else {
                    const r, const ov = @subWithOverflow(self.values.items[i], self.values.items[i - n]);
                    if (ov != 0) try result.appendNull() else try result.append(r);
                }
            }
            return result;
        }

        /// Clamp all non-null values to [lower, upper]. Nulls are preserved.
        pub fn clip(self: *Self, lower: T, upper: T) !*Self {
            comptime if (!is_numeric) @compileError("clip not supported for " ++ @typeName(T));
            const result = try self.deepCopy();
            errdefer result.deinit();
            for (result.values.items, 0..) |*v, i| {
                if (!result.isNull(i)) v.* = @max(lower, @min(upper, v.*));
            }
            return result;
        }

        /// Returns a new series with exact matches of old_value replaced by new_value.
        /// Null slots are never matched. String values are compared by content.
        pub fn replace(self: *Self, old_value: T, new_value: T) !*Self {
            const result = try self.deepCopy();
            errdefer result.deinit();
            for (result.values.items, 0..) |*v, i| {
                if (result.isNull(i)) continue;
                const matches = if (comptime T == strings.String)
                    std.mem.eql(u8, v.toSlice(), old_value.toSlice())
                else
                    v.* == old_value;
                if (matches) {
                    if (comptime T == strings.String) {
                        v.deinit();
                        v.* = try new_value.clone();
                    } else {
                        v.* = new_value;
                    }
                }
            }
            return result;
        }

        /// Returns a new series applying multiple replacements. First matching pair wins.
        pub fn replaceSlice(self: *Self, pairs: []const [2]T) !*Self {
            const result = try self.deepCopy();
            errdefer result.deinit();
            for (result.values.items, 0..) |*v, i| {
                if (result.isNull(i)) continue;
                for (pairs) |pair| {
                    const matches = if (comptime T == strings.String)
                        std.mem.eql(u8, v.toSlice(), pair[0].toSlice())
                    else
                        v.* == pair[0];
                    if (matches) {
                        if (comptime T == strings.String) {
                            v.deinit();
                            v.* = try pair[1].clone();
                        } else {
                            v.* = pair[1];
                        }
                        break;
                    }
                }
            }
            return result;
        }

        pub fn groupBy(self: *Self, allocator: std.mem.Allocator, dataframe: *Dataframe) !*GroupBy(T) {
            return GroupBy(T).init(allocator, dataframe, self);
        }
    };
}

// ---------------------------------------------------------------------------
// Cast helpers — three tiers matching Zig's own philosophy:
//
//   castSafe   — comptime-verified lossless widening; compile error if not safe.
//                Never errors at runtime (beyond OOM). Use when you *know* the
//                types are compatible (e.g. i32 → i64, u16 → f64).
//
//   cast       — strict runtime cast; returns error on overflow or parse failure.
//                float → int requires an exact integer value (1.0 ok, 1.5 → error).
//                Use when the data *should* be representable and you want a loud
//                failure if it isn't.
//
//   castLossy  — permissive; conversion failures become null rather than errors.
//                float → int truncates. Overflow → null. Bad string → null.
//                Use for best-effort ETL where corrupt values should be skipped.
// ---------------------------------------------------------------------------

/// Comptime predicate: is casting From → To guaranteed lossless?
/// True only for widening numerics, any → String, and bool → numeric.
pub fn isSafeCast(comptime From: type, comptime To: type) bool {
    if (From == To) return true;
    if (To == strings.String) return true; // formatting always succeeds
    if (From == strings.String) return false; // parsing can fail
    if (From == bool) return true; // 0 or 1 always fits any numeric
    if (To == bool) return false; // loses information

    const from_info = @typeInfo(From);
    const to_info = @typeInfo(To);

    // float → int: always lossy (truncation or range issues)
    if (comptime (From == f32 or From == f64) and to_info == .int) return false;

    // float widening
    if (From == f32 and To == f64) return true;
    if (From == f64 and To == f32) return false;
    if (From == f32 and To == f32) return true;
    if (From == f64 and To == f64) return true;

    // int → float: safe only when the mantissa can represent all values exactly.
    // f32 has 23 explicit mantissa bits; f64 has 52.
    if (from_info == .int and (To == f32 or To == f64)) {
        const mantissa: usize = if (To == f32) 23 else 52;
        const value_bits: usize = if (from_info.int.signedness == .signed)
            from_info.int.bits - 1
        else
            from_info.int.bits;
        return value_bits <= mantissa;
    }

    // int → int
    if (from_info == .int and to_info == .int) {
        const fb = from_info.int.bits;
        const tb = to_info.int.bits;
        const fs = from_info.int.signedness == .signed;
        const ts = to_info.int.signedness == .signed;
        if (fs == ts) return tb >= fb; // same sign: widening is safe
        if (!fs and ts) return tb > fb; // u8 → i16: need one extra bit
        return false; // signed → unsigned: could be negative
    }

    return false;
}

/// Strict: errors on overflow (int→smaller int) or non-integer float→int.
/// String parsing errors propagate directly.
fn castValueStrict(comptime From: type, comptime To: type, value: From, allocator: std.mem.Allocator) !To {
    if (comptime From == To) {
        if (comptime From == strings.String) return value.clone();
        return value;
    }

    if (comptime From == strings.String) {
        const slice = value.toSlice();
        if (comptime To == bool) return std.mem.eql(u8, slice, "true") or std.mem.eql(u8, slice, "1");
        if (comptime To == f32 or To == f64) return @floatCast(try std.fmt.parseFloat(f64, slice));
        const info = @typeInfo(To);
        if (comptime info == .int and info.int.signedness == .signed) {
            return @intCast(try std.fmt.parseInt(i128, slice, 10));
        } else {
            return @intCast(try std.fmt.parseInt(u128, slice, 10));
        }
    }

    if (comptime To == strings.String) {
        var s = try strings.String.init(allocator);
        errdefer s.deinit();
        var buf: [128]u8 = undefined;
        const slice = switch (comptime From) {
            f32, f64 => try std.fmt.bufPrint(&buf, "{d}", .{value}),
            bool => if (value) "true"[0..] else "false"[0..],
            else => try std.fmt.bufPrint(&buf, "{}", .{value}),
        };
        try s.appendSlice(slice);
        return s;
    }

    if (comptime From == bool) {
        const i: u1 = @intFromBool(value);
        if (comptime To == f32 or To == f64) return @floatFromInt(i);
        return @intCast(i);
    }
    if (comptime To == bool) {
        if (comptime From == f32 or From == f64) return value != 0.0;
        return value != 0;
    }

    // float → int: require exact integer value
    if (comptime (From == f32 or From == f64) and @typeInfo(To) == .int) {
        if (std.math.isNan(value) or std.math.isInf(value)) return error.InvalidCast;
        if (value != @trunc(value)) return error.LossyCast;
        return std.math.cast(To, @as(i128, @trunc(value))) orelse return error.Overflow;
    }

    if (comptime @typeInfo(From) == .int and (To == f32 or To == f64)) return @floatFromInt(value);
    if (comptime (From == f32 or From == f64) and (To == f32 or To == f64)) return @floatCast(value);

    // int → int: overflow is an error
    if (comptime @typeInfo(From) == .int and @typeInfo(To) == .int) {
        return std.math.cast(To, value) orelse error.Overflow;
    }

    @compileError("Unsupported cast from " ++ @typeName(From) ++ " to " ++ @typeName(To));
}

/// Lossy: conversion failures return null (→ null row) rather than an error.
/// float → int truncates. int overflow → null. String parse failure → null.
/// Only allocation errors propagate.
fn castValueLossy(comptime From: type, comptime To: type, value: From, allocator: std.mem.Allocator) !?To {
    if (comptime From == To) {
        if (comptime From == strings.String) return try value.clone();
        return value;
    }

    if (comptime From == strings.String) {
        const slice = value.toSlice();
        if (comptime To == bool) return std.mem.eql(u8, slice, "true") or std.mem.eql(u8, slice, "1");
        if (comptime To == f32 or To == f64) {
            const parsed = std.fmt.parseFloat(f64, slice) catch return null;
            return @as(To, @floatCast(parsed));
        }
        const info = @typeInfo(To);
        if (comptime info == .int and info.int.signedness == .signed) {
            const parsed = std.fmt.parseInt(i128, slice, 10) catch return null;
            return std.math.cast(To, parsed);
        } else {
            const parsed = std.fmt.parseInt(u128, slice, 10) catch return null;
            return std.math.cast(To, parsed);
        }
    }

    if (comptime To == strings.String) {
        var s = try strings.String.init(allocator);
        errdefer s.deinit();
        var buf: [128]u8 = undefined;
        const slice = switch (comptime From) {
            f32, f64 => try std.fmt.bufPrint(&buf, "{d}", .{value}),
            bool => if (value) "true"[0..] else "false"[0..],
            else => try std.fmt.bufPrint(&buf, "{}", .{value}),
        };
        try s.appendSlice(slice);
        return s;
    }

    if (comptime From == bool) {
        const i: u1 = @intFromBool(value);
        if (comptime To == f32 or To == f64) return @as(To, @floatFromInt(i));
        return @as(To, @intCast(i));
    }
    if (comptime To == bool) {
        if (comptime From == f32 or From == f64) return value != 0.0;
        return value != 0;
    }

    // float → int: truncate (lossy by definition), null for NaN/Inf
    if (comptime (From == f32 or From == f64) and @typeInfo(To) == .int) {
        if (std.math.isNan(value) or std.math.isInf(value)) return null;
        return std.math.cast(To, @as(i128, @trunc(value)));
    }

    if (comptime @typeInfo(From) == .int and (To == f32 or To == f64)) return @as(To, @floatFromInt(value));
    if (comptime (From == f32 or From == f64) and (To == f32 or To == f64)) return @as(To, @floatCast(value));

    // int → int: overflow → null
    if (comptime @typeInfo(From) == .int and @typeInfo(To) == .int) {
        return std.math.cast(To, value);
    }

    @compileError("Unsupported cast from " ++ @typeName(From) ++ " to " ++ @typeName(To));
}
