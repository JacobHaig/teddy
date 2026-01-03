const std = @import("std");
const VariantSeries = @import("variant_series.zig").VariantSeries;
const strings = @import("strings.zig");

fn canBeSlice(comptime T: type) bool {
    return @typeInfo(T) == .pointer and
        @typeInfo(T).pointer.is_const and
        @typeInfo(T).pointer.size == .one and
        @typeInfo(@typeInfo(T).pointer.child) == .array and
        @typeInfo(@typeInfo(T).pointer.child).array.child == u8 and
        @typeInfo(@typeInfo(T).pointer.child).array.sentinel_ptr != null;
}

pub fn Series(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        name: strings.String,
        values: std.ArrayList(T),

        /// Allocates a new Series on the heap. Caller owns the returned pointer and must call deinit.
        pub fn init(allocator: std.mem.Allocator) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);
            series_ptr.allocator = allocator;
            series_ptr.name = try strings.String.init(allocator);
            series_ptr.values = try std.ArrayList(T).initCapacity(allocator, 0);
            return series_ptr;
        }

        /// Allocates a new Series with a given capacity. Caller owns the returned pointer and must call deinit.
        pub fn initWithCapacity(allocator: std.mem.Allocator, cap: usize) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);
            series_ptr.allocator = allocator;
            series_ptr.name = try strings.String.init(allocator);
            series_ptr.values = try std.ArrayList(T).initCapacity(allocator, cap);
            return series_ptr;
        }

        /// Renames the series using a slice of u8. No ownership transfer.
        pub fn rename(self: *Self, new_name: []const u8) !void {
            self.name.clear();
            try self.name.appendSlice(new_name);
        }

        /// Renames the series using an UnmanagedString. Ownership of new_name is transferred to the series; caller must not deinit new_name after this call.
        pub fn renameOwned(self: *Self, new_name: strings.String) !void {
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
                []const u8 => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), "ConstString" });
                    for (self.values.items) |value| {
                        std.debug.print("{s}\n", .{value});
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
        // It uses the allocator to create a new string and formats the value.
        // The owner of the string is responsible for deallocating it.
        pub fn asStringAt(self: *Self, n: usize) !strings.String {
            var string = try strings.String.init(self.allocator);
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

        // getNameOwned returns a owned copy of the name string.
        pub fn getNameOwned(self: *Self) !strings.String {
            return try self.name.clone();
        }

        // getTypeToString returns the type of the series as an owned string.
        pub fn getTypeToString(self: *Self) !strings.String {
            var string = try strings.String.init(self.allocator);

            const value = switch (T) {
                strings.String => "String",
                []const u8 => "ConstString",
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
        }

        pub fn appendSlice(self: *Self, slice: []const T) !void {
            for (slice) |item| {
                try self.values.append(self.allocator, item);
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

        pub fn dropRow(self: *Self, index: usize) void {
            switch (T) {
                strings.String => {
                    var a = self.values.orderedRemove(index);
                    a.deinit();
                },
                inline else => _ = self.values.orderedRemove(index),
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
            for (self.values.items) |*value| {
                switch (comptime T) {
                    strings.String => {
                        var new_value = try strings.String.init(self.allocator);
                        errdefer new_value.deinit();
                        try new_value.appendSlice(value.toSlice());
                        try new_series.values.append(self.allocator, new_value);
                    },
                    inline else => {
                        try new_series.append(value.*);
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
        }

        pub fn compareSeries(self: *Self, other: *Self) bool {
            if (self.len() != other.len()) return false;

            for (self.values.items, 0..) |value, index| {
                if (value != other.values.items[index]) {
                    return false;
                }
            }

            return true;
        }

        pub fn toVariantSeries(self: *Self) VariantSeries {
            return switch (T) {
                bool => VariantSeries{ .bool = self },
                u8 => VariantSeries{ .uint8 = self },
                u16 => VariantSeries{ .uint16 = self },
                u32 => VariantSeries{ .uint32 = self },
                u64 => VariantSeries{ .uint64 = self },
                u128 => VariantSeries{ .uint128 = self },
                i8 => VariantSeries{ .int8 = self },
                i16 => VariantSeries{ .int16 = self },
                i32 => VariantSeries{ .int32 = self },
                i64 => VariantSeries{ .int64 = self },
                i128 => VariantSeries{ .int128 = self },
                f32 => VariantSeries{ .float32 = self },
                f64 => VariantSeries{ .float64 = self },
                []const u8 => VariantSeries{ .conststring = self },
                strings.String => VariantSeries{ .string = self },
                // Add other types as needed
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }
    };
}
