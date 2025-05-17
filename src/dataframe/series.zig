const std = @import("std");
const VariantSeries = @import("variant_series.zig").VariantSeries;
const UnmanagedString = @import("variant_series.zig").UnmanagedString;
const ManagedString = @import("variant_series.zig").ManagedString;

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
        name: UnmanagedString,
        values: std.ArrayList(T),

        pub fn init(allocator: std.mem.Allocator) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);

            series_ptr.allocator = allocator;
            series_ptr.name = try UnmanagedString.initCapacity(allocator, 0);
            series_ptr.values = std.ArrayList(T).init(allocator);

            return series_ptr;
        }

        pub fn initWithCapacity(allocator: std.mem.Allocator, cap: usize) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);

            series_ptr.allocator = allocator;
            series_ptr.name = try UnmanagedString.initCapacity(allocator, 0);
            series_ptr.values = try std.ArrayList(T).initCapacity(allocator, cap);

            return series_ptr;
        }

        // rename renames the series using a slice of u8.
        //
        pub fn rename(self: *Self, new_name: []const u8) !void {
            self.name.clearRetainingCapacity();
            try self.name.appendSlice(self.allocator, new_name);
        }

        // owned renames the series using an UnmanagedString.
        // Takes ownership, caller no longer owns.
        pub fn renameOwned(self: *Self, new_name: UnmanagedString) !void {
            if (self.name.items.len > 0) {
                self.name.deinit(self.allocator);
            }

            self.name = new_name;
        }

        pub fn deinit(self: *Self) void {
            switch (T) { // Deinit each item in the list
                UnmanagedString => {
                    for (self.values.items) |*item| {
                        item.deinit(self.allocator);
                    }
                },
                inline else => {},
            }
            self.name.deinit(self.allocator); // Deinit the name
            self.values.deinit(); // Deinit the List

            // self.allocator.free(self.name);
            self.allocator.destroy(self);
        }

        pub fn print(self: *Self) void {
            switch (comptime T) {
                UnmanagedString => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.items, "String" });
                    for (self.values.items) |value| {
                        std.debug.print("{s}\n", .{value.items});
                    }
                    std.debug.print("\n", .{});
                },
                []const u8 => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.items, "ConstString" });
                    for (self.values.items) |value| {
                        std.debug.print("{s}\n", .{value});
                    }
                    std.debug.print("\n", .{});
                },
                f32, f64 => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.items, "Float" });
                    for (self.values.items) |value| {
                        std.debug.print("{d}\n", .{value});
                    }
                    std.debug.print("\n", .{});
                },
                else => {
                    std.debug.print("{s}\n{s}\n--------\n", .{ self.name.items, @typeName(T) });
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
                UnmanagedString => std.debug.print("{s}", .{self.values.items[n].items}),
                []const u8 => std.debug.print("{s}", .{self.values.items[n]}),
                f32, f64 => std.debug.print("{d}", .{self.values.items[n]}),
                else => std.debug.print("{}", .{self.values.items[n]}),
            }
        }

        // as_string_at returns a string representation of the value at index n.
        // It uses the allocator to create a new string and formats the value.
        // The owner of the string is responsible for deallocating it.
        pub fn as_string_at(self: *Self, n: usize) !UnmanagedString {
            var string = try UnmanagedString.initCapacity(self.allocator, 0);

            switch (comptime T) {
                UnmanagedString => try string.writer(self.allocator).print("{s}", .{self.values.items[n].items}),
                []const u8 => try string.writer(self.allocator).print("{s}", .{self.values.items[n]}),
                f32, f64 => try string.writer(self.allocator).print("{d}", .{self.values.items[n]}),
                else => try string.writer(self.allocator).print("{}", .{self.values.items[n]}),
            }

            return string;
        }

        pub fn getNameOwned(self: *Self) !UnmanagedString {
            return try self.name.clone(self.allocator);
        }

        pub fn getTypeToString(self: *Self) !UnmanagedString {
            var string = try UnmanagedString.initCapacity(self.allocator, 0);

            const value = switch (T) {
                UnmanagedString => "String",
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

            try string.writer(self.allocator).print("{s}", .{value});

            return string;
        }

        pub fn len(self: *Self) usize {
            return self.values.items.len;
        }

        pub fn append(self: *Self, value: T) !void {
            try self.values.append(value);
        }

        // tryAppend takes a value of any type and appends it to the series.
        // It checks the type at compile time and ensures it matches the series type.
        // If the type is not compatible, it raises a compile-time error.
        pub fn tryAppend(self: *Self, value: anytype) !void {
            if (comptime T == UnmanagedString and @TypeOf(value) == []const u8) {
                var new_value = try UnmanagedString.initCapacity(self.allocator, value.len);
                errdefer new_value.deinit(self.allocator);

                new_value.appendSliceAssumeCapacity(value);
                try self.values.append(new_value);
            } else if (comptime T == UnmanagedString and canBeSlice(@TypeOf(value))) {
                var new_value = try UnmanagedString.initCapacity(self.allocator, value.len);
                errdefer new_value.deinit(self.allocator);

                new_value.appendSliceAssumeCapacity(value[0..]);
                try self.values.append(new_value);
            } else if (comptime T == UnmanagedString and @TypeOf(value) == UnmanagedString) {
                try self.values.append(value);
            } else {
                @compileError("Type mismatch in tryAppend(), expected String but got " ++ @typeName(@TypeOf(value)));
            }
        }

        pub fn dropRow(self: *Self, index: usize) void {
            switch (T) {
                UnmanagedString => {
                    var a = self.values.orderedRemove(index);
                    a.deinit(self.allocator);
                },
                inline else => _ = self.values.orderedRemove(index),
            }
        }

        pub fn applyInplace(self: *Self, comptime func: fn (x: T) T) void {
            for (self.values.items) |*value| {
                value.* = func(value.*);
            }
        }

        // deep_copy creates a deep copy of the Series.
        // It allocates a new Series and copies the values from the original Series.
        // If the type is String, it also copies the string data to the new Series as well.
        pub fn deep_copy(self: *Self) !*Self {
            const new_series = try Self.init(self.allocator);
            errdefer new_series.deinit();

            try new_series.rename(self.name);
            try new_series.values.ensureTotalCapacity(self.values.items.len);

            for (self.values.items) |*value| {
                switch (comptime T) {
                    UnmanagedString => {
                        var new_value = try UnmanagedString.initCapacity(self.allocator, value.items.len);
                        errdefer new_value.deinit(self.allocator);

                        new_value.appendSliceAssumeCapacity(value.items);
                        try new_series.values.append(new_value);
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
                UnmanagedString => {
                    for (n_limit..self.values.items.len) |i| {
                        self.values.items[i].deinit(self.allocator);
                    }
                },
                else => {},
            }

            self.values.shrinkAndFree(n_limit);
        }

        pub fn to_variant_series(self: *Self) VariantSeries {
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
                std.ArrayListUnmanaged(u8) => VariantSeries{ .string = self },

                // Add other types as needed
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }
    };
}
