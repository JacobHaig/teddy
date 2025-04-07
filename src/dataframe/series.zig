const std = @import("std");
const VariantSeries = @import("variant_series.zig").VariantSeries;
const String = @import("variant_series.zig").String;

pub fn Series(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        name: []u8,
        values: std.ArrayList(T),

        pub fn init(allocator: std.mem.Allocator) !*Self {
            const series_ptr = try allocator.create(Self);
            errdefer allocator.destroy(series_ptr);

            // Initialize fields directly, avoiding stack allocation
            series_ptr.allocator = allocator;
            series_ptr.name = try allocator.alloc(u8, 0);
            series_ptr.values = try std.ArrayList(T).initCapacity(allocator, 0);

            return series_ptr;
        }

        pub fn rename(self: *Self, new_name: []const u8) !void {
            self.allocator.free(self.name);
            self.name = try self.allocator.alloc(u8, new_name.len);
            std.mem.copyForwards(u8, self.name, new_name);
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.name);
            self.values.deinit();
            self.allocator.destroy(self);
        }

        pub fn print(self: Self) void {
            if (comptime T == std.ArrayListUnmanaged(u8)) {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name, "Bytes" });

                for (self.values.items) |value| {
                    std.debug.print("{s}\n", .{value.items});
                }
                std.debug.print("\n", .{});
            } else {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name, @typeName(T) });

                for (self.values.items) |value| {
                    std.debug.print("{}\n", .{value});
                }
                std.debug.print("\n", .{});
            }
        }

        pub fn append(self: *Self, value: T) !void {
            if (comptime T == std.ArrayListUnmanaged(u8)) {
                // var copy: T = .{};
                // try copy.appendSlice(self.allocator, value.items);
                // try self.values.append(copy); // No allocator here
                try self.values.append(value);
            } else {
                try self.values.append(value);
            }
        }

        pub fn try_append(self: *Self, comptime WantedType: type, value: []const u8) !void {
            if (comptime WantedType == std.ArrayListUnmanaged(u8)) {
                var naw_value = try String.initCapacity(self.allocator, value.len);
                errdefer naw_value.deinit(self.allocator);

                naw_value.appendSliceAssumeCapacity(value);
                try self.values.append(naw_value);
            } else {
                try self.values.append(value);
            }
        }

        pub fn as_series_type(self: *Self) VariantSeries {
            return switch (T) {
                bool => VariantSeries{ .bool = self },

                u8 => VariantSeries{ .uint8 = self },
                u16 => VariantSeries{ .uint16 = self },
                u32 => VariantSeries{ .uint32 = self },
                u64 => VariantSeries{ .uint64 = self },

                i8 => VariantSeries{ .int8 = self },
                i16 => VariantSeries{ .int16 = self },
                i32 => VariantSeries{ .int32 = self },
                i64 => VariantSeries{ .int64 = self },

                f32 => VariantSeries{ .float32 = self },
                f64 => VariantSeries{ .float64 = self },

                std.ArrayListUnmanaged(u8) => VariantSeries{ .string = self },

                // Add other types as needed
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }
    };
}
