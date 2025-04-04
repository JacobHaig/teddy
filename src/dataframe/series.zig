const std = @import("std");
const VariantSeries = @import("variant_series.zig").VariantSeries;

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
            series_ptr.values = std.ArrayList(T).init(allocator);

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
            std.debug.print("{s}\n{s}\n--------\n", .{ self.name, @typeName(T) });

            for (self.values.items) |value| {
                std.debug.print("{}\n", .{value});
            }
        }

        pub fn append(self: *Self, value: T) !void {
            try self.values.append(value);
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

                // Add other types as needed
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }
    };
}
