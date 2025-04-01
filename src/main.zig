//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");

/// This imports the separate module containing `root.zig`. Take a look in `build.zig` for details.
const lib = @import("zteddy_lib");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var series = try Series(i32).init(allocator);
    defer series.deinit();

    try series.rename("UWU");

    try series.append(15);
    try series.append(20);
    try series.append(30);

    series.print();

    std.debug.print("Series created with {} values\n", .{series.values.items.len});
}

const SeriesType = union(enum) {
    int16: *Series(i16),
    int32: *Series(i32),
    int64: *Series(i64),
    float32: *Series(f32),
    float64: *Series(f64),
    string: *Series([]u8),
};

const Dataframe = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    series: std.ArrayList(SeriesType),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Dataframe{
            .allocator = allocator,
            .series = std.ArrayList(SeriesType).init(allocator),
        };
    }

    pub fn deinit(self: Self) void {
        for (self.series.items) |series_type| {
            switch (series_type) {
                .int32 => |series| {
                    series.deinit();
                    self.series.allocator.destroy(series);
                },
            }
        }
        self.series.deinit();
    }
};

pub fn Series(comptime T: type) type {
    const TypedSeries = struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        name: []u8,
        values: std.ArrayList(T),

        pub fn init(allocator: std.mem.Allocator) !Self {
            return Self{
                .allocator = allocator,
                .name = try allocator.alloc(u8, 0),
                .values = std.ArrayList(T).init(allocator),
            };
        }

        pub fn rename(self: *Self, new_name: []const u8) !void {
            self.allocator.free(self.name);
            self.name = try self.allocator.alloc(u8, new_name.len);
            std.mem.copyForwards(u8, self.name, new_name);
        }

        pub fn deinit(self: Self) void {
            self.allocator.free(self.name);
            self.values.deinit();
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
    };

    return TypedSeries;
}
