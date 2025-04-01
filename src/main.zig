//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");

/// This imports the separate module containing `root.zig`. Take a look in `build.zig` for details.
const lib = @import("zeddy_lib");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var dataframe = Dataframe.init(allocator);
    defer dataframe.deinit();

    var series = try Series(i32).init(allocator);
    // defer series.deinit();

    try series.rename("UWU");

    try series.append(15);
    try series.append(20);
    try series.append(30);

    try dataframe.add_series(series.as_series_type());

    series.print();

    std.debug.print("Series created with {} values\n", .{series.values.items.len});
}

const SeriesType = union(enum) {
    // bool: *Series(bool),
    // byte: *Series(u8),
    // int16: *Series(i16),
    int32: *Series(i32),
    // int64: *Series(i64),
    // float32: *Series(f32),
    // float64: *Series(f64),
    // string: *Series([]u8),

    // Complex types
    // datetime: *Series(datetime),
    // category: *Series(category),
    // object: *Series(object),
    // list: *Series(list),
    // dict: *Series(dict),
    // custom: *Series(anyopaque), I dont know if this is a good idea.

    pub fn deinit(self: SeriesType) void {
        switch (self) {
            inline else => |ptr| {
                const Type = std.meta.Child(@TypeOf(ptr));
                std.debug.print("Deinitializing series of type {s}\n", .{@typeName(Type)});

                // Check if the type has a deinit method
                if (comptime @hasDecl(Type, "deinit")) {
                    ptr.deinit();
                } else {
                    @compileError("Type " ++ @typeName(Type) ++ " does not have a deinit method");
                }
            },
        }
    }
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
        for (self.series.items) |series| {
            series.deinit();
        }
        self.series.deinit();
    }

    pub fn add_series(self: *Self, series: SeriesType) !void {
        try self.series.append(series);
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

        pub fn as_series_type(self: Self) SeriesType {
            return switch (T) {
                i32 => SeriesType{ .int32 = &self },
                // bool => SeriesType.bool(self),
                // byte => SeriesType.byte(self),
                // int16 => SeriesType.int16(self),
                // int64 => SeriesType.int64(self),
                // float32 => SeriesType.float32(self),
                // float64 => SeriesType.float64(self),
                // string => SeriesType.string(self),
                else => @compileError("Unsupported type " ++ @typeName(T) ++ " for SeriesType conversion"),
            };
        }
    };

    return TypedSeries;
}
