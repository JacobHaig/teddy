//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");

/// This imports the separate module containing `root.zig`. Take a look in `build.zig` for details.
const lib = @import("zeddy_lib");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var dataframe = try Dataframe.create(allocator);
    defer dataframe.deinit();

    var series = try dataframe.create_series(i32);

    try series.rename("My Series");
    try series.append(15);
    try series.append(20);
    try series.append(30);

    series.print();

    const same_series = dataframe.get_series("My Series").?;

    same_series.print();

    std.debug.print("Series created with {} values\n", .{series.values.items.len});
}

const VariantSeries = union(enum) {
    const Self = @This();

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

    pub fn deinit(self: Self) void {
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

    pub fn print(self: Self) void {
        switch (self) {
            inline else => |ptr| {
                const Type = std.meta.Child(@TypeOf(ptr));
                std.debug.print("Deinitializing series of type {s}\n", .{@typeName(Type)});

                // Check if the type has a deinit method
                if (comptime @hasDecl(Type, "print")) {
                    ptr.print();
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
    series: std.ArrayList(VariantSeries),

    pub fn init(allocator: std.mem.Allocator) Self {
        return Dataframe{
            .allocator = allocator,
            .series = std.ArrayList(VariantSeries).init(allocator),
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*Self {
        const dataframe_ptr = try allocator.create(Self);
        errdefer allocator.destroy(dataframe_ptr);

        // Initialize fields directly, avoiding stack allocation
        dataframe_ptr.allocator = allocator;
        dataframe_ptr.series = std.ArrayList(VariantSeries).init(allocator);

        return dataframe_ptr;
    }

    pub fn deinit(self: *Self) void {
        for (self.series.items) |series| {
            series.deinit();
        }
        self.series.deinit();
    }

    pub fn create_series(self: *Self, comptime T: type) !*Series(T) {
        const series = try Series(T).create(self.allocator);
        try self.series.append(series.as_series_type());

        return series;
    }

    pub fn add_series(self: *Self, series: VariantSeries) !void {
        try self.series.append(series);
    }

    pub fn get_series(self: *Self, name: []const u8) ?*VariantSeries {
        for (self.series.items) |*series_type| {
            std.debug.print("The type is {}", .{@TypeOf(series_type)});

            switch (series_type.*) {
                inline else => |ptr| {
                    if (std.mem.eql(u8, ptr.name, name)) {
                        return series_type;
                    }
                },
            }
        }
        return null;
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

        pub fn create(allocator: std.mem.Allocator) !*Self {
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

        pub fn as_series_type(self: *Self) VariantSeries {
            return switch (T) {
                i32 => VariantSeries{ .int32 = self },
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
