const std = @import("std");
const Series = @import("series.zig").Series;
const VariantSeries = @import("variant_series.zig").VariantSeries;

pub const Dataframe = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    series: std.ArrayList(VariantSeries),

    pub fn init(allocator: std.mem.Allocator) !*Self {
        const dataframe_ptr = try allocator.create(Self);
        errdefer allocator.destroy(dataframe_ptr);

        // Initialize fields directly
        dataframe_ptr.allocator = allocator;
        dataframe_ptr.series = std.ArrayList(VariantSeries).init(allocator);

        return dataframe_ptr;
    }

    pub fn deinit(self: *Self) void {
        for (self.series.items) |*item| {
            item.deinit();
        }
        self.series.deinit();
        self.allocator.destroy(self);
    }

    pub fn create_series(self: *Self, comptime T: type) !*Series(T) {
        const series = try Series(T).init(self.allocator);
        errdefer series.deinit();

        try self.series.append(series.as_series_type());
        return series;
    }

    pub fn width(self: *Self) usize {
        return self.series.items.len;
    }

    pub fn height(self: *Self) usize {
        if (self.width() == 0) {
            return 0;
        }
        return self.series.items.ptr[0].len();
    }

    pub fn add_series(self: *Self, series: VariantSeries) !void {
        try self.series.append(series);
    }

    pub fn get_series(self: *Self, name: []const u8) ?*VariantSeries {
        for (self.series.items) |*series_type| {
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

    pub fn drop_series(self: *Self, column: []const u8) void {
        for (self.series.items, 0..) |*item, index| {
            if (std.mem.eql(u8, item.name(), column)) {
                var series = self.series.orderedRemove(index);
                series.deinit();

                return;
            }
        }
        // Error handling may not be required as the column does not exist.
    }

    pub fn drop_row(self: *Self, index: usize) void {
        for (self.series.items) |*item| {
            item.drop_row(index);
        }
    }

    pub fn apply_to_series_inplace(self: *Self, name: []const u8, comptime T: type, func: fn (x: T) T) void {
        const series = self.get_series(name) orelse return;

        switch (series.*) {
            inline else => |s| {
                if (comptime *Series(T) == @TypeOf(s)) {
                    for (s.values.items) |*value| {
                        value.* = func(value.*);
                    }
                }
            },
        }
    }
};

fn print_type_info(something: anytype) void {
    const t = @TypeOf(something);
    // std.debug.print("Type: ", .{t});
    std.debug.print("TypeName: {s}\n", .{@typeName(t)});
    std.debug.print("TypeInfo: {}\n", .{@typeInfo(t)});
}
