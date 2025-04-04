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
        for (self.series.items) |series| {
            series.deinit();
        }
        self.series.deinit();
    }

    pub fn create_series(self: *Self, comptime T: type) !*Series(T) {
        const series = try Series(T).init(self.allocator);
        try self.series.append(series.as_series_type());

        return series;
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
};
