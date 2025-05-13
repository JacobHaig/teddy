const std = @import("std");
const Series = @import("series.zig").Series;
const VariantSeries = @import("variant_series.zig").VariantSeries;
const UnmanagedString = @import("variant_series.zig").UnmanagedString;
const ManagedString = @import("variant_series.zig").ManagedString;
const stringer = @import("variant_series.zig").stringer;

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

        const new_series_var = series.*.to_variant_series();
        try self.series.append(new_series_var);
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

    pub fn apply_inplace(self: *Self, name: []const u8, comptime T: type, comptime func: fn (x: T) T) void {
        const series = self.get_series(name) orelse return;

        series.*.apply_inplace(T, func);
    }

    pub fn apply_new(self: *Self, new_name: []const u8, name: []const u8, comptime T: type, comptime func: fn (x: T) T) !void {
        const series = self.get_series(name) orelse return;
        var new_series = try series.deep_copy();

        new_series.apply_inplace(T, func);
        try new_series.rename(new_name);

        try self.add_series(new_series);
    }

    pub fn rename(self: *Self, name: []const u8, new_name: []const u8) void {
        const series = self.get_series(name) orelse return;
        try series.rename(new_name);
    }

    pub fn deep_copy(self: *Self) !*Self {
        const new_dataframe = try Self.init(self.allocator);
        errdefer new_dataframe.deinit();

        for (self.series.items) |*series| {
            var new_series = try series.*.deep_copy();
            errdefer new_series.deinit();

            try new_dataframe.series.append(new_series);
        }

        return new_dataframe;
    }

    pub fn limit(self: *Self, n_limit: usize) void {
        for (self.series.items) |*item| {
            item.limit(n_limit);
        }
    }

    pub fn print(self: *Self) !void {
        const max_rows = 100;
        const wwidth = self.width();
        const hheight = self.height();

        const print_rows = if (hheight > max_rows) max_rows else hheight;

        // Get string representation of each series. Name, Type, Values (up to max_rows)
        // Count the number of characters to get the max width for each column
        // Also include the header and datatype in the width calculation

        var all_series: std.ArrayList(std.ArrayList(ManagedString)) = std.ArrayList(std.ArrayList(ManagedString)).init(self.allocator);
        errdefer all_series.deinit();

        // Create a series of strings.
        for (0..wwidth) |w| {
            var string_series = std.ArrayList(ManagedString).init(self.allocator);
            var varseries = self.series.items[w];

            try string_series.append(try varseries.name_as_string()); // Name
            try string_series.append(try varseries.type_as_string()); // Type
            for (0..print_rows) |h| {
                try string_series.append(try varseries.as_string_at(h)); // Value
            }

            try all_series.append(string_series);
        }

        // Deinit the series of strings after use
        defer {
            for (all_series.items) |series| {
                for (series.items) |str| {
                    str.deinit();
                }
                series.deinit();
            }
            all_series.deinit();
        }

        // Calculate the max width for each column
        var max_widths = std.ArrayList(usize).init(self.allocator);
        defer max_widths.deinit();

        for (0..wwidth) |w| {
            var max_width: usize = 0;
            const series: std.ArrayList(ManagedString) = all_series.items[w];

            for (series.items) |str| {
                const len = str.items.len;
                if (len > max_width) {
                    max_width = len;
                }
            }
            try max_widths.append(max_width);
        }

        // Print the Table to stdout
        const writer = std.io.getStdOut().writer();
        // Print the header, type, and values. The header and type require us to include the +2
        for (0..print_rows + 2) |h| {
            for (0..wwidth) |w| {
                const str: []u8 = all_series.items[w].items[h].items;
                const custom_width = max_widths.items[w];

                try std.fmt.format(writer, "| {s:[width]} ", .{ .s = str, .width = custom_width });
            }
            try std.fmt.format(writer, "|\n", .{});
        }
    }
};

fn print_type_info(something: anytype) void {
    const t = @TypeOf(something);
    // std.debug.print("Type: ", .{t});
    std.debug.print("TypeName: {s}\n", .{@typeName(t)});
    std.debug.print("TypeInfo: {}\n", .{@typeInfo(t)});
}

test "basic manipulations" {
    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.create_series(UnmanagedString);
    try series.rename("Name");
    try series.append(try stringer(std.testing.allocator, "Alice"));
    try series.try_append(try stringer(std.testing.allocator, "Gary"));
    try series.try_append("Bob");
    // series.print();

    var series2 = try df.create_series(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(110000.0);
    // series2.print();

    df.apply_inplace("Salary", f32, struct {
        fn call(x: f32) f32 {
            return x / 52 / 40;
        }
    }.call);
    // series2.print();

    var series3 = try df.create_series(i32);
    try series3.rename("Age");
    try series3.append(15);
    try series3.append(20);
    try series3.append(30);
    // series3.print();

    const add_five = struct {
        fn call(x: i32) i32 {
            return x + 5;
        }
    }.call;
    df.apply_inplace("Age", i32, add_five);

    df.apply_inplace("Age", i32, struct {
        fn call(x: i32) i32 {
            return x + 10;
        }
    }.call);

    var df2 = try df.deep_copy();
    defer df2.deinit();

    df.drop_series("Age");
    df.limit(2);

    try std.testing.expectEqual(2, df.height());
    try std.testing.expectEqual(2, df.width());
    try std.testing.expectEqual(3, df2.height());
    try std.testing.expectEqual(3, df2.width());
}
