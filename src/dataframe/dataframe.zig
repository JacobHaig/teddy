const std = @import("std");

// const strings = @import("strings.zig");
const String = @import("strings.zig").String;

const Series = @import("series.zig").Series;
const VariantSeries = @import("variant_series.zig").VariantSeries;
pub const Reader = @import("reader.zig").Reader;

pub const Dataframe = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    series: std.ArrayList(VariantSeries),

    /// Allocates a new Dataframe on the heap. Caller owns the returned pointer and must call deinit.
    pub fn init(allocator: std.mem.Allocator) !*Self {
        const dataframe_ptr = try allocator.create(Self);
        errdefer allocator.destroy(dataframe_ptr);
        dataframe_ptr.allocator = allocator;
        dataframe_ptr.series = try std.ArrayList(VariantSeries).initCapacity(allocator, 0);
        return dataframe_ptr;
    }

    /// Deallocates all memory owned by this Dataframe, including all contained Series. After this call, the pointer is invalid.
    pub fn deinit(self: *Self) void {
        for (self.series.items) |*seriesItem| {
            seriesItem.deinit();
        }
        self.series.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Allocates a new Series and adds it to the Dataframe. Dataframe takes ownership. Returns a pointer to the Series for further use, but do not call deinit on it (Dataframe will deinit it).
    pub fn createSeries(self: *Self, comptime T: type) !*Series(T) {
        const series = try Series(T).init(self.allocator);
        errdefer series.deinit();
        const new_series_var = series.*.toVariantSeries();
        try self.series.append(self.allocator, new_series_var);
        return series;
    }

    /// Returns the number of columns in the Dataframe.
    pub fn width(self: *Self) usize {
        return self.series.items.len;
    }

    /// Returns the number of rows in the Dataframe.
    pub fn height(self: *Self) usize {
        if (self.width() == 0) {
            return 0;
        }
        return self.series.items.ptr[0].len();
    }

    /// Adds a VariantSeries to the Dataframe. Dataframe takes ownership and will deinit it. Caller must not deinit after this call.
    pub fn addSeries(self: *Self, series: VariantSeries) !void {
        try self.series.append(self.allocator, series);
    }

    /// Returns a pointer to the series with the given name, or null if not found. Dataframe retains ownership.
    pub fn getSeries(self: *Self, name: []const u8) ?*VariantSeries {
        for (self.series.items) |*series_type| {
            switch (series_type.*) {
                inline else => |ptr| {
                    if (std.mem.eql(u8, ptr.name.toSlice(), name)) {
                        return series_type;
                    }
                },
            }
        }
        return null;
    }

    /// Removes the series with the given name from the Dataframe. If not found, does nothing.
    pub fn dropSeries(self: *Self, column: []const u8) void {
        for (self.series.items, 0..) |*item, index| {
            if (std.mem.eql(u8, item.name(), column)) {
                var series = self.series.orderedRemove(index);
                series.deinit();
                return;
            }
        }
        // Error handling may not be required as the column does not exist.
    }

    /// Removes the row at the given index from all series in the Dataframe.
    pub fn dropRow(self: *Self, index: usize) void {
        for (self.series.items) |*item| {
            item.dropRow(index);
        }
    }

    /// Applies a function in-place to the series with the given name, if it exists.
    pub fn applyInplace(self: *Self, name: []const u8, comptime T: type, comptime func: fn (x: T) T) void {
        const series = self.getSeries(name) orelse return;
        series.*.applyInplace(T, func);
    }

    /// Creates a new series by applying a function to an existing series and adds it to the Dataframe.
    pub fn applyNew(self: *Self, new_name: []const u8, name: []const u8, comptime T: type, comptime func: fn (x: T) T) !void {
        const series = self.getSeries(name) orelse return;
        var new_series = try series.deepCopy();
        new_series.applyInplace(T, func);
        try new_series.rename(new_name);
        try self.addSeries(new_series);
    }

    /// Renames a series in the Dataframe from `name` to `new_name`.
    pub fn rename(self: *Self, name: []const u8, new_name: []const u8) !void {
        const series = self.getSeries(name) orelse return;
        try series.rename(new_name);
    }

    /// Allocates and returns a deep copy of this Dataframe. Caller owns the returned pointer and must call deinit.
    pub fn deepCopy(self: *Self) !*Self {
        const new_dataframe = try Self.init(self.allocator);
        errdefer new_dataframe.deinit();
        for (self.series.items) |*series| {
            var new_series = try series.*.deepCopy();
            errdefer new_series.deinit();
            try new_dataframe.series.append(self.allocator, new_series);
        }
        return new_dataframe;
    }

    /// Limits all series in the Dataframe to the first `n_limit` rows.
    pub fn limit(self: *Self, n_limit: usize) void {
        for (self.series.items) |*item| {
            item.limit(n_limit);
        }
    }

    /// Returns an ArrayList of all column names in the Dataframe. Caller must deinit the returned list.
    pub fn getColumnNames(self: *Self) !std.ArrayList([]const u8) {
        var names = std.ArrayList([]const u8).empty;
        errdefer names.deinit(self.allocator);
        for (self.series.items) |*item| {
            try names.append(self.allocator, item.name());
        }
        return names;
    }

    /// Compares this Dataframe to another for equality. Returns true if all columns and values are equal.
    pub fn compareDataframe(self: *Self, other: *Self) !bool {
        if (self.height() != other.height() or self.width() != other.width()) {
            return false;
        }
        var columns = try self.getColumnNames();
        defer columns.deinit(self.allocator);
        for (columns.items) |col_name| {
            const series_a = self.getSeries(col_name) orelse return false;
            const series_b = other.getSeries(col_name) orelse return false;
            if (series_a.len() != series_b.len()) {
                return false;
            }
            for (0..series_a.len()) |i| {
                var val_a = try series_a.asStringAt(i);
                var val_b = try series_b.asStringAt(i);
                defer val_a.deinit();
                defer val_b.deinit();
                if (!std.mem.eql(u8, val_a.toSlice(), val_b.toSlice())) {
                    return false;
                }
            }
        }
        return true;
    }

    pub fn print(self: *Self) !void {
        const max_rows = 100;
        const wwidth = self.width();
        const hheight = self.height();

        const print_rows = if (hheight > max_rows) max_rows else hheight;

        // Get string representation of each series. Name, Type, Values (up to max_rows)
        // Count the number of characters to get the max width for each column
        // Also include the header and datatype in the width calculation

        var all_series: std.ArrayList(std.ArrayList(String)) = std.ArrayList(std.ArrayList(String)).empty;
        errdefer all_series.deinit(self.allocator);

        // Create a series of strings.
        for (0..wwidth) |w| {
            var string_series = std.ArrayList(String).empty;
            var varseries = self.series.items[w];

            try string_series.append(self.allocator, try varseries.getNameOwned()); // Name
            try string_series.append(self.allocator, try varseries.getTypeAsString()); // Type
            for (0..print_rows) |h| {
                try string_series.append(self.allocator, try varseries.asStringAt(h)); // Value
            }

            try all_series.append(self.allocator, string_series);
        }

        // Deinit the series of strings after use
        defer {
            for (all_series.items) |*string_series| {
                for (string_series.items) |*str| {
                    str.deinit();
                }
                string_series.deinit(self.allocator);
            }
            all_series.deinit(self.allocator);
        }

        // Calculate the max width for each column
        var max_widths = std.ArrayList(usize).empty;
        defer max_widths.deinit(self.allocator);

        for (0..wwidth) |w| {
            var max_width: usize = 0;
            const series: std.ArrayList(String) = all_series.items[w];

            for (series.items) |str| {
                const len = str.toSlice().len;
                if (len > max_width) {
                    max_width = len;
                }
            }
            try max_widths.append(self.allocator, max_width);
        }

        // Print the Table to stdout
        // const writer = std.io.getStdOut().writer();
        var stdout_buffer: [1024]u8 = undefined;
        var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
        const stdout = &stdout_writer.interface;
        // Print the header, type, and values. The header and type require us to include the +2
        for (0..print_rows + 2) |h| {
            for (0..wwidth) |w| {
                const str: []u8 = @constCast(all_series.items[w].items[h].toSlice());
                const custom_width = max_widths.items[w];

                try stdout.print("| {s:[width]} ", .{ .s = str, .width = custom_width });
            }
            try stdout.print("|\n", .{});
        }
        try stdout.flush();
    }
};

fn printTypeInfo(something: anytype) void {
    const t = @TypeOf(something);
    // std.debug.print("Type: ", .{t});
    std.debug.print("TypeName: {s}\n", .{@typeName(t)});
    std.debug.print("TypeInfo: {}\n", .{@typeInfo(t)});
}

test "basic manipulations" {
    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.createSeries(String);
    try series.rename("Name");
    try series.append(try String.fromSlice(std.testing.allocator, "Alice"));
    try series.tryAppend(try String.fromSlice(std.testing.allocator, "Gary"));
    try series.tryAppend("Bob");

    var series2 = try df.createSeries(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(110000.0);

    df.applyInplace("Salary", f32, struct {
        fn call(x: f32) f32 {
            return x / 52 / 40;
        }
    }.call);

    var series3 = try df.createSeries(i32);
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
    df.applyInplace("Age", i32, add_five);

    df.applyInplace("Age", i32, struct {
        fn call(x: i32) i32 {
            return x + 10;
        }
    }.call);

    var df2 = try df.deepCopy();
    defer df2.deinit();

    df.dropSeries("Age");
    df.limit(2);

    try std.testing.expectEqual(2, df.height());
    try std.testing.expectEqual(2, df.width());
    try std.testing.expectEqual(3, df2.height());
    try std.testing.expectEqual(3, df2.width());
}

test "Dataframe: init, width, height, createSeries, addSeries, dropSeries, dropRow" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expect(df.width() == 0);
    try std.testing.expect(df.height() == 0);

    var s = try Series(i32).init(allocator);
    try s.rename("col1");
    try s.append(1);
    try s.append(2);
    try s.append(3);
    try df.addSeries(s.toVariantSeries());
    // Do NOT deinit s after ownership is transferred
    try std.testing.expect(df.width() == 1);
    try std.testing.expect(df.height() == 3);

    var s2 = try Series(i32).init(allocator);
    try s2.rename("col2");
    try s2.append(10);
    try s2.append(20);
    try s2.append(30);
    try df.addSeries(s2.toVariantSeries());
    // Do NOT deinit s2 after ownership is transferred
    try std.testing.expect(df.width() == 2);
    try std.testing.expect(df.height() == 3);

    df.dropSeries("col1");
    try std.testing.expect(df.width() == 1);
    df.dropRow(1);
    try std.testing.expect(df.height() == 2);
}

test "Dataframe: compareDataframe equality and inequality" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var s1 = try Series(i32).init(allocator);
    try s1.rename("col");
    try s1.append(1);
    try s1.append(2);
    try df1.addSeries(s1.toVariantSeries());
    // Do NOT deinit s1 after ownership is transferred

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var s2 = try Series(i32).init(allocator);
    try s2.rename("col");
    try s2.append(1);
    try s2.append(2);
    try df2.addSeries(s2.toVariantSeries());
    // Do NOT deinit s2 after ownership is transferred

    try std.testing.expect(try df1.compareDataframe(df2));
    // Add a new value to s2 (not owned by df2, so this is safe)
    var s3 = try Series(i32).init(allocator);
    try s3.rename("col");
    try s3.append(1);
    try s3.append(2);
    try s3.append(3);
    var df3 = try Dataframe.init(allocator);
    defer df3.deinit();
    try df3.addSeries(s3.toVariantSeries());
    try std.testing.expect(!(try df1.compareDataframe(df3)));
}

test "External Function Test: add5a" {
    const f = @import("functions.zig");

    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.createSeries(i32);
    try series.rename("Salary");
    try series.append(15000);
    try series.append(75000);

    df.applyInplace("Salary", i32, f.add5a);

    try std.testing.expect(series.len() == 2);
    try std.testing.expect(series.toSlice()[0] == 15005);
    try std.testing.expect(series.toSlice()[1] == 75005);
}

test "String re-export: can create and use String from top-level API" {
    const allocator = std.testing.allocator;
    var s = try String.init(allocator);
    defer s.deinit();
    try s.append('a');
    try s.append('b');
    try std.testing.expect(s.len() == 2);
    try std.testing.expect(s.toSlice()[0] == 'a');
    try std.testing.expect(s.toSlice()[1] == 'b');
}

test "memory management and ownership" {
    const series_mod = @import("series.zig");
    const csv_mod = @import("csv.zig");
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    var had_leak = false;
    defer {
        if (debug_allocator.deinit() == .leak) {
            std.debug.print("Memory leaks detected!\n", .{});
            had_leak = true;
        }
    }

    // Test Series allocation and deallocation
    var s = try series_mod.Series(String).init(allocator);
    defer s.deinit();
    try s.rename("Test Series");
    try s.tryAppend("Hello");
    try s.tryAppend("World");

    // Test deepCopy and ownership
    var s2 = try s.deepCopy();
    defer s2.deinit();
    try s2.rename("Copy");

    // Test CsvTokenizer allocation and deallocation
    const content =
        "A,B\n1,2\n3,4\n";
    var tokenizer = try csv_mod.CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();
    try tokenizer.readAll();
    try tokenizer.validate();

    // Test Dataframe allocation and deallocation
    var df = try tokenizer.createOwnedDataframe();
    defer df.deinit();
    try std.testing.expect(!had_leak);
}

test "dataframe series ownership" {
    const series_mod = @import("series.zig");
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    var had_leak = false;
    defer {
        if (debug_allocator.deinit() == .leak) {
            std.debug.print("Memory leaks detected!\n", .{});
            had_leak = true;
        }
    }

    // Create a dataframe and add a series to it, let dataframe own the series
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var s = try series_mod.Series(String).init(allocator);
    try s.rename("Test Series");
    try s.tryAppend("A");
    try s.tryAppend("B");
    try df.addSeries(s.toVariantSeries());
    // Do NOT deinit s, dataframe owns it now

    try std.testing.expect(!had_leak);
}
