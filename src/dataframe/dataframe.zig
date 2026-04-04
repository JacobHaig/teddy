const std = @import("std");

// const strings = @import("strings.zig");
const String = @import("strings.zig").String;

const Series = @import("series.zig").Series;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const BoxedGroupBy = @import("boxed_groupby.zig").BoxedGroupBy;
pub const Reader = @import("reader.zig").Reader;
pub const Writer = @import("writer.zig").Writer;
const GroupBy = @import("group.zig").GroupBy;

pub const Dataframe = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    series: std.ArrayList(BoxedSeries),

    /// Allocates a new Dataframe on the heap. Caller owns the returned pointer and must call deinit.
    pub fn init(allocator: std.mem.Allocator) !*Self {
        const dataframe_ptr = try allocator.create(Self);
        errdefer allocator.destroy(dataframe_ptr);
        dataframe_ptr.allocator = allocator;
        dataframe_ptr.series = try std.ArrayList(BoxedSeries).initCapacity(allocator, 0);
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
        const new_series_var = series.*.toBoxedSeries();
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

    /// Adds a BoxedSeries to the Dataframe. Dataframe takes ownership and will deinit it. Caller must not deinit after this call.
    pub fn addSeries(self: *Self, series: BoxedSeries) !void {
        try self.series.append(self.allocator, series);
    }

    /// Returns a pointer to the series with the given name, or null if not found. Dataframe retains ownership.
    pub fn getSeries(self: *Self, name: []const u8) ?*BoxedSeries {
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

    /// GroupBy method to group Dataframe rows by values in a Series.
    ///
    /// Usage:
    /// var group_by = try df.groupBy("column_name");
    /// defer group_by.deinit();
    pub fn groupBy(self: *Self, column: []const u8) !BoxedGroupBy {
        const boxed_series = self.getSeries(column) orelse return error.DoesNotExist;
        return boxed_series.groupBy(self.allocator, self);
    }

    /// Creates a new Dataframe containing only the rows at the given indices.
    /// Caller owns the returned pointer and must call deinit.
    pub fn filterByIndices(self: *Self, indices: []const usize) !*Self {
        const new_df = try Self.init(self.allocator);
        errdefer new_df.deinit();
        for (self.series.items) |*s| {
            const new_s = try s.filterByIndices(indices);
            try new_df.series.append(self.allocator, new_s);
        }
        return new_df;
    }

    /// Returns a new Dataframe with only the named columns. Caller owns the returned pointer.
    pub fn select(self: *Self, columns: []const []const u8) !*Self {
        const new_df = try Self.init(self.allocator);
        errdefer new_df.deinit();
        for (columns) |col_name| {
            var s = self.getSeries(col_name) orelse return error.ColumnNotFound;
            const new_s = try s.deepCopy();
            try new_df.series.append(self.allocator, new_s);
        }
        return new_df;
    }

    /// Returns a new Dataframe with the first n rows. Caller owns the returned pointer.
    pub fn head(self: *Self, n: usize) !*Self {
        const actual_n = @min(n, self.height());
        var indices = try std.ArrayList(usize).initCapacity(self.allocator, actual_n);
        defer indices.deinit(self.allocator);
        for (0..actual_n) |i| try indices.append(self.allocator, i);
        return self.filterByIndices(indices.items);
    }

    /// Returns a new Dataframe with the last n rows. Caller owns the returned pointer.
    pub fn tail(self: *Self, n: usize) !*Self {
        const h = self.height();
        const actual_n = @min(n, h);
        const start = h - actual_n;
        var indices = try std.ArrayList(usize).initCapacity(self.allocator, actual_n);
        defer indices.deinit(self.allocator);
        for (start..h) |i| try indices.append(self.allocator, i);
        return self.filterByIndices(indices.items);
    }

    pub const CompareOp = @import("boxed_series.zig").CompareOp;

    /// Returns a new Dataframe with rows where column matches the comparison.
    /// For numeric columns: df.filter("Age", i64, .gt, 30)
    /// For string columns:  df.filter("City", []const u8, .eq, "Riverside")
    pub fn filter(self: *Self, column: []const u8, comptime T: type, op: CompareOp, value: T) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;

        // Auto-convert string-like types to internal String for comparison
        if (comptime T == []const u8 or T == []u8 or isStringLiteral(T)) {
            var str_val = try String.fromSlice(self.allocator, value);
            defer str_val.deinit();
            var indices = try boxed.indicesWhere(String, self.allocator, op, str_val);
            defer indices.deinit(self.allocator);
            return self.filterByIndices(indices.items);
        }

        var indices = try boxed.indicesWhere(T, self.allocator, op, value);
        defer indices.deinit(self.allocator);
        return self.filterByIndices(indices.items);
    }

    fn isStringLiteral(comptime T: type) bool {
        return @typeInfo(T) == .pointer and
            @typeInfo(T).pointer.is_const and
            @typeInfo(T).pointer.size == .one and
            @typeInfo(@typeInfo(T).pointer.child) == .array and
            @typeInfo(@typeInfo(T).pointer.child).array.child == u8;
    }

    /// Returns a new Dataframe sorted by the named column. Caller owns the returned pointer.
    pub fn sort(self: *Self, column: []const u8, ascending: bool) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        var indices = try boxed.argSort(self.allocator, ascending);
        defer indices.deinit(self.allocator);
        return self.filterByIndices(indices.items);
    }

    /// Group by multiple columns using a composite string key.
    /// Returns a BoxedGroupBy keyed by composite string. Caller must deinit.
    /// The composite key column is added to the dataframe (named "_group_key").
    /// Call dropSeries("_group_key") after you're done with the GroupBy if you want to clean it up.
    pub fn groupByMultiple(self: *Self, columns: []const []const u8) !BoxedGroupBy {
        // Build composite key series
        var composite = try Series(String).init(self.allocator);
        errdefer composite.deinit();
        try composite.rename("_group_key");

        for (0..self.height()) |row| {
            var key = try String.init(self.allocator);
            errdefer key.deinit();
            for (columns, 0..) |col_name, ci| {
                if (ci > 0) try key.appendSlice("|");
                var s = self.getSeries(col_name) orelse {
                    composite.deinit();
                    return error.ColumnNotFound;
                };
                var str_val = try s.asStringAt(row);
                defer str_val.deinit();
                try key.appendSlice(str_val.toSlice());
            }
            try composite.values.append(self.allocator, key);
        }

        // Add composite to dataframe — it owns it, and GroupBy references it
        try self.series.append(self.allocator, composite.toBoxedSeries());

        return try self.groupBy("_group_key");
    }

    pub const CsvWriteOptions = @import("csv_writer.zig").WriteOptions;
    pub const JsonFormat = @import("json_writer.zig").JsonFormat;
    pub const JoinType = @import("join.zig").JoinType;

    /// Join this dataframe with another on a key column. Caller owns the returned pointer.
    pub fn join(self: *Self, other: *Self, on: []const u8, join_type: JoinType) !*Self {
        return @import("join.zig").join(self.allocator, self, other, on, join_type);
    }

    /// Write this Dataframe to a CSV string. Caller must free the returned slice.
    pub fn toCsvString(self: *Self, options: CsvWriteOptions) ![]u8 {
        return @import("csv_writer.zig").writeToString(self.allocator, self, options);
    }

    /// Returns a new Dataframe with summary statistics for numeric columns.
    /// Rows: count, mean, std, min, max. Caller owns the returned pointer.
    pub fn describe(self: *Self) !*Self {
        const result = try Self.init(self.allocator);
        errdefer result.deinit();

        // Create stat label column
        var stat_col = try result.createSeries(String);
        try stat_col.rename("stat");
        try stat_col.tryAppend("count");
        try stat_col.tryAppend("mean");
        try stat_col.tryAppend("std");
        try stat_col.tryAppend("min");
        try stat_col.tryAppend("max");

        // For each numeric column, compute stats
        for (self.series.items) |*s| {
            const stats = computeStats(s) orelse continue;
            var val_col = try result.createSeries(f64);
            try val_col.rename(s.name());
            try val_col.append(stats.count);
            try val_col.append(stats.mean_val);
            try val_col.append(stats.std_val);
            try val_col.append(stats.min_val);
            try val_col.append(stats.max_val);
        }

        return result;
    }

    const Stats = struct { count: f64, mean_val: f64, std_val: f64, min_val: f64, max_val: f64 };

    fn computeStats(s: *BoxedSeries) ?Stats {
        const mean_val = s.mean() orelse return null;
        return .{
            .count   = @floatFromInt(s.len()),
            .mean_val = mean_val,
            .std_val  = s.stdDev() orelse 0,
            .min_val  = s.min() orelse 0,
            .max_val  = s.max() orelse 0,
        };
    }

    /// Write this Dataframe to a JSON string. Caller must free the returned slice.
    pub fn toJsonString(self: *Self, format: JsonFormat) ![]u8 {
        return @import("json_writer.zig").writeToString(self.allocator, self, format);
    }

    /// Vertically concatenate another dataframe. Columns must match by name and type.
    /// Returns a new Dataframe. Caller owns the returned pointer.
    pub fn concat(self: *Self, other: *Self) !*Self {
        const new_df = try Self.init(self.allocator);
        errdefer new_df.deinit();

        for (self.series.items) |*s| {
            const col_name = s.name();
            const other_s = other.getSeries(col_name) orelse return error.ColumnNotFound;
            var new_s = try s.deepCopy();
            try new_s.appendSeries(other_s);
            try new_df.series.append(self.allocator, new_s);
        }
        return new_df;
    }

    /// Returns a new Dataframe with duplicate rows removed based on a column.
    /// Keeps the first occurrence. Caller owns the returned pointer.
    pub fn unique(self: *Self, column: []const u8) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        var indices = try boxed.uniqueIndices(self.allocator);
        defer indices.deinit(self.allocator);
        return self.filterByIndices(indices.items);
    }

    /// Shorthand for groupBy(column).count(). Returns a *Dataframe with key + count columns.
    pub fn valueCounts(self: *Self, column: []const u8) !*Self {
        var gb = try self.groupBy(column);
        defer gb.deinit();
        return gb.count();
    }

    /// Returns a new Dataframe with rows dropped where `column` is null. Caller owns the returned pointer.
    /// Private: collects row indices where `is_row_invalid` returns false, then filters.
    /// `column` is the single column to check (null means check all columns).
    fn dropNullsImpl(self: *Self, column: ?[]const u8) !*Self {
        // Validate the column name up front if one was given.
        if (column) |col| {
            if (self.getSeries(col) == null) return error.ColumnNotFound;
        }
        var valid_indices = std.ArrayList(usize).empty;
        defer valid_indices.deinit(self.allocator);
        outer: for (0..self.height()) |i| {
            if (column) |col| {
                // Safe to unwrap: validated above.
                if (self.getSeries(col).?.isNull(i)) continue :outer;
            } else {
                for (self.series.items) |*boxed| {
                    if (boxed.isNull(i)) continue :outer;
                }
            }
            try valid_indices.append(self.allocator, i);
        }
        return self.filterByIndices(valid_indices.items);
    }

    /// Returns a new Dataframe with rows dropped where `column` is null. Caller owns the returned pointer.
    pub fn dropNulls(self: *Self, column: []const u8) !*Self {
        return self.dropNullsImpl(column);
    }

    /// Returns a new Dataframe with rows dropped where any column is null. Caller owns the returned pointer.
    pub fn dropNullsAny(self: *Self) !*Self {
        return self.dropNullsImpl(null);
    }

    /// Returns a new Dataframe with nulls in `column` filled by `value`. Caller owns the returned pointer.
    /// Builds the result column-by-column: the target column goes through Series.fillNull (one pass,
    /// one allocation), all others are deep-copied. No full-dataframe copy is made.
    pub fn fillNull(self: *Self, column: []const u8, comptime T: type, value: T) !*Self {
        const result = try Self.init(self.allocator);
        errdefer result.deinit();

        var found = false;
        for (self.series.items) |*boxed| {
            if (std.mem.eql(u8, boxed.name(), column)) {
                const typed: *Series(T) = switch (boxed.*) {
                    inline else => |s| if (@TypeOf(s) == *Series(T)) s else return error.TypeMismatch,
                };
                const filled = try typed.fillNull(value);
                errdefer filled.deinit();
                try result.addSeries(filled.toBoxedSeries());
                found = true;
            } else {
                try result.addSeries(try boxed.deepCopy());
            }
        }

        if (!found) return error.ColumnNotFound;
        return result;
    }

    fn castImpl(self: *Self, column: []const u8, comptime Target: type, comptime mode: enum { safe, strict, lossy }) !*Self {
        const result = try Self.init(self.allocator);
        errdefer result.deinit();
        var found = false;
        for (self.series.items) |*boxed| {
            if (std.mem.eql(u8, boxed.name(), column)) {
                const new_s = switch (mode) {
                    .safe => try boxed.castSafe(Target),
                    .strict => try boxed.cast(Target),
                    .lossy => try boxed.castLossy(Target),
                };
                try result.addSeries(new_s);
                found = true;
            } else {
                try result.addSeries(try boxed.deepCopy());
            }
        }
        if (!found) return error.ColumnNotFound;
        return result;
    }

    /// Comptime-verified lossless cast. Compile error if the types are not safely widening.
    pub fn castSafe(self: *Self, column: []const u8, comptime Target: type) !*Self {
        return self.castImpl(column, Target, .safe);
    }

    /// Strict cast. Returns error if any value overflows or fails to parse.
    pub fn cast(self: *Self, column: []const u8, comptime Target: type) !*Self {
        return self.castImpl(column, Target, .strict);
    }

    /// Permissive cast. Conversion failures become null; float→int truncates.
    pub fn castLossy(self: *Self, column: []const u8, comptime Target: type) !*Self {
        return self.castImpl(column, Target, .lossy);
    }

    /// Returns a new Dataframe with rows [start..end). Caller owns the returned pointer.
    pub fn slice(self: *Self, start: usize, end: usize) !*Self {
        const actual_end = @min(end, self.height());
        if (start >= actual_end) return Self.init(self.allocator);
        var indices = try std.ArrayList(usize).initCapacity(self.allocator, actual_end - start);
        defer indices.deinit(self.allocator);
        for (start..actual_end) |i| try indices.append(self.allocator, i);
        return self.filterByIndices(indices.items);
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

        // Build the entire table into a buffer, then write all at once
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);

        // +------+------+  top border
        try buf.append(self.allocator, '+');
        for (0..wwidth) |w| {
            if (w > 0) try buf.append(self.allocator, '+');
            for (0..max_widths.items[w] + 2) |_| try buf.append(self.allocator, '-');
        }
        try buf.appendSlice(self.allocator, "+\n");

        // | Name | Type |  header + type rows
        for (0..2) |h| {
            try buf.append(self.allocator, '|');
            for (0..wwidth) |w| {
                const str = all_series.items[w].items[h].toSlice();
                const cw = max_widths.items[w];
                try buf.append(self.allocator, ' ');
                const pad = cw - str.len;
                for (0..pad) |_| try buf.append(self.allocator, ' ');
                try buf.appendSlice(self.allocator, str);
                try buf.append(self.allocator, ' ');
                try buf.append(self.allocator, '|');
            }
            try buf.append(self.allocator, '\n');
        }

        // +------+------+  separator after header
        try buf.append(self.allocator, '+');
        for (0..wwidth) |w| {
            if (w > 0) try buf.append(self.allocator, '+');
            for (0..max_widths.items[w] + 2) |_| try buf.append(self.allocator, '-');
        }
        try buf.appendSlice(self.allocator, "+\n");

        // | val  | val  |  data rows
        for (0..print_rows) |h| {
            try buf.append(self.allocator, '|');
            for (0..wwidth) |w| {
                const str = all_series.items[w].items[h + 2].toSlice();
                const cw = max_widths.items[w];
                try buf.append(self.allocator, ' ');
                const pad = cw - str.len;
                for (0..pad) |_| try buf.append(self.allocator, ' ');
                try buf.appendSlice(self.allocator, str);
                try buf.append(self.allocator, ' ');
                try buf.append(self.allocator, '|');
            }
            try buf.append(self.allocator, '\n');
        }

        // +------+------+  bottom border
        try buf.append(self.allocator, '+');
        for (0..wwidth) |w| {
            if (w > 0) try buf.append(self.allocator, '+');
            for (0..max_widths.items[w] + 2) |_| try buf.append(self.allocator, '-');
        }
        try buf.appendSlice(self.allocator, "+\n");

        // Write all at once
        std.debug.print("{s}", .{buf.items});
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
    try df.addSeries(s.toBoxedSeries());
    // Do NOT deinit s after ownership is transferred
    try std.testing.expect(df.width() == 1);
    try std.testing.expect(df.height() == 3);

    var s2 = try Series(i32).init(allocator);
    try s2.rename("col2");
    try s2.append(10);
    try s2.append(20);
    try s2.append(30);
    try df.addSeries(s2.toBoxedSeries());
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
    try df1.addSeries(s1.toBoxedSeries());
    // Do NOT deinit s1 after ownership is transferred

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var s2 = try Series(i32).init(allocator);
    try s2.rename("col");
    try s2.append(1);
    try s2.append(2);
    try df2.addSeries(s2.toBoxedSeries());
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
    try df3.addSeries(s3.toBoxedSeries());
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
    const csv_mod = @import("csv_reader.zig");
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

    // Test CSV parsing allocation and deallocation
    const content =
        "A,B\n1,2\n3,4\n";

    // Test Dataframe allocation and deallocation
    var df = try csv_mod.parse(allocator, content, .{});
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
    try df.addSeries(s.toBoxedSeries());
    // Do NOT deinit s, dataframe owns it now

    try std.testing.expect(!had_leak);
}

test "groupBy: count groups" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);
    try category.append(2);
    try category.append(1);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    try std.testing.expect(counts.height() == 2);
    try std.testing.expect(counts.width() == 2);
    const count_series = counts.getSeries("count") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(usize, 3), count_series.usize.values.items[0]);
    try std.testing.expectEqual(@as(usize, 2), count_series.usize.values.items[1]);
}

test "groupBy: sum by group" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);
    try category.append(2);

    var values = try df.createSeries(i32);
    try values.rename("values");
    try values.append(10);
    try values.append(20);
    try values.append(15);
    try values.append(25);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var sum_result = try gb.sum("values");
    defer sum_result.deinit();

    try std.testing.expect(sum_result.height() == 2);
    const sum_col = sum_result.getSeries("values") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 25), sum_col.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 45), sum_col.int32.values.items[1]);
}

test "groupBy: mean by group" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);

    var values = try df.createSeries(i32);
    try values.rename("values");
    try values.append(10);
    try values.append(20);
    try values.append(20);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var mean_result = try gb.mean("values");
    defer mean_result.deinit();

    try std.testing.expect(mean_result.height() == 2);
    // mean of [10, 20] for group 1 = 15.0, mean of [20] for group 2 = 20.0
    const mean_col = mean_result.getSeries("values") orelse return error.DoesNotExist;
    try std.testing.expectEqual(15.0, mean_col.float64.values.items[0]);
    try std.testing.expectEqual(20.0, mean_col.float64.values.items[1]);
}

test "Dataframe: filterByIndices creates correct subset" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col1 = try df.createSeries(i32);
    try col1.rename("a");
    try col1.append(10);
    try col1.append(20);
    try col1.append(30);
    try col1.append(40);

    var col2 = try df.createSeries(i32);
    try col2.rename("b");
    try col2.append(1);
    try col2.append(2);
    try col2.append(3);
    try col2.append(4);

    var filtered = try df.filterByIndices(&[_]usize{ 0, 2 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
    try std.testing.expectEqual(@as(usize, 2), filtered.width());

    const a = filtered.getSeries("a") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), a.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), a.int32.values.items[1]);

    const b = filtered.getSeries("b") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 1), b.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 3), b.int32.values.items[1]);
}

test "Dataframe: select picks named columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try df.createSeries(i32);
    try a.rename("a");
    try a.append(1);

    var b = try df.createSeries(i32);
    try b.rename("b");
    try b.append(2);

    var c = try df.createSeries(i32);
    try c.rename("c");
    try c.append(3);

    var selected = try df.select(&[_][]const u8{ "a", "c" });
    defer selected.deinit();

    try std.testing.expectEqual(@as(usize, 2), selected.width());
    try std.testing.expect(selected.getSeries("a") != null);
    try std.testing.expect(selected.getSeries("b") == null);
    try std.testing.expect(selected.getSeries("c") != null);
}

test "Dataframe: select missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try df.createSeries(i32);
    try a.rename("a");
    try a.append(1);

    try std.testing.expectError(error.ColumnNotFound, df.select(&[_][]const u8{"nope"}));
}

test "Dataframe: head returns first n rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);

    var h = try df.head(2);
    defer h.deinit();

    try std.testing.expectEqual(@as(usize, 2), h.height());
    const s = h.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
}

test "Dataframe: tail returns last n rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);

    var t = try df.tail(2);
    defer t.deinit();

    try std.testing.expectEqual(@as(usize, 2), t.height());
    const s = t.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[1]);
}

test "Dataframe: slice returns row range" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);
    try col.append(40);

    var s = try df.slice(1, 3);
    defer s.deinit();

    try std.testing.expectEqual(@as(usize, 2), s.height());
    const xs = s.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), xs.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), xs.int32.values.items[1]);
}

test "Dataframe: head with n > height returns all rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    var h = try df.head(100);
    defer h.deinit();

    try std.testing.expectEqual(@as(usize, 1), h.height());
}

test "Dataframe: sort ascending" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(30);
    try col.append(10);
    try col.append(20);

    var sorted = try df.sort("x", true);
    defer sorted.deinit();

    const s = sorted.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[2]);
}

test "Dataframe: sort descending" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(30);
    try col.append(20);

    var sorted = try df.sort("x", false);
    defer sorted.deinit();

    const s = sorted.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[2]);
}

test "Dataframe: sort preserves other columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var ages = try df.createSeries(i32);
    try ages.rename("age");
    try ages.append(30);
    try ages.append(10);
    try ages.append(20);

    var names = try df.createSeries(String);
    try names.rename("name");
    try names.tryAppend("Alice");
    try names.tryAppend("Bob");
    try names.tryAppend("Carol");

    var sorted = try df.sort("age", true);
    defer sorted.deinit();

    const n = sorted.getSeries("name") orelse return error.DoesNotExist;
    try std.testing.expectEqualStrings("Bob", n.string.values.items[0].toSlice());
    try std.testing.expectEqualStrings("Carol", n.string.values.items[1].toSlice());
    try std.testing.expectEqualStrings("Alice", n.string.values.items[2].toSlice());
}

test "Dataframe: filter gt" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);
    try col.append(5);

    var filtered = try df.filter("x", i32, .gt, 15);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
    const s = filtered.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[1]);
}

test "Dataframe: filter eq" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);

    var filtered = try df.filter("x", i32, .eq, 1);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
}

test "Dataframe: filter no matches returns empty" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    var filtered = try df.filter("x", i32, .gt, 100);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 0), filtered.height());
}

test "Dataframe: filter missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    try std.testing.expectError(error.ColumnNotFound, df.filter("nope", i32, .eq, 1));
}

test "Dataframe: unique removes duplicates" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);
    try col.append(3);
    try col.append(2);

    var u = try df.unique("x");
    defer u.deinit();

    try std.testing.expectEqual(@as(usize, 3), u.height());
}

test "Dataframe: valueCounts returns key and count" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);

    var vc = try df.valueCounts("x");
    defer vc.deinit();

    try std.testing.expectEqual(@as(usize, 2), vc.width());
    try std.testing.expectEqual(@as(usize, 2), vc.height());
    try std.testing.expect(vc.getSeries("x") != null);
    try std.testing.expect(vc.getSeries("count") != null);
}

test "Dataframe: concat stacks rows" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var a1 = try df1.createSeries(i32);
    try a1.rename("x");
    try a1.append(1);
    try a1.append(2);

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var a2 = try df2.createSeries(i32);
    try a2.rename("x");
    try a2.append(3);

    var combined = try df1.concat(df2);
    defer combined.deinit();

    try std.testing.expectEqual(@as(usize, 3), combined.height());
    const s = combined.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 1), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 2), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 3), s.int32.values.items[2]);
}

test "Dataframe: concat missing column returns error" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var a1 = try df1.createSeries(i32);
    try a1.rename("x");
    try a1.append(1);

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var a2 = try df2.createSeries(i32);
    try a2.rename("y");
    try a2.append(1);

    try std.testing.expectError(error.ColumnNotFound, df1.concat(df2));
}

test "Dataframe: describe returns summary statistics" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);

    var str_col = try df.createSeries(String);
    try str_col.rename("name");
    try str_col.tryAppend("a");
    try str_col.tryAppend("b");

    var desc = try df.describe();
    defer desc.deinit();

    // Should have "stat" + "x" columns (string column skipped)
    try std.testing.expectEqual(@as(usize, 2), desc.width());
    try std.testing.expectEqual(@as(usize, 5), desc.height());

    const x_col = desc.getSeries("x") orelse return error.DoesNotExist;
    // count=2, mean=15, std=5, min=10, max=20
    try std.testing.expectEqual(@as(f64, 2.0), x_col.float64.values.items[0]);
    try std.testing.expectEqual(@as(f64, 15.0), x_col.float64.values.items[1]);
    try std.testing.expectEqual(@as(f64, 5.0), x_col.float64.values.items[2]);
    try std.testing.expectEqual(@as(f64, 10.0), x_col.float64.values.items[3]);
    try std.testing.expectEqual(@as(f64, 20.0), x_col.float64.values.items[4]);
}

test "Dataframe: groupByMultiple composite key" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var state = try df.createSeries(String);
    try state.rename("state");
    try state.tryAppend("NJ");
    try state.tryAppend("PA");
    try state.tryAppend("NJ");

    var city = try df.createSeries(String);
    try city.rename("city");
    try city.tryAppend("Riverside");
    try city.tryAppend("Phila");
    try city.tryAppend("Riverside");

    var val = try df.createSeries(i32);
    try val.rename("val");
    try val.append(10);
    try val.append(20);
    try val.append(30);

    var gb = try df.groupByMultiple(&[_][]const u8{ "state", "city" });
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    // NJ|Riverside=2, PA|Phila=1
    try std.testing.expectEqual(@as(usize, 2), counts.height());
}

// Pull in tests from submodules
test {
    _ = @import("json_reader.zig");
    _ = @import("json_writer.zig");
    _ = @import("csv_writer.zig");
    _ = @import("join.zig");
    _ = @import("writer.zig");
}

// --- Nullable DataFrame Tests ---

test "Dataframe: dropNulls removes rows where column is null" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.appendNull();
    try col.append(3);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.dropNulls("x");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
    const s = result.getSeries("x").?;
    for (0..result.height()) |i| try std.testing.expect(!s.isNull(i));
}

test "Dataframe: dropNulls unknown column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.dropNulls("nope"));
}

test "Dataframe: dropNullsAny removes rows with any null" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i64).init(allocator);
    try a.rename("a");
    try a.append(1);
    try a.appendNull(); // row 1 has null in "a"
    try a.append(3);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i64).init(allocator);
    try b.rename("b");
    try b.append(10);
    try b.append(20);
    try b.appendNull(); // row 2 has null in "b"
    try df.addSeries(b.toBoxedSeries());

    var result = try df.dropNullsAny();
    defer result.deinit();

    // Only row 0 has no nulls in either column.
    try std.testing.expectEqual(@as(usize, 1), result.height());
}

test "Dataframe: dropNullsAny with no nulls returns full copy" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.dropNullsAny();
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
}

test "Dataframe: fillNull replaces nulls in target column only" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i64).init(allocator);
    try a.rename("a");
    try a.append(1);
    try a.appendNull();
    try a.append(3);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i64).init(allocator);
    try b.rename("b");
    try b.appendNull();
    try b.append(20);
    try b.append(30);
    try df.addSeries(b.toBoxedSeries());

    var result = try df.fillNull("a", i64, 99);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.height());

    // Column "a": null at index 1 should be filled.
    const ra = result.getSeries("a").?;
    try std.testing.expect(!ra.isNull(0));
    try std.testing.expect(!ra.isNull(1));
    try std.testing.expect(!ra.isNull(2));
    var v = try ra.asStringAt(1);
    defer v.deinit();
    try std.testing.expectEqualStrings("99", v.toSlice());

    // Column "b": still has its original null at index 0.
    const rb = result.getSeries("b").?;
    try std.testing.expect(rb.isNull(0));
}

test "Dataframe: fillNull unknown column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.fillNull("nope", i64, 0));
}

test "Dataframe: fillNull type mismatch returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.appendNull();
    try df.addSeries(col.toBoxedSeries());

    try std.testing.expectError(error.TypeMismatch, df.fillNull("x", f64, 0.0));
}

test "Dataframe: cast i32 column to f64" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i32).init(allocator);
    try col.rename("val");
    try col.append(1);
    try col.append(2);
    try col.append(3);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.cast("val", f64);
    defer result.deinit();

    const s = result.getSeries("val") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("f64", s.typeName());
    try std.testing.expectEqual(@as(usize, 3), s.len());
}

test "Dataframe: cast preserves other columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i32).init(allocator);
    try a.rename("a");
    try a.append(10);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i32).init(allocator);
    try b.rename("b");
    try b.append(20);
    try df.addSeries(b.toBoxedSeries());

    var result = try df.cast("a", f64);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.width());
    const sa = result.getSeries("a") orelse return error.TestFailed;
    const sb = result.getSeries("b") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("f64", sa.typeName());
    try std.testing.expectEqualStrings("i32", sb.typeName());
}

test "Dataframe: cast missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.cast("nope", f64));
}
