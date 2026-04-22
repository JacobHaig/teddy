const std = @import("std");

// const strings = @import("strings.zig");
pub const String = @import("strings.zig").String;

pub const Series = @import("series.zig").Series;
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
            if (stats.min_val) |v| try val_col.append(v) else try val_col.appendNull();
            if (stats.max_val) |v| try val_col.append(v) else try val_col.appendNull();
        }

        return result;
    }

    const Stats = struct { count: f64, mean_val: f64, std_val: f64, min_val: ?f64, max_val: ?f64 };

    fn computeStats(s: *BoxedSeries) ?Stats {
        const mean_val = s.mean() orelse return null;
        return .{
            .count    = @floatFromInt(s.len() - s.nullCount()),
            .mean_val = mean_val,
            .std_val  = s.stdDev() orelse 0,
            .min_val  = s.min(),
            .max_val  = s.max(),
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

    // --- Column transform helpers (replace one column, deepCopy the rest) ---

    fn columnTransform(self: *Self, column: []const u8, new_boxed: BoxedSeries) !*Self {
        var nb = new_boxed;
        const result = try Self.init(self.allocator);
        errdefer result.deinit();
        var found = false;
        for (self.series.items) |*boxed| {
            if (!found and std.mem.eql(u8, boxed.name(), column)) {
                try result.addSeries(nb);
                found = true;
            } else {
                try result.addSeries(try boxed.deepCopy());
            }
        }
        if (!found) {
            nb.deinit();
            return error.ColumnNotFound;
        }
        return result;
    }

    /// Running cumulative sum of `column`. Returns new Dataframe with column replaced.
    pub fn cumSum(self: *Self, column: []const u8) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.cumSum());
    }

    /// Running cumulative minimum of `column`.
    pub fn cumMin(self: *Self, column: []const u8) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.cumMin());
    }

    /// Running cumulative maximum of `column`.
    pub fn cumMax(self: *Self, column: []const u8) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.cumMax());
    }

    /// Running cumulative product of `column`.
    pub fn cumProd(self: *Self, column: []const u8) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.cumProd());
    }

    /// Shift `column` by n positions. Positive shifts down (prepends nulls).
    pub fn shift(self: *Self, column: []const u8, n: i64) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.shift(n));
    }

    /// Strict element-wise diff of `column` by lag n. Underflow on unsigned → error.
    pub fn diff(self: *Self, column: []const u8, n: usize) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.diff(n));
    }

    /// Permissive element-wise diff. Underflow/overflow → null.
    pub fn diffLossy(self: *Self, column: []const u8, n: usize) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.diffLossy(n));
    }

    /// Clamp `column` values to [lower, upper]. Type must match the column.
    pub fn clip(self: *Self, column: []const u8, comptime T: type, lower: T, upper: T) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.clip(T, lower, upper));
    }

    /// Replace exact matches in `column`. Type must match the column.
    pub fn replace(self: *Self, column: []const u8, comptime T: type, old_value: T, new_value: T) !*Self {
        var boxed = self.getSeries(column) orelse return error.ColumnNotFound;
        return self.columnTransform(column, try boxed.replace(T, old_value, new_value));
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

