const std = @import("std");
const Allocator = @import("std").mem.Allocator;

const VariantSeries = @import("variant_series.zig").VariantSeries;
const Series = @import("series.zig").Series;
const Dataframe = @import("dataframe.zig").Dataframe;

/// Group struct for grouping DataFrame rows by values in a Series
/// T is the type of the group key (e.g., String, i32, etc.)
/// T must be hashable and comparable
/// The Hash map maps from group key (T) to a list of row indices (usize)
///
/// Usage:
/// var group_by = try GroupBy(String).init(allocator, &dataframe);
/// defer group_by.deinit();
///
/// But generally, you would use DataFrame.groupBy method instead of directly using this struct.
pub fn GroupBy(comptime T: type) type {
    return struct {
        const Self = @This();

        allocator: Allocator,
        dataframe: *Dataframe,
        series: *Series(T),
        groups: std.AutoHashMap(T, std.ArrayList(usize)),

        /// Initialize a GroupBy struct
        /// Will group the rows of the dataframe based on the values in the variant_series
        /// Caller owns the returned pointer and must call deinit.
        /// The variant_series is used for as the grouping key type T.
        pub fn init(allocator: Allocator, dataframe: *Dataframe, series: *Series) !*GroupBy(type) {
            const ptr = allocator.create(Self);
            errdefer allocator.destroy(ptr);

            ptr.allocator = allocator;
            ptr.dataframe = dataframe;
            ptr.series = series;

            ptr.groups = try std.AutoHashMap(T, std.ArrayList(usize)).init(allocator);
            return ptr;
        }

        pub fn deinit(self: *GroupBy) void {
            // Free all the ArrayLists in the hash map
            var it = self.groups.iterator();
            while (it.next()) |entry| {
                entry.value.deinit(self.allocator);
            }
            self.groups.deinit();

            // Now free the GroupBy struct itself
            self.allocator.destroy(self);
        }

        fn setupGroups(self: *GroupBy) !void {
            const series_len = self.series.len();
            for (series_len) |i| {
                const key = switch (self.series.getValue(i)) {
                    .Some => |v| v.*.toType(T),
                    .None => continue,
                };

                var list = self.groups.get(key);
                if (list == null) {
                    var new_list = try std.ArrayList(usize).initCapacity(self.allocator, 4);
                    try new_list.append(self.allocator, i);
                    try self.groups.put(self.allocator, key, new_list);
                } else {
                    try list.?.append(self.allocator, i);
                }
            }
        }

        // pub fn aggregate(self: *GroupBy, agg_fn: AggFn) DataFrame {
        // }
    };
}
