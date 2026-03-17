const std = @import("std");
const GroupBy = @import("group.zig").GroupBy;
const String = @import("strings.zig").String;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Series = @import("series.zig").Series;
const Dataframe = @import("dataframe.zig").Dataframe;

pub const BoxedGroupBy = union(enum) {
    const Self = @This();

    bool: *GroupBy(bool),
    uint8: *GroupBy(u8),
    uint16: *GroupBy(u16),
    uint32: *GroupBy(u32),
    uint64: *GroupBy(u64),
    uint128: *GroupBy(u128),
    usize: *GroupBy(usize),
    int8: *GroupBy(i8),
    int16: *GroupBy(i16),
    int32: *GroupBy(i32),
    int64: *GroupBy(i64),
    int128: *GroupBy(i128),
    float32: *GroupBy(f32),
    float64: *GroupBy(f64),
    string: *GroupBy(String),

    pub fn deinit(self: *Self) void {
        switch (self.*) {
            inline else => |gb| gb.deinit(),
        }
    }

    pub fn count(self: *Self) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.count(),
        }
    }

    pub fn sum(self: *Self, column: []const u8) !BoxedSeries {
        switch (self.*) {
            inline else => |gb| return gb.sum(column),
        }
    }

    pub fn mean(self: *Self, column: []const u8) !BoxedSeries {
        switch (self.*) {
            inline else => |gb| return gb.mean(column),
        }
    }
};
