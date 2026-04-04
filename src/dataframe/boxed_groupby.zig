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
    isize: *GroupBy(isize),
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

    pub fn sum(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.sum(column),
        }
    }

    pub fn mean(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.mean(column),
        }
    }

    pub fn min(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.min(column),
        }
    }

    pub fn max(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.max(column),
        }
    }

    pub fn stdDev(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.stdDev(column),
        }
    }

    pub fn prod(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.prod(column),
        }
    }

    pub fn first(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.first(column),
        }
    }

    pub fn last(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.last(column),
        }
    }

    pub fn median(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.median(column),
        }
    }

    pub fn nunique(self: *Self, column: []const u8) !*Dataframe {
        switch (self.*) {
            inline else => |gb| return gb.nunique(column),
        }
    }
};
