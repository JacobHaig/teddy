const std = @import("std");
const Series = @import("series.zig").Series;
const String = @import("strings.zig").String;
const GroupBy = @import("group.zig").GroupBy;
const BoxedGroupBy = @import("boxed_groupby.zig").BoxedGroupBy;
const Dataframe = @import("dataframe.zig").Dataframe;

pub const BoxedSeries = union(enum) {
    const Self = @This();

    bool: *Series(bool),
    uint8: *Series(u8),
    uint16: *Series(u16),
    uint32: *Series(u32),
    uint64: *Series(u64),
    uint128: *Series(u128),
    usize: *Series(usize),
    int8: *Series(i8),
    int16: *Series(i16),
    int32: *Series(i32),
    int64: *Series(i64),
    int128: *Series(i128),
    float32: *Series(f32),
    float64: *Series(f64),
    string: *Series(String),

    /// Deallocates the contained Series. After this call, the BoxedSeries is invalid.
    pub fn deinit(self: *Self) void {
        switch (self.*) {
            inline else => |p| p.deinit(),
        }
    }

    pub fn print(self: *const Self) void {
        switch (self.*) {
            inline else => |p| p.print(),
        }
    }

    pub fn len(self: *const Self) usize {
        switch (self.*) {
            inline else => |p| return p.len(),
        }
    }

    pub fn name(self: *const Self) []const u8 {
        switch (self.*) {
            inline else => |p| return p.name.toSlice(),
        }
    }

    pub fn dropRow(self: *Self, index: usize) void {
        switch (self.*) {
            inline else => |p| p.dropRow(index),
        }
    }

    pub fn applyInplace(self: *Self, comptime T: type, comptime func: fn (x: T) T) void {
        switch (self.*) {
            inline else => |s| {
                // std.debug.print("Type: {}\n", .{@TypeOf(s)});
                if (comptime *Series(T) == @TypeOf(s)) {
                    s.*.applyInplace(func);
                }
            },
        }
    }

    pub fn rename(self: *Self, new_name: []const u8) !void {
        switch (self.*) {
            inline else => |s| try s.rename(new_name),
        }
    }

    /// Returns a new BoxedSeries containing a deep-copied Series. Caller must call deinit on the returned BoxedSeries.
    pub fn deepCopy(self: *Self) !Self {
        switch (self.*) {
            inline else => |s| {
                const series = try s.*.deepCopy();
                return series.toBoxedSeries();
            },
        }
    }

    pub fn limit(self: *Self, n_limit: usize) void {
        switch (self.*) {
            inline else => |s| s.*.limit(n_limit),
        }
    }

    pub fn printAt(self: *Self, n: usize) void {
        switch (self.*) {
            inline else => |s| s.*.printAt(n),
        }
    }

    pub fn asStringAt(self: *Self, n: usize) !String {
        switch (self.*) {
            inline else => |s| return s.*.asStringAt(n),
        }
    }

    pub fn getNameOwned(self: *const Self) !String {
        switch (self.*) {
            inline else => |p| return p.getNameOwned(),
        }
    }

    pub fn getTypeAsString(self: *Self) !String {
        switch (self.*) {
            inline else => |s| return s.*.getTypeAsString(),
        }
    }

    pub fn groupBy(self: *Self, allocator: std.mem.Allocator, dataframe: *Dataframe) !BoxedGroupBy {
        switch (self.*) {
            inline else => |s| {
                const gb = try s.*.groupBy(allocator, dataframe);
                return gb.toBoxedGroupBy();
            },
        }
    }

    /// Returns the type of the contained Series
    /// It also can not be used at runtime
    pub fn getType(self: *Self) type {
        switch (self.*) {
            .bool => return bool,
            .uint8 => return u8,
            .uint16 => return u16,
            .uint32 => return u32,
            .uint64 => return u64,
            .uint128 => return u128,
            .usize => return usize,
            .int8 => return i8,
            .int16 => return i16,
            .int32 => return i32,
            .int64 => return i64,
            .int128 => return i128,
            .float32 => return f32,
            .float64 => return f64,
            .string => return String,
        }
    }
};
