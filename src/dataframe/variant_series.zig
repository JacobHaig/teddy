const std = @import("std");
const Series = @import("series.zig").Series;

pub const String = std.ArrayListUnmanaged(u8);

pub fn stringer(allocator: std.mem.Allocator, str: []const u8) !String {
    var name = try String.initCapacity(allocator, str.len);
    errdefer name.deinit(allocator);

    name.appendSliceAssumeCapacity(str);
    return name;
}

pub const VariantSeries = union(enum) {
    const Self = @This();

    bool: *Series(bool),
    uint8: *Series(u8),
    uint16: *Series(u16),
    uint32: *Series(u32),
    uint64: *Series(u64),
    uint128: *Series(u128),
    int8: *Series(i8),
    int16: *Series(i16),
    int32: *Series(i32),
    int64: *Series(i64),
    int128: *Series(i128),
    float32: *Series(f32),
    float64: *Series(f64),
    string: *Series(String),

    pub fn deinit(self: *Self) void {
        switch (self.*) {
            inline else => |p| p.deinit(),
        }
    }

    pub fn print(self: *Self) void {
        switch (self.*) {
            inline else => |p| p.print(),
        }
    }

    pub fn len(self: *Self) usize {
        switch (self.*) {
            inline else => |p| return p.len(),
        }
    }

    pub fn name(self: *Self) []const u8 {
        switch (self.*) {
            inline else => |p| return p.name,
        }
    }

    pub fn drop_row(self: *Self, index: usize) void {
        switch (self.*) {
            inline else => |p| p.drop_row(index),
        }
    }

    pub fn apply_inplace(self: *Self, comptime T: type, comptime func: fn (x: T) T) void {
        switch (self.*) {
            inline else => |s| {
                // std.debug.print("Type: {}\n", .{@TypeOf(s)});
                if (comptime *Series(T) == @TypeOf(s)) {
                    s.*.apply_inplace(func);
                }
            },
        }
    }

    // pub fn deep_copy(self: *Self) !*Self {
    //     switch (self.*) {
    //         inline else => |s| return s.*.deep_copy(),
    //     }
    // }

    pub fn limit(self: *Self, n_limit: usize) void {
        switch (self.*) {
            inline else => |s| s.*.limit(n_limit),
        }
    }

    pub fn get_type(self: *Self) type {
        switch (self.*) {
            .bool => return bool,
            .uint8 => return u8,
            .uint16 => return u16,
            .uint32 => return u32,
            .uint64 => return u64,
            .int8 => return i8,
            .int16 => return i16,
            .int32 => return i32,
            .int64 => return i64,
            .float32 => return f32,
            .float64 => return f64,
            .string => return String,
        }
    }
};
