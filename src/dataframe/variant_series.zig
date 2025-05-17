const std = @import("std");
const Series = @import("series.zig").Series;

pub const UnmanagedString = std.ArrayListUnmanaged(u8);
pub const ManagedString = std.ArrayList(u8);

pub fn stringer(allocator: std.mem.Allocator, str: []const u8) !UnmanagedString {
    var name = try UnmanagedString.initCapacity(allocator, str.len);
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

    conststring: *Series([]const u8),
    string: *Series(UnmanagedString),

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
            inline else => |p| return p.name,
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

    pub fn deepCopy(self: *Self) !Self {
        switch (self.*) {
            inline else => |s| {
                const series = try s.*.deep_copy();
                return series.to_variant_series();
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

    pub fn as_string_at(self: *Self, n: usize) !UnmanagedString {
        switch (self.*) {
            inline else => |s| return s.*.as_string_at(n),
        }
    }

    pub fn getNameOwned(self: *const Self) !UnmanagedString {
        switch (self.*) {
            inline else => |p| return p.getNameOwned(),
        }
    }

    pub fn getTypeToString(self: *Self) !UnmanagedString {
        switch (self.*) {
            inline else => |s| return s.*.getTypeToString(),
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
            .string => return UnmanagedString,
        }
    }
};
