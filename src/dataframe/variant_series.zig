const std = @import("std");
const Series = @import("series.zig").Series;

pub const VariantSeries = union(enum) {
    const Self = @This();

    bool: *Series(bool),
    uint8: *Series(u8),
    uint16: *Series(u16),
    uint32: *Series(u32),
    uint64: *Series(u64),
    int8: *Series(i8),
    int16: *Series(i16),
    int32: *Series(i32),
    int64: *Series(i64),
    float32: *Series(f32),
    float64: *Series(f64),
    // string: *Series([]u8),

    pub fn deinit(self: Self) void {
        switch (self) {
            inline else => |p| p.deinit(),
        }
    }

    pub fn print(self: Self) void {
        switch (self) {
            inline else => |p| p.print(),
        }
    }
};
