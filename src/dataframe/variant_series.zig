const std = @import("std");
const Series = @import("series.zig").Series;

pub const VariantSeries = union(enum) {
    const Self = @This();

    // bool: *Series(bool),
    // byte: *Series(u8),
    // int16: *Series(i16),
    int32: *Series(i32),
    // int64: *Series(i64),
    // float32: *Series(f32),
    // float64: *Series(f64),
    // string: *Series([]u8),

    pub fn deinit(self: Self) void {
        switch (self) {
            inline else => |ptr| {
                const Type = std.meta.Child(@TypeOf(ptr));
                std.debug.print("Deinitializing series of type {s}\n", .{@typeName(Type)});

                // Check if the type has a deinit method
                if (comptime @hasDecl(Type, "deinit")) {
                    ptr.deinit();
                } else {
                    @compileError("Type " ++ @typeName(Type) ++ " does not have a deinit method");
                }
            },
        }
    }

    pub fn print(self: Self) void {
        switch (self) {
            inline else => |ptr| {
                const Type = std.meta.Child(@TypeOf(ptr));
                std.debug.print("Printing series of type {s}\n", .{@typeName(Type)});

                // Check if the type has a print method
                if (comptime @hasDecl(Type, "print")) {
                    ptr.print();
                } else {
                    @compileError("Type " ++ @typeName(Type) ++ " does not have a print method");
                }
            },
        }
    }
};
