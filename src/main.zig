const std = @import("std");

const dataframe = @import("dataframe.zig");
const variant_series = @import("dataframe/variant_series.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    defer {
        if (debug_allocator.deinit() == .leak) std.debug.print("Memory leaks detected!\n", .{});
    }

    var df_reader = try dataframe.Reader.init(allocator);
    defer df_reader.deinit();

    var df3 = try df_reader
        .withFileType(.csv)
        .withPath("data/addresses.csv")
        // .withPath("data/stock_apple.csv")
        .withDelimiter(',')
        .withHeaders(true)
        .withSkipRows(0)
        .load();
    defer df3.deinit();

    std.debug.print("height: {} width: {}\n", .{ df3.height(), df3.width() });
    try df3.print();

    // const s = df3.get_series("First Name") orelse return error.doesntExist;
    // s.print();
}
