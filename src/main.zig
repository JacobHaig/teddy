const std = @import("std");

const teddy = @import("teddy");
const dataframe = teddy;

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    defer {
        if (debug_allocator.deinit() == .leak) std.debug.print("Memory leaks detected!\n", .{});
    }

    var df_reader = try dataframe.Reader.init(allocator);
    defer df_reader.deinit();

    var df3 = try df_reader
        .withFileType(.parquet)
        .withPath("data/addresses.parquet")
        .withDelimiter(',')
        .withHeaders(true)
        .withSkipRows(0)
        .load();
    defer df3.deinit();

    std.debug.print("height: {} width: {}\n", .{ df3.height(), df3.width() });
    try df3.print();

    var group_by = try df3.groupBy("Zip");
    defer group_by.deinit();

    var zip_count = try group_by.count();
    defer zip_count.deinit();

    std.debug.print("Count by Zip:\n", .{});
    try zip_count.print();
}
