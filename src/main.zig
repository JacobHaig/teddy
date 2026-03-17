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

    const df4 = try df3.sort("Age", true);
    defer df4.deinit();
    std.debug.print("Sorted by Age:\n", .{});
    try df4.print();

    const df5 = try df3.filter("Age", i64, .gte, 30);
    defer df5.deinit();
    std.debug.print("Filtered Age >= 30:\n", .{});
    try df5.print();

    const df6 = try df3.filter("City", []const u8, .eq, "Riverside");
    defer df6.deinit();
    std.debug.print("\nFiltered City == Riverside:\n", .{});
    try df6.print();

    const df7 = try df3.select(&.{ "First Name", "City" });
    defer df7.deinit();
    std.debug.print("\nSelected First Name and City:\n", .{});
    try df7.print();

    const s = try df3.toJsonString(.rows);
    defer allocator.free(s);
    std.debug.print("\n{s}\n", .{s});

    const df8 = try df3.describe();
    defer df8.deinit();
    std.debug.print("\nDataframe Description:\n", .{});
    try df8.print();
}
