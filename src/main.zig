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

    try df3.print();

    std.debug.print("\nCount by Zip:\n", .{});
    var group_by = try df3.groupBy("Zip");
    defer group_by.deinit();
    var zip_count = try group_by.count();
    defer zip_count.deinit();
    try zip_count.print();

    std.debug.print("\nSorted by Age:\n", .{});
    const df4 = try df3.sort("Age", true);
    defer df4.deinit();
    try df4.print();

    std.debug.print("\nFiltered Age >= 30:\n", .{});
    const df5 = try df3.filter("Age", i64, .gte, 30);
    defer df5.deinit();
    try df5.print();

    std.debug.print("\nFiltered City == Riverside:\n", .{});
    const df6 = try df3.filter("City", []const u8, .eq, "Riverside");
    defer df6.deinit();
    try df6.print();

    std.debug.print("\nSelected First Name and City:\n", .{});
    const df7 = try df3.select(&.{ "First Name", "City" });
    defer df7.deinit();
    try df7.print();

    std.debug.print("\nDescribe:\n", .{});
    const df8 = try df3.describe();
    defer df8.deinit();
    try df8.print();

    // writer example
    var df_writer = try dataframe.Writer.init(allocator);
    defer df_writer.deinit();

    try df_writer
        .withFileType(.csv)
        .withPath("data/addresses_out.csv")
        .withDelimiter(',')
        .withHeader(true)
        .save(df3);
}
