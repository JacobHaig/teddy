const std = @import("std");
const df = @import("dataframe.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var dataframe = try df.Dataframe.create(allocator);
    defer dataframe.deinit();

    var series = try dataframe.create_series(i32);

    try series.rename("My Series");
    try series.append(15);
    try series.append(20);
    try series.append(30);

    series.print();

    const same_series = dataframe.get_series("My Series").?;

    same_series.print();

    std.debug.print("Series created with {} values\n", .{series.values.items.len});
}
