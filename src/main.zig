const std = @import("std");
const df = @import("dataframe.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var dataframe = try df.Dataframe.init(allocator);
    defer dataframe.deinit();

    var series = try dataframe.create_series(i32);
    try series.rename("My Series");
    try series.append(15);
    try series.append(20);
    try series.append(30);
    series.print();

    var series2 = try dataframe.create_series(f32);
    try series2.rename("My Float Series");
    try series2.append(15);
    try series2.append(20);
    try series2.append(30);
    series2.print();

    // std.debug.print("Series created with {} values\n", .{series.values.items.len});
}
