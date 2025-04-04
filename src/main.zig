const std = @import("std");
const dataframe = @import("dataframe.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var df = try dataframe.Dataframe.init(allocator);
    defer df.deinit();

    var series = try df.create_series(i32);
    try series.rename("My Series");
    try series.append(15);
    try series.append(20);
    try series.append(30);
    series.print();

    var series2 = try df.create_series(f32);
    try series2.rename("My Float Series");
    try series2.append(15);
    try series2.append(20);
    try series2.append(30);
    series2.print();

    try df.append_row_struct();

    // std.debug.print("Series created with {} values\n", .{series.values.items.len});
}
