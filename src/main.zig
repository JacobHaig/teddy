const std = @import("std");
const dataframe = @import("dataframe.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();

    var df = try dataframe.Dataframe.init(allocator);
    defer df.deinit();

    var series = try df.create_series(std.ArrayListUnmanaged(u8));
    try series.rename("Name");

    var name = try std.ArrayListUnmanaged(u8).initCapacity(allocator, 0);
    try name.appendSlice(allocator, "Hello!");
    try series.append(name);

    var name2 = try std.ArrayListUnmanaged(u8).initCapacity(allocator, 0);
    try name2.appendSlice(allocator, "Bob");
    try series.append(name2);
    // try series.append("Jacob");
    series.print();

    var series2 = try df.create_series(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(120000.0);
    series2.print();

    const a = "asdad";
    _ = a;

    var series3 = try df.create_series(i32);
    try series3.rename("Age");
    try series3.append(15);
    try series3.append(20);
    try series3.append(30);
    series3.print();

    // std.debug.print("Series created with {} values\n", .{series.values.items.len});
}
