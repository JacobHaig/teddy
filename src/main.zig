const std = @import("std");
const print = std.debug.print;

const dataframe = @import("dataframe.zig");
const variant_series = @import("dataframe/variant_series.zig");

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    defer {
        const leaked = debug_allocator.deinit();
        if (leaked == .leak) {
            print("Memory leaks detected!\n", .{});
        }
    }

    // var df_reader = try dataframe.Reader.init(allocator);
    // defer df_reader.deinit();

    // var df3 = df_reader
    //     .set_file_type(dataframe.FileType.csv("test.csv"))
    //     .set_delimiter(',')
    //     .set_has_header(true)
    //     .set_skip_rows(0)
    //     .load() catch |err|
    //     {
    //         print("Error loading CSV file: {}\n", .{err});
    //         return err;
    //     };

    // print("height: {} width: {}\n", .{ df3.height(), df3.width() });

    var df = try dataframe.Dataframe.init(allocator);
    defer df.deinit();

    var series = try df.create_series(dataframe.String);
    try series.rename("Name");
    try series.append(try variant_series.stringer(allocator, "Alice"));
    try series.try_append(try variant_series.stringer(allocator, "Gary"));
    try series.try_append("Bob");
    series.print();

    var series2 = try df.create_series(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(110000.0);
    series2.print();

    df.apply_inplace("Salary", f32, struct {
        fn call(x: f32) f32 {
            return x / 52 / 40;
        }
    }.call);

    series2.print();

    var series3 = try df.create_series(i32);
    try series3.rename("Age");
    try series3.append(15);
    try series3.append(20);
    try series3.append(30);
    series3.print();

    df.apply_inplace("Age", i32, add_ten);

    df.apply_inplace("Age", i32, struct {
        fn call(x: i32) i32 {
            return x + 10;
        }
    }.call);

    series3.print();

    var df2 = try df.deep_copy();
    defer df2.deinit();

    df.drop_series("Age");
    // print("height: {} width: {}\n", .{ df.height(), df.width() });
    // df.drop_row(1);
    df.limit(2);

    std.debug.print("height: {} width: {}\n", .{ df.height(), df.width() });
    std.debug.print("height: {} width: {}\n", .{ df2.height(), df2.width() });

    const cwd_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd_path);

    // Print it to stdout
    std.debug.print("Current working directory: {s}\n", .{cwd_path});
}

fn add_ten(x: i32) i32 {
    return x + 10;
}
