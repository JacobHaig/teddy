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

    var df_reader = try dataframe.Reader.init(allocator);
    defer df_reader.deinit();

    var df3 = df_reader
        .set_file_type(.csv)
        .set_path("data\\addresses.csv")
        .set_delimiter(',')
        .set_has_header(true)
        .set_skip_rows(0)
        .load() catch |err| {
        print("Error loading CSV file: {}\n", .{err});
        return err;
    };
    defer df3.deinit();
    print("height: {} width: {}\n", .{ df3.height(), df3.width() });
}
