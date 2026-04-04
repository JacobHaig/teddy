// Test aggregator — imported by build.zig test compilation unit
const std = @import("std");

comptime {
    _ = @import("dataframe_test.zig");
    _ = @import("series_test.zig");
    _ = @import("strings_test.zig");
    _ = @import("csv_writer_test.zig");
    _ = @import("json_writer_test.zig");
    _ = @import("json_reader_test.zig");
    _ = @import("join_test.zig");
    _ = @import("writer_test.zig");
}
