// Test aggregator — imported by build.zig test compilation unit
const std = @import("std");

comptime {
    _ = @import("dataframe_test.zig");
    _ = @import("functions_test.zig");
    _ = @import("series_test.zig");
    _ = @import("strings_test.zig");
    _ = @import("csv_writer_test.zig");
    _ = @import("json_writer_test.zig");
    _ = @import("json_reader_test.zig");
    _ = @import("join_test.zig");
    _ = @import("writer_test.zig");
    _ = @import("raw.zig");
    _ = @import("date.zig");
    _ = @import("time.zig");
    _ = @import("timestamp.zig");
    _ = @import("decimal.zig");
    _ = @import("binary.zig");
    _ = @import("fixed_bytes.zig");
    _ = @import("uuid.zig");
    _ = @import("interval.zig");
    _ = @import("nested.zig");
    _ = @import("nested_assembly.zig");
    _ = @import("nested_shred.zig");
    _ = @import("nested_json.zig");
    _ = @import("parquet_test.zig");
    _ = @import("native_format_test.zig");
    _ = @import("regression_test.zig");
}
