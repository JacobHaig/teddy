const std = @import("std");

comptime {
    _ = @import("types_test.zig");
    _ = @import("encoding_writer_test.zig");
    _ = @import("encoding_reader_test.zig");
    _ = @import("thrift_writer_test.zig");
    _ = @import("column_reader_test.zig");
    _ = @import("column_writer_test.zig");
    _ = @import("parquet_reader.zig"); // inline reader tests (read fixtures from disk)
    _ = @import("parquet_writer.zig"); // inline writer round-trip tests
    _ = @import("malformed_test.zig"); // Phase 11 Unit C: malformed-input battery
}
