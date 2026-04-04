const std = @import("std");

comptime {
    _ = @import("types_test.zig");
    _ = @import("encoding_writer_test.zig");
    _ = @import("encoding_reader_test.zig");
    _ = @import("thrift_writer_test.zig");
    _ = @import("column_reader_test.zig");
    _ = @import("column_writer_test.zig");
}
