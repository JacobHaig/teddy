// ============================================================
// Parquet Module — Native Zig Parquet file reader
// ============================================================
//
// Standalone module for reading Apache Parquet files.
// Returns ParquetColumn/ParquetResult types that can be converted
// to application-specific data structures.
//
// Usage:
//   const parquet = @import("parquet");
//   var result = try parquet.readParquet(allocator, file_data);
//   defer result.deinit();
//   for (result.columns) |col| { ... }
//
// This module is designed and written by Claude.

const reader = @import("reader.zig");

// Re-export public API
pub const readParquet = reader.readParquet;

// Re-export public types
pub const types = @import("types.zig");
pub const ParquetColumn = types.ParquetColumn;
pub const ParquetResult = types.ParquetResult;
pub const PhysicalType = types.PhysicalType;
pub const ConvertedType = types.ConvertedType;

// Pull in tests from all submodules
test {
    _ = @import("thrift.zig");
    _ = @import("types.zig");
    _ = @import("metadata.zig");
    _ = @import("encoding.zig");
    _ = @import("snappy.zig");
    _ = @import("column_reader.zig");
    _ = @import("reader.zig");
}
