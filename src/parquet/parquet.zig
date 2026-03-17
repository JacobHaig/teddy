// ============================================================
// Parquet Module — Native Zig Parquet file reader/writer
// ============================================================

const reader = @import("parquet_reader.zig");
const writer_mod = @import("parquet_writer.zig");

// Re-export reader API
pub const readParquet = reader.readParquet;

// Re-export writer API
pub const writeParquet = writer_mod.writeParquet;
pub const WriteOptions = writer_mod.WriteOptions;
pub const ColumnData = @import("column_writer.zig").ColumnData;

// Re-export public types
pub const types = @import("types.zig");
pub const ParquetColumn = types.ParquetColumn;
pub const ParquetResult = types.ParquetResult;
pub const PhysicalType = types.PhysicalType;
pub const ConvertedType = types.ConvertedType;
pub const CompressionCodec = types.CompressionCodec;

// Pull in tests from all submodules
test {
    _ = @import("thrift_reader.zig");
    _ = @import("thrift_writer.zig");
    _ = @import("types.zig");
    _ = @import("metadata.zig");
    _ = @import("encoding_reader.zig");
    _ = @import("encoding_writer.zig");
    _ = @import("snappy.zig");
    _ = @import("column_reader.zig");
    _ = @import("column_writer.zig");
    _ = @import("parquet_reader.zig");
    _ = @import("parquet_writer.zig");
}
