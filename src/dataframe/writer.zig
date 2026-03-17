const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const csv_writer = @import("csv_writer.zig");
const json_writer = @import("json_writer.zig");
const parquet_adapter = @import("parquet.zig");
const parquet = @import("parquet");

pub const FileType = @import("reader.zig").FileType;

pub const Writer = struct {
    const Self = @This();

    allocator: Allocator,
    file_type: FileType,
    path: ?[]const u8,

    // CSV options
    delimiter: u8,
    include_header: bool,

    // JSON options
    json_format: json_writer.JsonFormat,

    // Parquet options
    compression: parquet.CompressionCodec,

    pub fn init(allocator: Allocator) !*Self {
        const ptr = try allocator.create(Self);
        ptr.* = .{
            .allocator = allocator,
            .file_type = .csv,
            .path = null,
            .delimiter = ',',
            .include_header = true,
            .json_format = .rows,
            .compression = .uncompressed,
        };
        return ptr;
    }

    pub fn deinit(self: *Self) void {
        if (self.path) |p| self.allocator.free(p);
        self.allocator.destroy(self);
    }

    pub fn withFileType(self: *Self, ft: FileType) *Self {
        self.file_type = ft;
        return self;
    }

    pub fn withPath(self: *Self, path: []const u8) *Self {
        if (self.path) |old| self.allocator.free(old);
        self.path = self.allocator.dupe(u8, path) catch null;
        return self;
    }

    pub fn withDelimiter(self: *Self, d: u8) *Self {
        self.delimiter = d;
        return self;
    }

    pub fn withHeader(self: *Self, h: bool) *Self {
        self.include_header = h;
        return self;
    }

    pub fn withJsonFormat(self: *Self, f: json_writer.JsonFormat) *Self {
        self.json_format = f;
        return self;
    }

    pub fn withCompression(self: *Self, c: parquet.CompressionCodec) *Self {
        self.compression = c;
        return self;
    }

    /// Serialize the DataFrame to bytes. Caller owns the returned slice.
    pub fn toString(self: *Self, df: *Dataframe) ![]u8 {
        return switch (self.file_type) {
            .csv => csv_writer.writeToString(self.allocator, df, .{
                .delimiter = self.delimiter,
                .include_header = self.include_header,
            }),
            .json => json_writer.writeToString(self.allocator, df, self.json_format),
            .parquet => blk: {
                var cols = try parquet_adapter.fromDataframe(self.allocator, df);
                defer cols.deinit();
                break :blk parquet.writeParquet(self.allocator, cols.columns, .{
                    .compression = self.compression,
                });
            },
            else => error.UnsupportedFileType,
        };
    }

    /// Serialize and write to file at the configured path.
    pub fn save(self: *Self, df: *Dataframe) !void {
        const data = try self.toString(df);
        defer self.allocator.free(data);

        const path = self.path orelse return error.InvalidFilePath;
        const cwd = std.Io.Dir.cwd();
        const io = std.Io.Threaded.global_single_threaded.io();
        try cwd.writeFile(io, .{ .sub_path = path, .data = data });
    }
};

// ============================================================
// Tests
// ============================================================

test "writer: CSV toString" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.csv);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expect(output.len > 0);
}

test "writer: JSON toString rows" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.rows);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("[{\"x\":1}]", output);
}

test "writer: JSON toString columns" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.columns);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("{\"x\":[1,2]}", output);
}

test "writer: Parquet toString and read back" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i64).init(allocator);
    try col.rename("val");
    try col.append(10);
    try col.append(20);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.parquet);

    const output = try w.toString(df);
    defer allocator.free(output);

    // Read it back
    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.num_rows);
    try std.testing.expectEqual(@as(i64, 10), result.columns[0].int64s.?[0]);
    try std.testing.expectEqual(@as(i64, 20), result.columns[0].int64s.?[1]);
}

test "writer: builder pattern chaining" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator);
    defer w.deinit();
    _ = w.withFileType(.csv).withDelimiter(';').withHeader(false);
    try std.testing.expectEqual(@as(u8, ';'), w.delimiter);
    try std.testing.expectEqual(false, w.include_header);
}
