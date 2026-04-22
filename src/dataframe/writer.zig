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
    io: std.Io,
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
            .io = std.Io.Threaded.global_single_threaded.io(),
            .file_type = .csv,
            .path = null,
            .delimiter = ',',
            .include_header = true,
            .json_format = .rows,
            .compression = .uncompressed,
        };
        return ptr;
    }

    pub fn withIo(self: *Self, io: std.Io) *Self {
        self.io = io;
        return self;
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
        try cwd.writeFile(self.io, .{ .sub_path = path, .data = data });
    }
};

