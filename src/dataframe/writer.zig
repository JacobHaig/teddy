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
    /// Set to true when withPath() fails to duplicate the path string due to
    /// OOM. save() surfaces this as error.OutOfMemory rather than the
    /// misleading error.InvalidFilePath that would otherwise occur.
    path_alloc_failed: bool,

    // CSV options
    delimiter: u8,
    include_header: bool,

    // JSON options
    json_format: json_writer.JsonFormat,

    // Parquet options
    compression: parquet.CompressionCodec,
    emit_int96: bool,

    pub fn init(allocator: Allocator, io: std.Io) !*Self {
        const ptr = try allocator.create(Self);
        ptr.* = .{
            .allocator = allocator,
            .io = io,
            .file_type = .csv,
            .path = null,
            .path_alloc_failed = false,
            .delimiter = ',',
            .include_header = true,
            .json_format = .rows,
            .compression = .uncompressed,
            .emit_int96 = false,
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

    /// Set the file path to write to. Returns `self` for chaining.
    ///
    /// If the path string cannot be duplicated (OOM), the failure is recorded
    /// in `path_alloc_failed`. `save()` will then return `error.OutOfMemory`.
    ///
    /// The old path is freed only after the new one is secured, so a failed
    /// allocation never discards the previously-valid path.
    pub fn withPath(self: *Self, path: []const u8) *Self {
        const new_path = self.allocator.dupe(u8, path) catch {
            self.path_alloc_failed = true;
            return self;
        };
        // New path secured — now safe to release the old one.
        if (self.path) |old| self.allocator.free(old);
        self.path = new_path;
        self.path_alloc_failed = false;
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

    pub fn withEmitInt96(self: *Self, v: bool) *Self {
        self.emit_int96 = v;
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
                var cols = try parquet_adapter.fromDataframe(self.allocator, df, .{ .emit_int96 = self.emit_int96 });
                defer cols.deinit();
                break :blk parquet.writeParquet(self.allocator, cols.columns, .{
                    .compression = self.compression,
                    .emit_int96 = self.emit_int96,
                });
            },
            .tdf => @import("native_format.zig").writeToString(self.allocator, df),
            else => error.UnsupportedFileType,
        };
    }

    /// Serialize and write to file at the configured path.
    pub fn save(self: *Self, df: *Dataframe) !void {
        if (self.path_alloc_failed) return error.OutOfMemory;
        const data = try self.toString(df);
        defer self.allocator.free(data);

        const path = self.path orelse return error.InvalidFilePath;
        const cwd = std.Io.Dir.cwd();
        try cwd.writeFile(self.io, .{ .sub_path = path, .data = data });
    }
};

