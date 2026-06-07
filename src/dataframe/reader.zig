const std = @import("std");
const builtin = @import("builtin");

const dataframe = @import("dataframe.zig");
const csv = @import("csv_reader.zig");

pub const FileType = union(enum) {
    none,
    csv,
    json,
    parquet,
};

pub const Reader = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    io: std.Io,

    file_type: FileType,
    path: ?[]const u8, // Owned
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    /// Set to true when withPath() fails to duplicate the path string due to
    /// OOM. load() surfaces this as error.OutOfMemory rather than silently
    /// falling through to error.InvalidFilePath later.
    path_alloc_failed: bool,

    pub fn init(allocator: std.mem.Allocator, io: std.Io) !*Self {
        const reader_ptr = try allocator.create(Reader);
        errdefer allocator.destroy(reader_ptr);

        reader_ptr.allocator = allocator;
        reader_ptr.io = io;
        reader_ptr.path = null;
        reader_ptr.file_type = FileType.csv;
        reader_ptr.delimiter = ',';
        reader_ptr.has_header = true;
        reader_ptr.skip_rows = 0;
        reader_ptr.path_alloc_failed = false;

        return reader_ptr;
    }

    pub fn deinit(self: *Self) void {
        if (self.path) |path| {
            self.allocator.free(path);
        }
        self.allocator.destroy(self);
    }

    pub fn withFileType(self: *Self, file_type: FileType) *Self {
        self.file_type = file_type;
        return self;
    }

    /// Set the file path to read from. Returns `self` for chaining.
    ///
    /// If the path string cannot be duplicated (OOM), the failure is recorded
    /// in `path_alloc_failed`. `load()` will then return `error.OutOfMemory`
    /// rather than the misleading `error.InvalidFilePath` that would otherwise
    /// occur when `self.path` is still null.
    ///
    /// The previous path (if any) is freed only after the new one is secured,
    /// so a failed allocation never leaves the reader without a valid path.
    pub fn withPath(self: *Self, path: []const u8) *Self {
        // Duplicate first; only replace on success.
        const new_path: []u8 = self.allocator.alloc(u8, path.len) catch {
            self.path_alloc_failed = true;
            return self;
        };
        std.mem.copyForwards(u8, new_path, path);

        // Change slash direction based on os
        if (builtin.target.os.tag == .windows) {
            std.mem.replaceScalar(u8, new_path, '/', '\\');
        } else {
            std.mem.replaceScalar(u8, new_path, '\\', '/');
        }

        // Now it is safe to free the old path and install the new one.
        if (self.path) |old| self.allocator.free(old);
        self.path = new_path;
        self.path_alloc_failed = false;
        return self;
    }

    pub fn withDelimiter(self: *Self, delimiter: u8) *Self {
        self.delimiter = delimiter;
        return self;
    }

    pub fn withHeaders(self: *Self, has_header: bool) *Self {
        self.has_header = has_header;
        return self;
    }

    pub fn withSkipRows(self: *Self, skip_rows: usize) *Self {
        self.skip_rows = skip_rows;
        return self;
    }

    // Loads data from the configured source
    // Returns a new dataframe with the loaded data
    // Ownership of the dataframe is transferred to the caller
    pub fn load(self: *Self) !*dataframe.Dataframe {
        if (self.path_alloc_failed) return error.OutOfMemory;
        return switch (self.file_type) {
            .csv => self.readCsv(),
            .json => self.readJson(),
            .parquet => self.readParquet(),
            else => error.FileDoesNotExist,
        };
    }

    fn readCsv(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const cwd = std.Io.Dir.cwd();
        const content = try cwd.readFileAlloc(self.io, filename, self.allocator, .unlimited);
        defer self.allocator.free(content);

        return try csv.parse(self.allocator, content, .{
            .delimiter = self.delimiter,
            .has_header = self.has_header,
            .skip_rows = self.skip_rows,
        });
    }

    fn readJson(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const cwd = std.Io.Dir.cwd();
        const content = try cwd.readFileAlloc(self.io, filename, self.allocator, .unlimited);
        defer self.allocator.free(content);

        const json_reader = @import("json_reader.zig");
        return try json_reader.parse(self.allocator, content, .{});
    }

    fn readParquet(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const cwd = std.Io.Dir.cwd();
        const content = try cwd.readFileAlloc(self.io, filename, self.allocator, .unlimited);
        defer self.allocator.free(content);

        const parquet_mod = @import("parquet");
        const parquet_adapter = @import("parquet.zig");

        var result = try parquet_mod.readParquet(self.allocator, content);
        defer result.deinit();

        return try parquet_adapter.toDataframe(self.allocator, &result);
    }
};
