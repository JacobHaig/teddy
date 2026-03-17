const std = @import("std");
const builtin = @import("builtin");

const dataframe = @import("dataframe.zig");
const csv = @import("csv.zig");

pub const FileType = union(enum) {
    none,
    csv,
    json,
    parquet,
};

pub const Reader = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    file_type: FileType,
    path: ?[]const u8, // Owned
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    // column_names: []const u8,
    // column_types: []const u8,
    // column_indices: []const usize,
    // column_count: usize,
    // row_count: usize,

    pub fn init(allocator: std.mem.Allocator) !*Self {
        const reader_ptr = try allocator.create(Reader);
        errdefer allocator.destroy(reader_ptr);

        reader_ptr.allocator = allocator;
        reader_ptr.path = null;
        reader_ptr.file_type = FileType.csv;
        reader_ptr.delimiter = ',';
        reader_ptr.has_header = true;
        reader_ptr.skip_rows = 0;

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

    pub fn withPath(self: *Self, path: []const u8) *Self {
        // Copy string into new owned string
        const newPath: []u8 = self.allocator.alloc(u8, path.len) catch return self;
        std.mem.copyForwards(u8, newPath, path);

        // Change slash direction based on os
        if (builtin.target.os.tag == .windows) {
            std.mem.replaceScalar(u8, newPath, '/', '\\');
        } else {
            std.mem.replaceScalar(u8, newPath, '\\', '/');
        }

        self.path = newPath;
        // std.debug.print("{?s}", .{self.path});
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
        const io = std.Io.Threaded.global_single_threaded.io();
        const content = try cwd.readFileAlloc(io, filename, self.allocator, .unlimited);
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
        const io = std.Io.Threaded.global_single_threaded.io();
        const content = try cwd.readFileAlloc(io, filename, self.allocator, .unlimited);
        defer self.allocator.free(content);

        const json_reader = @import("json_reader.zig");
        return try json_reader.parse(self.allocator, content, .{});
    }

    fn readParquet(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const cwd = std.Io.Dir.cwd();
        const io = std.Io.Threaded.global_single_threaded.io();
        const content = try cwd.readFileAlloc(io, filename, self.allocator, .unlimited);
        defer self.allocator.free(content);

        const parquet_mod = @import("parquet");
        const parquet_adapter = @import("parquet.zig");

        var result = try parquet_mod.readParquet(self.allocator, content);
        defer result.deinit();

        return try parquet_adapter.toDataframe(self.allocator, &result);
    }
};
