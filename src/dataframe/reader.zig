const std = @import("std");

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
    path: ?[]const u8,
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
        self.allocator.destroy(self);
    }

    pub fn withFileType(self: *Self, file_type: FileType) *Self {
        self.file_type = file_type;
        return self;
    }

    pub fn withPath(self: *Self, path: []const u8) *Self {
        self.path = path;
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
            else => error.FileDoesNotExist,
        };
    }

    fn readCsv(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, std.math.maxInt(usize));
        defer self.*.allocator.free(content);

        const tokenizer = try csv.CsvTokenizer.init(self.allocator, content, .{ .delimiter = self.delimiter, .has_header = self.has_header, .skip_rows = self.skip_rows });
        defer tokenizer.deinit();

        try tokenizer.readAll();
        try tokenizer.validate();
        // try tokenizer.print();

        const df = try tokenizer.createOwnedDataframe();

        return df;
    }

    fn readJson(self: *Self) void {
        // Implement JSON reading logic here
        _ = self;
    }

    fn readParquet(self: *Self) void {
        // Implement Parquet reading logic here
        _ = self;
    }
};
