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

    pub fn set_file_type(self: *Self, file_type: FileType) *Self {
        self.file_type = file_type;
        return self;
    }

    pub fn set_path(self: *Self, path: []const u8) *Self {
        self.path = path;
        return self;
    }

    pub fn set_delimiter(self: *Self, delimiter: u8) *Self {
        self.delimiter = delimiter;
        return self;
    }

    pub fn set_has_header(self: *Self, has_header: bool) *Self {
        self.has_header = has_header;
        return self;
    }

    pub fn set_skip_rows(self: *Self, skip_rows: usize) *Self {
        self.skip_rows = skip_rows;
        return self;
    }

    pub fn load(self: *Self) !*dataframe.Dataframe {
        switch (self.file_type) {
            .csv => return self.read_csv(),
            else => return error.InvalidFileType,
        }
    }

    fn read_csv(self: *Self) !*dataframe.Dataframe {
        const filename = self.path orelse return error.InvalidFilePath;

        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, std.math.maxInt(usize));
        defer self.*.allocator.free(content);

        const tokenizer = try csv.CsvTokenizer.init(self.allocator, content, .{ .delimiter = self.delimiter });
        defer tokenizer.deinit();

        try tokenizer.read_all();
        try tokenizer.validation();

        const df = try tokenizer.to_dataframe();
        errdefer df.deinit();

        return df;
    }

    fn read_json(self: *Self) void {
        // Implement JSON reading logic here
        _ = self;
    }

    fn read_parquet(self: *Self) void {
        // Implement Parquet reading logic here
        _ = self;
    }
};
