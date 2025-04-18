const std = @import("std");

const dataframe = @import("dataframe.zig");

const FileType = union(enum) {
    none,
    csv: []const u8,
    json: []const u8,
    parquet: []const u8,
};

pub const Reader = struct {
    const Self = @This();

    allocator: *std.mem.Allocator,

    file_type: FileType,
    delimiter: u8,
    has_header: bool,
    skip_rows: usize,
    // column_names: []const u8,
    // column_types: []const u8,
    // column_indices: []const usize,
    // column_count: usize,
    // row_count: usize,

    pub fn init(allocator: *std.mem.Allocator) !*Self {
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
    }

    pub fn set_delimiter(self: *Self, delimiter: u8) *Self {
        self.delimiter = delimiter;
    }

    pub fn set_has_header(self: *Self, has_header: bool) *Self {
        self.has_header = has_header;
    }

    pub fn set_skip_rows(self: *Self, skip_rows: usize) *Self {
        self.skip_rows = skip_rows;
    }

    pub fn load(self: *Self) dataframe.Dataframe {
        switch (self.file_type) {
            .csv => self.read_csv(),
            .json => self.read_json(),
            .parquet => self.read_parquet(),
            else => @compileError("Unsupported file type"),
        }
    }

    fn read_csv(self: *Self) void {
        const filename = self.file_type.csv;

        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, std.math.maxInt(usize));
        defer self.*.allocator.free(content);

        // std.mem.spli
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
