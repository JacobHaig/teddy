const std = @import("std");
const print = std.debug.print;

const dataframe = @import("dataframe.zig");
const variant_series = @import("dataframe/variant_series.zig");

pub fn main2() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    defer {
        const leaked = debug_allocator.deinit();
        if (leaked == .leak) {
            print("Memory leaks detected!\n", .{});
        } else {
            print("No memory leaks detected.\n", .{});
        }
    }

    // var df_reader = try dataframe.Reader.init(allocator);
    // defer df_reader.deinit();

    // var df3 = df_reader
    //     .set_file_type(dataframe.FileType.csv("test.csv"))
    //     .set_delimiter(',')
    //     .set_has_header(true)
    //     .set_skip_rows(0)
    //     .load() catch |err|
    //     {
    //         print("Error loading CSV file: {}\n", .{err});
    //         return err;
    //     };

    // print("height: {} width: {}\n", .{ df3.height(), df3.width() });

    var df = try dataframe.Dataframe.init(allocator);
    defer df.deinit();

    var series = try df.create_series(dataframe.String);
    try series.rename("Name");
    try series.append(try variant_series.stringer(allocator, "Alice"));
    try series.try_append(try variant_series.stringer(allocator, "Gary"));
    try series.try_append("Bob");
    series.print();

    var series2 = try df.create_series(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(110000.0);
    series2.print();

    df.apply_inplace("Salary", f32, struct {
        fn call(x: f32) f32 {
            return x / 52 / 40;
        }
    }.call);

    series2.print();

    var series3 = try df.create_series(i32);
    try series3.rename("Age");
    try series3.append(15);
    try series3.append(20);
    try series3.append(30);
    series3.print();

    df.apply_inplace("Age", i32, add_ten);

    df.apply_inplace("Age", i32, struct {
        fn call(x: i32) i32 {
            return x + 10;
        }
    }.call);

    series3.print();

    var df2 = try df.deep_copy();
    defer df2.deinit();

    df.drop_series("Age");
    // print("height: {} width: {}\n", .{ df.height(), df.width() });
    // df.drop_row(1);
    df.limit(2);

    print("height: {} width: {}\n", .{ df.height(), df.width() });
    print("height: {} width: {}\n", .{ df2.height(), df2.width() });

    const series22 = df2.get_series("Age") orelse return;
    series22.*.print();

    const cwd_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    defer allocator.free(cwd_path);

    // Print it to stdout
    std.debug.print("Current working directory: {s}\n", .{cwd_path});
}

fn add_ten(x: i32) i32 {
    return x + 10;
}

test "parse_csv" {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    // defer {
    //     const leaked = debug_allocator.deinit();
    //     if (leaked == .leak) {
    //         print("Memory leaks detected!\n", .{});
    //     } else {
    //         print("No memory leaks detected.\n", .{});
    //     }
    // }

    // const cwd_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    // defer allocator.free(cwd_path);

    // // Print it to stdout
    // std.debug.print("Current working directory: {s}\n", .{cwd_path});

    const file = try std.fs.cwd().openFile("./data/addresses.csv", .{});
    defer file.close();

    const content = try file.readToEndAlloc(allocator, std.math.maxInt(usize));
    defer allocator.free(content);

    print("{s}\n", .{content});

    var tokenizer = try CsvTokenizer.init(allocator, "Hello there!,     \"I am a test\"");

    while (true) {
        const token = tokenizer.next() orelse break;
        print("{s}\n", .{token});
    }
}

pub fn main() !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    // defer {
    //     const leaked = debug_allocator.deinit();
    //     if (leaked == .leak) {
    //         print("Memory leaks detected!\n", .{});
    //     } else {
    //         print("No memory leaks detected.\n", .{});
    //     }
    // }

    // const cwd_path = try std.fs.cwd().realpathAlloc(allocator, ".");
    // defer allocator.free(cwd_path);

    // // Print it to stdout
    // std.debug.print("Current working directory: {s}\n", .{cwd_path});

    const file = try std.fs.cwd().openFile("./data_hidden/output copy.csv", .{});
    defer file.close();

    const content = try file.readToEndAlloc(allocator, std.math.maxInt(usize));
    defer allocator.free(content);

    var tokenizer = try CsvTokenizer.init(allocator, content, .{});

    try tokenizer.print();

    // const a = try tokenizer.read_all();
}

pub const CsvTokenizer = struct {
    const Self = @This();
    const Row = std.ArrayList([]const u8);
    const CsvError = error{ EndOfFile, EndOfLine, ParsingError };
    const CsvTokenizerFlags = struct { delimiter: u8 = ',' };

    allocator: std.mem.Allocator,
    content: []const u8,
    index: u32,

    flags: CsvTokenizerFlags,

    pub fn init(allocator: std.mem.Allocator, content: []const u8, csvFlags: CsvTokenizerFlags) !*Self {
        const ptr_csv_tokenizer = try allocator.create(Self);
        errdefer allocator.destroy(ptr_csv_tokenizer);

        ptr_csv_tokenizer.allocator = allocator;
        ptr_csv_tokenizer.content = content;
        ptr_csv_tokenizer.index = 0;

        ptr_csv_tokenizer.flags = csvFlags;

        return ptr_csv_tokenizer;
    }

    fn validation(self: *Self) !void {
        const rows = try self.read_all();

        std.debug.print("\n", .{});

        if (rows.items.len == 0) {
            return CsvError.ParsingError;
        }

        std.debug.print("ROWS {} ", .{rows.items.len});

        // const expected_row_len = rows.items[0].len;

        for (rows.items, 0..) |row, index| {
            // if (row.len != expected_row_len) {
            // return CsvError.ParsingError;
            // }
            std.debug.print("{} {}\n", .{ index + 1, row.items.len });
        }
    }

    fn print(self: *Self) !void {
        const rows = try self.read_all();

        // for (rows.items, 0..) |item, index| {
        //     std.debug.print("{}: ", .{index + 1});

        //     for (item.items) |ele| {
        //         std.debug.print("{s}{c}", .{ ele, self.flags.delimiter });
        //     }
        //     std.debug.print("\n", .{});
        // }

        for (rows.items) |row| {
            for (row.items) |ele| {
                std.debug.print("{s}{c}", .{ ele, self.flags.delimiter });
            }
            std.debug.print("\n", .{});
        }
    }

    fn read_all(self: *Self) !std.ArrayList(Row) {
        var rows = std.ArrayList(Row).init(self.allocator);
        errdefer rows.deinit();

        while (true) {
            const row = self.read_row() catch |err| {
                if (err == CsvError.EndOfFile) {
                    break;
                }
                return err;
            };
            try rows.append(row);
        }
        return rows;
    }

    fn read_row(self: *Self) !Row {
        var row = Row.init(self.allocator);
        errdefer row.deinit();

        while (true) {
            const token = self.next() catch |err| {
                // std.debug.print("{}\n", .{err});
                if (err == CsvError.EndOfFile) {
                    if (row.items.len == 0) {
                        return err;
                    }

                    // break; // In this Case, we have a row to return
                }
                if (err == CsvError.EndOfLine) {
                    break;
                }
                return err;
            };
            // std.debug.print("{s}\n", .{token});
            try row.append(token);
        }

        return row;
    }

    fn next(self: *Self) ![]const u8 {
        if (self.index >= self.content.len) {
            return CsvError.EndOfFile;
        }

        if (self.content[self.index] == '\r') {
            if (self.content[self.index + 1] == '\n') {
                self.index += 2;
            } else {
                self.index += 1;
            }
            return CsvError.EndOfLine;
        }
        if (self.content[self.index] == '\n') {
            self.index += 1;
            return CsvError.EndOfLine;
        }

        const start = self.index;

        while (self.index < self.content.len) {
            const char = self.content[self.index];

            if (char == self.flags.delimiter or char == '\n' or char == '\r') {
                const token = self.content[start..self.index];
                if (char == ',') {
                    self.index += 1; // Skip the comma, leaving the \n or \r to create a new row
                }
                return token;
            }

            self.index += 1; // Move the index forward
        }

        return CsvError.EndOfFile;
    }
};
