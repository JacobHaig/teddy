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

pub const CsvTokenizer = struct {
    const Self = @This();
    const Row = std.ArrayList([]const u8);

    allocator: std.mem.Allocator,
    content: []const u8,
    index: u32,

    inside_quotes: bool = false,

    pub fn init(allocator: std.mem.Allocator, content: []const u8) !*Self {
        const ptr_csv_tokenizer = try allocator.create(Self);
        errdefer allocator.destroy(ptr_csv_tokenizer);

        ptr_csv_tokenizer.allocator = allocator;
        ptr_csv_tokenizer.content = content;
        ptr_csv_tokenizer.index = 0;

        return ptr_csv_tokenizer;
    }

    fn read_all(self: *Self) !std.ArrayList(Row) {
        var rows = std.ArrayList(Row).init(self.allocator);
        errdefer rows.deinit();

        while (true) {
            const row = self.read_row() catch |err| {
                if (err == std.io.Eof) {
                    break;
                } else {
                    std.debug.print("Error reading row: {}\n", .{err});
                    return rows;
                }
            };

            try rows.append(row);
        }

        return rows;
    }

    fn read_row(self: *Self) Row {
        var row = Row.init(self.allocator);
        errdefer row.deinit();

        while (self.next()) |token| {
            try row.append(token);
        }

        return row;
    }

    fn next(self: *Self) ?[]const u8 {
        if (self.index >= self.content.len) {
            return null;
        }
        var start = self.index;
        var reached_content = false;

        while (self.index < self.content.len) {
            const char = self.peek_char() orelse return null;

            print("char: {c}\n", .{char});

            if (reached_content == false and char == ' ') {
                start += 1;
                self.index += 1;
                continue;
            } else {
                reached_content = true;
            }

            if (char == '"' and self.inside_quotes == false) {
                self.inside_quotes = !self.inside_quotes;
                start += 1;
                self.index += 1;
                continue;
            }

            if (char == '"' and self.inside_quotes == true) {
                if ((self.peek_next_char() orelse ' ') != '"') {
                    self.inside_quotes = !self.inside_quotes;
                    break;
                } else {
                    self.index += 1; // Skip the second quote
                }
            }

            // if (char == '"' and self.inside_quotes == false) {
            //     self.inside_quotes = !self.inside_quotes;
            //     start += 1;
            // }

            // if (char == '"' and self.inside_quotes == true) {
            //     if (self.index + 1 < self.content.len and self.content[self.index + 1] != '"') {
            //         self.inside_quotes = !self.inside_quotes;

            //         // const token = self.content[start..self.index];
            //         // return token;
            //     }
            // }

            if (self.content[self.index] == ',' and !self.inside_quotes) {
                break;
            }

            self.index += 1;
        }

        const token = self.content[start..self.index];
        self.index += 1; // Skip the comma
        return token;
    }

    fn peek_char(self: *Self) ?u8 {
        if (self.index >= self.content.len) {
            return null;
        }

        return self.content[self.index];
    }

    fn peek_next_char(self: *Self) ?u8 {
        if (self.index + 1 >= self.content.len) {
            return null;
        }

        return self.content[self.index + 1];
    }
};
