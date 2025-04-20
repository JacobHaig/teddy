const std = @import("std");

pub const CsvTokenizer = struct {
    const Self = @This();
    const Row = std.ArrayList([]const u8);
    const Rows = std.ArrayList(Row);
    const CsvError = error{ EndOfFile, EndOfLine, ParsingError };
    const CsvTokenizerFlags = struct { delimiter: u8 = ',' };

    allocator: std.mem.Allocator,
    content: []const u8,
    index: u32,
    flags: CsvTokenizerFlags,
    rows: Rows,

    pub fn init(allocator: std.mem.Allocator, content: []const u8, csvFlags: CsvTokenizerFlags) !*Self {
        const ptr_csv_tokenizer = try allocator.create(Self);
        errdefer allocator.destroy(ptr_csv_tokenizer);

        ptr_csv_tokenizer.allocator = allocator;
        ptr_csv_tokenizer.content = content;
        ptr_csv_tokenizer.index = 0;
        ptr_csv_tokenizer.flags = csvFlags;

        return ptr_csv_tokenizer;
    }

    pub fn validation(self: *Self) !void {
        if (self.rows.items.len == 0) {
            return CsvError.ParsingError;
        }

        const expected_row_len: usize = self.rows.items.ptr[0].items.len;

        for (self.rows.items, 0..) |row, index| {
            if (row.items.len != expected_row_len) {
                std.debug.print("\nRow: {} Expected Row of Len: {} Got: {}\n", .{ index + 1, expected_row_len, row.items.len });

                return CsvError.ParsingError;
            }
        }
    }

    pub fn print(self: *Self) !void {
        for (self.rows.items) |row| {
            std.debug.print("Row: ", .{});
            for (row.items, 0..) |ele, index| {
                if (index != 0) {
                    std.debug.print("{c}", .{self.flags.delimiter});
                }
                std.debug.print("{s}", .{ele});
            }
            std.debug.print("\n", .{});
        }
    }

    pub fn read_all(self: *Self) !void {
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
        self.rows = rows;
    }

    fn read_row(self: *Self) !Row {
        var row = Row.init(self.allocator);
        errdefer row.deinit();

        while (true) {
            const token = self.next() catch |err| {
                if (err == CsvError.EndOfFile) {
                    if (row.items.len == 0) {
                        return err;
                    }
                }
                if (err == CsvError.EndOfLine) {
                    break;
                }
                return err;
            };
            try row.append(token);
        }

        return row;
    }

    fn next(self: *Self) ![]const u8 {
        if (self.index >= self.content.len) {
            return CsvError.EndOfFile;
        }

        // Handle \r\n and \n line endings
        if (self.content[self.index] == '\r' or self.content[self.index] == '\n') {
            self.index += 1;

            if (self.index < self.content.len) {
                if (self.content[self.index] == '\n') {
                    self.index += 1;
                }
            }

            return CsvError.EndOfLine;
        }

        // Handle non quoted strings
        const start = self.index;

        if (self.content[self.index] == '\"') {
            self.index += 1; // Skip the opening quote

            while (self.index < self.content.len) {
                const char = self.content[self.index];

                if (char == '\"') {
                    if (self.index + 1 < self.content.len) {
                        const next_char = self.content[self.index + 1];

                        // If the char after the first quote is a quote, move forward twice
                        if (next_char == '\"') {
                            self.index += 2; // Skip the escaped quote
                            continue;
                        }

                        // If there is no second quote, must be the end...
                        const token = self.content[start .. self.index + 1];

                        if (next_char == self.flags.delimiter) {
                            self.index += 1; // Skip the comma, leaving the \n or \r to create a new row
                        } else if (next_char != '\n' and next_char != '\r') {
                            return CsvError.ParsingError;
                        }

                        self.index += 1; // Skip the escaped quote and the delimiter
                        return token;
                    }
                }

                self.index += 1; // Move the index forward
            }
        } else {
            // If there is no quote, we can just read until the delimiter or end of line
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
        }

        return CsvError.EndOfFile;
    }
};

fn print_char(c: u8) void {
    switch (c) {
        'a'...'z' => std.debug.print("'{c}'\n", .{c}),
        'A'...'Z' => std.debug.print("'{c}'\n", .{c}),
        '0'...'9' => std.debug.print("'{c}'\n", .{c}),
        '"' => std.debug.print("'{c}'\n", .{c}),
        ' ' => std.debug.print("'{c}'\n", .{c}),
        ',' => std.debug.print("'{c}'\n", .{c}),
        '\n' => std.debug.print("'\\n'\n", .{}),
        '\r' => std.debug.print("'\\r'\n", .{}),
        else => std.debug.print("? '{c}' '{}'\n", .{ c, c }),
    }
}
