const std = @import("std");

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

    pub fn validation(self: *Self) !void {
        const rows = try self.read_all();

        if (rows.items.len == 0) {
            return CsvError.ParsingError;
        }

        const expected_row_len: usize = rows.items.ptr[0].items.len;

        for (rows.items, 0..) |row, index| {
            if (row.items.len != expected_row_len) {
                std.debug.print("Row: {} Expected Row of Len: {} Got: {}\n", .{ index + 1, expected_row_len, row.items.len });
                return CsvError.ParsingError;
            }
        }
    }

    pub fn print(self: *Self) !void {
        const rows = try self.read_all();

        for (rows.items) |row| {
            for (row.items) |ele| {
                std.debug.print("{s}{c}", .{ ele, self.flags.delimiter });
            }
            std.debug.print("\n", .{});
        }
    }

    pub fn read_all(self: *Self) !std.ArrayList(Row) {
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

        // Handle \r\n and \n line endings
        if (self.content[self.index] == '\r') {
            if (self.index + 1 >= self.content.len) {
                return CsvError.EndOfFile;
            }

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

        // Handle non quoted strings
        const start = self.index;

        if (self.content[self.index] == '\"') {
            self.index += 1; // Skip the opening quote

            while (self.index < self.content.len) {
                const char = self.content[self.index];

                if (char == '\"') {
                    // If the char after the first quote is a quote, move forward twice
                    if (self.index + 1 < self.content.len) {
                        const next_char = self.content[self.index + 1];
                        if (next_char == '\"') {
                            self.index += 2; // Skip the escaped quote
                            continue;
                        }
                    }

                    // If the char after the closing quote is a delimiter, return the token
                    if (self.index + 1 < self.content.len) {
                        const next_char = self.content[self.index + 1];

                        if (next_char == ',') {
                            const token = self.content[start .. self.index + 1];

                            self.index += 2; // Skip the escaped quote and the delimiter
                            return token;
                        }
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
