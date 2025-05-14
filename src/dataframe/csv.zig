const std = @import("std");
const dataframe = @import("dataframe.zig");
const variant_series = @import("variant_series.zig");
const ManagedString = @import("variant_series.zig").ManagedString;
const UnmanagedString = @import("variant_series.zig").UnmanagedString;

pub const CsvTokenizer = struct {
    const Self = @This();
    const Row = std.ArrayList(CSVType);
    const Rows = std.ArrayList(Row);
    const CsvError = error{ EndOfFile, EndOfLine, ParsingError };
    const CsvTokenizerFlags = struct {
        delimiter: u8 = ',',
        skip_rows: usize = 0,
        has_header: bool = true,
    };

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

    pub fn deinit(self: *Self) void {
        for (self.rows.items) |*row| row.deinit();

        self.rows.deinit();
        self.allocator.destroy(self);
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
            for (row.items) |item| {
                switch (item) {
                    .value => |v| std.debug.print("'{s}' ", .{v}),
                    .quoted_value => |v| std.debug.print("'{s}' ", .{v}),
                    else => std.debug.print("", .{}),
                }
            }
            std.debug.print("\n", .{});
        }
    }

    // TODO: Consider parsing the datatypes of each column here.
    // This could reduce the overall memory footprint of the dataframe.
    pub fn to_dataframe(self: *Self) !*dataframe.Dataframe {
        const df = try dataframe.Dataframe.init(self.allocator);
        errdefer df.deinit();

        const w = self.rows.items.ptr[0].items.len;
        const h = self.rows.items.len;

        for (0..w) |dw| {
            var series = try df.create_series(variant_series.UnmanagedString);
            var starting_feild: usize = 0;

            if (self.flags.has_header) {
                const string = try self.rows.items[0].items[dw].to_string(self.allocator);
                try series.rename(string.items);
                starting_feild += 1;
            }

            starting_feild += self.flags.skip_rows;

            for (starting_feild..h) |dh| {
                const string = try self.rows.items[dh].items[dw].to_string(self.allocator);
                try series.append(string);
            }
        }

        return df;
    }

    const CSVType = union(enum) {
        value: []const u8,
        quoted_value: []const u8,
        end_of_line: void,
        end_of_file: void,
        parsing_error: void,

        fn print(self: CSVType) void {
            switch (self) {
                .value => |v| std.debug.print("Value:{s}\n", .{v}),
                .quoted_value => |v| std.debug.print("QuotedValue:{s}\n", .{v}),
                .end_of_line => std.debug.print("End of Line\n", .{}),
                .end_of_file => std.debug.print("End of File\n", .{}),
                .parsing_error => std.debug.print("Parsing Error\n", .{}),
            }
        }

        fn to_string(self: CSVType, allocator: std.mem.Allocator) !UnmanagedString {
            switch (self) {
                .value => |v| {
                    const str: UnmanagedString = try variant_series.stringer(allocator, v);
                    return str;
                },
                .quoted_value => |v| {
                    var str: UnmanagedString = try variant_series.stringer(allocator, v);
                    std.debug.print("Quoted Value: {s}\n", .{str.items});
                    _ = str.orderedRemove(0);
                    _ = str.orderedRemove(str.items.len - 1);
                    std.debug.print("Quoted Value: {s}\n", .{str.items});
                    return str;
                },
                else => {
                    unreachable;
                },
            }
        }
    };

    pub fn read_all(self: *Self) !void {
        var rows = std.ArrayList(Row).init(self.allocator);
        errdefer rows.deinit();

        while (true) {
            var row = Row.init(self.allocator);
            errdefer row.deinit();

            const err = try self.read_row(&row);

            if (err == .end_of_file) {
                if (row.items.len != 0) {
                    try rows.append(row);
                }
                break;
            }

            if (err == .parsing_error) {
                return CsvError.ParsingError;
            }

            try rows.append(row);
        }

        self.rows = rows;
    }

    fn read_row(self: *Self, row: *Row) !CSVType {
        while (true) {
            const csv_token_type = self.next();

            if (csv_token_type == .parsing_error) {
                return .parsing_error;
            }

            if (csv_token_type == .end_of_file) {
                return .end_of_file;
            }

            if (csv_token_type == .end_of_line) {
                return .end_of_line;
            }

            // If we have some value, append it to the row
            try row.append(csv_token_type);
        }
        return .end_of_file;
    }

    fn next(self: *Self) CSVType {
        if (self.index >= self.content.len) {
            return .end_of_file;
        }

        // Handle \r\n and \n line endings
        if (self.content[self.index] == '\r' or self.content[self.index] == '\n') {
            self.index += 1;

            if (self.index < self.content.len) {
                if (self.content[self.index] == '\n') {
                    self.index += 1;
                }
            }

            return .end_of_line;
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
                            return .parsing_error;
                        }

                        self.index += 1; // Skip the escaped quote and the delimiter
                        return .{ .quoted_value = token };
                    }
                }

                self.index += 1; // Move the index forward
            }
            // If we reach the end of the content, we need to return the last token
            if (start != self.index) {
                const token = self.content[start..self.index];
                return .{ .value = token };
            }
        } else {
            // If there is no quote, we can just read until the delimiter or end of line
            while (self.index < self.content.len) {
                const char = self.content[self.index];

                if (char == self.flags.delimiter or char == '\n' or char == '\r') {
                    const token = self.content[start..self.index];
                    if (char == self.flags.delimiter) {
                        self.index += 1; // Skip the comma, leaving the \n or \r to create a new row
                    }
                    return .{ .value = token };
                }

                self.index += 1; // Move the index forward
            }
            // If we reach the end of the content, we need to return the last token
            if (start != self.index) {
                const token = self.content[start..self.index];
                return .{ .value = token };
            }
        }

        return .end_of_line;
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

test "parse_csv_text" {
    const content =
        \\First Name,Last Name,Age,Address,City,State,Zip
        \\John,Doe,52,120 jefferson st.,Riverside, NJ, 08075
        \\Jack,McGinnis,23,220 hobo Av.,Phila, PA,09119
        \\"John ""Da Man""",Repici,38,120 Jefferson St.,Riverside, NJ,"08075"
        \\Stephen,Tyler,96,"7452, Terrace ""At the Plaza"" road",SomeTown,SD," 91234"
        \\,Blankman,14,,SomeTown, SD, 00298
        \\"Joan ""the bone"", Anne",Jet,56,"9th, at Terrace plc",Desert City,CO,00123
    ;

    var tokenizer = try CsvTokenizer.init(std.testing.allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.read_all();
    try tokenizer.validation();
}
