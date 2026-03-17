const std = @import("std");
const dataframe = @import("dataframe.zig");
const series = @import("series.zig");
const boxed_series = @import("boxed_series.zig");
const strings = @import("strings.zig");

const CSVType = union(enum) {
    value: []const u8,
    quoted_value: []const u8,
    end_of_line: void,
    end_of_file: void,
    parsing_error: void,

    /// Returns the inner text content for value types.
    /// For quoted_value, strips the surrounding quotes (but does NOT unescape "").
    /// For unquoted value, returns the slice as-is.
    fn getRawSlice(self: CSVType) []const u8 {
        return switch (self) {
            .value => |v| v,
            .quoted_value => |v| {
                // Strip surrounding quotes: "hello" -> hello
                if (v.len >= 2 and v[0] == '"' and v[v.len - 1] == '"') {
                    return v[1 .. v.len - 1];
                }
                return v;
            },
            else => unreachable,
        };
    }

    fn print(self: CSVType) void {
        switch (self) {
            .value => |v| std.debug.print("Value:{s}\n", .{v}),
            .quoted_value => |v| std.debug.print("QuotedValue:{s}\n", .{v}),
            .end_of_line => std.debug.print("End of Line\n", .{}),
            .end_of_file => std.debug.print("End of File\n", .{}),
            .parsing_error => std.debug.print("Parsing Error\n", .{}),
        }
    }

    /// Creates an owned String from this token's text content.
    /// For quoted values, strips surrounding quotes and unescapes doubled quotes ("" -> ").
    /// Ownership of the returned String is transferred to the caller.
    fn createString(self: CSVType, allocator: std.mem.Allocator) !strings.String {
        switch (self) {
            .value => |v| {
                return try strings.String.fromSlice(allocator, v);
            },
            .quoted_value => |v| {
                var str = try strings.String.fromSlice(allocator, v);
                defer str.deinit();
                // Remove the first and last quotes from the string
                _ = str.remove(0);
                _ = str.remove(str.len() - 1);
                // Replace double quotes with a single quote. There may be the opportunity to optimize this further.
                const new_str: []u8 = try std.mem.replaceOwned(u8, allocator, str.toSlice(), "\"\"", "\"");
                defer allocator.free(new_str);
                return try strings.String.fromSlice(allocator, new_str);
            },
            else => unreachable,
        }
    }
};

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
    index: usize,
    flags: CsvTokenizerFlags,
    rows: Rows,
    after_delimiter: bool, // Tracks if we just consumed a delimiter (for trailing empty fields)

    /// Allocates a new CsvTokenizer on the heap. Caller owns the returned pointer and must call deinit.
    pub fn init(allocator: std.mem.Allocator, content: []const u8, csvFlags: CsvTokenizerFlags) !*Self {
        const ptr_csv_tokenizer = try allocator.create(Self);
        errdefer allocator.destroy(ptr_csv_tokenizer);

        ptr_csv_tokenizer.allocator = allocator;
        ptr_csv_tokenizer.content = content;
        ptr_csv_tokenizer.index = 0;
        ptr_csv_tokenizer.flags = csvFlags;
        ptr_csv_tokenizer.rows = Rows.empty;
        ptr_csv_tokenizer.after_delimiter = false;

        return ptr_csv_tokenizer;
    }

    /// Deallocates all memory owned by this CsvTokenizer, including all rows. After this call, the pointer is invalid.
    pub fn deinit(self: *Self) void {
        for (self.rows.items) |*row| {
            row.deinit(self.allocator);
        }

        self.rows.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn validate(self: *Self) !void {
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

    const InferredType = enum { int64, float64, string };

    /// Infer the best numeric type for a column by scanning all data values.
    /// Tries i64 first, then f64, falls back to string.
    /// A column where all values are empty is treated as string.
    fn inferColumnType(self: *Self, col: usize, data_start: usize) InferredType {
        const h = self.rows.items.len;
        var all_int = true;
        var all_float = true;
        var has_value = false; // Bug fix: track whether we saw any non-empty value

        for (data_start..h) |row_idx| {
            const raw = self.rows.items[row_idx].items[col].getRawSlice();
            // Skip empty values — they don't disqualify a numeric type
            if (raw.len == 0) continue;

            has_value = true;

            if (all_int) {
                _ = std.fmt.parseInt(i64, raw, 10) catch {
                    all_int = false;
                };
            }
            if (all_float) {
                _ = std.fmt.parseFloat(f64, raw) catch {
                    all_float = false;
                };
            }
            if (!all_int and !all_float) break;
        }

        if (!has_value) return .string;
        if (all_int) return .int64;
        if (all_float) return .float64;
        return .string;
    }

    /// Creates and returns a new Dataframe with inferred column types.
    /// Numeric columns are stored as i64 or f64; everything else as String.
    /// Caller owns the returned pointer and must call deinit.
    pub fn createOwnedDataframe(self: *Self) !*dataframe.Dataframe {
        const df = try dataframe.Dataframe.init(self.allocator);
        errdefer df.deinit();

        const w = self.rows.items.ptr[0].items.len;
        const h = self.rows.items.len;

        for (0..w) |dw| {
            var data_start: usize = 0;

            // Extract header name if present
            var header_name: ?strings.String = null;
            defer if (header_name) |*hn| hn.deinit();

            if (self.flags.has_header) {
                header_name = try self.rows.items[0].items[dw].createString(self.allocator);
                data_start = 1;
            }
            data_start += self.flags.skip_rows;

            const col_type = self.inferColumnType(dw, data_start);

            switch (col_type) {
                .int64 => {
                    var s = try df.createSeries(i64);
                    if (header_name) |hn| try s.renameOwned(try hn.clone());
                    for (data_start..h) |dh| {
                        const raw = self.rows.items[dh].items[dw].getRawSlice();
                        const val = if (raw.len == 0) 0 else try std.fmt.parseInt(i64, raw, 10);
                        try s.append(val);
                    }
                },
                .float64 => {
                    var s = try df.createSeries(f64);
                    if (header_name) |hn| try s.renameOwned(try hn.clone());
                    for (data_start..h) |dh| {
                        const raw = self.rows.items[dh].items[dw].getRawSlice();
                        const val = if (raw.len == 0) 0.0 else try std.fmt.parseFloat(f64, raw);
                        try s.append(val);
                    }
                },
                .string => {
                    var s = try df.createSeries(strings.String);
                    if (header_name) |hn| try s.renameOwned(try hn.clone());
                    for (data_start..h) |dh| {
                        const str = try self.rows.items[dh].items[dw].createString(self.allocator);
                        try s.append(str);
                    }
                },
            }
        }

        return df;
    }

    pub fn readAll(self: *Self) !void {
        var rows = std.ArrayList(Row).empty;

        errdefer {
            for (rows.items) |*row| {
                row.deinit(self.allocator);
            }
            rows.deinit(self.allocator);
        }

        while (true) {
            var row = Row.empty;
            errdefer row.deinit(self.allocator);

            const result = try self.readRow(&row);

            if (result == .end_of_file) {
                if (row.items.len != 0) {
                    try rows.append(self.allocator, row);
                }
                break;
            }

            if (result == .parsing_error) {
                // Clean up the current row before returning error
                row.deinit(self.allocator);
                return CsvError.ParsingError;
            }

            try rows.append(self.allocator, row);
        }

        self.rows = rows;
    }

    fn readRow(self: *Self, row: *Row) !CSVType {
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
            try row.append(self.allocator, csv_token_type);
        }
        return .end_of_file;
    }

    fn next(self: *Self) CSVType {
        if (self.index >= self.content.len) {
            if (self.after_delimiter) {
                self.after_delimiter = false;
                return .{ .value = self.content[self.index..self.index] }; // empty slice
            }
            return .end_of_file;
        }

        // Handle \r\n and \n line endings
        if (self.content[self.index] == '\r' or self.content[self.index] == '\n') {
            // Bug fix: if we just consumed a delimiter, emit the trailing empty field first
            if (self.after_delimiter) {
                self.after_delimiter = false;
                return .{ .value = self.content[self.index..self.index] }; // empty slice
            }

            self.index += 1;

            if (self.index < self.content.len) {
                if (self.content[self.index] == '\n') {
                    self.index += 1;
                }
            }

            return .end_of_line;
        }

        self.after_delimiter = false;

        // Handle quoted strings
        const start = self.index;

        if (self.content[self.index] == '\"') {
            self.index += 1; // Skip the opening quote

            while (self.index < self.content.len) {
                const char = self.content[self.index];

                if (char == '\"') {
                    // Check if this is an escaped quote ("") or a closing quote
                    if (self.index + 1 < self.content.len) {
                        const next_char = self.content[self.index + 1];

                        // Escaped quote: skip both and continue
                        if (next_char == '\"') {
                            self.index += 2;
                            continue;
                        }

                        // Closing quote followed by delimiter, newline, or carriage return
                        const token = self.content[start .. self.index + 1];

                        if (next_char == self.flags.delimiter) {
                            self.index += 2; // Skip closing quote + delimiter
                            self.after_delimiter = true;
                        } else if (next_char == '\n' or next_char == '\r') {
                            self.index += 1; // Skip closing quote, leave newline for next call
                        } else {
                            return .parsing_error;
                        }

                        return .{ .quoted_value = token };
                    }

                    // Bug fix: closing quote is the very last character in content
                    const token = self.content[start .. self.index + 1];
                    self.index += 1;
                    return .{ .quoted_value = token };
                }

                self.index += 1;
            }

            // If we reached EOF without finding a closing quote, this is malformed.
            // Return as value to avoid losing data, but the quotes won't be stripped properly.
            if (start != self.index) {
                const token = self.content[start..self.index];
                return .{ .value = token };
            }
        } else {
            // Handle unquoted strings: read until delimiter, newline, or EOF
            while (self.index < self.content.len) {
                const char = self.content[self.index];

                if (char == self.flags.delimiter or char == '\n' or char == '\r') {
                    const token = self.content[start..self.index];
                    if (char == self.flags.delimiter) {
                        self.index += 1; // Skip delimiter, leave newline for next call
                        self.after_delimiter = true;
                    }
                    return .{ .value = token };
                }

                self.index += 1;
            }
            // Reached EOF — return remaining content as value
            if (start != self.index) {
                const token = self.content[start..self.index];
                return .{ .value = token };
            }
        }

        return .end_of_line;
    }
};

fn printChar(c: u8) void {
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

test "parse_csv_text3" {
    const content =
        \\First Name,Last Name
        \\John,"Doe"
        \\Jack,McGinnis
        \\"John ""Da Man""",Repici
        \\Stephen,Tyler
        \\,Blankman
        \\"Joan ""the bone"", Anne",Jet
    ;

    var tokenizer = try CsvTokenizer.init(std.testing.allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();
    // try tokenizer.print();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const compare_df = try dataframe.Dataframe.init(std.testing.allocator);
    defer compare_df.deinit();

    const series1 = try series.Series(strings.String).init(std.testing.allocator);
    try series1.rename("First Name");
    try series1.tryAppend("John");
    try series1.tryAppend("Jack");
    try series1.tryAppend("John \"Da Man\"");
    try series1.tryAppend("Stephen");
    try series1.tryAppend("");
    try series1.tryAppend("Joan \"the bone\", Anne");
    try compare_df.addSeries(series1.toBoxedSeries());
    const series2 = try series.Series(strings.String).init(std.testing.allocator);
    try series2.rename("Last Name");
    try series2.tryAppend("Doe");
    try series2.tryAppend("McGinnis");
    try series2.tryAppend("Repici");
    try series2.tryAppend("Tyler");
    try series2.tryAppend("Blankman");
    try series2.tryAppend("Jet");
    try compare_df.addSeries(series2.toBoxedSeries());

    std.debug.print("Comparing Dataframes...\n", .{});
    std.debug.print("Dataframe Height: {} Width: {}\n", .{ df.height(), df.width() });
    std.debug.print("Dataframe Height: {} Width: {}\n", .{ compare_df.height(), compare_df.width() });

    if (try df.compareDataframe(compare_df)) {
        std.debug.print("Dataframes are equal!\n", .{});
    } else {
        std.debug.print("Dataframes are NOT equal!\n", .{});
        try df.print();
        try std.testing.expect(false);
    }
}

test "parse_csv_text2" {
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

    try tokenizer.readAll();
    try tokenizer.validate();

    try tokenizer.print();
}

test "CsvTokenizer: init, readAll, validate, createOwnedDataframe" {
    const allocator = std.testing.allocator;
    const content = "A,B\n1,2\n3,4\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();
    try tokenizer.readAll();
    try tokenizer.validate();
    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();
    try std.testing.expect(df.width() == 2);
    try std.testing.expect(df.height() == 2);
}

test "CsvTokenizer: print output" {
    const allocator = std.testing.allocator;
    const content = "A,B\nfoo,bar\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();
    try tokenizer.readAll();
    try tokenizer.validate();
    try tokenizer.print();
}

// --- New Tests ---

test "csv: deinit without readAll does not crash" {
    // Bug fix: rows was uninitialized in init, causing UB on deinit
    const allocator = std.testing.allocator;
    var tokenizer = try CsvTokenizer.init(allocator, "A,B\n1,2\n", .{ .delimiter = ',' });
    // Immediately deinit without calling readAll
    tokenizer.deinit();
}

test "csv: quoted field at end of file without trailing newline" {
    // Bug fix: closing quote at EOF was returned as .value instead of .quoted_value
    const allocator = std.testing.allocator;
    const content = "name,val\nhello,\"world\"";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    // The quoted value "world" should have quotes stripped
    const val_series = df.getSeries("val") orelse return error.ColumnNotFound;
    const str_val = val_series.string.values.items[0].toSlice();
    try std.testing.expectEqualStrings("world", str_val);
}

test "csv: quoted field with escaped quotes at EOF" {
    const allocator = std.testing.allocator;
    const content = "a\n\"he said \"\"hi\"\"\"";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("a") orelse return error.ColumnNotFound;
    const val = s.string.values.items[0].toSlice();
    try std.testing.expectEqualStrings("he said \"hi\"", val);
}

test "csv: type inference detects integers" {
    const allocator = std.testing.allocator;
    const content = "num\n1\n2\n3\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    // Should be i64, not string
    const s = df.getSeries("num") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 1), s.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 2), s.int64.values.items[1]);
    try std.testing.expectEqual(@as(i64, 3), s.int64.values.items[2]);
}

test "csv: type inference detects floats" {
    const allocator = std.testing.allocator;
    const content = "price\n1.5\n2.7\n3.9\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("price") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .float64);
    try std.testing.expectEqual(@as(f64, 1.5), s.float64.values.items[0]);
}

test "csv: mixed int and float column infers as float" {
    const allocator = std.testing.allocator;
    const content = "val\n1\n2.5\n3\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    // "1" parses as float too, but "2.5" doesn't parse as int -> float64
    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .float64);
}

test "csv: all-empty column infers as string" {
    // Bug fix: all-empty column was inferred as int64 (filled with zeros)
    const allocator = std.testing.allocator;
    const content = "a,b\n,1\n,2\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("a") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
}

test "csv: quoted numeric values are inferred as numeric" {
    // Bug fix: getRawSlice included surrounding quotes, so "123" failed parseInt
    const allocator = std.testing.allocator;
    const content = "id\n\"100\"\n\"200\"\n\"300\"\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("id") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 100), s.int64.values.items[0]);
}

test "csv: no trailing newline" {
    const allocator = std.testing.allocator;
    const content = "a,b\n1,2\n3,4";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "csv: CRLF line endings" {
    const allocator = std.testing.allocator;
    const content = "a,b\r\n1,2\r\n3,4\r\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "csv: tab delimiter" {
    const allocator = std.testing.allocator;
    const content = "a\tb\n1\t2\n3\t4\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = '\t' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "csv: single column, single row" {
    const allocator = std.testing.allocator;
    const content = "x\n42\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 1), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    const s = df.getSeries("x") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 42), s.int64.values.items[0]);
}

test "csv: empty cells among numbers default to zero" {
    const allocator = std.testing.allocator;
    // Use two columns so empty cell is between delimiters, not an empty line
    const content = "key,val\na,10\nb,\nc,30\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 10), s.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 0), s.int64.values.items[1]);
    try std.testing.expectEqual(@as(i64, 30), s.int64.values.items[2]);
}

test "csv: negative integers are detected" {
    const allocator = std.testing.allocator;
    const content = "val\n-5\n10\n-20\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, -5), s.int64.values.items[0]);
}

test "csv: string column with one non-numeric value stays string" {
    const allocator = std.testing.allocator;
    const content = "val\n1\nhello\n3\n";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
}

test "csv: trailing empty field at end of file" {
    // Bug fix: "a,b\n1," should have row [1, ""] not just [1]
    const allocator = std.testing.allocator;
    const content = "a,b\n1,";
    var tokenizer = try CsvTokenizer.init(allocator, content, .{ .delimiter = ',' });
    defer tokenizer.deinit();

    try tokenizer.readAll();
    try tokenizer.validate();

    const df = try tokenizer.createOwnedDataframe();
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    const s = df.getSeries("b") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
    try std.testing.expectEqualStrings("", s.string.values.items[0].toSlice());
}
