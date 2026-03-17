const std = @import("std");
const Allocator = std.mem.Allocator;

const dataframe = @import("dataframe.zig");
const series = @import("series.zig");
const strings = @import("strings.zig");
const String = strings.String;

// ============================================================
// Types
// ============================================================

/// Options controlling how CSV content is parsed.
pub const ParseOptions = struct {
    delimiter: u8 = ',',
    has_header: bool = true,
    skip_rows: usize = 0,
};

/// A single field extracted from CSV content.
/// For quoted fields, `raw` is the inner text (outer quotes removed)
/// but escaped quotes ("") are NOT yet unescaped.
pub const Field = struct {
    raw: []const u8,
    quoted: bool,

    /// Returns the raw text of the field (suitable for type inference).
    pub fn text(self: Field) []const u8 {
        return self.raw;
    }

    /// Returns true if the field contains no text.
    pub fn isEmpty(self: Field) bool {
        return self.raw.len == 0;
    }

    /// Creates an owned String from this field.
    /// For quoted fields, unescapes "" -> ".
    /// Caller owns the returned String.
    pub fn createString(self: Field, allocator: Allocator) !String {
        if (!self.quoted) {
            return String.fromSlice(allocator, self.raw);
        }
        // Fast path: no escaped quotes
        if (std.mem.indexOf(u8, self.raw, "\"\"") == null) {
            return String.fromSlice(allocator, self.raw);
        }
        // Unescape "" -> "
        const unescaped = try std.mem.replaceOwned(u8, allocator, self.raw, "\"\"", "\"");
        defer allocator.free(unescaped);
        return String.fromSlice(allocator, unescaped);
    }
};

/// Lexical token produced by the Scanner.
const Token = union(enum) {
    field: Field,
    row_end: void,
    eof: void,
};

/// Inferred data type for a column.
pub const ColumnType = enum {
    int64,
    float64,
    string,
};

// ============================================================
// Scanner — zero-allocation state machine tokenizer
// ============================================================

/// Low-level CSV scanner that produces a stream of Tokens from raw content.
/// Does not allocate — all Field slices point into the original content buffer.
const Scanner = struct {
    content: []const u8,
    pos: usize,
    delimiter: u8,
    after_delim: bool,

    fn init(content: []const u8, delimiter: u8) Scanner {
        return .{
            .content = content,
            .pos = 0,
            .delimiter = delimiter,
            .after_delim = false,
        };
    }

    /// Returns the next token from the CSV content.
    fn next(self: *Scanner) Token {
        // Trailing empty field: delimiter was the last thing before newline/EOF
        if (self.pos >= self.content.len) {
            if (self.after_delim) {
                self.after_delim = false;
                return .{ .field = .{ .raw = self.content[self.pos..self.pos], .quoted = false } };
            }
            return .eof;
        }

        const ch = self.content[self.pos];

        // Newline -> emit trailing empty field if needed, then row_end
        if (ch == '\r' or ch == '\n') {
            if (self.after_delim) {
                self.after_delim = false;
                return .{ .field = .{ .raw = self.content[self.pos..self.pos], .quoted = false } };
            }
            self.pos += 1;
            if (ch == '\r' and self.pos < self.content.len and self.content[self.pos] == '\n') {
                self.pos += 1;
            }
            return .row_end;
        }

        self.after_delim = false;

        if (ch == '"') return self.scanQuoted();
        return self.scanUnquoted();
    }

    /// Scan a quoted field: opening quote already at self.pos.
    fn scanQuoted(self: *Scanner) Token {
        self.pos += 1; // skip opening quote
        const start = self.pos;

        while (self.pos < self.content.len) {
            if (self.content[self.pos] == '"') {
                // Escaped quote ""
                if (self.pos + 1 < self.content.len and self.content[self.pos + 1] == '"') {
                    self.pos += 2;
                    continue;
                }
                // Closing quote
                const raw = self.content[start..self.pos];
                self.pos += 1; // skip closing quote

                // Consume trailing delimiter if present
                if (self.pos < self.content.len and self.content[self.pos] == self.delimiter) {
                    self.pos += 1;
                    self.after_delim = true;
                }

                return .{ .field = .{ .raw = raw, .quoted = true } };
            }
            self.pos += 1;
        }

        // Unterminated quote — return content as unquoted field
        return .{ .field = .{ .raw = self.content[start..self.pos], .quoted = false } };
    }

    /// Scan an unquoted field: current char is not quote, delimiter, or newline.
    fn scanUnquoted(self: *Scanner) Token {
        const start = self.pos;

        while (self.pos < self.content.len) {
            const ch = self.content[self.pos];
            if (ch == self.delimiter) {
                const raw = self.content[start..self.pos];
                self.pos += 1; // skip delimiter
                self.after_delim = true;
                return .{ .field = .{ .raw = raw, .quoted = false } };
            }
            if (ch == '\r' or ch == '\n') {
                break;
            }
            self.pos += 1;
        }

        // Reached newline or EOF
        return .{ .field = .{ .raw = self.content[start..self.pos], .quoted = false } };
    }
};

// ============================================================
// Table — intermediate scanned representation
// ============================================================

/// A structured table of Fields scanned from CSV content.
/// Fields are zero-copy slices into the original content buffer.
const Table = struct {
    const Row = std.ArrayList(Field);

    allocator: Allocator,
    rows: std.ArrayList(Row),

    /// Scan CSV content into a Table of Fields.
    fn scan(allocator: Allocator, content: []const u8, delimiter: u8) !Table {
        var scanner = Scanner.init(content, delimiter);
        var rows = std.ArrayList(Row).empty;
        errdefer {
            for (rows.items) |*r| r.deinit(allocator);
            rows.deinit(allocator);
        }

        var current = Row.empty;
        errdefer current.deinit(allocator);

        while (true) {
            switch (scanner.next()) {
                .field => |f| try current.append(allocator, f),
                .row_end => {
                    try rows.append(allocator, current);
                    current = Row.empty;
                },
                .eof => {
                    if (current.items.len > 0) {
                        try rows.append(allocator, current);
                        current = Row.empty;
                    }
                    break;
                },
            }
        }

        return .{ .allocator = allocator, .rows = rows };
    }

    /// Free all row arrays. Fields are zero-copy and need no cleanup.
    fn deinit(self: *Table) void {
        for (self.rows.items) |*r| r.deinit(self.allocator);
        self.rows.deinit(self.allocator);
    }

    /// Validate that all rows have the same number of fields.
    fn validate(self: *const Table) !void {
        if (self.rows.items.len == 0) return error.EmptyInput;
        const expected = self.rows.items[0].items.len;
        for (self.rows.items, 0..) |row, i| {
            if (row.items.len != expected) {
                std.debug.print("\nRow: {} Expected {} fields, got {}\n", .{ i + 1, expected, row.items.len });
                return error.InconsistentRowWidth;
            }
        }
    }

    /// Infer the best numeric type for a column by scanning all data values.
    /// Tries i64 first, then f64, falls back to string.
    /// A column where every value is empty is treated as string.
    fn inferColumnType(self: *const Table, col: usize, data_start: usize) ColumnType {
        var all_int = true;
        var all_float = true;
        var has_value = false;

        for (data_start..self.rows.items.len) |i| {
            const raw = self.rows.items[i].items[col].text();
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

    /// Build a Dataframe from this Table with type-inferred columns.
    /// Caller owns the returned Dataframe.
    fn toDataframe(self: *const Table, allocator: Allocator, options: ParseOptions) !*dataframe.Dataframe {
        const df = try dataframe.Dataframe.init(allocator);
        errdefer df.deinit();

        const width = self.rows.items[0].items.len;
        const height = self.rows.items.len;

        for (0..width) |col| {
            var data_start: usize = 0;

            // Extract header name if present
            var header: ?String = null;
            defer if (header) |*h| h.deinit();

            if (options.has_header) {
                header = try self.rows.items[0].items[col].createString(allocator);
                data_start = 1;
            }
            data_start += options.skip_rows;

            switch (self.inferColumnType(col, data_start)) {
                .int64 => {
                    var s = try df.createSeries(i64);
                    if (header) |h| try s.renameOwned(try h.clone());
                    for (data_start..height) |row_i| {
                        const raw = self.rows.items[row_i].items[col].text();
                        try s.append(if (raw.len == 0) 0 else try std.fmt.parseInt(i64, raw, 10));
                    }
                },
                .float64 => {
                    var s = try df.createSeries(f64);
                    if (header) |h| try s.renameOwned(try h.clone());
                    for (data_start..height) |row_i| {
                        const raw = self.rows.items[row_i].items[col].text();
                        try s.append(if (raw.len == 0) 0.0 else try std.fmt.parseFloat(f64, raw));
                    }
                },
                .string => {
                    var s = try df.createSeries(String);
                    if (header) |h| try s.renameOwned(try h.clone());
                    for (data_start..height) |row_i| {
                        try s.append(try self.rows.items[row_i].items[col].createString(allocator));
                    }
                },
            }
        }

        return df;
    }

    /// Debug: print all fields in the table.
    fn print(self: *const Table) void {
        for (self.rows.items) |row| {
            for (row.items) |field| {
                if (field.quoted) {
                    std.debug.print("'\"{s}\"' ", .{field.raw});
                } else {
                    std.debug.print("'{s}' ", .{field.raw});
                }
            }
            std.debug.print("\n", .{});
        }
    }
};

// ============================================================
// Public API
// ============================================================

/// Parse CSV content into a Dataframe.
/// Single entry point: handles scanning, validation, type inference, and construction.
/// Caller owns the returned Dataframe.
pub fn parse(allocator: Allocator, content: []const u8, options: ParseOptions) !*dataframe.Dataframe {
    var table = try Table.scan(allocator, content, options.delimiter);
    defer table.deinit();
    try table.validate();
    return try table.toDataframe(allocator, options);
}

// ============================================================
// Tests
// ============================================================

test "parse: basic CSV with headers" {
    const allocator = std.testing.allocator;
    const content = "A,B\n1,2\n3,4\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "parse: quoted fields with escaped quotes" {
    const allocator = std.testing.allocator;
    const content =
        \\First Name,Last Name
        \\John,"Doe"
        \\Jack,McGinnis
        \\"John ""Da Man""",Repici
        \\Stephen,Tyler
        \\,Blankman
        \\"Joan ""the bone"", Anne",Jet
    ;

    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const compare_df = try dataframe.Dataframe.init(allocator);
    defer compare_df.deinit();

    const series1 = try series.Series(String).init(allocator);
    try series1.rename("First Name");
    try series1.tryAppend("John");
    try series1.tryAppend("Jack");
    try series1.tryAppend("John \"Da Man\"");
    try series1.tryAppend("Stephen");
    try series1.tryAppend("");
    try series1.tryAppend("Joan \"the bone\", Anne");
    try compare_df.addSeries(series1.toBoxedSeries());

    const series2 = try series.Series(String).init(allocator);
    try series2.rename("Last Name");
    try series2.tryAppend("Doe");
    try series2.tryAppend("McGinnis");
    try series2.tryAppend("Repici");
    try series2.tryAppend("Tyler");
    try series2.tryAppend("Blankman");
    try series2.tryAppend("Jet");
    try compare_df.addSeries(series2.toBoxedSeries());

    if (!try df.compareDataframe(compare_df)) {
        try df.print();
        try std.testing.expect(false);
    }
}

test "parse: multi-column addresses CSV" {
    const allocator = std.testing.allocator;
    const content =
        \\First Name,Last Name,Age,Address,City,State,Zip
        \\John,Doe,52,120 jefferson st.,Riverside, NJ, 08075
        \\Jack,McGinnis,23,220 hobo Av.,Phila, PA,09119
        \\"John ""Da Man""",Repici,38,120 Jefferson St.,Riverside, NJ,"08075"
        \\Stephen,Tyler,96,"7452, Terrace ""At the Plaza"" road",SomeTown,SD," 91234"
        \\,Blankman,14,,SomeTown, SD, 00298
        \\"Joan ""the bone"", Anne",Jet,56,"9th, at Terrace plc",Desert City,CO,00123
    ;

    const df = try parse(allocator, content, .{});
    defer df.deinit();
    try std.testing.expectEqual(@as(usize, 7), df.width());
    try std.testing.expectEqual(@as(usize, 6), df.height());
}

test "parse: quoted field at end of file without trailing newline" {
    const allocator = std.testing.allocator;
    const content = "name,val\nhello,\"world\"";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    const val_series = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("world", val_series.string.values.items[0].toSlice());
}

test "parse: quoted field with escaped quotes at EOF" {
    const allocator = std.testing.allocator;
    const content = "a\n\"he said \"\"hi\"\"\"";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("a") orelse return error.ColumnNotFound;
    try std.testing.expectEqualStrings("he said \"hi\"", s.string.values.items[0].toSlice());
}

test "parse: type inference detects integers" {
    const allocator = std.testing.allocator;
    const content = "num\n1\n2\n3\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("num") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 1), s.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 2), s.int64.values.items[1]);
    try std.testing.expectEqual(@as(i64, 3), s.int64.values.items[2]);
}

test "parse: type inference detects floats" {
    const allocator = std.testing.allocator;
    const content = "price\n1.5\n2.7\n3.9\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("price") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .float64);
    try std.testing.expectEqual(@as(f64, 1.5), s.float64.values.items[0]);
}

test "parse: mixed int and float column infers as float" {
    const allocator = std.testing.allocator;
    const content = "val\n1\n2.5\n3\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .float64);
}

test "parse: all-empty column infers as string" {
    const allocator = std.testing.allocator;
    const content = "a,b\n,1\n,2\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("a") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
}

test "parse: quoted numeric values are inferred as numeric" {
    const allocator = std.testing.allocator;
    const content = "id\n\"100\"\n\"200\"\n\"300\"\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("id") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 100), s.int64.values.items[0]);
}

test "parse: no trailing newline" {
    const allocator = std.testing.allocator;
    const content = "a,b\n1,2\n3,4";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "parse: CRLF line endings" {
    const allocator = std.testing.allocator;
    const content = "a,b\r\n1,2\r\n3,4\r\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "parse: tab delimiter" {
    const allocator = std.testing.allocator;
    const content = "a\tb\n1\t2\n3\t4\n";
    const df = try parse(allocator, content, .{ .delimiter = '\t' });
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 2), df.height());
}

test "parse: single column, single row" {
    const allocator = std.testing.allocator;
    const content = "x\n42\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 1), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    const s = df.getSeries("x") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 42), s.int64.values.items[0]);
}

test "parse: empty cells among numbers default to zero" {
    const allocator = std.testing.allocator;
    const content = "key,val\na,10\nb,\nc,30\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, 10), s.int64.values.items[0]);
    try std.testing.expectEqual(@as(i64, 0), s.int64.values.items[1]);
    try std.testing.expectEqual(@as(i64, 30), s.int64.values.items[2]);
}

test "parse: negative integers are detected" {
    const allocator = std.testing.allocator;
    const content = "val\n-5\n10\n-20\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .int64);
    try std.testing.expectEqual(@as(i64, -5), s.int64.values.items[0]);
}

test "parse: string column with one non-numeric value stays string" {
    const allocator = std.testing.allocator;
    const content = "val\n1\nhello\n3\n";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    const s = df.getSeries("val") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
}

test "parse: trailing empty field at end of file" {
    const allocator = std.testing.allocator;
    const content = "a,b\n1,";
    const df = try parse(allocator, content, .{});
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 2), df.width());
    try std.testing.expectEqual(@as(usize, 1), df.height());

    const s = df.getSeries("b") orelse return error.ColumnNotFound;
    try std.testing.expect(s.* == .string);
    try std.testing.expectEqualStrings("", s.string.values.items[0].toSlice());
}

test "Scanner: produces correct token sequence" {
    var scanner = Scanner.init("a,b\n1,2\n", ',');

    // Row 1: a, b
    try std.testing.expectEqualStrings("a", scanner.next().field.raw);
    try std.testing.expectEqualStrings("b", scanner.next().field.raw);
    try std.testing.expect(scanner.next() == .row_end);

    // Row 2: 1, 2
    try std.testing.expectEqualStrings("1", scanner.next().field.raw);
    try std.testing.expectEqualStrings("2", scanner.next().field.raw);
    try std.testing.expect(scanner.next() == .row_end);

    // EOF
    try std.testing.expect(scanner.next() == .eof);
}

test "Scanner: quoted field with comma inside" {
    var scanner = Scanner.init("\"a,b\",c\n", ',');

    const f1 = scanner.next().field;
    try std.testing.expectEqualStrings("a,b", f1.raw);
    try std.testing.expect(f1.quoted);

    const f2 = scanner.next().field;
    try std.testing.expectEqualStrings("c", f2.raw);
    try std.testing.expect(!f2.quoted);

    try std.testing.expect(scanner.next() == .row_end);
    try std.testing.expect(scanner.next() == .eof);
}

test "Scanner: trailing empty field before newline" {
    var scanner = Scanner.init("a,\n", ',');

    try std.testing.expectEqualStrings("a", scanner.next().field.raw);
    // Empty field after delimiter, before newline
    try std.testing.expectEqualStrings("", scanner.next().field.raw);
    try std.testing.expect(scanner.next() == .row_end);
    try std.testing.expect(scanner.next() == .eof);
}

test "Scanner: trailing empty field at EOF" {
    var scanner = Scanner.init("a,", ',');

    try std.testing.expectEqualStrings("a", scanner.next().field.raw);
    try std.testing.expectEqualStrings("", scanner.next().field.raw);
    try std.testing.expect(scanner.next() == .eof);
}

test "Table: scan and validate" {
    const allocator = std.testing.allocator;
    var table = try Table.scan(allocator, "a,b\n1,2\n3,4\n", ',');
    defer table.deinit();

    try table.validate();
    try std.testing.expectEqual(@as(usize, 3), table.rows.items.len);
    try std.testing.expectEqual(@as(usize, 2), table.rows.items[0].items.len);
}

test "Table: deinit without toDataframe is safe" {
    const allocator = std.testing.allocator;
    var table = try Table.scan(allocator, "A,B\n1,2\n", ',');
    table.deinit();
    // No crash = success
}

test "Field: createString unescapes quotes" {
    const allocator = std.testing.allocator;

    const plain = Field{ .raw = "hello", .quoted = false };
    var s1 = try plain.createString(allocator);
    defer s1.deinit();
    try std.testing.expectEqualStrings("hello", s1.toSlice());

    const quoted = Field{ .raw = "he said \"\"hi\"\"", .quoted = true };
    var s2 = try quoted.createString(allocator);
    defer s2.deinit();
    try std.testing.expectEqualStrings("he said \"hi\"", s2.toSlice());

    const no_escape = Field{ .raw = "simple", .quoted = true };
    var s3 = try no_escape.createString(allocator);
    defer s3.deinit();
    try std.testing.expectEqualStrings("simple", s3.toSlice());
}
