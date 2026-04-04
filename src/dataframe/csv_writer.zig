const std = @import("std");
const Allocator = std.mem.Allocator;
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;

pub const WriteOptions = struct {
    delimiter: u8 = ',',
    include_header: bool = true,
};

/// Write a Dataframe to CSV format, returns an owned slice.
/// Caller must free the returned slice with allocator.free().
pub fn writeToString(allocator: Allocator, df: *Dataframe, options: WriteOptions) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);

    const w = df.width();
    const h = df.height();

    if (options.include_header) {
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, options.delimiter);
            try appendCsvField(&buf, allocator, df.series.items[col].name(), options.delimiter);
        }
        try buf.append(allocator, '\n');
    }

    for (0..h) |row| {
        for (0..w) |col| {
            if (col > 0) try buf.append(allocator, options.delimiter);
            var str = try df.series.items[col].asStringAt(row);
            defer str.deinit();
            try appendCsvField(&buf, allocator, str.toSlice(), options.delimiter);
        }
        try buf.append(allocator, '\n');
    }

    return buf.toOwnedSlice(allocator);
}

fn appendCsvField(buf: *std.ArrayList(u8), allocator: Allocator, field: []const u8, delimiter: u8) !void {
    var needs_quoting = false;
    for (field) |c| {
        if (c == delimiter or c == '"' or c == '\n' or c == '\r') {
            needs_quoting = true;
            break;
        }
    }

    if (needs_quoting) {
        try buf.append(allocator, '"');
        for (field) |c| {
            if (c == '"') try buf.append(allocator, '"');
            try buf.append(allocator, c);
        }
        try buf.append(allocator, '"');
    } else {
        try buf.appendSlice(allocator, field);
    }
}

