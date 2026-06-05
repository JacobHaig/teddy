//! FixedBytes column type (Phase 6d-2a.4).
//!
//! Holds fixed-length non-UTF8 byte payloads (unannotated
//! FIXED_LEN_BYTE_ARRAY). The FLBA type_length is stored on the column-level
//! meta field as `width`; null until set by the parquet adapter or the user.

const std = @import("std");
const parquet = @import("parquet");

pub const FixedBytes = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub const type_name = "FixedBytes";

    /// Column width on the Series — the FLBA type_length per the spec.
    /// null until set by the parquet adapter or the user.
    pub const ColumnMeta = struct {
        width: ?i32 = null,
    };

    /// Empty value — used as the appendNull placeholder.
    pub fn init(allocator: std.mem.Allocator) !FixedBytes {
        return .{ .allocator = allocator, .bytes = try allocator.alloc(u8, 0) };
    }

    pub fn fromSlice(allocator: std.mem.Allocator, data: []const u8) !FixedBytes {
        return .{ .allocator = allocator, .bytes = try allocator.dupe(u8, data) };
    }

    pub fn deinit(self: *FixedBytes) void {
        self.allocator.free(self.bytes);
    }

    pub fn clone(self: *const FixedBytes) !FixedBytes {
        return fromSlice(self.allocator, self.bytes);
    }

    pub fn eql(self: *const FixedBytes, other: *const FixedBytes) bool {
        return std.mem.eql(u8, self.bytes, other.bytes);
    }

    pub fn toSlice(self: *const FixedBytes) []const u8 {
        return self.bytes;
    }

    pub fn len(self: *const FixedBytes) usize {
        return self.bytes.len;
    }

    /// Borrowed sub-slice; bounds-checked.
    pub fn slice(self: *const FixedBytes, start: usize, end: usize) ![]const u8 {
        if (start > end or end > self.bytes.len) return error.OutOfRange;
        return self.bytes[start..end];
    }

    /// Lowercase hex, no prefix (what CSV/JSON/print render).
    pub fn format(self: FixedBytes, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.bytes) |b| try writer.print("{x:0>2}", .{b});
    }
};

const Series = @import("series.zig").Series;

test "FixedBytes: fromSlice/clone/eql/deinit own memory" {
    const allocator = std.testing.allocator;
    var a = try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02, 0x03, 0x04 });
    defer a.deinit();
    var b = try a.clone();
    defer b.deinit();
    try std.testing.expect(a.eql(&b));
    try std.testing.expect(a.bytes.ptr != b.bytes.ptr);
    var c = try FixedBytes.init(allocator);
    defer c.deinit();
    try std.testing.expect(!a.eql(&c));
}

test "FixedBytes: format renders lowercase hex" {
    const allocator = std.testing.allocator;
    var r = try FixedBytes.fromSlice(allocator, &.{ 0xCA, 0xFE, 0xBA, 0xBE });
    defer r.deinit();
    var buf: [16]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{r});
    try std.testing.expectEqualStrings("cafebabe", out);
}

test "FixedBytes: toSlice returns owned bytes" {
    const allocator = std.testing.allocator;
    var b = try FixedBytes.fromSlice(allocator, &.{ 0x11, 0x22, 0x33, 0x44 });
    defer b.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0x11, 0x22, 0x33, 0x44 }, b.toSlice());
}

test "FixedBytes: len returns byte count" {
    const allocator = std.testing.allocator;
    var b = try FixedBytes.fromSlice(allocator, &.{ 1, 2, 3, 4 });
    defer b.deinit();
    try std.testing.expectEqual(@as(usize, 4), b.len());
}

test "FixedBytes: slice happy path" {
    const allocator = std.testing.allocator;
    var b = try FixedBytes.fromSlice(allocator, &.{ 0x10, 0x20, 0x30, 0x40 });
    defer b.deinit();
    const sub = try b.slice(1, 3);
    try std.testing.expectEqualSlices(u8, &.{ 0x20, 0x30 }, sub);
}

test "FixedBytes: slice OutOfRange start>end" {
    const allocator = std.testing.allocator;
    var b = try FixedBytes.fromSlice(allocator, &.{ 1, 2, 3, 4 });
    defer b.deinit();
    try std.testing.expectError(error.OutOfRange, b.slice(3, 1));
}

test "FixedBytes: slice OutOfRange end>len" {
    const allocator = std.testing.allocator;
    var b = try FixedBytes.fromSlice(allocator, &.{ 1, 2, 3, 4 });
    defer b.deinit();
    try std.testing.expectError(error.OutOfRange, b.slice(0, 5));
}

test "FixedBytes: Series ownership through append/dropRow/limit" {
    const allocator = std.testing.allocator;
    var s = try Series(FixedBytes).init(allocator);
    defer s.deinit();
    try s.rename("fixed");
    try s.append(try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02, 0x03, 0x04 }));
    try s.append(try FixedBytes.fromSlice(allocator, &.{ 0x05, 0x06, 0x07, 0x08 }));
    try s.append(try FixedBytes.fromSlice(allocator, &.{ 0x09, 0x0A, 0x0B, 0x0C }));
    s.dropRow(1);
    try std.testing.expectEqual(@as(usize, 2), s.len());
    s.limit(1);
    try std.testing.expectEqual(@as(usize, 1), s.len());
}

test "FixedBytes: deepCopy clones values independently" {
    const allocator = std.testing.allocator;
    var s = try Series(FixedBytes).init(allocator);
    defer s.deinit();
    try s.append(try FixedBytes.fromSlice(allocator, &.{ 0xAA, 0xBB, 0xCC, 0xDD }));
    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.values.items[0].eql(&s.values.items[0]));
    try std.testing.expect(copy.values.items[0].bytes.ptr != s.values.items[0].bytes.ptr);
}

test "FixedBytes: ColumnMeta width=4 defaults null and survives deepCopy" {
    const allocator = std.testing.allocator;
    var s = try Series(FixedBytes).init(allocator);
    defer s.deinit();
    // Default: null
    try std.testing.expect(s.meta.width == null);
    // Set width=4 and verify deepCopy propagates it
    s.meta = .{ .width = 4 };
    try s.append(try FixedBytes.fromSlice(allocator, &.{ 0x01, 0x02, 0x03, 0x04 }));
    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.meta.width != null);
    try std.testing.expectEqual(@as(i32, 4), copy.meta.width.?);
}
