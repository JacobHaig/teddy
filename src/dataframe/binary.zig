//! Binary column type (Phase 6d-2a.4).
//!
//! Holds variable-length non-UTF8 byte payloads (unannotated BYTE_ARRAY,
//! BSON). String remains the type for UTF8-annotated data. Column-level
//! metadata preserves the BSON annotation for lossless re-emit; both fields
//! are null for plain binary.

const std = @import("std");
const parquet = @import("parquet");

pub const Binary = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub const type_name = "Binary";

    /// Stored on Series(Binary).meta. Preserves the BSON annotation for
    /// lossless re-emit; both null for plain binary.
    pub const ColumnMeta = struct {
        converted_type: ?parquet.ConvertedType = null,
        logical_type: ?parquet.LogicalType = null,
    };

    /// Empty value — used as the appendNull placeholder.
    pub fn init(allocator: std.mem.Allocator) !Binary {
        return .{ .allocator = allocator, .bytes = try allocator.alloc(u8, 0) };
    }

    pub fn fromSlice(allocator: std.mem.Allocator, data: []const u8) !Binary {
        return .{ .allocator = allocator, .bytes = try allocator.dupe(u8, data) };
    }

    pub fn deinit(self: *Binary) void {
        self.allocator.free(self.bytes);
    }

    pub fn clone(self: *const Binary) !Binary {
        return fromSlice(self.allocator, self.bytes);
    }

    pub fn eql(self: *const Binary, other: *const Binary) bool {
        return std.mem.eql(u8, self.bytes, other.bytes);
    }

    pub fn toSlice(self: *const Binary) []const u8 {
        return self.bytes;
    }

    pub fn len(self: *const Binary) usize {
        return self.bytes.len;
    }

    /// Borrowed sub-slice; bounds-checked.
    pub fn slice(self: *const Binary, start: usize, end: usize) ![]const u8 {
        if (start > end or end > self.bytes.len) return error.OutOfRange;
        return self.bytes[start..end];
    }

    /// Lowercase hex, no prefix (what CSV/JSON/print render).
    pub fn format(self: Binary, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.bytes) |b| try writer.print("{x:0>2}", .{b});
    }
};

const Series = @import("series.zig").Series;

test "Binary: fromSlice/clone/eql/deinit own memory" {
    const allocator = std.testing.allocator;
    var a = try Binary.fromSlice(allocator, &.{ 0x01, 0x02 });
    defer a.deinit();
    var b = try a.clone();
    defer b.deinit();
    try std.testing.expect(a.eql(&b));
    try std.testing.expect(a.bytes.ptr != b.bytes.ptr);
    var c = try Binary.init(allocator);
    defer c.deinit();
    try std.testing.expect(!a.eql(&c));
}

test "Binary: format renders lowercase hex" {
    const allocator = std.testing.allocator;
    var r = try Binary.fromSlice(allocator, &.{ 0xDE, 0xAD, 0xBE, 0xEF });
    defer r.deinit();
    var buf: [16]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{r});
    try std.testing.expectEqualStrings("deadbeef", out);
}

test "Binary: toSlice returns owned bytes" {
    const allocator = std.testing.allocator;
    var b = try Binary.fromSlice(allocator, &.{ 0xAA, 0xBB });
    defer b.deinit();
    try std.testing.expectEqualSlices(u8, &.{ 0xAA, 0xBB }, b.toSlice());
}

test "Binary: len returns byte count" {
    const allocator = std.testing.allocator;
    var b = try Binary.fromSlice(allocator, &.{ 1, 2, 3 });
    defer b.deinit();
    try std.testing.expectEqual(@as(usize, 3), b.len());
}

test "Binary: slice happy path" {
    const allocator = std.testing.allocator;
    var b = try Binary.fromSlice(allocator, &.{ 0x10, 0x20, 0x30, 0x40 });
    defer b.deinit();
    const sub = try b.slice(1, 3);
    try std.testing.expectEqualSlices(u8, &.{ 0x20, 0x30 }, sub);
}

test "Binary: slice OutOfRange start>end" {
    const allocator = std.testing.allocator;
    var b = try Binary.fromSlice(allocator, &.{ 1, 2, 3 });
    defer b.deinit();
    try std.testing.expectError(error.OutOfRange, b.slice(2, 1));
}

test "Binary: slice OutOfRange end>len" {
    const allocator = std.testing.allocator;
    var b = try Binary.fromSlice(allocator, &.{ 1, 2, 3 });
    defer b.deinit();
    try std.testing.expectError(error.OutOfRange, b.slice(0, 4));
}

test "Binary: Series ownership through append/dropRow/limit" {
    const allocator = std.testing.allocator;
    var s = try Series(Binary).init(allocator);
    defer s.deinit();
    try s.rename("binaries");
    try s.append(try Binary.fromSlice(allocator, &.{ 0x01 }));
    try s.append(try Binary.fromSlice(allocator, &.{ 0x02 }));
    try s.append(try Binary.fromSlice(allocator, &.{ 0x03 }));
    s.dropRow(1);
    try std.testing.expectEqual(@as(usize, 2), s.len());
    s.limit(1);
    try std.testing.expectEqual(@as(usize, 1), s.len());
}

test "Binary: deepCopy clones values independently" {
    const allocator = std.testing.allocator;
    var s = try Series(Binary).init(allocator);
    defer s.deinit();
    try s.append(try Binary.fromSlice(allocator, &.{ 0xAA, 0xBB }));
    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.values.items[0].eql(&s.values.items[0]));
    try std.testing.expect(copy.values.items[0].bytes.ptr != s.values.items[0].bytes.ptr);
}

test "Binary: ColumnMeta defaults and deepCopy propagation" {
    const allocator = std.testing.allocator;
    var s = try Series(Binary).init(allocator);
    defer s.deinit();
    // Defaults: both null
    try std.testing.expect(s.meta.converted_type == null);
    try std.testing.expect(s.meta.logical_type == null);
    // Set meta and verify deepCopy propagates it
    s.meta = .{ .converted_type = .bson, .logical_type = null };
    try s.append(try Binary.fromSlice(allocator, &.{ 0x01 }));
    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.meta.converted_type != null);
    try std.testing.expectEqual(parquet.ConvertedType.bson, copy.meta.converted_type.?);
}
