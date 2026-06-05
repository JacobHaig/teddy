//! Raw parquet payload column type (Phase 6d-2a.0).
//!
//! Holds the undecoded bytes of one parquet value. Column-level metadata
//! (preserved physical type, annotations, FLBA width) lives on
//! `Series(Raw).meta` via the ColumnMeta capability — enough to re-emit the
//! column bit-faithfully on write. Used as the fallback for deferred logical
//! types (nested, VARIANT, GEOMETRY, GEOGRAPHY) and for INT96 until slice
//! 6d-2a.2 decodes it to Timestamp.

const std = @import("std");
const parquet = @import("parquet");

pub const Raw = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub const type_name = "Raw";

    /// Stored on Series(Raw).meta — see ColumnMetaFor in series.zig.
    pub const ColumnMeta = struct {
        physical_type: parquet.PhysicalType = .byte_array,
        converted_type: ?parquet.ConvertedType = null,
        logical_type: ?parquet.LogicalType = null,
        type_length: ?i32 = null,
    };

    /// Empty value — used as the appendNull placeholder.
    pub fn init(allocator: std.mem.Allocator) !Raw {
        return .{ .allocator = allocator, .bytes = try allocator.alloc(u8, 0) };
    }

    pub fn fromSlice(allocator: std.mem.Allocator, data: []const u8) !Raw {
        return .{ .allocator = allocator, .bytes = try allocator.dupe(u8, data) };
    }

    pub fn deinit(self: *Raw) void {
        self.allocator.free(self.bytes);
    }

    pub fn clone(self: *const Raw) !Raw {
        return fromSlice(self.allocator, self.bytes);
    }

    pub fn eql(self: *const Raw, other: *const Raw) bool {
        return std.mem.eql(u8, self.bytes, other.bytes);
    }

    pub fn toSlice(self: *const Raw) []const u8 {
        return self.bytes;
    }

    /// Lowercase hex, no prefix (what CSV/JSON/print render).
    pub fn format(self: Raw, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.bytes) |b| try writer.print("{x:0>2}", .{b});
    }
};

test "Raw: fromSlice/clone/eql/deinit own memory" {
    const allocator = std.testing.allocator;
    var a = try Raw.fromSlice(allocator, &.{ 0x01, 0x02 });
    defer a.deinit();
    var b = try a.clone();
    defer b.deinit();
    try std.testing.expect(a.eql(&b));
    try std.testing.expect(a.bytes.ptr != b.bytes.ptr);
    var c = try Raw.init(allocator);
    defer c.deinit();
    try std.testing.expect(!a.eql(&c));
}

test "Raw: format renders lowercase hex" {
    const allocator = std.testing.allocator;
    var r = try Raw.fromSlice(allocator, &.{ 0xDE, 0xAD, 0xBE, 0xEF });
    defer r.deinit();
    var buf: [16]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{r});
    try std.testing.expectEqualStrings("deadbeef", out);
}
