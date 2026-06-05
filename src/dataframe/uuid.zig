//! Uuid column type (Phase 6d-2a.5): 128-bit Universally Unique Identifier.
//! Wire format: 16 raw bytes, RFC 4122 layout. Canonical text:
//! xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (lowercase hex, hyphens at the
//! standard positions). Parse accepts both lowercase and uppercase input.

const std = @import("std");

pub const Uuid = struct {
    bytes: [16]u8,

    pub const type_name = "Uuid";

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    pub fn fromBytes(b: [16]u8) Uuid {
        return .{ .bytes = b };
    }

    pub fn toBytes(self: Uuid) [16]u8 {
        return self.bytes;
    }

    // -----------------------------------------------------------------------
    // Comparison — POD convention: pointer args, no alloc
    // -----------------------------------------------------------------------

    pub fn eql(self: *const Uuid, other: *const Uuid) bool {
        return std.mem.eql(u8, &self.bytes, &other.bytes);
    }

    /// Total ordering: bytewise lexicographic comparison of the 16-byte
    /// big-endian representation, which gives RFC 4122 canonical-form ordering.
    pub fn order(self: *const Uuid, other: *const Uuid) std.math.Order {
        return std.mem.order(u8, &self.bytes, &other.bytes);
    }

    // -----------------------------------------------------------------------
    // Text codec
    // -----------------------------------------------------------------------

    /// Canonical lowercase `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
    pub fn format(self: Uuid, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        const b = self.bytes;
        try writer.print(
            "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}",
            .{ b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15] },
        );
    }

    /// Parse the canonical 36-character hyphenated UUID form (case-insensitive).
    /// Returns `error.InvalidUuid` for any violation:
    ///   - length != 36
    ///   - hyphens missing at positions 8, 13, 18, 23
    ///   - non-hex character anywhere else
    pub fn parse(str: []const u8) !Uuid {
        if (str.len != 36) return error.InvalidUuid;
        if (str[8] != '-' or str[13] != '-' or str[18] != '-' or str[23] != '-') {
            return error.InvalidUuid;
        }
        // Build a contiguous 32-hex-char view by skipping hyphens.
        var hex: [32]u8 = undefined;
        var hi: usize = 0;
        for (str, 0..) |c, i| {
            if (i == 8 or i == 13 or i == 18 or i == 23) continue; // hyphen already checked
            hex[hi] = c;
            hi += 1;
        }
        // Decode hex pairs into bytes.
        var out: [16]u8 = undefined;
        for (0..16) |i| {
            const hi_nibble = hexDigit(hex[i * 2]) catch return error.InvalidUuid;
            const lo_nibble = hexDigit(hex[i * 2 + 1]) catch return error.InvalidUuid;
            out[i] = (hi_nibble << 4) | lo_nibble;
        }
        return .{ .bytes = out };
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn hexDigit(c: u8) !u8 {
        return switch (c) {
            '0'...'9' => c - '0',
            'a'...'f' => c - 'a' + 10,
            'A'...'F' => c - 'A' + 10,
            else => error.InvalidUuid,
        };
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Uuid: format of a known UUID" {
    const u = Uuid{ .bytes = .{
        0x12, 0x34, 0x56, 0x78,
        0x9a, 0xbc,
        0xde, 0xf0,
        0x12, 0x34,
        0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
    } };
    var buf: [40]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{u});
    try std.testing.expectEqualStrings("12345678-9abc-def0-1234-56789abcdef0", out);
}

test "Uuid: parse then format is identity" {
    const canonical = "550e8400-e29b-41d4-a716-446655440000";
    const u = try Uuid.parse(canonical);
    var buf: [40]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{u});
    try std.testing.expectEqualStrings(canonical, out);
}

test "Uuid: parse uppercase input gives same bytes as lowercase" {
    const lower = try Uuid.parse("550e8400-e29b-41d4-a716-446655440000");
    const upper = try Uuid.parse("550E8400-E29B-41D4-A716-446655440000");
    try std.testing.expect(lower.eql(&upper));
}

test "Uuid: parse rejects bad inputs" {
    // Length 35 (one character short)
    try std.testing.expectError(error.InvalidUuid, Uuid.parse("550e8400-e29b-41d4-a716-44665544000"));
    // Missing first hyphen (replaced with 'X')
    try std.testing.expectError(error.InvalidUuid, Uuid.parse("550e8400Xe29b-41d4-a716-446655440000"));
    // Non-hex character
    try std.testing.expectError(error.InvalidUuid, Uuid.parse("550e8400-e29b-41d4-a716-44665544000Z"));
    // Empty string
    try std.testing.expectError(error.InvalidUuid, Uuid.parse(""));
}

test "Uuid: order and eql" {
    const a = Uuid{ .bytes = .{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 } };
    const b = Uuid{ .bytes = .{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02 } };
    const a2 = a;

    try std.testing.expect(a.eql(&a2));
    try std.testing.expect(!a.eql(&b));
    try std.testing.expectEqual(std.math.Order.lt, a.order(&b));
    try std.testing.expectEqual(std.math.Order.gt, b.order(&a));
    try std.testing.expectEqual(std.math.Order.eq, a.order(&a2));
}

test "Uuid: Series(Uuid) argSort" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Uuid).init(allocator);
    defer s.deinit();
    try s.rename("id");

    const ua = Uuid{ .bytes = .{ 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    const ub = Uuid{ .bytes = .{ 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    const uc = Uuid{ .bytes = .{ 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    try s.append(ua);
    try s.append(ub);
    try s.append(uc);

    var indices = try s.argSort(allocator, true);
    defer indices.deinit(allocator);

    // ascending: ub (0x01) < uc (0x02) < ua (0x03), so indices [1, 2, 0]
    try std.testing.expectEqual(@as(usize, 1), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
    try std.testing.expectEqual(@as(usize, 0), indices.items[2]);
}

test "Uuid: Series(Uuid) asStringAt" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Uuid).init(allocator);
    defer s.deinit();
    try s.rename("id");
    try s.append(try Uuid.parse("00000000-0000-0000-0000-000000000001"));

    var str = try s.asStringAt(0);
    defer str.deinit();
    try std.testing.expectEqualStrings("00000000-0000-0000-0000-000000000001", str.toSlice());
}
