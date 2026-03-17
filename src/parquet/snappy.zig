const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================
// Snappy Decompression (native Zig implementation)
// ============================================================

pub const SnappyError = error{
    CorruptInput,
    UnexpectedEof,
    OffsetOutOfRange,
    OutputOverflow,
    OutOfMemory,
};

/// Decompress Snappy-compressed data.
/// Returns an allocator-owned buffer containing the decompressed data.
pub fn decompress(allocator: Allocator, input: []const u8) SnappyError![]u8 {
    var pos: usize = 0;

    // Read uncompressed length (Snappy varint)
    const uncompressed_len = readSnappyVarint(input, &pos) orelse return SnappyError.CorruptInput;
    if (uncompressed_len > 1 << 27) return SnappyError.CorruptInput; // sanity limit: 128MB

    const output = allocator.alloc(u8, uncompressed_len) catch return SnappyError.OutOfMemory;
    errdefer allocator.free(output);

    var out_pos: usize = 0;

    while (pos < input.len) {
        const tag = input[pos];
        pos += 1;
        const tag_type = tag & 0x03;

        switch (tag_type) {
            0 => {
                // Literal
                const len = try decodeLiteralLength(input, tag, &pos);
                if (pos + len > input.len) return SnappyError.UnexpectedEof;
                if (out_pos + len > output.len) return SnappyError.OutputOverflow;
                @memcpy(output[out_pos .. out_pos + len], input[pos .. pos + len]);
                pos += len;
                out_pos += len;
            },
            1 => {
                // Copy with 1-byte offset
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                if (pos >= input.len) return SnappyError.UnexpectedEof;
                const offset: usize = (@as(usize, tag >> 5) << 8) | @as(usize, input[pos]);
                pos += 1;
                if (offset == 0 or offset > out_pos) return SnappyError.OffsetOutOfRange;
                if (out_pos + length > output.len) return SnappyError.OutputOverflow;
                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
            2 => {
                // Copy with 2-byte offset
                const length: usize = @as(usize, tag >> 2) + 1;
                if (pos + 2 > input.len) return SnappyError.UnexpectedEof;
                const offset: usize = @as(usize, input[pos]) | (@as(usize, input[pos + 1]) << 8);
                pos += 2;
                if (offset == 0 or offset > out_pos) return SnappyError.OffsetOutOfRange;
                if (out_pos + length > output.len) return SnappyError.OutputOverflow;
                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
            3 => {
                // Copy with 4-byte offset
                const length: usize = @as(usize, tag >> 2) + 1;
                if (pos + 4 > input.len) return SnappyError.UnexpectedEof;
                const offset: usize = @as(usize, input[pos]) |
                    (@as(usize, input[pos + 1]) << 8) |
                    (@as(usize, input[pos + 2]) << 16) |
                    (@as(usize, input[pos + 3]) << 24);
                pos += 4;
                if (offset == 0 or offset > out_pos) return SnappyError.OffsetOutOfRange;
                if (out_pos + length > output.len) return SnappyError.OutputOverflow;
                copyOverlapping(output, out_pos, offset, length);
                out_pos += length;
            },
            else => unreachable,
        }
    }

    if (out_pos != output.len) return SnappyError.CorruptInput;
    return output;
}

/// Decode literal length from tag byte and possibly additional bytes.
fn decodeLiteralLength(input: []const u8, tag: u8, pos: *usize) SnappyError!usize {
    const short_len = @as(usize, tag >> 2);
    if (short_len < 60) {
        return short_len + 1;
    }
    // 60..63: read 1..4 additional bytes for length
    const extra_bytes: usize = short_len - 59;
    if (pos.* + extra_bytes > input.len) return SnappyError.UnexpectedEof;
    var len: usize = 0;
    for (0..extra_bytes) |i| {
        len |= @as(usize, input[pos.* + i]) << @intCast(i * 8);
    }
    pos.* += extra_bytes;
    return len + 1;
}

/// Copy `length` bytes from output[out_pos - offset] to output[out_pos].
/// Handles overlapping copies (e.g., run-length patterns where offset < length).
fn copyOverlapping(output: []u8, out_pos: usize, offset: usize, length: usize) void {
    const src_start = out_pos - offset;
    // Byte-by-byte copy to handle overlaps correctly
    for (0..length) |i| {
        output[out_pos + i] = output[src_start + i];
    }
}

/// Read a Snappy varint (unsigned, up to 32 bits).
fn readSnappyVarint(data: []const u8, pos: *usize) ?usize {
    var result: usize = 0;
    var shift: u5 = 0;
    while (pos.* < data.len) {
        const byte = data[pos.*];
        pos.* += 1;
        result |= @as(usize, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) return result;
        shift +%= 7;
        if (shift >= 32) return null;
    }
    return null;
}

// ============================================================
// Tests
// ============================================================

test "snappy: literal only" {
    const allocator = std.testing.allocator;
    // Snappy format: varint(5) = [0x05], then literal tag for 5 bytes: (4 << 2 | 0) = 0x10
    // Then 5 bytes: "hello"
    const input = [_]u8{ 0x05, 0x10, 'h', 'e', 'l', 'l', 'o' };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("hello", output);
}

test "snappy: literal + copy" {
    const allocator = std.testing.allocator;
    // Uncompressed: "abcabc" (6 bytes)
    // Snappy:
    //   varint(6) = [0x06]
    //   literal "abc": tag = (2 << 2 | 0) = 0x08, then 'a','b','c'
    //   copy with 2-byte offset: length=3, offset=3
    //     tag = ((3-1) << 2 | 2) = 0x0A, offset LE = [0x03, 0x00]
    const input = [_]u8{ 0x06, 0x08, 'a', 'b', 'c', 0x0A, 0x03, 0x00 };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("abcabc", output);
}

test "snappy: overlapping copy (run-length pattern)" {
    const allocator = std.testing.allocator;
    // Uncompressed: "aaaaaa" (6 bytes)
    // Snappy:
    //   varint(6) = [0x06]
    //   literal "a": tag = (0 << 2 | 0) = 0x00, then 'a'
    //   copy with 2-byte offset: length=5, offset=1
    //     tag = ((5-1) << 2 | 2) = 0x12, offset LE = [0x01, 0x00]
    const input = [_]u8{ 0x06, 0x00, 'a', 0x12, 0x01, 0x00 };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("aaaaaa", output);
}

test "snappy: copy with 1-byte offset" {
    const allocator = std.testing.allocator;
    // Uncompressed: "abcdabcd" (8 bytes)
    // Snappy:
    //   varint(8) = [0x08]
    //   literal "abcd": tag = (3 << 2 | 0) = 0x0C, then 'a','b','c','d'
    //   copy 1-byte offset: length = 4 (encoded as 0 since formula is ((tag>>2)&7)+4)
    //     tag = (0 << 2 | (0 << 5) | 1) = 0x01, offset byte = 0x04
    //   Wait, copy1: length = ((tag >> 2) & 7) + 4, offset = ((tag >> 5) << 8) | next_byte
    //   For length=4: (tag >> 2) & 7 = 0, so bits 2-4 = 0
    //   For offset=4: (tag >> 5) << 8 = 0, next_byte = 4. So tag >> 5 = 0.
    //   tag = (0 << 2) | (0 << 5) | 1 = 0x01
    const input = [_]u8{ 0x08, 0x0C, 'a', 'b', 'c', 'd', 0x01, 0x04 };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("abcdabcd", output);
}

test "snappy: empty input" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 0
    const input = [_]u8{0x00};
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqual(@as(usize, 0), output.len);
}

test "snappy: corrupt — truncated literal" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 5, literal tag for 5 bytes but only 2 bytes follow
    const input = [_]u8{ 0x05, 0x10, 'h', 'e' };
    try std.testing.expectError(SnappyError.UnexpectedEof, decompress(allocator, &input));
}

test "snappy: corrupt — output overflow" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 2, but literal says 5 bytes
    const input = [_]u8{ 0x02, 0x10, 'h', 'e', 'l', 'l', 'o' };
    try std.testing.expectError(SnappyError.OutputOverflow, decompress(allocator, &input));
}

test "snappy: corrupt — copy offset out of range" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 6
    // Literal "ab" (2 bytes): tag = (1 << 2 | 0) = 0x04, then 'a', 'b'
    // Copy with 2-byte offset, length=2, offset=5 (out of range since only 2 bytes written)
    //   tag = ((2-1) << 2 | 2) = 0x06, offset LE = [0x05, 0x00]
    const input = [_]u8{ 0x06, 0x04, 'a', 'b', 0x06, 0x05, 0x00 };
    try std.testing.expectError(SnappyError.OffsetOutOfRange, decompress(allocator, &input));
}

test "snappy: corrupt — offset zero" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 4
    // Literal "ab": tag = 0x04, 'a', 'b'
    // Copy with 2-byte offset, length=2, offset=0 (invalid)
    //   tag = 0x06, offset LE = [0x00, 0x00]
    const input = [_]u8{ 0x04, 0x04, 'a', 'b', 0x06, 0x00, 0x00 };
    try std.testing.expectError(SnappyError.OffsetOutOfRange, decompress(allocator, &input));
}

test "snappy: corrupt — too short for varint" {
    const allocator = std.testing.allocator;
    // Empty input (no varint for uncompressed length)
    const input = [_]u8{};
    try std.testing.expectError(SnappyError.CorruptInput, decompress(allocator, &input));
}

test "snappy: corrupt — uncompressed length mismatch" {
    const allocator = std.testing.allocator;
    // Uncompressed length = 10, but literal only gives 5 bytes
    const input = [_]u8{ 0x0A, 0x10, 'h', 'e', 'l', 'l', 'o' };
    try std.testing.expectError(SnappyError.CorruptInput, decompress(allocator, &input));
}

test "snappy: long literal (60+ tag)" {
    const allocator = std.testing.allocator;
    // Build a literal of 100 bytes using the extended length encoding
    // Tag: short_len = 60, so tag byte = (60 << 2 | 0) = 0xF0
    // Then 1 extra byte for length: 100 - 1 = 99 = 0x63
    var input_buf: [103]u8 = undefined;
    input_buf[0] = 100; // varint: uncompressed length = 100
    input_buf[1] = 0xF0; // literal tag: short_len=60 → 1 extra byte
    input_buf[2] = 0x63; // 99 = length - 1
    for (3..103) |i| {
        input_buf[i] = @intCast(i % 256);
    }

    const output = try decompress(allocator, &input_buf);
    defer allocator.free(output);
    try std.testing.expectEqual(@as(usize, 100), output.len);
    // Verify first and last bytes
    try std.testing.expectEqual(@as(u8, 3 % 256), output[0]);
    try std.testing.expectEqual(@as(u8, 102 % 256), output[99]);
}

test "snappy: copy with 2-byte offset repeated pattern" {
    const allocator = std.testing.allocator;
    // Uncompressed: "ababababab" (10 bytes)
    // Snappy:
    //   varint(10) = [0x0A]
    //   literal "ab": tag = (1 << 2 | 0) = 0x04, then 'a','b'
    //   copy 2-byte offset: length=8, offset=2
    //     tag = ((8-1) << 2 | 2) = 0x1E, offset LE = [0x02, 0x00]
    const input = [_]u8{ 0x0A, 0x04, 'a', 'b', 0x1E, 0x02, 0x00 };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("ababababab", output);
}

test "snappy: single byte literal" {
    const allocator = std.testing.allocator;
    // Uncompressed: "x" (1 byte)
    // varint(1) = [0x01], literal tag for 1 byte: (0 << 2 | 0) = 0x00, then 'x'
    const input = [_]u8{ 0x01, 0x00, 'x' };
    const output = try decompress(allocator, &input);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("x", output);
}
