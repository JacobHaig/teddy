//! Native TDF (Teddy DataFrame) serialization format — Phase 8 (slices .0+.1).
//!
//! A 1:1, uncompressed, lossless on-disk mirror of teddy's in-memory
//! dataframe. Little-endian throughout. Implements EVERY BoxedSeries variant
//! including `.nested` (tag 26): recursive value encoding + the column's
//! SchemaNode tree as type_meta.
//!
//! ON-DISK LAYOUT (all multi-byte integers little-endian):
//!
//!   HEADER
//!     magic          : [8]u8  = "TEDDYDF1"
//!     format_version : u16    = 1
//!     flags          : u16    = 0 (reserved)
//!     num_columns    : u32
//!     num_rows       : u64
//!   SCHEMA (num_columns entries, in column order)
//!     name_len       : u32 ; name bytes
//!     type_tag       : u8  (see tagFor / tag table below)
//!     type_meta      : variant-specific (FixedBytes / Raw only)
//!     has_validity   : u8  (0|1)
//!   DATA (num_columns blocks, in column order)
//!     [if has_validity] validity bitmap: ceil(num_rows/8) bytes,
//!                       bit i set = PRESENT (= !isNull(i)); LSB-first per byte
//!     values         : num_rows entries (placeholders at null slots), per-type
//!
//! TYPE TAG TABLE (u8):
//!   0 bool, 1 i8, 2 i16, 3 i32, 4 i64, 5 i128, 6 isize, 7 u8, 8 u16,
//!   9 u32, 10 u64, 11 u128, 12 usize, 13 f32, 14 f64, 15 f16, 16 String,
//!   17 Date, 18 Time, 19 Timestamp, 20 Decimal, 21 Binary, 22 FixedBytes,
//!   23 Uuid, 24 Interval, 25 Raw, 26 Nested.
//!
//! PER-TYPE VALUE BYTE LAYOUT (one entry per row):
//!   bool                : 1 byte (0|1)
//!   i8/u8               : 1 byte
//!   i16/u16             : 2 bytes LE
//!   i32/u32/f32         : 4 bytes LE (floats: @bitCast to uN then writeInt)
//!   i64/u64/f64/isize/usize : 8 bytes LE  (isize/usize fixed at 8 bytes on wire)
//!   i128/u128           : 16 bytes LE
//!   f16                 : 2 bytes LE (@bitCast u16)
//!   Date                : i32 days (4 bytes LE)
//!   Time                : i64 value (8) + u8 unit + u8 utc        = 10 bytes
//!   Timestamp           : i64 value (8) + u8 unit + u8 utc + u8 origin = 11 bytes
//!   Decimal             : i256 unscaled (32 LE) + u8 precision + i8 scale = 34 bytes
//!   Uuid                : 16 raw bytes
//!   Interval            : u32 months + u32 days + u32 millis (12 bytes LE)
//!   String/Binary/Raw/FixedBytes : u32 len + len bytes (per row)
//!
//! TYPE_META (schema header, only these tags carry it):
//!   FixedBytes(22): i32 width  (-1 when meta.width is null)
//!   Raw(25): u8 physical(@intFromEnum)
//!            u8 has_converted + u8 converted(@intFromEnum, 0 if absent)
//!            LogicalType: u8 present(0|1); if present u8 tag + params
//!            i32 type_length (-1 when null)
//!   Nested(26): u8 schema_present(0|1); if present, the column's SchemaNode
//!            tree (recursive). Per node:
//!              name           : u32 len + bytes
//!              repetition     : u8 (@intFromEnum FieldRepetitionType)
//!              physical       : u8 present(0|1) + u8 (@intFromEnum, 0 if absent)
//!              converted      : u8 present(0|1) + u8 (@intFromEnum, 0 if absent)
//!              logical        : writeLogicalType (u8 present + tag + params)
//!              type_length    : u8 present(0|1) + i32
//!              scale          : u8 present(0|1) + i32
//!              precision      : u8 present(0|1) + i32
//!              max_def        : u8
//!              max_rep        : u8
//!              leaf_index     : u8 present(0|1) + u64
//!              children       : u32 count + each child recursively
//!            NOTE: optional i32 fields (type_length/scale/precision) use an
//!            explicit present-flag (NOT a -1 sentinel) since scale/precision
//!            can legitimately be negative; applied uniformly to all three.
//!
//! NESTED VALUE LAYOUT (tag 26, one entry per row, recursive):
//!   u8 tag = @intFromEnum(std.meta.activeTag(value)); then payload by tag:
//!     null_     : nothing
//!     boolean   : u8
//!     int       : i64
//!     uint      : u64
//!     float     : f64 (@bitCast u64)
//!     date      : i32 days
//!     time      : i64 value + u8 unit + u8 utc
//!     timestamp : i64 value + u8 unit + u8 utc + u8 origin
//!     decimal   : i256 unscaled (32 LE) + u8 precision + i8 scale
//!     uuid      : 16 raw bytes
//!     interval  : u32 months + u32 days + u32 millis
//!     string    : u32 len + bytes
//!     bytes     : u32 len + bytes
//!     list      : u32 count + each element (recurse)
//!     strukt    : u32 count + each field (recurse)
//!     map       : u32 count + each entry {key (recurse), value (recurse)}
//!   LogicalType encoding (1-byte tag = @intFromEnum(LogicalType) + params):
//!     decimal(5)   : i32 scale + i32 precision
//!     time(7)      : u8 is_adjusted_to_utc + u8 unit
//!     timestamp(8) : u8 is_adjusted_to_utc + u8 unit
//!     integer(10)  : i8 bit_width (1 byte) + u8 is_signed
//!     all others   : no params

const std = @import("std");
const Allocator = std.mem.Allocator;

const Dataframe = @import("dataframe.zig").Dataframe;
const Series = @import("series.zig").Series;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const String = @import("strings.zig").String;
const Raw = @import("raw.zig").Raw;
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Binary = @import("binary.zig").Binary;
const FixedBytes = @import("fixed_bytes.zig").FixedBytes;
const Uuid = @import("uuid.zig").Uuid;
const Interval = @import("interval.zig").Interval;
const Nested = @import("nested.zig").Nested;

const parquet = @import("parquet");
const PhysicalType = parquet.PhysicalType;
const ConvertedType = parquet.ConvertedType;
const LogicalType = parquet.LogicalType;
const TimeUnit = parquet.TimeUnit;
const SchemaNode = parquet.types.SchemaNode;
const FieldRepetitionType = parquet.types.FieldRepetitionType;

/// Max recursion depth for nested encode/decode. Bounds untrusted input on the
/// read path (deeply-nested adversarial buffers) and guards the write path.
const MAX_NESTING_DEPTH: u32 = 64;

const MAGIC = "TEDDYDF1";
const FORMAT_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// Type tags (stable, append-only)
// ---------------------------------------------------------------------------

const Tag = struct {
    const bool_: u8 = 0;
    const i8_: u8 = 1;
    const i16_: u8 = 2;
    const i32_: u8 = 3;
    const i64_: u8 = 4;
    const i128_: u8 = 5;
    const isize_: u8 = 6;
    const u8_: u8 = 7;
    const u16_: u8 = 8;
    const u32_: u8 = 9;
    const u64_: u8 = 10;
    const u128_: u8 = 11;
    const usize_: u8 = 12;
    const f32_: u8 = 13;
    const f64_: u8 = 14;
    const f16_: u8 = 15;
    const string_: u8 = 16;
    const date_: u8 = 17;
    const time_: u8 = 18;
    const timestamp_: u8 = 19;
    const decimal_: u8 = 20;
    const binary_: u8 = 21;
    const fixed_bytes_: u8 = 22;
    const uuid_: u8 = 23;
    const interval_: u8 = 24;
    const raw_: u8 = 25;
    const nested_: u8 = 26;
};

/// The wire tag for a boxed series's active variant.
fn tagFor(boxed: *const BoxedSeries) u8 {
    return switch (boxed.*) {
        .bool => Tag.bool_,
        .int8 => Tag.i8_,
        .int16 => Tag.i16_,
        .int32 => Tag.i32_,
        .int64 => Tag.i64_,
        .int128 => Tag.i128_,
        .isize => Tag.isize_,
        .uint8 => Tag.u8_,
        .uint16 => Tag.u16_,
        .uint32 => Tag.u32_,
        .uint64 => Tag.u64_,
        .uint128 => Tag.u128_,
        .usize => Tag.usize_,
        .float32 => Tag.f32_,
        .float64 => Tag.f64_,
        .float16 => Tag.f16_,
        .string => Tag.string_,
        .date => Tag.date_,
        .time => Tag.time_,
        .timestamp => Tag.timestamp_,
        .decimal => Tag.decimal_,
        .binary => Tag.binary_,
        .fixed_bytes => Tag.fixed_bytes_,
        .uuid => Tag.uuid_,
        .interval => Tag.interval_,
        .raw => Tag.raw_,
        .nested => Tag.nested_,
    };
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

const Buf = std.ArrayList(u8);

fn putInt(buf: *Buf, a: Allocator, comptime T: type, value: T) !void {
    var tmp: [@divExact(@typeInfo(T).int.bits, 8)]u8 = undefined;
    std.mem.writeInt(T, &tmp, value, .little);
    try buf.appendSlice(a, &tmp);
}

fn putFloat(buf: *Buf, a: Allocator, comptime F: type, comptime U: type, value: F) !void {
    try putInt(buf, a, U, @as(U, @bitCast(value)));
}

/// len-prefixed (u32) byte payload.
fn putBytes(buf: *Buf, a: Allocator, bytes: []const u8) !void {
    const n = std.math.cast(u32, bytes.len) orelse return error.TdfPayloadTooLarge;
    try putInt(buf, a, u32, n);
    try buf.appendSlice(a, bytes);
}

/// Serialize a dataframe to a freshly allocated TDF byte buffer. Caller owns
/// the returned slice. Nested columns serialize their SchemaNode meta and
/// recursive per-row values; deeply nested values past MAX_NESTING_DEPTH
/// return error.NestingTooDeep.
pub fn writeToString(allocator: Allocator, df: *Dataframe) ![]u8 {
    var buf = Buf.empty;
    errdefer buf.deinit(allocator);

    const num_columns = std.math.cast(u32, df.width()) orelse return error.TdfTooManyColumns;
    const num_rows_usize = df.height();
    const num_rows = std.math.cast(u64, num_rows_usize) orelse return error.TdfTooManyRows;

    // HEADER
    try buf.appendSlice(allocator, MAGIC);
    try putInt(&buf, allocator, u16, FORMAT_VERSION);
    try putInt(&buf, allocator, u16, 0); // flags
    try putInt(&buf, allocator, u32, num_columns);
    try putInt(&buf, allocator, u64, num_rows);

    // SCHEMA
    for (df.series.items) |*boxed| {
        const tag = tagFor(boxed);
        const name = boxed.name();
        try putBytes(&buf, allocator, name);
        try putInt(&buf, allocator, u8, tag);
        try writeTypeMeta(&buf, allocator, boxed);
        const has_validity: u8 = if (hasValidity(boxed)) 1 else 0;
        try putInt(&buf, allocator, u8, has_validity);
    }

    // DATA
    for (df.series.items) |*boxed| {
        try writeColumnData(&buf, allocator, boxed, num_rows_usize);
    }

    return buf.toOwnedSlice(allocator);
}

fn hasValidity(boxed: *const BoxedSeries) bool {
    return switch (boxed.*) {
        inline else => |s| s.validity != null,
    };
}

fn writeTypeMeta(buf: *Buf, a: Allocator, boxed: *const BoxedSeries) !void {
    switch (boxed.*) {
        .fixed_bytes => |s| {
            const w: i32 = s.meta.width orelse -1;
            try putInt(buf, a, i32, w);
        },
        .raw => |s| {
            const m = s.meta;
            try putInt(buf, a, u8, @intFromEnum(m.physical_type));
            if (m.converted_type) |ct| {
                try putInt(buf, a, u8, 1);
                try putInt(buf, a, u8, @intFromEnum(ct));
            } else {
                try putInt(buf, a, u8, 0);
                try putInt(buf, a, u8, 0);
            }
            try writeLogicalType(buf, a, m.logical_type);
            const tl: i32 = m.type_length orelse -1;
            try putInt(buf, a, i32, tl);
        },
        .nested => |s| {
            if (s.meta.schema) |node| {
                try putInt(buf, a, u8, 1);
                try writeSchemaNode(buf, a, node);
            } else {
                try putInt(buf, a, u8, 0);
            }
        },
        else => {},
    }
}

/// present-flag (u8) + i32 payload. Used for SchemaNode's optional i32 fields
/// (type_length/scale/precision), which can legitimately be negative.
fn putOptI32(buf: *Buf, a: Allocator, opt: ?i32) !void {
    if (opt) |v| {
        try putInt(buf, a, u8, 1);
        try putInt(buf, a, i32, v);
    } else {
        try putInt(buf, a, u8, 0);
        try putInt(buf, a, i32, 0);
    }
}

/// Serialize one SchemaNode (recursive). Layout documented in the module header.
fn writeSchemaNode(buf: *Buf, a: Allocator, node: *const SchemaNode) !void {
    try putBytes(buf, a, node.name);
    try putInt(buf, a, u8, @intFromEnum(node.repetition));

    if (node.physical) |p| {
        try putInt(buf, a, u8, 1);
        try putInt(buf, a, u8, @intFromEnum(p));
    } else {
        try putInt(buf, a, u8, 0);
        try putInt(buf, a, u8, 0);
    }

    if (node.converted) |c| {
        try putInt(buf, a, u8, 1);
        try putInt(buf, a, u8, @intFromEnum(c));
    } else {
        try putInt(buf, a, u8, 0);
        try putInt(buf, a, u8, 0);
    }

    try writeLogicalType(buf, a, node.logical);
    try putOptI32(buf, a, node.type_length);
    try putOptI32(buf, a, node.scale);
    try putOptI32(buf, a, node.precision);

    try putInt(buf, a, u8, node.max_def);
    try putInt(buf, a, u8, node.max_rep);

    if (node.leaf_index) |li| {
        try putInt(buf, a, u8, 1);
        const v = std.math.cast(u64, li) orelse return error.TdfPayloadTooLarge;
        try putInt(buf, a, u64, v);
    } else {
        try putInt(buf, a, u8, 0);
        try putInt(buf, a, u64, 0);
    }

    const child_count = std.math.cast(u32, node.children.len) orelse return error.TdfPayloadTooLarge;
    try putInt(buf, a, u32, child_count);
    for (node.children) |*child| try writeSchemaNode(buf, a, child);
}

fn writeLogicalType(buf: *Buf, a: Allocator, opt: ?LogicalType) !void {
    if (opt == null) {
        try putInt(buf, a, u8, 0);
        return;
    }
    try putInt(buf, a, u8, 1);
    const lt = opt.?;
    try putInt(buf, a, u8, @intFromEnum(lt));
    switch (lt) {
        .decimal => |p| {
            try putInt(buf, a, i32, p.scale);
            try putInt(buf, a, i32, p.precision);
        },
        .time => |p| {
            try putInt(buf, a, u8, @intFromBool(p.is_adjusted_to_utc));
            try putInt(buf, a, u8, @intFromEnum(p.unit));
        },
        .timestamp => |p| {
            try putInt(buf, a, u8, @intFromBool(p.is_adjusted_to_utc));
            try putInt(buf, a, u8, @intFromEnum(p.unit));
        },
        .integer => |p| {
            try putInt(buf, a, i8, p.bit_width);
            try putInt(buf, a, u8, @intFromBool(p.is_signed));
        },
        else => {},
    }
}

fn writeValidityBitmap(buf: *Buf, a: Allocator, boxed: *BoxedSeries, num_rows: usize) !void {
    const nbytes = (num_rows + 7) / 8;
    var byte_idx: usize = 0;
    while (byte_idx < nbytes) : (byte_idx += 1) {
        var b: u8 = 0;
        var bit: u3 = 0;
        while (true) {
            const row = byte_idx * 8 + bit;
            if (row < num_rows and !boxed.isNull(row)) {
                b |= (@as(u8, 1) << bit);
            }
            if (bit == 7) break;
            bit += 1;
        }
        try buf.append(a, b);
    }
}

fn writeColumnData(buf: *Buf, a: Allocator, boxed: *BoxedSeries, num_rows: usize) !void {
    if (hasValidity(boxed)) try writeValidityBitmap(buf, a, boxed, num_rows);

    switch (boxed.*) {
        .bool => |s| for (s.values.items) |v| try buf.append(a, @intFromBool(v)),
        .int8 => |s| for (s.values.items) |v| try putInt(buf, a, i8, v),
        .int16 => |s| for (s.values.items) |v| try putInt(buf, a, i16, v),
        .int32 => |s| for (s.values.items) |v| try putInt(buf, a, i32, v),
        .int64 => |s| for (s.values.items) |v| try putInt(buf, a, i64, v),
        .int128 => |s| for (s.values.items) |v| try putInt(buf, a, i128, v),
        .isize => |s| for (s.values.items) |v| try putInt(buf, a, i64, @intCast(v)),
        .uint8 => |s| for (s.values.items) |v| try putInt(buf, a, u8, v),
        .uint16 => |s| for (s.values.items) |v| try putInt(buf, a, u16, v),
        .uint32 => |s| for (s.values.items) |v| try putInt(buf, a, u32, v),
        .uint64 => |s| for (s.values.items) |v| try putInt(buf, a, u64, v),
        .uint128 => |s| for (s.values.items) |v| try putInt(buf, a, u128, v),
        .usize => |s| for (s.values.items) |v| try putInt(buf, a, u64, @intCast(v)),
        .float32 => |s| for (s.values.items) |v| try putFloat(buf, a, f32, u32, v),
        .float64 => |s| for (s.values.items) |v| try putFloat(buf, a, f64, u64, v),
        .float16 => |s| for (s.values.items) |v| try putFloat(buf, a, f16, u16, v),
        .string => |s| for (s.values.items) |*v| try putBytes(buf, a, v.toSlice()),
        .date => |s| for (s.values.items) |v| try putInt(buf, a, i32, v.days),
        .time => |s| for (s.values.items) |v| {
            try putInt(buf, a, i64, v.value);
            try putInt(buf, a, u8, @intFromEnum(v.unit));
            try putInt(buf, a, u8, @intFromBool(v.utc));
        },
        .timestamp => |s| for (s.values.items) |v| {
            try putInt(buf, a, i64, v.value);
            try putInt(buf, a, u8, @intFromEnum(v.unit));
            try putInt(buf, a, u8, @intFromBool(v.utc));
            try putInt(buf, a, u8, @intFromEnum(v.origin));
        },
        .decimal => |s| for (s.values.items) |v| {
            try putInt(buf, a, i256, v.unscaled);
            try putInt(buf, a, u8, v.precision);
            try putInt(buf, a, i8, v.scale);
        },
        .uuid => |s| for (s.values.items) |v| try buf.appendSlice(a, &v.bytes),
        .interval => |s| for (s.values.items) |v| {
            try putInt(buf, a, u32, v.months);
            try putInt(buf, a, u32, v.days);
            try putInt(buf, a, u32, v.millis);
        },
        .binary => |s| for (s.values.items) |*v| try putBytes(buf, a, v.toSlice()),
        .fixed_bytes => |s| for (s.values.items) |*v| try putBytes(buf, a, v.toSlice()),
        .raw => |s| for (s.values.items) |*v| try putBytes(buf, a, v.toSlice()),
        .nested => |s| for (s.values.items) |*v| try writeNested(buf, a, v, 0),
    }
}

/// Recursively serialize one Nested value. Layout documented in the module
/// header. Scalar leaves reuse the EXACT byte forms pinned by slice .0.
fn writeNested(buf: *Buf, a: Allocator, value: *const Nested, depth: u32) !void {
    if (depth >= MAX_NESTING_DEPTH) return error.NestingTooDeep;
    const tag = std.meta.activeTag(value.*);
    try putInt(buf, a, u8, @intFromEnum(tag));
    switch (value.*) {
        .null_ => {},
        .boolean => |v| try putInt(buf, a, u8, @intFromBool(v)),
        .int => |v| try putInt(buf, a, i64, v),
        .uint => |v| try putInt(buf, a, u64, v),
        .float => |v| try putFloat(buf, a, f64, u64, v),
        .date => |v| try putInt(buf, a, i32, v.days),
        .time => |v| {
            try putInt(buf, a, i64, v.value);
            try putInt(buf, a, u8, @intFromEnum(v.unit));
            try putInt(buf, a, u8, @intFromBool(v.utc));
        },
        .timestamp => |v| {
            try putInt(buf, a, i64, v.value);
            try putInt(buf, a, u8, @intFromEnum(v.unit));
            try putInt(buf, a, u8, @intFromBool(v.utc));
            try putInt(buf, a, u8, @intFromEnum(v.origin));
        },
        .decimal => |v| {
            try putInt(buf, a, i256, v.unscaled);
            try putInt(buf, a, u8, v.precision);
            try putInt(buf, a, i8, v.scale);
        },
        .uuid => |v| try buf.appendSlice(a, &v.bytes),
        .interval => |v| {
            try putInt(buf, a, u32, v.months);
            try putInt(buf, a, u32, v.days);
            try putInt(buf, a, u32, v.millis);
        },
        .string => |*v| try putBytes(buf, a, v.toSlice()),
        .bytes => |*v| try putBytes(buf, a, v.toSlice()),
        .list => |*l| {
            const n = std.math.cast(u32, l.items.len) orelse return error.TdfPayloadTooLarge;
            try putInt(buf, a, u32, n);
            for (l.items) |*item| try writeNested(buf, a, item, depth + 1);
        },
        .strukt => |*st| {
            const n = std.math.cast(u32, st.fields.len) orelse return error.TdfPayloadTooLarge;
            try putInt(buf, a, u32, n);
            for (st.fields) |*field| try writeNested(buf, a, field, depth + 1);
        },
        .map => |*m| {
            const n = std.math.cast(u32, m.entries.len) orelse return error.TdfPayloadTooLarge;
            try putInt(buf, a, u32, n);
            for (m.entries) |*entry| {
                try writeNested(buf, a, &entry.key, depth + 1);
                try writeNested(buf, a, &entry.value, depth + 1);
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

const Cursor = struct {
    bytes: []const u8,
    pos: usize = 0,

    fn need(self: *Cursor, n: usize) ![]const u8 {
        if (self.pos + n > self.bytes.len) return error.CorruptTdf;
        const out = self.bytes[self.pos .. self.pos + n];
        self.pos += n;
        return out;
    }

    fn readInt(self: *Cursor, comptime T: type) !T {
        const n = @divExact(@typeInfo(T).int.bits, 8);
        const slice = try self.need(n);
        return std.mem.readInt(T, slice[0..n], .little);
    }

    fn readFloat(self: *Cursor, comptime F: type, comptime U: type) !F {
        const bits = try self.readInt(U);
        return @as(F, @bitCast(bits));
    }

    fn readByte(self: *Cursor) !u8 {
        const slice = try self.need(1);
        return slice[0];
    }

    /// u32 length-prefixed payload; returns a borrowed slice into the buffer.
    fn readBytes(self: *Cursor) ![]const u8 {
        const len = try self.readInt(u32);
        return self.need(len);
    }
};

fn validEnum(comptime E: type, raw: anytype) !E {
    return std.enums.fromInt(E, raw) orelse error.CorruptTdf;
}

/// Parse a TDF byte buffer into a freshly allocated Dataframe. Caller owns the
/// returned pointer and must call deinit. Hardened against truncated/corrupt
/// input: every read is bounds-checked, lengths are checked-cast, enums are
/// validated. Returns error.InvalidTdf / error.UnsupportedFormatVersion /
/// error.CorruptTdf / error.NestingTooDeep as appropriate.
pub fn parse(allocator: Allocator, bytes: []const u8) !*Dataframe {
    var cur = Cursor{ .bytes = bytes };

    // HEADER
    const magic = try cur.need(8);
    if (!std.mem.eql(u8, magic, MAGIC)) return error.InvalidTdf;
    const version = try cur.readInt(u16);
    if (version != FORMAT_VERSION) return error.UnsupportedFormatVersion;
    _ = try cur.readInt(u16); // flags (reserved)
    const num_columns = try cur.readInt(u32);
    const num_rows_u64 = try cur.readInt(u64);
    const num_rows = std.math.cast(usize, num_rows_u64) orelse return error.CorruptTdf;

    const df = try Dataframe.init(allocator);
    errdefer df.deinit();

    // SCHEMA: read all entries up front so DATA can be decoded in order.
    // Name slices borrow into `bytes` (valid for the call duration).
    const ColSchema = struct {
        name: []const u8,
        tag: u8,
        has_validity: bool = false,
        fb_width: i32 = -1,
        raw_meta: Raw.ColumnMeta = .{},
        /// Allocated nested schema tree (Nested columns only); ownership moves
        /// to the series in the DATA pass, after which this is set null.
        nested_schema: ?*SchemaNode = null,
    };
    const schemas = try allocator.alloc(ColSchema, num_columns);
    defer allocator.free(schemas);

    // Any nested schema tree allocated in the schema pass but not yet handed to
    // a series must be freed if a later read fails (it would otherwise leak —
    // df.deinit can't reach it). After the DATA pass consumes it, the slot is
    // nulled so this loop skips it.
    var built_schemas: usize = 0;
    errdefer {
        for (schemas[0..built_schemas]) |*sc| {
            if (sc.nested_schema) |node| {
                node.deinit(allocator);
                allocator.destroy(node);
            }
        }
    }

    for (schemas) |*sc| {
        const name = try cur.readBytes();
        const tag = try cur.readByte();
        if (tag > Tag.nested_) return error.CorruptTdf;
        sc.* = .{ .name = name, .tag = tag };
        switch (tag) {
            Tag.fixed_bytes_ => sc.fb_width = try cur.readInt(i32),
            Tag.raw_ => sc.raw_meta = try readRawMeta(&cur),
            Tag.nested_ => sc.nested_schema = try readNestedMeta(allocator, &cur),
            else => {},
        }
        sc.has_validity = (try cur.readByte()) != 0;
        built_schemas += 1;
    }

    // DATA: build one Series per schema entry, in order. createSeries hands the
    // series to df immediately, so df.deinit (via errdefer) frees any partially
    // built column if a later read fails.
    for (schemas) |*sc| {
        switch (sc.tag) {
            Tag.bool_ => try readScalarColumn(allocator, df, bool, &cur, sc.name, sc.has_validity, num_rows),
            Tag.i8_ => try readScalarColumn(allocator, df, i8, &cur, sc.name, sc.has_validity, num_rows),
            Tag.i16_ => try readScalarColumn(allocator, df, i16, &cur, sc.name, sc.has_validity, num_rows),
            Tag.i32_ => try readScalarColumn(allocator, df, i32, &cur, sc.name, sc.has_validity, num_rows),
            Tag.i64_ => try readScalarColumn(allocator, df, i64, &cur, sc.name, sc.has_validity, num_rows),
            Tag.i128_ => try readScalarColumn(allocator, df, i128, &cur, sc.name, sc.has_validity, num_rows),
            Tag.isize_ => try readScalarColumn(allocator, df, isize, &cur, sc.name, sc.has_validity, num_rows),
            Tag.u8_ => try readScalarColumn(allocator, df, u8, &cur, sc.name, sc.has_validity, num_rows),
            Tag.u16_ => try readScalarColumn(allocator, df, u16, &cur, sc.name, sc.has_validity, num_rows),
            Tag.u32_ => try readScalarColumn(allocator, df, u32, &cur, sc.name, sc.has_validity, num_rows),
            Tag.u64_ => try readScalarColumn(allocator, df, u64, &cur, sc.name, sc.has_validity, num_rows),
            Tag.u128_ => try readScalarColumn(allocator, df, u128, &cur, sc.name, sc.has_validity, num_rows),
            Tag.usize_ => try readScalarColumn(allocator, df, usize, &cur, sc.name, sc.has_validity, num_rows),
            Tag.f32_ => try readScalarColumn(allocator, df, f32, &cur, sc.name, sc.has_validity, num_rows),
            Tag.f64_ => try readScalarColumn(allocator, df, f64, &cur, sc.name, sc.has_validity, num_rows),
            Tag.f16_ => try readScalarColumn(allocator, df, f16, &cur, sc.name, sc.has_validity, num_rows),
            Tag.string_ => try readScalarColumn(allocator, df, String, &cur, sc.name, sc.has_validity, num_rows),
            Tag.date_ => try readScalarColumn(allocator, df, Date, &cur, sc.name, sc.has_validity, num_rows),
            Tag.time_ => try readScalarColumn(allocator, df, Time, &cur, sc.name, sc.has_validity, num_rows),
            Tag.timestamp_ => try readScalarColumn(allocator, df, Timestamp, &cur, sc.name, sc.has_validity, num_rows),
            Tag.decimal_ => try readScalarColumn(allocator, df, Decimal, &cur, sc.name, sc.has_validity, num_rows),
            Tag.binary_ => try readScalarColumn(allocator, df, Binary, &cur, sc.name, sc.has_validity, num_rows),
            Tag.uuid_ => try readScalarColumn(allocator, df, Uuid, &cur, sc.name, sc.has_validity, num_rows),
            Tag.interval_ => try readScalarColumn(allocator, df, Interval, &cur, sc.name, sc.has_validity, num_rows),
            Tag.fixed_bytes_ => {
                const s = try readScalarColumnReturning(allocator, df, FixedBytes, &cur, sc.name, sc.has_validity, num_rows);
                s.meta.width = if (sc.fb_width < 0) null else sc.fb_width;
            },
            Tag.raw_ => {
                const s = try readScalarColumnReturning(allocator, df, Raw, &cur, sc.name, sc.has_validity, num_rows);
                s.meta = sc.raw_meta;
            },
            Tag.nested_ => {
                const s = try readNestedColumn(allocator, df, &cur, sc.name, sc.has_validity, num_rows);
                // Transfer ownership of the schema tree to the series; null the
                // schema-pass slot so the errdefer above no longer frees it.
                if (sc.nested_schema) |node| {
                    s.meta = .{ .schema = node, .allocator = allocator };
                    sc.nested_schema = null;
                }
            },
            else => return error.CorruptTdf,
        }
    }

    if (cur.pos != cur.bytes.len) return error.CorruptTdf;
    return df;
}

/// Read one decoded value of element type T from the cursor.
fn readValue(comptime T: type, allocator: Allocator, cur: *Cursor) !T {
    return switch (T) {
        bool => (try cur.readByte()) != 0,
        i8, i16, i32, i64, i128 => try cur.readInt(T),
        u8, u16, u32, u64, u128 => try cur.readInt(T),
        // isize/usize travel as fixed 8-byte ints on the wire.
        isize => std.math.cast(isize, try cur.readInt(i64)) orelse error.CorruptTdf,
        usize => std.math.cast(usize, try cur.readInt(u64)) orelse error.CorruptTdf,
        f32 => try cur.readFloat(f32, u32),
        f64 => try cur.readFloat(f64, u64),
        f16 => try cur.readFloat(f16, u16),
        String => try String.fromSlice(allocator, try cur.readBytes()),
        Binary => try Binary.fromSlice(allocator, try cur.readBytes()),
        FixedBytes => try FixedBytes.fromSlice(allocator, try cur.readBytes()),
        Raw => try Raw.fromSlice(allocator, try cur.readBytes()),
        Date => Date{ .days = try cur.readInt(i32) },
        Time => blk: {
            const value = try cur.readInt(i64);
            const unit = try validEnum(TimeUnit, try cur.readByte());
            const utc = (try cur.readByte()) != 0;
            break :blk Time{ .value = value, .unit = unit, .utc = utc };
        },
        Timestamp => blk: {
            const value = try cur.readInt(i64);
            const unit = try validEnum(TimeUnit, try cur.readByte());
            const utc = (try cur.readByte()) != 0;
            const origin = try validEnum(Timestamp.Origin, try cur.readByte());
            break :blk Timestamp{ .value = value, .unit = unit, .utc = utc, .origin = origin };
        },
        Decimal => blk: {
            const unscaled = try cur.readInt(i256);
            const precision = try cur.readByte();
            const scale = try cur.readInt(i8);
            break :blk Decimal{ .unscaled = unscaled, .precision = precision, .scale = scale };
        },
        Uuid => blk: {
            const slice = try cur.need(16);
            var u: Uuid = undefined;
            @memcpy(&u.bytes, slice[0..16]);
            break :blk u;
        },
        Interval => blk: {
            const months = try cur.readInt(u32);
            const days = try cur.readInt(u32);
            const millis = try cur.readInt(u32);
            break :blk Interval{ .months = months, .days = days, .millis = millis };
        },
        else => @compileError("readValue: unsupported type " ++ @typeName(T)),
    };
}

/// Decode a full column of element type T: optional validity bitmap then
/// num_rows values. Returns the series pointer (still owned by df).
fn readScalarColumnReturning(
    allocator: Allocator,
    df: *Dataframe,
    comptime T: type,
    cur: *Cursor,
    name: []const u8,
    has_validity: bool,
    num_rows: usize,
) !*Series(T) {
    const s = try df.createSeries(T);
    try s.rename(name);

    // Validity first (PRESENT bits), so we can construct the bool ArrayList.
    var validity: ?std.ArrayList(bool) = null;
    errdefer if (validity) |*v| v.deinit(allocator);
    if (has_validity) {
        const nbytes = (num_rows + 7) / 8;
        const bitmap = try cur.need(nbytes);
        var v = try std.ArrayList(bool).initCapacity(allocator, num_rows);
        var i: usize = 0;
        while (i < num_rows) : (i += 1) {
            const byte = bitmap[i / 8];
            const bit: u3 = @intCast(i % 8);
            const present = (byte & (@as(u8, 1) << bit)) != 0;
            try v.append(allocator, present);
        }
        validity = v;
    }

    // Values (always num_rows entries, including placeholders at null slots).
    const hasDeinit = comptime @import("series.zig").hasMethod(T, "deinit");
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        var value = try readValue(T, allocator, cur);
        // If the append fails, the freshly decoded owning value would leak
        // (it isn't in s.values yet, so df.deinit can't reach it).
        errdefer if (comptime hasDeinit) value.deinit();
        try s.values.append(allocator, value);
    }

    // Assign the decoded validity directly (kept exact, matches in-memory shape).
    s.validity = validity;
    validity = null;
    return s;
}

fn readScalarColumn(
    allocator: Allocator,
    df: *Dataframe,
    comptime T: type,
    cur: *Cursor,
    name: []const u8,
    has_validity: bool,
    num_rows: usize,
) !void {
    _ = try readScalarColumnReturning(allocator, df, T, cur, name, has_validity, num_rows);
}

fn readRawMeta(cur: *Cursor) !Raw.ColumnMeta {
    var m = Raw.ColumnMeta{};
    const phys_raw = try cur.readByte();
    m.physical_type = try validEnum(PhysicalType, phys_raw);
    const has_converted = try cur.readByte();
    const converted_raw = try cur.readByte();
    if (has_converted != 0) {
        m.converted_type = try validEnum(ConvertedType, converted_raw);
    }
    m.logical_type = try readLogicalType(cur);
    const tl = try cur.readInt(i32);
    m.type_length = if (tl < 0) null else tl;
    return m;
}

fn readLogicalType(cur: *Cursor) !?LogicalType {
    const present = try cur.readByte();
    if (present == 0) return null;
    const tag = try cur.readByte();
    const LtTag = @typeInfo(LogicalType).@"union".tag_type.?;
    const lt_tag = try validEnum(LtTag, tag);
    return switch (lt_tag) {
        .decimal => LogicalType{ .decimal = .{
            .scale = try cur.readInt(i32),
            .precision = try cur.readInt(i32),
        } },
        .time => LogicalType{ .time = .{
            .is_adjusted_to_utc = (try cur.readByte()) != 0,
            .unit = try validEnum(TimeUnit, try cur.readByte()),
        } },
        .timestamp => LogicalType{ .timestamp = .{
            .is_adjusted_to_utc = (try cur.readByte()) != 0,
            .unit = try validEnum(TimeUnit, try cur.readByte()),
        } },
        .integer => LogicalType{ .integer = .{
            .bit_width = try cur.readInt(i8),
            .is_signed = (try cur.readByte()) != 0,
        } },
        // Parameterless variants: reconstruct directly from the tag.
        inline else => |t| @unionInit(LogicalType, @tagName(t), {}),
    };
}

// ---------------------------------------------------------------------------
// Nested reader
// ---------------------------------------------------------------------------

/// present-flag (u8) + i32 → optional i32 (pairs with putOptI32).
fn readOptI32(cur: *Cursor) !?i32 {
    const present = try cur.readByte();
    const value = try cur.readInt(i32);
    return if (present != 0) value else null;
}

/// Read the Nested type_meta: a present flag then (if present) a SchemaNode
/// tree allocated from `allocator`. Caller owns the returned node (or null).
fn readNestedMeta(allocator: Allocator, cur: *Cursor) !?*SchemaNode {
    const present = try cur.readByte();
    if (present == 0) return null;
    const node = try allocator.create(SchemaNode);
    errdefer allocator.destroy(node);
    node.* = try readSchemaNode(allocator, cur, 0);
    return node;
}

/// Recursively decode one SchemaNode (layout in the module header). On a
/// mid-decode failure, frees any already-decoded name/children.
fn readSchemaNode(allocator: Allocator, cur: *Cursor, depth: u32) (error{ CorruptTdf, NestingTooDeep, OutOfMemory })!SchemaNode {
    if (depth >= MAX_NESTING_DEPTH) return error.NestingTooDeep;

    const name = try allocator.dupe(u8, try cur.readBytes());
    errdefer allocator.free(name);

    const repetition = try validEnum(FieldRepetitionType, try cur.readByte());

    const has_physical = try cur.readByte();
    const physical_raw = try cur.readByte();
    const physical: ?PhysicalType = if (has_physical != 0) try validEnum(PhysicalType, physical_raw) else null;

    const has_converted = try cur.readByte();
    const converted_raw = try cur.readByte();
    const converted: ?ConvertedType = if (has_converted != 0) try validEnum(ConvertedType, converted_raw) else null;

    const logical = try readLogicalType(cur);
    const type_length = try readOptI32(cur);
    const scale = try readOptI32(cur);
    const precision = try readOptI32(cur);

    const max_def = try cur.readByte();
    const max_rep = try cur.readByte();

    const has_leaf_index = try cur.readByte();
    const leaf_index_raw = try cur.readInt(u64);
    const leaf_index: ?usize = if (has_leaf_index != 0)
        (std.math.cast(usize, leaf_index_raw) orelse return error.CorruptTdf)
    else
        null;

    const child_count = try cur.readInt(u32);
    var children: []SchemaNode = &.{};
    if (child_count > 0) {
        const n = std.math.cast(usize, child_count) orelse return error.CorruptTdf;
        children = try allocator.alloc(SchemaNode, n);
        var done: usize = 0;
        errdefer {
            for (children[0..done]) |*c| c.deinit(allocator);
            allocator.free(children);
        }
        var i: usize = 0;
        while (i < n) : (i += 1) {
            children[i] = try readSchemaNode(allocator, cur, depth + 1);
            done = i + 1;
        }
    }

    return .{
        .name = name,
        .repetition = repetition,
        .physical = physical,
        .converted = converted,
        .logical = logical,
        .type_length = type_length,
        .scale = scale,
        .precision = precision,
        .max_def = max_def,
        .max_rep = max_rep,
        .leaf_index = leaf_index,
        .children = children,
    };
}

/// Decode a Nested column: optional validity bitmap then num_rows recursive
/// Nested values. Returns the series pointer (still owned by df).
fn readNestedColumn(
    allocator: Allocator,
    df: *Dataframe,
    cur: *Cursor,
    name: []const u8,
    has_validity: bool,
    num_rows: usize,
) !*Series(Nested) {
    const s = try df.createSeries(Nested);
    try s.rename(name);

    var validity: ?std.ArrayList(bool) = null;
    errdefer if (validity) |*v| v.deinit(allocator);
    if (has_validity) {
        const nbytes = (num_rows + 7) / 8;
        const bitmap = try cur.need(nbytes);
        var v = try std.ArrayList(bool).initCapacity(allocator, num_rows);
        var i: usize = 0;
        while (i < num_rows) : (i += 1) {
            const byte = bitmap[i / 8];
            const bit: u3 = @intCast(i % 8);
            const present = (byte & (@as(u8, 1) << bit)) != 0;
            try v.append(allocator, present);
        }
        validity = v;
    }

    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        var value = try decodeNested(allocator, cur, 0);
        // Not yet in s.values, so df.deinit can't reach it — free on append fail.
        errdefer value.deinit();
        try s.values.append(allocator, value);
    }

    s.validity = validity;
    validity = null;
    return s;
}

/// Recursively decode one Nested value. Tag is validated; containers allocate
/// their children slice and recurse. On a mid-decode container failure, every
/// already-decoded child is freed (mirrors Nested.clone's errdefer discipline).
fn decodeNested(allocator: Allocator, cur: *Cursor, depth: u32) (error{ CorruptTdf, NestingTooDeep, OutOfMemory })!Nested {
    if (depth >= MAX_NESTING_DEPTH) return error.NestingTooDeep;

    const tag = try validEnum(Nested.Tag, try cur.readByte());
    switch (tag) {
        .null_ => return .null_,
        .boolean => return .{ .boolean = (try cur.readByte()) != 0 },
        .int => return .{ .int = try cur.readInt(i64) },
        .uint => return .{ .uint = try cur.readInt(u64) },
        .float => return .{ .float = try cur.readFloat(f64, u64) },
        .date => return .{ .date = .{ .days = try cur.readInt(i32) } },
        .time => {
            const value = try cur.readInt(i64);
            const unit = try validEnum(TimeUnit, try cur.readByte());
            const utc = (try cur.readByte()) != 0;
            return .{ .time = .{ .value = value, .unit = unit, .utc = utc } };
        },
        .timestamp => {
            const value = try cur.readInt(i64);
            const unit = try validEnum(TimeUnit, try cur.readByte());
            const utc = (try cur.readByte()) != 0;
            const origin = try validEnum(Timestamp.Origin, try cur.readByte());
            return .{ .timestamp = .{ .value = value, .unit = unit, .utc = utc, .origin = origin } };
        },
        .decimal => {
            const unscaled = try cur.readInt(i256);
            const precision = try cur.readByte();
            const scale = try cur.readInt(i8);
            return .{ .decimal = .{ .unscaled = unscaled, .precision = precision, .scale = scale } };
        },
        .uuid => {
            const slice = try cur.need(16);
            var u: Uuid = undefined;
            @memcpy(&u.bytes, slice[0..16]);
            return .{ .uuid = u };
        },
        .interval => {
            const months = try cur.readInt(u32);
            const days = try cur.readInt(u32);
            const millis = try cur.readInt(u32);
            return .{ .interval = .{ .months = months, .days = days, .millis = millis } };
        },
        .string => return .{ .string = try String.fromSlice(allocator, try cur.readBytes()) },
        .bytes => return .{ .bytes = try Binary.fromSlice(allocator, try cur.readBytes()) },
        .list => {
            const count = std.math.cast(usize, try cur.readInt(u32)) orelse return error.CorruptTdf;
            const items = try allocator.alloc(Nested, count);
            var done: usize = 0;
            errdefer {
                for (items[0..done]) |*it| it.deinit();
                if (items.len > 0) allocator.free(items);
            }
            var i: usize = 0;
            while (i < count) : (i += 1) {
                items[i] = try decodeNested(allocator, cur, depth + 1);
                done = i + 1;
            }
            return .{ .list = .{ .allocator = allocator, .items = items } };
        },
        .strukt => {
            const count = std.math.cast(usize, try cur.readInt(u32)) orelse return error.CorruptTdf;
            const fields = try allocator.alloc(Nested, count);
            var done: usize = 0;
            errdefer {
                for (fields[0..done]) |*f| f.deinit();
                if (fields.len > 0) allocator.free(fields);
            }
            var i: usize = 0;
            while (i < count) : (i += 1) {
                fields[i] = try decodeNested(allocator, cur, depth + 1);
                done = i + 1;
            }
            return .{ .strukt = .{ .allocator = allocator, .fields = fields } };
        },
        .map => {
            const count = std.math.cast(usize, try cur.readInt(u32)) orelse return error.CorruptTdf;
            const entries = try allocator.alloc(Nested.MapEntry, count);
            var done: usize = 0;
            errdefer {
                for (entries[0..done]) |*e| {
                    e.key.deinit();
                    e.value.deinit();
                }
                if (entries.len > 0) allocator.free(entries);
            }
            var i: usize = 0;
            while (i < count) : (i += 1) {
                var key = try decodeNested(allocator, cur, depth + 1);
                errdefer key.deinit();
                const value = try decodeNested(allocator, cur, depth + 1);
                entries[i] = .{ .key = key, .value = value };
                done = i + 1;
            }
            return .{ .map = .{ .allocator = allocator, .entries = entries } };
        },
    }
}
