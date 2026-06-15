//! Nested column value type (Phase 6d-2b): one row of a LIST/MAP/STRUCT
//! parquet column as an owned, typed tree. Struct fields are positional —
//! names live in the column's SchemaNode (Series(Nested).meta). As of Phase
//! 13.1 nested columns also WRITE back to parquet (shredding, nested_shred.zig),
//! requiring the column to carry its SchemaNode (else NestedWriteRequiresSchema).

const std = @import("std");
const strings = @import("strings.zig");
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Uuid = @import("uuid.zig").Uuid;
const Interval = @import("interval.zig").Interval;
const Binary = @import("binary.zig").Binary;
const parquet = @import("parquet");

pub const Nested = union(enum) {
    null_,
    boolean: bool,
    int: i64, // all signed widths normalize here
    uint: u64, // all unsigned widths
    float: f64, // f16/f32/f64 (lossless widening)
    date: Date,
    time: Time,
    timestamp: Timestamp,
    decimal: Decimal,
    uuid: Uuid,
    interval: Interval,
    string: strings.String, // owned
    bytes: Binary, // owned
    list: List, // owned
    strukt: Struct, // owned, positional
    map: Map, // owned

    pub const List = struct { allocator: std.mem.Allocator, items: []Nested };
    pub const Struct = struct { allocator: std.mem.Allocator, fields: []Nested };
    pub const MapEntry = struct { key: Nested, value: Nested };
    pub const Map = struct { allocator: std.mem.Allocator, entries: []MapEntry };

    pub const type_name = "Nested";

    pub const Tag = std.meta.Tag(Nested);

    // -----------------------------------------------------------------------
    // Column metadata — owned parquet schema subtree
    // -----------------------------------------------------------------------

    pub const ColumnMeta = struct {
        /// Owned parquet subtree for this column (names, kinds, leaf types).
        /// null for hand-built columns. `allocator` must accompany `schema`
        /// (both set together) so deinit/clone can free/duplicate the tree.
        schema: ?*parquet.types.SchemaNode = null,
        allocator: ?std.mem.Allocator = null,

        pub fn deinit(self: *ColumnMeta) void {
            if (self.schema) |node| {
                const alloc = self.allocator.?;
                node.deinit(alloc);
                alloc.destroy(node);
                self.schema = null;
            }
        }

        pub fn clone(self: *const ColumnMeta) !ColumnMeta {
            if (self.schema) |node| {
                const alloc = self.allocator.?;
                const new_node = try alloc.create(parquet.types.SchemaNode);
                errdefer alloc.destroy(new_node);
                new_node.* = try cloneNode(alloc, node);
                return .{ .schema = new_node, .allocator = alloc };
            }
            return .{ .schema = null, .allocator = self.allocator };
        }
    };

    /// Deep-copy a SchemaNode tree. Names and children arrays are freshly
    /// allocated so the copy aliases nothing in the original.
    pub fn cloneNode(alloc: std.mem.Allocator, node: *const parquet.types.SchemaNode) !parquet.types.SchemaNode {
        const name_copy = try alloc.dupe(u8, node.name);
        errdefer alloc.free(name_copy);

        var children_copy: []parquet.types.SchemaNode = &.{};
        if (node.children.len > 0) {
            children_copy = try alloc.alloc(parquet.types.SchemaNode, node.children.len);
            var done: usize = 0;
            errdefer {
                for (children_copy[0..done]) |*c| c.deinit(alloc);
                alloc.free(children_copy);
            }
            for (node.children, 0..) |*child, i| {
                children_copy[i] = try cloneNode(alloc, child);
                done = i + 1;
            }
        }

        return .{
            .name = name_copy,
            .repetition = node.repetition,
            .physical = node.physical,
            .converted = node.converted,
            .logical = node.logical,
            .type_length = node.type_length,
            .scale = node.scale,
            .precision = node.precision,
            .max_def = node.max_def,
            .max_rep = node.max_rep,
            .leaf_index = node.leaf_index,
            .children = children_copy,
        };
    }

    // -----------------------------------------------------------------------
    // Capabilities
    // -----------------------------------------------------------------------

    /// appendNull placeholder. The allocator is genuinely unused: a null row of
    /// a nested column IS `.null_`, which is semantically PERFECT for this type
    /// (no separate "empty" sentinel needed — unlike Raw/Binary's empty slice).
    pub fn init(allocator: std.mem.Allocator) !Nested {
        _ = allocator;
        return .null_;
    }

    /// Recursively frees owned arms. Safe on the zero value `.null_` and on
    /// cloned trees (no aliasing across clones).
    pub fn deinit(self: *Nested) void {
        switch (self.*) {
            .string => |*s| s.deinit(),
            .bytes => |*b| b.deinit(),
            .list => |*l| {
                for (l.items) |*item| item.deinit();
                if (l.items.len > 0) l.allocator.free(l.items);
            },
            .strukt => |*st| {
                for (st.fields) |*field| field.deinit();
                if (st.fields.len > 0) st.allocator.free(st.fields);
            },
            .map => |*m| {
                for (m.entries) |*entry| {
                    entry.key.deinit();
                    entry.value.deinit();
                }
                if (m.entries.len > 0) m.allocator.free(m.entries);
            },
            // POD scalar arms and .null_ own nothing.
            else => {},
        }
    }

    /// Deep copy. Container arms carry their own allocator and allocate fresh
    /// element storage; scalar owned arms (string/bytes) clone via their own
    /// methods. The copy aliases nothing in the original.
    pub fn clone(self: *const Nested) !Nested {
        switch (self.*) {
            .string => |*s| return .{ .string = try s.clone() },
            .bytes => |*b| return .{ .bytes = try b.clone() },
            .list => |*l| {
                const items = try l.allocator.alloc(Nested, l.items.len);
                var done: usize = 0;
                errdefer {
                    for (items[0..done]) |*it| it.deinit();
                    if (items.len > 0) l.allocator.free(items);
                }
                for (l.items, 0..) |*item, i| {
                    items[i] = try item.clone();
                    done = i + 1;
                }
                return .{ .list = .{ .allocator = l.allocator, .items = items } };
            },
            .strukt => |*st| {
                const fields = try st.allocator.alloc(Nested, st.fields.len);
                var done: usize = 0;
                errdefer {
                    for (fields[0..done]) |*f| f.deinit();
                    if (fields.len > 0) st.allocator.free(fields);
                }
                for (st.fields, 0..) |*field, i| {
                    fields[i] = try field.clone();
                    done = i + 1;
                }
                return .{ .strukt = .{ .allocator = st.allocator, .fields = fields } };
            },
            .map => |*m| {
                const entries = try m.allocator.alloc(MapEntry, m.entries.len);
                var done: usize = 0;
                errdefer {
                    for (entries[0..done]) |*e| {
                        e.key.deinit();
                        e.value.deinit();
                    }
                    if (entries.len > 0) m.allocator.free(entries);
                }
                for (m.entries, 0..) |*entry, i| {
                    var key = try entry.key.clone();
                    errdefer key.deinit();
                    const value = try entry.value.clone();
                    entries[i] = .{ .key = key, .value = value };
                    done = i + 1;
                }
                return .{ .map = .{ .allocator = m.allocator, .entries = entries } };
            },
            // POD scalar arms and .null_ copy by value.
            else => return self.*,
        }
    }

    /// Deep structural equality: tag must match, then payload element-wise for
    /// containers. Differing tags (incl. one null, one not) are unequal.
    pub fn eql(self: *const Nested, other: *const Nested) bool {
        const ta = std.meta.activeTag(self.*);
        const tb = std.meta.activeTag(other.*);
        if (ta != tb) return false;
        return switch (self.*) {
            .null_ => true,
            .boolean => |v| v == other.boolean,
            .int => |v| v == other.int,
            .uint => |v| v == other.uint,
            .float => |v| v == other.float,
            .date => |v| v.eql(&other.date),
            .time => |v| v.eql(&other.time),
            .timestamp => |v| v.eql(&other.timestamp),
            .decimal => |v| v.eql(&other.decimal),
            .uuid => |v| v.eql(&other.uuid),
            .interval => |v| v.eql(&other.interval),
            .string => |*v| v.eql(&other.string),
            .bytes => |*v| v.eql(&other.bytes),
            .list => |*l| blk: {
                const o = &other.list;
                if (l.items.len != o.items.len) break :blk false;
                for (l.items, o.items) |*a, *b| {
                    if (!a.eql(b)) break :blk false;
                }
                break :blk true;
            },
            .strukt => |*st| blk: {
                const o = &other.strukt;
                if (st.fields.len != o.fields.len) break :blk false;
                for (st.fields, o.fields) |*a, *b| {
                    if (!a.eql(b)) break :blk false;
                }
                break :blk true;
            },
            .map => |*m| blk: {
                const o = &other.map;
                if (m.entries.len != o.entries.len) break :blk false;
                for (m.entries, o.entries) |*a, *b| {
                    if (!a.key.eql(&b.key) or !a.value.eql(&b.value)) break :blk false;
                }
                break :blk true;
            },
        };
    }

    /// Deterministic total STORAGE ordering (tag index, then payload,
    /// lexicographic for containers) — NOT a semantic comparison; exists so
    /// argSort/filters stay total (Interval precedent). Consistent with eql:
    /// `order(a, b) == .eq` iff `a.eql(b)`.
    pub fn order(self: *const Nested, other: *const Nested) std.math.Order {
        const ta = @intFromEnum(std.meta.activeTag(self.*));
        const tb = @intFromEnum(std.meta.activeTag(other.*));
        if (ta != tb) return std.math.order(ta, tb);
        return switch (self.*) {
            .null_ => .eq,
            .boolean => |v| std.math.order(@intFromBool(v), @intFromBool(other.boolean)),
            .int => |v| std.math.order(v, other.int),
            .uint => |v| std.math.order(v, other.uint),
            .float => |v| std.math.order(v, other.float),
            .date => |v| v.order(&other.date),
            .time => |v| v.order(&other.time),
            .timestamp => |v| v.order(&other.timestamp),
            .decimal => |v| v.order(&other.decimal),
            .uuid => |v| v.order(&other.uuid),
            .interval => |v| v.order(&other.interval),
            .string => |*v| std.mem.order(u8, v.toSlice(), other.string.toSlice()),
            .bytes => |*v| std.mem.order(u8, v.toSlice(), other.bytes.toSlice()),
            .list => |*l| orderSlices(l.items, other.list.items),
            .strukt => |*st| orderSlices(st.fields, other.strukt.fields),
            .map => |*m| blk: {
                const o = &other.map;
                const n = @min(m.entries.len, o.entries.len);
                for (m.entries[0..n], o.entries[0..n]) |*a, *b| {
                    const ko = a.key.order(&b.key);
                    if (ko != .eq) break :blk ko;
                    const vo = a.value.order(&b.value);
                    if (vo != .eq) break :blk vo;
                }
                break :blk std.math.order(m.entries.len, o.entries.len);
            },
        };
    }

    /// Lexicographic element-wise ordering, then by length (shorter < longer).
    fn orderSlices(a: []const Nested, b: []const Nested) std.math.Order {
        const n = @min(a.len, b.len);
        for (a[0..n], b[0..n]) |*x, *y| {
            const o = x.order(y);
            if (o != .eq) return o;
        }
        return std.math.order(a.len, b.len);
    }

    /// Deep structural hash — pairs with eql for GroupBy/join contexts.
    /// Hashes the tag, then payload bytes (value bytes for PODs, slices for
    /// string/bytes, recursive for containers).
    pub fn hash(self: *const Nested) u64 {
        var hasher = std.hash.Wyhash.init(0);
        self.hashInto(&hasher);
        return hasher.final();
    }

    fn hashInto(self: *const Nested, hasher: *std.hash.Wyhash) void {
        const tag = std.meta.activeTag(self.*);
        hasher.update(std.mem.asBytes(&@intFromEnum(tag)));
        switch (self.*) {
            .null_ => {},
            .boolean => |v| hasher.update(std.mem.asBytes(&v)),
            .int => |v| hasher.update(std.mem.asBytes(&v)),
            .uint => |v| hasher.update(std.mem.asBytes(&v)),
            .float => |v| {
                const bits: u64 = @bitCast(v);
                hasher.update(std.mem.asBytes(&bits));
            },
            .date => |v| hasher.update(std.mem.asBytes(&v.days)),
            .time => |v| {
                const nanos = v.toNanos();
                hasher.update(std.mem.asBytes(&nanos));
            },
            .timestamp => |v| {
                const nanos = v.toNanos();
                hasher.update(std.mem.asBytes(&nanos));
            },
            .decimal => |v| {
                hasher.update(std.mem.asBytes(&v.unscaled));
                hasher.update(std.mem.asBytes(&v.scale));
            },
            .uuid => |v| hasher.update(&v.bytes),
            .interval => |v| {
                hasher.update(std.mem.asBytes(&v.months));
                hasher.update(std.mem.asBytes(&v.days));
                hasher.update(std.mem.asBytes(&v.millis));
            },
            .string => |*v| hasher.update(v.toSlice()),
            .bytes => |*v| hasher.update(v.toSlice()),
            .list => |*l| {
                for (l.items) |*item| item.hashInto(hasher);
            },
            .strukt => |*st| {
                for (st.fields) |*field| field.hashInto(hasher);
            },
            .map => |*m| {
                for (m.entries) |*entry| {
                    entry.key.hashInto(hasher);
                    entry.value.hashInto(hasher);
                }
            },
        }
    }

    /// JSON-ish rendering: null bare, scalars bare (numbers/bool) or quoted
    /// (dates/timestamps/uuid/decimal/interval/time via their own format),
    /// strings JSON-escaped, bytes as quoted hex, lists `[..]`, structs `{..}`
    /// positional (names not in value — they live in the column schema), maps
    /// `{key: value, ..}`.
    pub fn format(self: Nested, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        switch (self) {
            .null_ => try writer.writeAll("null"),
            .boolean => |v| try writer.print("{}", .{v}),
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .date => |v| try writer.print("\"{f}\"", .{v}),
            .time => |v| try writer.print("\"{f}\"", .{v}),
            .timestamp => |v| try writer.print("\"{f}\"", .{v}),
            .decimal => |v| try writer.print("\"{f}\"", .{v}),
            .uuid => |v| try writer.print("\"{f}\"", .{v}),
            .interval => |v| try writer.print("\"{f}\"", .{v}),
            .string => |v| try writeJsonString(writer, v.toSlice()),
            .bytes => |v| {
                try writer.writeByte('"');
                for (v.toSlice()) |b| try writer.print("{x:0>2}", .{b});
                try writer.writeByte('"');
            },
            .list => |l| {
                try writer.writeByte('[');
                for (l.items, 0..) |*item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try item.format(writer);
                }
                try writer.writeByte(']');
            },
            .strukt => |st| {
                try writer.writeByte('{');
                for (st.fields, 0..) |*field, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try field.format(writer);
                }
                try writer.writeByte('}');
            },
            .map => |m| {
                try writer.writeByte('{');
                for (m.entries, 0..) |*entry, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try entry.key.format(writer);
                    try writer.writeAll(": ");
                    try entry.value.format(writer);
                }
                try writer.writeByte('}');
            },
        }
    }

    /// Quote + escape (\" \\ \n \r \t) — mirrors json_writer's appendJsonString.
    fn writeJsonString(writer: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
        try writer.writeByte('"');
        for (s) |c| {
            switch (c) {
                '"' => try writer.writeAll("\\\""),
                '\\' => try writer.writeAll("\\\\"),
                '\n' => try writer.writeAll("\\n"),
                '\r' => try writer.writeAll("\\r"),
                '\t' => try writer.writeAll("\\t"),
                else => try writer.writeByte(c),
            }
        }
        try writer.writeByte('"');
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    pub fn kind(self: *const Nested) Tag {
        return std.meta.activeTag(self.*);
    }

    pub fn listLen(self: *const Nested) !usize {
        return switch (self.*) {
            .list => |l| l.items.len,
            else => error.NotAList,
        };
    }

    pub fn listAt(self: *const Nested, i: usize) !*const Nested {
        switch (self.*) {
            .list => |*l| {
                if (i >= l.items.len) return error.OutOfRange;
                return &l.items[i];
            },
            else => return error.NotAList,
        }
    }

    pub fn structAt(self: *const Nested, i: usize) !*const Nested {
        switch (self.*) {
            .strukt => |*st| {
                if (i >= st.fields.len) return error.OutOfRange;
                return &st.fields[i];
            },
            else => return error.NotAStruct,
        }
    }

    pub fn mapLen(self: *const Nested) !usize {
        return switch (self.*) {
            .map => |m| m.entries.len,
            else => error.NotAMap,
        };
    }

    pub fn mapAt(self: *const Nested, i: usize) !*const MapEntry {
        switch (self.*) {
            .map => |*m| {
                if (i >= m.entries.len) return error.OutOfRange;
                return &m.entries[i];
            },
            else => return error.NotAMap,
        }
    }
};

const Series = @import("series.zig").Series;
const TimeUnit = parquet.TimeUnit;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build `["x", 42]` as a list (one owned string + one int).
fn buildSampleList(allocator: std.mem.Allocator) !Nested {
    const items = try allocator.alloc(Nested, 2);
    items[0] = .{ .string = try strings.String.fromSlice(allocator, "x") };
    items[1] = .{ .int = 42 };
    return .{ .list = .{ .allocator = allocator, .items = items } };
}

/// Build a 3-level tree: list of structs holding {int, string}.
/// `[{1, "a"}, {2, "b"}]`
fn buildThreeLevel(allocator: std.mem.Allocator) !Nested {
    const outer = try allocator.alloc(Nested, 2);
    var built: usize = 0;
    errdefer {
        for (outer[0..built]) |*o| o.deinit();
        allocator.free(outer);
    }
    {
        const fields = try allocator.alloc(Nested, 2);
        fields[0] = .{ .int = 1 };
        fields[1] = .{ .string = try strings.String.fromSlice(allocator, "a") };
        outer[0] = .{ .strukt = .{ .allocator = allocator, .fields = fields } };
        built = 1;
    }
    {
        const fields = try allocator.alloc(Nested, 2);
        fields[0] = .{ .int = 2 };
        fields[1] = .{ .string = try strings.String.fromSlice(allocator, "b") };
        outer[1] = .{ .strukt = .{ .allocator = allocator, .fields = fields } };
        built = 2;
    }
    return .{ .list = .{ .allocator = allocator, .items = outer } };
}

fn formatToBuf(buf: []u8, value: Nested) ![]u8 {
    return std.fmt.bufPrint(buf, "{f}", .{value});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Nested: deinit frees a 3-level tree (leak check)" {
    const allocator = std.testing.allocator;
    var tree = try buildThreeLevel(allocator);
    tree.deinit();
}

test "Nested: deinit safe on .null_ and scalar arms" {
    var n: Nested = .null_;
    n.deinit();
    var i: Nested = .{ .int = 7 };
    i.deinit();
    var b: Nested = .{ .boolean = true };
    b.deinit();
}

test "Nested: clone is independent (mutate original, copy unaffected)" {
    const allocator = std.testing.allocator;
    var original = try buildThreeLevel(allocator);
    defer original.deinit();

    var copy = try original.clone();
    defer copy.deinit();

    try std.testing.expect(original.eql(&copy));

    // Pointer inequality at every owned level.
    try std.testing.expect(original.list.items.ptr != copy.list.items.ptr);
    const o_struct = &original.list.items[0].strukt;
    const c_struct = &copy.list.items[0].strukt;
    try std.testing.expect(o_struct.fields.ptr != c_struct.fields.ptr);
    try std.testing.expect(o_struct.fields[1].string.toSlice().ptr != c_struct.fields[1].string.toSlice().ptr);

    // Mutate the original's owned string — copy must be unaffected.
    original.list.items[0].strukt.fields[1].string.clear();
    try original.list.items[0].strukt.fields[1].string.appendSlice("ZZZ");
    try std.testing.expectEqualStrings("a", copy.list.items[0].strukt.fields[1].string.toSlice());
    try std.testing.expect(!original.eql(&copy));
}

test "Nested: clone of a map is independent" {
    const allocator = std.testing.allocator;
    const entries = try allocator.alloc(Nested.MapEntry, 1);
    entries[0] = .{
        .key = .{ .string = try strings.String.fromSlice(allocator, "k") },
        .value = .{ .int = 1 },
    };
    var m: Nested = .{ .map = .{ .allocator = allocator, .entries = entries } };
    defer m.deinit();

    var copy = try m.clone();
    defer copy.deinit();
    try std.testing.expect(m.eql(&copy));
    try std.testing.expect(m.map.entries.ptr != copy.map.entries.ptr);
}

test "Nested: eql equal/leaf-diff/length-diff/tag-diff" {
    const allocator = std.testing.allocator;
    var a = try buildThreeLevel(allocator);
    defer a.deinit();
    var b = try buildThreeLevel(allocator);
    defer b.deinit();
    try std.testing.expect(a.eql(&b));

    // Differ in a leaf.
    b.list.items[0].strukt.fields[0] = .{ .int = 999 };
    try std.testing.expect(!a.eql(&b));

    // Differ in length.
    var short_items = try allocator.alloc(Nested, 1);
    short_items[0] = .{ .int = 1 };
    var short: Nested = .{ .list = .{ .allocator = allocator, .items = short_items } };
    defer short.deinit();
    var one_item = try allocator.alloc(Nested, 1);
    one_item[0] = .{ .int = 1 };
    var one: Nested = .{ .list = .{ .allocator = allocator, .items = one_item } };
    defer one.deinit();
    var two_items = try allocator.alloc(Nested, 2);
    two_items[0] = .{ .int = 1 };
    two_items[1] = .{ .int = 2 };
    var two: Nested = .{ .list = .{ .allocator = allocator, .items = two_items } };
    defer two.deinit();
    try std.testing.expect(!one.eql(&two));

    // Differ in tag.
    const x: Nested = .{ .int = 1 };
    const y: Nested = .{ .uint = 1 };
    try std.testing.expect(!x.eql(&y));
    const z: Nested = .null_;
    try std.testing.expect(!x.eql(&z));
}

test "Nested: order is total and consistent with eql" {
    const allocator = std.testing.allocator;

    // list [1] < [1,2] < [2]
    var l1_items = try allocator.alloc(Nested, 1);
    l1_items[0] = .{ .int = 1 };
    var l1: Nested = .{ .list = .{ .allocator = allocator, .items = l1_items } };
    defer l1.deinit();

    var l12_items = try allocator.alloc(Nested, 2);
    l12_items[0] = .{ .int = 1 };
    l12_items[1] = .{ .int = 2 };
    var l12: Nested = .{ .list = .{ .allocator = allocator, .items = l12_items } };
    defer l12.deinit();

    var l2_items = try allocator.alloc(Nested, 1);
    l2_items[0] = .{ .int = 2 };
    var l2: Nested = .{ .list = .{ .allocator = allocator, .items = l2_items } };
    defer l2.deinit();

    try std.testing.expectEqual(std.math.Order.lt, l1.order(&l12));
    try std.testing.expectEqual(std.math.Order.lt, l12.order(&l2));
    try std.testing.expectEqual(std.math.Order.lt, l1.order(&l2));
    try std.testing.expectEqual(std.math.Order.gt, l2.order(&l1));

    // Tag ordering: null_ (tag 0) < boolean (tag 1) < int (tag 2).
    const n0: Nested = .null_;
    const nb: Nested = .{ .boolean = false };
    const ni: Nested = .{ .int = -100 };
    try std.testing.expectEqual(std.math.Order.lt, n0.order(&nb));
    try std.testing.expectEqual(std.math.Order.lt, nb.order(&ni));

    // order == .eq iff eql.
    var a = try buildThreeLevel(allocator);
    defer a.deinit();
    var b = try buildThreeLevel(allocator);
    defer b.deinit();
    try std.testing.expectEqual(std.math.Order.eq, a.order(&b));
    try std.testing.expect(a.eql(&b));
}

test "Nested: hash equal trees match, differing trees differ" {
    const allocator = std.testing.allocator;
    var a = try buildThreeLevel(allocator);
    defer a.deinit();
    var b = try buildThreeLevel(allocator);
    defer b.deinit();
    try std.testing.expectEqual(a.hash(), b.hash());

    b.list.items[1].strukt.fields[0] = .{ .int = -7 };
    try std.testing.expect(a.hash() != b.hash());

    // Scalars differing by tag should (very likely) differ.
    const x: Nested = .{ .int = 1 };
    const y: Nested = .{ .uint = 1 };
    try std.testing.expect(x.hash() != y.hash());
}

test "Nested: format pins exact strings" {
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;

    try std.testing.expectEqualStrings("null", try formatToBuf(&buf, .null_));
    try std.testing.expectEqualStrings("42", try formatToBuf(&buf, .{ .int = 42 }));
    try std.testing.expectEqualStrings("true", try formatToBuf(&buf, .{ .boolean = true }));

    // String with an embedded quote → escaped.
    var s = try strings.String.fromSlice(allocator, "a\"b");
    defer s.deinit();
    try std.testing.expectEqualStrings("\"a\\\"b\"", try formatToBuf(&buf, .{ .string = s }));

    // [1, 2]
    var l_items = try allocator.alloc(Nested, 2);
    l_items[0] = .{ .int = 1 };
    l_items[1] = .{ .int = 2 };
    var l: Nested = .{ .list = .{ .allocator = allocator, .items = l_items } };
    defer l.deinit();
    try std.testing.expectEqualStrings("[1, 2]", try formatToBuf(&buf, l));

    // [] empty list
    var empty: Nested = .{ .list = .{ .allocator = allocator, .items = &.{} } };
    defer empty.deinit();
    try std.testing.expectEqualStrings("[]", try formatToBuf(&buf, empty));

    // struct {1, "y"}
    var st_fields = try allocator.alloc(Nested, 2);
    st_fields[0] = .{ .int = 1 };
    st_fields[1] = .{ .string = try strings.String.fromSlice(allocator, "y") };
    var st: Nested = .{ .strukt = .{ .allocator = allocator, .fields = st_fields } };
    defer st.deinit();
    try std.testing.expectEqualStrings("{1, \"y\"}", try formatToBuf(&buf, st));

    // map {"k": 1}
    var entries = try allocator.alloc(Nested.MapEntry, 1);
    entries[0] = .{ .key = .{ .string = try strings.String.fromSlice(allocator, "k") }, .value = .{ .int = 1 } };
    var m: Nested = .{ .map = .{ .allocator = allocator, .entries = entries } };
    defer m.deinit();
    try std.testing.expectEqualStrings("{\"k\": 1}", try formatToBuf(&buf, m));

    // nested combo: [{1, "a"}, {2, "b"}]
    var combo = try buildThreeLevel(allocator);
    defer combo.deinit();
    try std.testing.expectEqualStrings("[{1, \"a\"}, {2, \"b\"}]", try formatToBuf(&buf, combo));
}

test "Nested: accessors happy and error paths" {
    const allocator = std.testing.allocator;
    var l = try buildSampleList(allocator);
    defer l.deinit();

    try std.testing.expectEqual(Nested.Tag.list, l.kind());
    try std.testing.expectEqual(@as(usize, 2), try l.listLen());
    try std.testing.expectEqual(Nested.Tag.string, (try l.listAt(0)).kind());
    try std.testing.expectError(error.OutOfRange, l.listAt(5));
    try std.testing.expectError(error.NotAStruct, l.structAt(0));
    try std.testing.expectError(error.NotAMap, l.mapLen());

    // struct accessors
    var st_fields = try allocator.alloc(Nested, 1);
    st_fields[0] = .{ .int = 9 };
    var st: Nested = .{ .strukt = .{ .allocator = allocator, .fields = st_fields } };
    defer st.deinit();
    try std.testing.expectEqual(@as(i64, 9), (try st.structAt(0)).int);
    try std.testing.expectError(error.OutOfRange, st.structAt(1));
    try std.testing.expectError(error.NotAList, st.listLen());

    // map accessors
    var entries = try allocator.alloc(Nested.MapEntry, 1);
    entries[0] = .{ .key = .{ .int = 1 }, .value = .{ .int = 2 } };
    var m: Nested = .{ .map = .{ .allocator = allocator, .entries = entries } };
    defer m.deinit();
    try std.testing.expectEqual(@as(usize, 1), try m.mapLen());
    try std.testing.expectEqual(@as(i64, 1), (try m.mapAt(0)).key.int);
    try std.testing.expectError(error.OutOfRange, m.mapAt(2));
}

test "Nested: Series append/appendNull/deepCopy/filterByIndices (leak-checked)" {
    const allocator = std.testing.allocator;
    var s = try Series(Nested).init(allocator);
    defer s.deinit();
    try s.rename("nested");

    try s.append(try buildSampleList(allocator));
    try s.appendNull();
    try s.append(try buildThreeLevel(allocator));

    try std.testing.expectEqual(@as(usize, 3), s.len());
    try std.testing.expect(s.isNull(1));

    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.values.items[0].eql(&s.values.items[0]));
    try std.testing.expect(copy.isNull(1));
    // deep copy: list storage is independent
    try std.testing.expect(copy.values.items[0].list.items.ptr != s.values.items[0].list.items.ptr);

    const filtered = try s.filterByIndices(&.{ 2, 0 });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(usize, 2), filtered.len());
    try std.testing.expect(filtered.values.items[1].eql(&s.values.items[0]));
}

test "Nested: Series argSort smoke" {
    const allocator = std.testing.allocator;
    var s = try Series(Nested).init(allocator);
    defer s.deinit();
    try s.append(.{ .int = 3 });
    try s.append(.{ .int = 1 });
    try s.append(.{ .int = 2 });
    var idx = try s.argSort(allocator, true);
    defer idx.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), idx.items[0]);
    try std.testing.expectEqual(@as(usize, 2), idx.items[1]);
    try std.testing.expectEqual(@as(usize, 0), idx.items[2]);
}

test "Nested: Series asStringAt equals format output" {
    const allocator = std.testing.allocator;
    var s = try Series(Nested).init(allocator);
    defer s.deinit();
    try s.append(try buildSampleList(allocator));
    try s.appendNull();

    var str0 = try s.asStringAt(0);
    defer str0.deinit();
    try std.testing.expectEqualStrings("[\"x\", 42]", str0.toSlice());

    var str1 = try s.asStringAt(1);
    defer str1.deinit();
    try std.testing.expectEqualStrings("null", str1.toSlice());
}

test "Nested: ColumnMeta clone deep-copies SchemaNode tree" {
    const allocator = std.testing.allocator;

    // Build a small SchemaNode tree: root group with one leaf child.
    const child = try allocator.alloc(parquet.types.SchemaNode, 1);
    child[0] = .{
        .name = try allocator.dupe(u8, "elem"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 1,
        .max_rep = 0,
        .children = &.{},
    };
    const root = try allocator.create(parquet.types.SchemaNode);
    root.* = .{
        .name = try allocator.dupe(u8, "mylist"),
        .repetition = .optional,
        .children = child,
    };

    var s = try Series(Nested).init(allocator);
    defer s.deinit();
    s.meta = .{ .schema = root, .allocator = allocator };

    const copy = try s.deepCopy();
    defer copy.deinit();

    // Copy must have its own tree.
    try std.testing.expect(copy.meta.schema != null);
    try std.testing.expect(copy.meta.schema.? != s.meta.schema.?);
    try std.testing.expect(copy.meta.schema.?.children.ptr != s.meta.schema.?.children.ptr);
    try std.testing.expectEqualStrings("mylist", copy.meta.schema.?.name);
    try std.testing.expectEqualStrings("elem", copy.meta.schema.?.children[0].name);

    // Mutate the original's name byte — copy unaffected.
    s.meta.schema.?.name[0] = 'X';
    try std.testing.expectEqualStrings("mylist", copy.meta.schema.?.name);
}
