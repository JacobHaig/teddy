//! Dremel record shredding (Phase 13.1): the inverse of nested_assembly. Walks
//! each row's Nested value tree against the column's SchemaNode and emits, per
//! leaf, a `(rep, def, maybe-value)` sequence — exactly what the reader reads
//! back. Shredding is the precise mirror of `nested_assembly.assembleNode`'s
//! shape dispatch (leaf / list3 / list2 / map / strukt), inverted.
//!
//! Level semantics (identical to the reader's contract):
//!   For a leaf with ancestors A1..An (root excluded):
//!   - max_def = count of ancestors-or-self that are optional OR repeated.
//!   - max_rep = count of repeated ancestors-or-self.
//!   - A leaf entry has a value iff def == max_def.
//!   - rep of the FIRST entry of a record is 0; deeper list continuations carry
//!     the repeated-ancestor depth at which the element continues.
//!   - A null/absent ancestor or empty list emits ONE entry per descendant leaf
//!     recording the def at which definition stopped, no value, at the
//!     appropriate rep (the incoming rep for the slot).

const std = @import("std");
const Allocator = std.mem.Allocator;
const parquet = @import("parquet");
const Nested = @import("nested.zig").Nested;
const Decimal = @import("decimal.zig").Decimal;

const SchemaNode = parquet.types.SchemaNode;
const PhysicalType = parquet.types.PhysicalType;

pub const ShredError = error{
    NestedShredMismatch,
    DecimalTooWide,
    TimestampOverflow,
    UnexpectedPhysicalType,
} || Allocator.Error;

const max_depth = 64;

// ---------------------------------------------------------------------------
// Leaf value payload (allocator-backed). byte_arrays own their slices.
// ---------------------------------------------------------------------------

pub const LeafValues = union(enum) {
    int32s: std.ArrayList(i32),
    int64s: std.ArrayList(i64),
    floats: std.ArrayList(f32),
    doubles: std.ArrayList(f64),
    booleans: std.ArrayList(bool),
    byte_arrays: std.ArrayList([]const u8),

    fn initForPhysical(physical: PhysicalType) LeafValues {
        return switch (physical) {
            .int32 => .{ .int32s = .empty },
            .int64 => .{ .int64s = .empty },
            .float => .{ .floats = .empty },
            .double => .{ .doubles = .empty },
            .boolean => .{ .booleans = .empty },
            .byte_array, .fixed_len_byte_array, .int96 => .{ .byte_arrays = .empty },
        };
    }

    fn deinit(self: *LeafValues, allocator: Allocator) void {
        switch (self.*) {
            .int32s => |*v| v.deinit(allocator),
            .int64s => |*v| v.deinit(allocator),
            .floats => |*v| v.deinit(allocator),
            .doubles => |*v| v.deinit(allocator),
            .booleans => |*v| v.deinit(allocator),
            .byte_arrays => |*v| {
                for (v.items) |b| allocator.free(b);
                v.deinit(allocator);
            },
        }
    }
};

/// One leaf's def/rep/value streams. `node` is borrowed (the leaf's schema).
/// `deinit` frees def/rep/values (and any owned byte slices).
pub const LeafStreams = struct {
    node: *const SchemaNode,
    def: std.ArrayList(u16),
    rep: std.ArrayList(u16),
    values: LeafValues,

    pub fn deinit(self: *LeafStreams, allocator: Allocator) void {
        self.def.deinit(allocator);
        self.rep.deinit(allocator);
        self.values.deinit(allocator);
    }
};

// ---------------------------------------------------------------------------
// Shape detection — identical to nested_assembly.shapeOf.
// ---------------------------------------------------------------------------

const Shape = enum { leaf, list3, list2, map, strukt };

fn shapeOf(node: *const SchemaNode) Shape {
    if (node.isLeaf()) {
        if (node.repetition == .repeated) return .list2;
        return .leaf;
    }
    if (node.children.len == 1 and node.children[0].repetition == .repeated) {
        const mid = &node.children[0];
        if (mid.children.len == 2) return .map;
        if (mid.children.len == 1) return .list3;
        return .list2;
    }
    if (node.repetition == .repeated) return .list2;
    return .strukt;
}

/// The def level at which `node` ITSELF becomes defined. Mirrors
/// nested_assembly.nodeDefinedDef.
fn nodeDefinedDef(node: *const SchemaNode, parent_def: u16) u16 {
    return switch (node.repetition) {
        .required => parent_def,
        .optional, .repeated => parent_def + 1,
    };
}

// ---------------------------------------------------------------------------
// Shred state: the leaf streams + pointer-identity lookup, mirroring the
// reader's positional leaf order (pre-order leaf collection).
// ---------------------------------------------------------------------------

const Shredder = struct {
    allocator: Allocator,
    leaves: []LeafStreams,

    /// The stream bound to a specific leaf SchemaNode (pointer identity, O(n)).
    fn streamForNode(self: *Shredder, leaf: *const SchemaNode) *LeafStreams {
        for (self.leaves) |*l| {
            if (l.node == leaf) return l;
        }
        unreachable; // every leaf is bound during setup
    }

    /// Emit an "absent" entry (no value) into EVERY leaf of `node`'s subtree at
    /// `def`/`rep`. Used when an optional ancestor is null or a list is
    /// empty/null: each descendant leaf records exactly ONE entry.
    fn emitAbsentSubtree(self: *Shredder, node: *const SchemaNode, def: u16, rep: u16) ShredError!void {
        if (node.isLeaf()) {
            const l = self.streamForNode(node);
            try l.def.append(self.allocator, def);
            try l.rep.append(self.allocator, rep);
            return;
        }
        for (node.children) |*c| try self.emitAbsentSubtree(c, def, rep);
    }
};

/// Append `node`'s leaves to `out` in pre-order (mirrors the reader's
/// leaf_index assignment + assembly's collectSubtreeLeaves).
fn collectLeaves(allocator: Allocator, node: *const SchemaNode, out: *std.ArrayList(*const SchemaNode)) Allocator.Error!void {
    if (node.isLeaf()) {
        try out.append(allocator, node);
        return;
    }
    for (node.children) |*c| try collectLeaves(allocator, c, out);
}

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Shred one top-level nested column into per-leaf (def, rep, value) streams.
/// `node` is the column's top-level SchemaNode (group or repeated leaf).
/// `values` holds `num_rows` Nested values (a null row is `.null_` OR marked by
/// `validity[r] == false`). Returns owned LeafStreams (one per leaf, pre-order);
/// caller frees each via deinit and frees the slice.
pub fn shredColumn(
    allocator: Allocator,
    node: *const SchemaNode,
    values: []const Nested,
    validity: ?[]const bool,
    num_rows: usize,
) ShredError![]LeafStreams {
    var ordered = std.ArrayList(*const SchemaNode).empty;
    defer ordered.deinit(allocator);
    try collectLeaves(allocator, node, &ordered);

    const leaves = try allocator.alloc(LeafStreams, ordered.items.len);
    var inited: usize = 0;
    errdefer {
        for (leaves[0..inited]) |*l| l.deinit(allocator);
        allocator.free(leaves);
    }
    for (ordered.items, 0..) |leaf_node, i| {
        leaves[i] = .{
            .node = leaf_node,
            .def = .empty,
            .rep = .empty,
            .values = LeafValues.initForPhysical(leaf_node.physical orelse return error.UnexpectedPhysicalType),
        };
        inited = i + 1;
    }

    var shredder = Shredder{ .allocator = allocator, .leaves = leaves };

    for (0..num_rows) |r| {
        const is_null = if (validity) |v| !v[r] else false;
        if (is_null or values[r] == .null_) {
            // Null at the column root: every leaf records one absent entry at
            // def 0, rep 0 (a new record that is entirely undefined).
            try shredder.emitAbsentSubtree(node, 0, 0);
            continue;
        }
        try shredNode(&shredder, node, &values[r], 0, 0, 0);
    }

    return leaves;
}

// ---------------------------------------------------------------------------
// Recursive per-row shredding — inverse of nested_assembly.assembleNode.
// ---------------------------------------------------------------------------

/// Shred `value` against `node`. `def_floor` is the def level contributed by
/// ancestors (this node's parent's def); `rep` is the rep level the FIRST leaf
/// slot emitted by this subtree carries (0 at record start, or the enclosing
/// list's depth for a continuation element).
fn shredNode(
    s: *Shredder,
    node: *const SchemaNode,
    value: *const Nested,
    def_floor: u16,
    rep: u16,
    depth: usize,
) ShredError!void {
    if (depth >= max_depth) return error.NestedShredMismatch;
    switch (shapeOf(node)) {
        .leaf => return shredLeaf(s, node, value, def_floor, rep),
        .strukt => return shredStruct(s, node, value, def_floor, rep, depth),
        .list3, .list2 => return shredList(s, node, value, def_floor, rep, depth),
        .map => return shredMap(s, node, value, def_floor, rep, depth),
    }
}

fn shredLeaf(
    s: *Shredder,
    node: *const SchemaNode,
    value: *const Nested,
    def_floor: u16,
    rep: u16,
) ShredError!void {
    const l = s.streamForNode(node);
    if (value.* == .null_) {
        // Optional leaf, null at its own level: def stops at the floor.
        if (node.repetition != .optional) return error.NestedShredMismatch;
        try l.def.append(s.allocator, def_floor);
        try l.rep.append(s.allocator, rep);
        return;
    }
    // Present value: def == max_def, value materialized.
    try l.def.append(s.allocator, node.max_def);
    try l.rep.append(s.allocator, rep);
    try nestedScalarToLeaf(s.allocator, node, value, &l.values);
}

fn shredStruct(
    s: *Shredder,
    node: *const SchemaNode,
    value: *const Nested,
    def_floor: u16,
    rep: u16,
    depth: usize,
) ShredError!void {
    const own_def = nodeDefinedDef(node, def_floor);

    // Optional struct that is absent: one absent entry per descendant leaf at
    // the floor def (the struct itself is undefined).
    if (value.* == .null_) {
        if (node.repetition != .optional) return error.NestedShredMismatch;
        return s.emitAbsentSubtree(node, def_floor, rep);
    }
    if (value.* != .strukt) return error.NestedShredMismatch;
    const st = value.strukt;
    if (st.fields.len != node.children.len) return error.NestedShredMismatch;

    // Each field is shredded independently; each child's first emitted slot
    // carries the incoming rep (a struct does not repeat). own_def is the def
    // floor the children build on.
    for (node.children, 0..) |*child, i| {
        try shredNode(s, child, &st.fields[i], own_def, rep, depth + 1);
    }
}

/// Resolve the LIST's repeated middle, the element node, and the relevant def
/// levels. Mirrors nested_assembly.resolveList.
const ListParts = struct {
    element: *const SchemaNode,
    /// def at which the LIST itself is present (non-null, possibly empty).
    list_present_def: u16,
    /// def floor passed to each element's recursion.
    element_floor_def: u16,
    /// rep level a continuation element (2nd+) carries.
    list_rep: u16,
};

fn resolveList(node: *const SchemaNode, def_floor: u16) ListParts {
    if (node.repetition == .repeated) {
        // 2-level legacy: node itself is the repeated element.
        return .{
            .element = node,
            .list_present_def = def_floor,
            .element_floor_def = def_floor + 1,
            .list_rep = @intCast(node.max_rep),
        };
    }
    // 3-level: node group (optional adds a def level) -> repeated middle -> elem.
    const list_def = nodeDefinedDef(node, def_floor);
    const mid = &node.children[0];
    return .{
        .element = &mid.children[0],
        .list_present_def = list_def,
        .element_floor_def = list_def + 1,
        .list_rep = @intCast(mid.max_rep),
    };
}

fn shredList(
    s: *Shredder,
    node: *const SchemaNode,
    value: *const Nested,
    def_floor: u16,
    rep: u16,
    depth: usize,
) ShredError!void {
    const p = resolveList(node, def_floor);

    // Null list: optional group not present at this row.
    if (value.* == .null_) {
        if (node.repetition != .optional) return error.NestedShredMismatch;
        return s.emitAbsentSubtree(node, def_floor, rep);
    }
    if (value.* != .list) return error.NestedShredMismatch;
    const items = value.list.items;

    // Empty list: list present but no elements. One absent entry per descendant
    // leaf at list_present_def.
    if (items.len == 0) {
        return s.emitAbsentSubtree(node, p.list_present_def, rep);
    }

    // Non-empty: the FIRST element carries the incoming rep; subsequent elements
    // carry this list's own repetition depth (list_rep).
    for (items, 0..) |*item, i| {
        const elem_rep: u16 = if (i == 0) rep else p.list_rep;
        try shredNode(s, p.element, item, p.element_floor_def, elem_rep, depth + 1);
    }
}

fn shredMap(
    s: *Shredder,
    node: *const SchemaNode,
    value: *const Nested,
    def_floor: u16,
    rep: u16,
    depth: usize,
) ShredError!void {
    // MAP: node (group) -> repeated key_value -> {key, value}. Same level
    // structure as a 3-level list whose element is the key_value struct.
    const list_def = nodeDefinedDef(node, def_floor);
    const mid = &node.children[0]; // repeated key_value
    const key_node = &mid.children[0];
    const value_node = &mid.children[1];
    const element_floor_def = list_def + 1;
    const map_rep: u16 = @intCast(mid.max_rep);

    if (value.* == .null_) {
        if (node.repetition != .optional) return error.NestedShredMismatch;
        return s.emitAbsentSubtree(node, def_floor, rep);
    }
    if (value.* != .map) return error.NestedShredMismatch;
    const entries = value.map.entries;

    if (entries.len == 0) {
        return s.emitAbsentSubtree(node, list_def, rep);
    }

    for (entries, 0..) |*entry, i| {
        const entry_rep: u16 = if (i == 0) rep else map_rep;
        // key (required) + value: both descend from the key_value element. The
        // first leaf-slot of THIS entry carries entry_rep; within the entry the
        // value leaf also begins at entry_rep (the key_value does not repeat
        // between its key and value — they are one element).
        try shredNode(s, key_node, &entry.key, element_floor_def, entry_rep, depth + 1);
        try shredNode(s, value_node, &entry.value, element_floor_def, entry_rep, depth + 1);
    }
}

// ---------------------------------------------------------------------------
// Leaf value mapping — inverse of nested_assembly.leafValueToNested. Maps a
// Nested scalar to the leaf's physical encoding using node.physical +
// annotations. Owned byte slices are duped into the leaf's value list.
// ---------------------------------------------------------------------------

fn nestedScalarToLeaf(
    allocator: Allocator,
    node: *const SchemaNode,
    value: *const Nested,
    out: *LeafValues,
) ShredError!void {
    const physical = node.physical orelse return error.UnexpectedPhysicalType;
    switch (value.*) {
        .boolean => |b| {
            if (out.* != .booleans) return error.NestedShredMismatch;
            try out.booleans.append(allocator, b);
        },
        .int => |v| try appendInt(allocator, physical, out, v),
        .uint => |v| try appendInt(allocator, physical, out, @bitCast(v)),
        .float => |v| switch (physical) {
            .float => try appendFloat32(allocator, out, @floatCast(v)),
            .double => try appendFloat64(allocator, out, v),
            // FLOAT16 is FLBA(2): re-encode the half bits.
            .fixed_len_byte_array => {
                const h: f16 = @floatCast(v);
                const bytes = try allocator.alloc(u8, 2);
                errdefer allocator.free(bytes);
                std.mem.writeInt(u16, bytes[0..2], @bitCast(h), .little);
                try appendBytes(allocator, out, bytes);
            },
            else => return error.NestedShredMismatch,
        },
        .string => |*sv| {
            const dup = try allocator.dupe(u8, sv.toSlice());
            errdefer allocator.free(dup);
            try appendBytes(allocator, out, dup);
        },
        .bytes => |*bv| {
            const dup = try allocator.dupe(u8, bv.toSlice());
            errdefer allocator.free(dup);
            try appendBytes(allocator, out, dup);
        },
        .date => |d| try appendInt(allocator, physical, out, @as(i64, d.days)),
        .time => |t| {
            // TIME(MILLIS) is INT32-backed; micros/nanos are INT64.
            const per: i128 = switch (t.unit) {
                .millis => 1_000_000,
                .micros => 1_000,
                .nanos => 1,
            };
            const v = std.math.cast(i64, @divTrunc(t.toNanos(), per)) orelse return error.TimestampOverflow;
            try appendInt(allocator, physical, out, v);
        },
        .timestamp => |ts| {
            if (physical == .int96) {
                const bytes = try allocator.alloc(u8, 12);
                errdefer allocator.free(bytes);
                const enc = ts.toInt96Bytes() catch return error.TimestampOverflow;
                @memcpy(bytes, &enc);
                try appendBytes(allocator, out, bytes);
            } else {
                const per: i128 = switch (ts.unit) {
                    .millis => 1_000_000,
                    .micros => 1_000,
                    .nanos => 1,
                };
                const v = std.math.cast(i64, @divTrunc(ts.toNanos(), per)) orelse return error.TimestampOverflow;
                try appendInt(allocator, physical, out, v);
            }
        },
        .decimal => |dec| switch (physical) {
            .int32 => try appendInt(allocator, physical, out, std.math.cast(i64, dec.unscaled) orelse return error.DecimalTooWide),
            .int64 => try appendInt(allocator, physical, out, std.math.cast(i64, dec.unscaled) orelse return error.DecimalTooWide),
            .fixed_len_byte_array, .byte_array => {
                const width: usize = if (node.type_length) |tl| @intCast(tl) else Decimal.minBytesForPrecision(dec.precision);
                const bytes = try allocator.alloc(u8, width);
                errdefer allocator.free(bytes);
                Decimal.toBeBytes(dec.unscaled, bytes) catch return error.DecimalTooWide;
                try appendBytes(allocator, out, bytes);
            },
            else => return error.NestedShredMismatch,
        },
        .uuid => |u| {
            const bytes = try allocator.alloc(u8, 16);
            errdefer allocator.free(bytes);
            @memcpy(bytes, &u.bytes);
            try appendBytes(allocator, out, bytes);
        },
        .interval => |iv| {
            const bytes = try allocator.alloc(u8, 12);
            errdefer allocator.free(bytes);
            const enc = iv.toLeBytes();
            @memcpy(bytes, &enc);
            try appendBytes(allocator, out, bytes);
        },
        .null_, .list, .strukt, .map => return error.NestedShredMismatch,
    }
}

fn appendInt(allocator: Allocator, physical: PhysicalType, out: *LeafValues, v: i64) ShredError!void {
    switch (physical) {
        .int32 => {
            if (out.* != .int32s) return error.NestedShredMismatch;
            try out.int32s.append(allocator, std.math.cast(i32, v) orelse return error.NestedShredMismatch);
        },
        .int64 => {
            if (out.* != .int64s) return error.NestedShredMismatch;
            try out.int64s.append(allocator, v);
        },
        else => return error.NestedShredMismatch,
    }
}

fn appendFloat32(allocator: Allocator, out: *LeafValues, v: f32) ShredError!void {
    if (out.* != .floats) return error.NestedShredMismatch;
    try out.floats.append(allocator, v);
}

fn appendFloat64(allocator: Allocator, out: *LeafValues, v: f64) ShredError!void {
    if (out.* != .doubles) return error.NestedShredMismatch;
    try out.doubles.append(allocator, v);
}

fn appendBytes(allocator: Allocator, out: *LeafValues, bytes: []const u8) ShredError!void {
    if (out.* != .byte_arrays) {
        allocator.free(bytes);
        return error.NestedShredMismatch;
    }
    try out.byte_arrays.append(allocator, bytes);
}

// ===========================================================================
// Unit tests — the assembly pins, inverted. SchemaNodes are hand-built to
// match the trees in nested_assembly's tests; def/rep/value streams must
// exactly reproduce the reader's per-leaf streams.
// ===========================================================================

const testing = std.testing;
const strings = @import("strings.zig");

fn dupName(allocator: Allocator, s: []const u8) ![]u8 {
    return allocator.dupe(u8, s);
}

fn expectU16s(expected: []const u16, got: []const u16) !void {
    try testing.expectEqualSlices(u16, expected, got);
}

test "shredColumn: list<i64> rows {[1,2], null, []} inverts assembly pin" {
    const allocator = testing.allocator;
    // optional group l { repeated group list { optional int64 element } }
    const element = SchemaNode{
        .name = try dupName(allocator, "element"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 3,
        .max_rep = 1,
        .leaf_index = 0,
    };
    var mid_children = [_]SchemaNode{element};
    const mid = SchemaNode{
        .name = try dupName(allocator, "list"),
        .repetition = .repeated,
        .max_def = 2,
        .max_rep = 1,
        .children = &mid_children,
    };
    var l_children = [_]SchemaNode{mid};
    var l = SchemaNode{
        .name = try dupName(allocator, "l"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &l_children,
    };
    defer {
        allocator.free(element.name);
        allocator.free(mid.name);
        allocator.free(l.name);
    }

    // Rows: [1,2], null, [].
    var row0_items = [_]Nested{ .{ .int = 1 }, .{ .int = 2 } };
    const row0 = Nested{ .list = .{ .allocator = allocator, .items = &row0_items } };
    const row1 = Nested.null_;
    const row2 = Nested{ .list = .{ .allocator = allocator, .items = &.{} } };
    const values = [_]Nested{ row0, row1, row2 };

    const leaves = try shredColumn(allocator, &l, &values, null, 3);
    defer {
        for (leaves) |*lf| lf.deinit(allocator);
        allocator.free(leaves);
    }

    try testing.expectEqual(@as(usize, 1), leaves.len);
    try expectU16s(&.{ 3, 3, 0, 1 }, leaves[0].def.items);
    try expectU16s(&.{ 0, 1, 0, 0 }, leaves[0].rep.items);
    try testing.expectEqualSlices(i64, &.{ 1, 2 }, leaves[0].values.int64s.items);
}

test "shredColumn: optional struct{required a, optional b}" {
    const allocator = testing.allocator;
    const a_node = SchemaNode{
        .name = try dupName(allocator, "a"),
        .repetition = .required,
        .physical = .int64,
        .max_def = 1,
        .max_rep = 0,
        .leaf_index = 0,
    };
    const b_node = SchemaNode{
        .name = try dupName(allocator, "b"),
        .repetition = .optional,
        .physical = .byte_array,
        .converted = .utf8,
        .max_def = 2,
        .max_rep = 0,
        .leaf_index = 1,
    };
    var s_children = [_]SchemaNode{ a_node, b_node };
    var s = SchemaNode{
        .name = try dupName(allocator, "s"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &s_children,
    };
    defer {
        allocator.free(a_node.name);
        allocator.free(b_node.name);
        allocator.free(s.name);
    }

    // Rows: {a:1,b:"x"}, {a:2,b:null}, null. Field arrays are stack-allocated;
    // free only the owned String "x" at the end (no container deinit).
    var f0 = [_]Nested{ .{ .int = 1 }, .{ .string = try strings.String.fromSlice(allocator, "x") } };
    defer f0[1].deinit();
    const row0 = Nested{ .strukt = .{ .allocator = allocator, .fields = &f0 } };
    var f1 = [_]Nested{ .{ .int = 2 }, .null_ };
    const row1 = Nested{ .strukt = .{ .allocator = allocator, .fields = &f1 } };
    const row2 = Nested.null_;
    const values = [_]Nested{ row0, row1, row2 };

    const leaves = try shredColumn(allocator, &s, &values, null, 3);
    defer {
        for (leaves) |*lf| lf.deinit(allocator);
        allocator.free(leaves);
    }

    try testing.expectEqual(@as(usize, 2), leaves.len);
    // a: def {1,1,0}, no rep stream needed but we emit rep zeros; values {1,2}.
    try expectU16s(&.{ 1, 1, 0 }, leaves[0].def.items);
    try expectU16s(&.{ 0, 0, 0 }, leaves[0].rep.items);
    try testing.expectEqualSlices(i64, &.{ 1, 2 }, leaves[0].values.int64s.items);
    // b: def {2,1,0}, values {"x"}.
    try expectU16s(&.{ 2, 1, 0 }, leaves[1].def.items);
    try testing.expectEqual(@as(usize, 1), leaves[1].values.byte_arrays.items.len);
    try testing.expectEqualStrings("x", leaves[1].values.byte_arrays.items[0]);
}

test "shredColumn: list<list<i64>> rows {[[1],[2,3]], [[]], null}" {
    const allocator = testing.allocator;
    const v_node = SchemaNode{
        .name = try dupName(allocator, "element"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 5,
        .max_rep = 2,
        .leaf_index = 0,
    };
    var inner_list_children = [_]SchemaNode{v_node};
    const inner_list = SchemaNode{
        .name = try dupName(allocator, "list"),
        .repetition = .repeated,
        .max_def = 4,
        .max_rep = 2,
        .children = &inner_list_children,
    };
    var inner_elem_children = [_]SchemaNode{inner_list};
    const inner_elem = SchemaNode{
        .name = try dupName(allocator, "element"),
        .repetition = .optional,
        .max_def = 3,
        .max_rep = 1,
        .children = &inner_elem_children,
    };
    var outer_list_children = [_]SchemaNode{inner_elem};
    const outer_list = SchemaNode{
        .name = try dupName(allocator, "list"),
        .repetition = .repeated,
        .max_def = 2,
        .max_rep = 1,
        .children = &outer_list_children,
    };
    var ll_children = [_]SchemaNode{outer_list};
    var ll = SchemaNode{
        .name = try dupName(allocator, "ll"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &ll_children,
    };
    defer {
        allocator.free(v_node.name);
        allocator.free(inner_list.name);
        allocator.free(inner_elem.name);
        allocator.free(outer_list.name);
        allocator.free(ll.name);
    }

    // row0 [[1],[2,3]]
    var items_a = [_]Nested{.{ .int = 1 }};
    const inner0a = Nested{ .list = .{ .allocator = allocator, .items = &items_a } };
    var items_b = [_]Nested{ .{ .int = 2 }, .{ .int = 3 } };
    const inner0b = Nested{ .list = .{ .allocator = allocator, .items = &items_b } };
    var o0 = [_]Nested{ inner0a, inner0b };
    const row0 = Nested{ .list = .{ .allocator = allocator, .items = &o0 } };
    // row1 [[]]
    const empty_inner = Nested{ .list = .{ .allocator = allocator, .items = &.{} } };
    var o1 = [_]Nested{empty_inner};
    const row1 = Nested{ .list = .{ .allocator = allocator, .items = &o1 } };
    // row2 null
    const row2 = Nested.null_;
    const values = [_]Nested{ row0, row1, row2 };

    const leaves = try shredColumn(allocator, &ll, &values, null, 3);
    defer {
        for (leaves) |*lf| lf.deinit(allocator);
        allocator.free(leaves);
    }

    try expectU16s(&.{ 5, 5, 5, 3, 0 }, leaves[0].def.items);
    try expectU16s(&.{ 0, 1, 2, 0, 0 }, leaves[0].rep.items);
    try testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, leaves[0].values.int64s.items);
}

test "shredColumn: map<string,i64> rows {{\"a\":1}, {}}" {
    const allocator = testing.allocator;
    const key_node = SchemaNode{
        .name = try dupName(allocator, "key"),
        .repetition = .required,
        .physical = .byte_array,
        .converted = .utf8,
        .max_def = 2,
        .max_rep = 1,
        .leaf_index = 0,
    };
    const value_node = SchemaNode{
        .name = try dupName(allocator, "value"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 3,
        .max_rep = 1,
        .leaf_index = 1,
    };
    var kv_children = [_]SchemaNode{ key_node, value_node };
    const kv = SchemaNode{
        .name = try dupName(allocator, "key_value"),
        .repetition = .repeated,
        .max_def = 2,
        .max_rep = 1,
        .children = &kv_children,
    };
    var m_children = [_]SchemaNode{kv};
    var m = SchemaNode{
        .name = try dupName(allocator, "m"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &m_children,
    };
    defer {
        allocator.free(key_node.name);
        allocator.free(value_node.name);
        allocator.free(kv.name);
        allocator.free(m.name);
    }

    // Rows: {"a":1}, {}. Entry array is stack-allocated; free only the owned
    // key String "a" (no container deinit).
    var e0 = [_]Nested.MapEntry{.{ .key = .{ .string = try strings.String.fromSlice(allocator, "a") }, .value = .{ .int = 1 } }};
    defer e0[0].key.deinit();
    const row0 = Nested{ .map = .{ .allocator = allocator, .entries = &e0 } };
    const row1 = Nested{ .map = .{ .allocator = allocator, .entries = &.{} } };
    const values = [_]Nested{ row0, row1 };

    const leaves = try shredColumn(allocator, &m, &values, null, 2);
    defer {
        for (leaves) |*lf| lf.deinit(allocator);
        allocator.free(leaves);
    }

    // key.def {2,1} rep {0,0}; value.def {3,1} rep {0,0}.
    try expectU16s(&.{ 2, 1 }, leaves[0].def.items);
    try expectU16s(&.{ 0, 0 }, leaves[0].rep.items);
    try testing.expectEqualStrings("a", leaves[0].values.byte_arrays.items[0]);
    try expectU16s(&.{ 3, 1 }, leaves[1].def.items);
    try expectU16s(&.{ 0, 0 }, leaves[1].rep.items);
    try testing.expectEqualSlices(i64, &.{1}, leaves[1].values.int64s.items);
}

test "shredColumn: validity null marks an absent top-level row" {
    const allocator = testing.allocator;
    const element = SchemaNode{
        .name = try dupName(allocator, "element"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 3,
        .max_rep = 1,
        .leaf_index = 0,
    };
    var mid_children = [_]SchemaNode{element};
    const mid = SchemaNode{
        .name = try dupName(allocator, "list"),
        .repetition = .repeated,
        .max_def = 2,
        .max_rep = 1,
        .children = &mid_children,
    };
    var l_children = [_]SchemaNode{mid};
    var l = SchemaNode{
        .name = try dupName(allocator, "l"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &l_children,
    };
    defer {
        allocator.free(element.name);
        allocator.free(mid.name);
        allocator.free(l.name);
    }

    // Row 0 marked null via validity (stored value is a placeholder empty list);
    // row 1 present [5].
    var r1_items = [_]Nested{.{ .int = 5 }};
    const r1 = Nested{ .list = .{ .allocator = allocator, .items = &r1_items } };
    const values = [_]Nested{ Nested.null_, r1 };
    const validity = [_]bool{ false, true };

    const leaves = try shredColumn(allocator, &l, &values, &validity, 2);
    defer {
        for (leaves) |*lf| lf.deinit(allocator);
        allocator.free(leaves);
    }
    try expectU16s(&.{ 0, 3 }, leaves[0].def.items);
    try expectU16s(&.{ 0, 0 }, leaves[0].rep.items);
    try testing.expectEqualSlices(i64, &.{5}, leaves[0].values.int64s.items);
}
