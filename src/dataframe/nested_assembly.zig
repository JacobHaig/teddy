//! Dremel record assembly (Phase 6d-2b.2): reconstructs per-row Nested value
//! trees for one top-level nested column from its leaves' (values, def, rep)
//! streams and the column's SchemaNode subtree.
//!
//! Level semantics (the contract — everything is derived from these):
//!   For a leaf with ancestors A1..An (root excluded):
//!   - max_def = count of ancestors-or-self that are optional OR repeated.
//!   - max_rep = count of repeated ancestors-or-self.
//!   - Each leaf entry i has (def, rep, maybe-value); value present iff
//!     def == max_def.
//!   - def = d: the first d def-contributing nodes on the path are "defined";
//!     definition stops at node d+1 (null if optional, empty-list if repeated).
//!   - rep = r: r == 0 starts a NEW ROW; r == k continues the k-th repeated
//!     ancestor (innermost lists repeat at higher r).
//!
//! Structural shape detection (group nodes carry no LIST/MAP annotation in the
//! schema tree, so we detect by shape):
//!   - 3-level LIST : group with one `repeated` child that has exactly 1 child
//!                    (the element). Standard `repeated group list { elem }`.
//!   - MAP          : group with one `repeated` child that has exactly 2
//!                    children (key, value). Standard `repeated group
//!                    key_value { key; value }`.
//!   - 2-level LIST : a node that is itself `repeated` (bare repeated leaf or
//!                    repeated group) — legacy unannotated list.
//!   - STRUCT       : any other group.

const std = @import("std");
const Allocator = std.mem.Allocator;
const parquet = @import("parquet");
const Nested = @import("nested.zig").Nested;
const strings = @import("strings.zig");
const Date = @import("date.zig").Date;
const Time = @import("time.zig").Time;
const Timestamp = @import("timestamp.zig").Timestamp;
const Decimal = @import("decimal.zig").Decimal;
const Uuid = @import("uuid.zig").Uuid;
const Interval = @import("interval.zig").Interval;
const Binary = @import("binary.zig").Binary;

const SchemaNode = parquet.types.SchemaNode;
const ParquetColumn = parquet.ParquetColumn;

pub const AssemblyError = error{
    CorruptLevels,
    InvalidUuid,
    InvalidInterval,
    InvalidFloat16,
    InvalidInt96,
    UnexpectedPhysicalType,
    DecimalTooWide,
    TimestampOverflow,
} || Allocator.Error;

/// Cursor over one leaf's def/rep/value streams. `value_idx` advances only when
/// def == max_def (a value is materialized).
const LeafCursor = struct {
    col: *const ParquetColumn,
    node: *const SchemaNode,
    pos: usize = 0, // index into def_levels / rep_levels
    value_idx: usize = 0, // index into the typed value arrays
    len: usize, // length of the def stream

    fn defAt(self: *const LeafCursor) u16 {
        const d = self.col.def_levels orelse return 0;
        return d[self.pos];
    }

    fn repAt(self: *const LeafCursor) u16 {
        const r = self.col.rep_levels orelse return 0;
        // A leaf with max_rep == 0 has no rep stream; rep is always 0.
        if (r.len == 0) return 0;
        return r[self.pos];
    }

    fn atEnd(self: *const LeafCursor) bool {
        return self.pos >= self.len;
    }
};

/// Per-column assembly scratch: the leaf cursors (one per leaf, leaf_index
/// order) plus a map from each SchemaNode pointer to the contiguous span of
/// leaves under it. We resolve subtree spans by leaf_index ranges.
const Assembler = struct {
    allocator: Allocator,
    cursors: []LeafCursor,

    /// The cursor bound to a specific leaf SchemaNode. Bindings are POSITIONAL:
    /// cursor i is the i-th pre-order leaf of the column's root `node`, so this
    /// is an O(n) pointer-identity lookup — no reliance on leaf_index or names.
    fn cursorForNode(self: *Assembler, leaf: *const SchemaNode) *LeafCursor {
        for (self.cursors) |*c| {
            if (c.node == leaf) return c;
        }
        unreachable; // every leaf node is bound during setup
    }

    /// The cursor range covering `node`'s subtree. Cursors are stored in
    /// pre-order, and a subtree's leaves are a contiguous pre-order run, so the
    /// range is [start, start+count) where membership is decided by pointer
    /// identity against the subtree's pre-order leaves.
    fn cursorsInSubtree(self: *Assembler, node: *const SchemaNode) []LeafCursor {
        var start: usize = self.cursors.len;
        var count: usize = 0;
        for (self.cursors, 0..) |*c, i| {
            if (leafInSubtree(node, c.node)) {
                if (i < start) start = i;
                count += 1;
            }
        }
        if (count == 0) return self.cursors[0..0];
        return self.cursors[start .. start + count];
    }

    /// The first (leftmost pre-order) leaf cursor of `node`'s subtree. Used to
    /// "peek" definedness: all leaves in a subtree share def/rep behavior at
    /// ancestor levels, so the first leaf's def tests ancestor nullity.
    fn firstCursor(self: *Assembler, node: *const SchemaNode) *LeafCursor {
        const span = self.cursorsInSubtree(node);
        return &span[0];
    }

    /// Advance every leaf cursor in `node`'s subtree by one entry. Used when an
    /// optional ancestor is null or a list is empty/null: each descendant leaf
    /// records exactly ONE entry for that situation.
    fn consumeSubtree(self: *Assembler, node: *const SchemaNode) void {
        for (self.cursorsInSubtree(node)) |*c| c.pos += 1;
    }
};

/// True iff `leaf` is one of `node`'s descendant leaves (pointer identity).
fn leafInSubtree(node: *const SchemaNode, leaf: *const SchemaNode) bool {
    if (node.isLeaf()) return node == leaf;
    for (node.children) |*c| {
        if (leafInSubtree(c, leaf)) return true;
    }
    return false;
}

/// Append `node`'s leaves to `out` in pre-order. Mirrors the reader's
/// leaf_index assignment order, so positional zip against the column's leaves
/// slice is exact.
fn collectSubtreeLeaves(
    allocator: Allocator,
    node: *const SchemaNode,
    out: *std.ArrayList(*const SchemaNode),
) Allocator.Error!void {
    if (node.isLeaf()) {
        try out.append(allocator, node);
        return;
    }
    for (node.children) |*c| try collectSubtreeLeaves(allocator, c, out);
}

// ---------------------------------------------------------------------------
// Shape detection
// ---------------------------------------------------------------------------

const Shape = enum { leaf, list3, list2, map, strukt };

fn shapeOf(node: *const SchemaNode) Shape {
    if (node.isLeaf()) {
        // A bare `repeated` leaf is a legacy 2-level list.
        if (node.repetition == .repeated) return .list2;
        return .leaf;
    }
    // Group node. Detect LIST / MAP by the standard middle-repeated shape.
    if (node.children.len == 1 and node.children[0].repetition == .repeated) {
        const mid = &node.children[0];
        if (mid.children.len == 2) return .map;
        if (mid.children.len == 1) return .list3;
        // repeated group with !=1,2 children — treat as a 2-level list of the
        // repeated group (assemble the group per element).
        return .list2;
    }
    // A directly-repeated group with content is a 2-level list.
    if (node.repetition == .repeated) return .list2;
    return .strukt;
}

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Assemble one top-level nested column into per-row Nested trees.
/// `node` is the top-level child (group or repeated leaf). `leaves` are this
/// column's leaves in leaf_index order. Returns an owned slice of `num_rows`
/// Nested values; caller frees each via deinit and frees the slice.
pub fn assembleColumn(
    allocator: Allocator,
    node: *const SchemaNode,
    leaves: []const *const ParquetColumn,
    num_rows: usize,
) AssemblyError![]Nested {
    // Bind cursors positionally: the i-th column leaf maps to the i-th pre-order
    // leaf of `node`. Both are in the reader's leaf_index order, so this is an
    // exact zip — robust against duplicate leaf names across sibling subtrees
    // (e.g. struct{p: struct{x}, q: struct{x}}).
    var ordered_leaves = std.ArrayList(*const SchemaNode).empty;
    defer ordered_leaves.deinit(allocator);
    try collectSubtreeLeaves(allocator, node, &ordered_leaves);
    if (ordered_leaves.items.len != leaves.len) return error.CorruptLevels;

    var cursors = try allocator.alloc(LeafCursor, leaves.len);
    defer allocator.free(cursors);
    for (leaves, 0..) |col, i| {
        const leaf_node = ordered_leaves.items[i];
        const stream_len = if (col.def_levels) |d| d.len else col.num_rows;
        cursors[i] = .{ .col = col, .node = leaf_node, .len = stream_len };
    }

    var assembler = Assembler{ .allocator = allocator, .cursors = cursors };

    const out = try allocator.alloc(Nested, num_rows);
    var built: usize = 0;
    errdefer {
        for (out[0..built]) |*v| v.deinit();
        allocator.free(out);
    }

    for (0..num_rows) |row| {
        // Every row's first entry across all leaves must begin at rep 0.
        for (cursors) |*c| {
            if (c.atEnd()) return error.CorruptLevels;
            if (c.repAt() != 0) return error.CorruptLevels;
        }
        out[row] = try assembleNode(&assembler, node, 0, 0);
        built = row + 1;
    }

    // All cursors must be exhausted together.
    for (cursors) |*c| {
        if (!c.atEnd()) return error.CorruptLevels;
    }

    return out;
}

// ---------------------------------------------------------------------------
// Recursive per-row assembly
// ---------------------------------------------------------------------------

/// Assemble the value of `node` for the current row position.
/// `def_floor`/`rep_floor` are the def/rep contributions consumed by ancestors
/// (i.e. the def/rep level at which THIS node's parent is fully defined and the
/// repetition depth at which a NEW element of this node's nearest enclosing
/// list begins). Advances the relevant leaf cursors.
fn assembleNode(
    a: *Assembler,
    node: *const SchemaNode,
    def_floor: u16,
    rep_floor: u16,
) AssemblyError!Nested {
    switch (shapeOf(node)) {
        .leaf => return assembleLeaf(a, node),
        .strukt => return assembleStruct(a, node, def_floor, rep_floor),
        .list3, .list2 => return assembleList(a, node, def_floor, rep_floor),
        .map => return assembleMap(a, node, def_floor, rep_floor),
    }
}

/// The def level at which `node` ITSELF becomes "defined" (present). For an
/// optional or repeated node this is parent_def + 1; for a required node it is
/// parent_def (required nodes add no def level).
fn nodeDefinedDef(node: *const SchemaNode, parent_def: u16) u16 {
    return switch (node.repetition) {
        .required => parent_def,
        .optional, .repeated => parent_def + 1,
    };
}

fn assembleLeaf(a: *Assembler, node: *const SchemaNode) AssemblyError!Nested {
    const c = a.cursorForNode(node);
    if (c.atEnd()) return error.CorruptLevels;
    const def = c.defAt();
    if (def > node.max_def) return error.CorruptLevels;
    c.pos += 1;
    if (def == node.max_def) {
        return leafValueToNested(a.allocator, c);
    }
    // def < max_def at a leaf reached by assembleNode directly means this leaf
    // is optional and null at its OWN level (no list/struct ancestor below the
    // floor that the recursion above already handled). Produce .null_.
    return .null_;
}

fn assembleStruct(
    a: *Assembler,
    node: *const SchemaNode,
    def_floor: u16,
    rep_floor: u16,
) AssemblyError!Nested {
    const own_def = nodeDefinedDef(node, def_floor);

    // Optional struct: peek any descendant leaf. If its def < own_def, the
    // struct itself is null at this row: consume one entry from EVERY leaf in
    // the subtree and return .null_.
    if (node.repetition == .optional) {
        const peek = a.firstCursor(node);
        if (peek.atEnd()) return error.CorruptLevels;
        if (peek.defAt() < own_def) {
            a.consumeSubtree(node);
            return .null_;
        }
    }

    const fields = try a.allocator.alloc(Nested, node.children.len);
    var built: usize = 0;
    errdefer {
        for (fields[0..built]) |*f| f.deinit();
        if (fields.len > 0) a.allocator.free(fields);
    }
    for (node.children, 0..) |*child, i| {
        fields[i] = try assembleNode(a, child, own_def, rep_floor);
        built = i + 1;
    }
    return .{ .strukt = .{ .allocator = a.allocator, .fields = fields } };
}

/// Resolve the LIST's repeated middle node, the element node, the element's
/// def_floor and the rep level that a NEW element of this list carries.
const ListParts = struct {
    repeated: *const SchemaNode, // the repeated node
    element: *const SchemaNode, // the element subtree to assemble per item
    /// def at which the LIST itself is present (non-null), but possibly empty.
    list_present_def: u16,
    /// def at which one element is present (the element's value is defined up
    /// to the element node level, ancestors-defined).
    element_floor_def: u16,
    /// rep level that the FIRST entry of a continuation element carries.
    list_rep: u16,
};

fn resolveList(node: *const SchemaNode, def_floor: u16, rep_floor: u16) ListParts {
    // 3-level: node (optional/required group) -> repeated middle -> element.
    // 2-level: node IS the repeated leaf/group (element == node content).
    if (node.repetition == .repeated) {
        // 2-level legacy: node itself is the repeated element.
        const list_present_def = def_floor; // can't represent null/empty distinctly in 2-level
        _ = list_present_def;
        return .{
            .repeated = node,
            .element = node,
            .list_present_def = def_floor,
            .element_floor_def = def_floor + 1, // repeated adds a def level
            .list_rep = rep_floor + 1,
        };
    }
    // 3-level. node is a group; its (optional) presence adds a def level.
    const list_def = nodeDefinedDef(node, def_floor); // node present
    const mid = &node.children[0]; // repeated middle (adds a def + rep level)
    return .{
        .repeated = mid,
        .element = &mid.children[0],
        .list_present_def = list_def,
        .element_floor_def = list_def + 1, // repeated middle present => one element
        .list_rep = rep_floor + 1,
    };
}

fn assembleList(
    a: *Assembler,
    node: *const SchemaNode,
    def_floor: u16,
    rep_floor: u16,
) AssemblyError!Nested {
    const p = resolveList(node, def_floor, rep_floor);
    const peek = a.firstCursor(node);
    if (peek.atEnd()) return error.CorruptLevels;
    const first_def = peek.defAt();

    // Null list: the list group is optional and not present at this row.
    if (node.repetition == .optional and first_def < p.list_present_def) {
        a.consumeSubtree(node);
        return .null_;
    }

    // Empty list: list present but no elements. def == list_present_def (and,
    // for 3-level, < element_floor_def). One entry recorded across the subtree.
    if (first_def < p.element_floor_def) {
        a.consumeSubtree(node);
        return .{ .list = .{ .allocator = a.allocator, .items = &.{} } };
    }

    // Non-empty list: loop elements. The first element is consumed with the
    // current position; subsequent elements continue while the next entry's rep
    // (on the peek leaf) == p.list_rep (this list's repetition depth). A higher
    // rep belongs to a deeper list (handled by the element's own recursion); a
    // lower rep ends this list.
    var items = std.ArrayList(Nested).empty;
    errdefer {
        for (items.items) |*it| it.deinit();
        items.deinit(a.allocator);
    }
    while (true) {
        const item = try assembleNode(a, p.element, p.element_floor_def, p.list_rep);
        try items.append(a.allocator, item);
        // Peek the next entry on the subtree's first leaf to decide continuation.
        const pk = a.firstCursor(node);
        if (pk.atEnd()) break;
        if (pk.repAt() != p.list_rep) break;
    }
    const owned = try items.toOwnedSlice(a.allocator);
    return .{ .list = .{ .allocator = a.allocator, .items = owned } };
}

fn assembleMap(
    a: *Assembler,
    node: *const SchemaNode,
    def_floor: u16,
    rep_floor: u16,
) AssemblyError!Nested {
    // MAP: node (group) -> repeated key_value -> {key, value}. Same level
    // structure as a 3-level list whose element is the key_value struct.
    const list_def = nodeDefinedDef(node, def_floor); // map present
    const mid = &node.children[0]; // repeated key_value
    const key_node = &mid.children[0];
    const value_node = &mid.children[1];
    const element_floor_def = list_def + 1;
    const map_rep = rep_floor + 1;

    const peek = a.firstCursor(node);
    if (peek.atEnd()) return error.CorruptLevels;
    const first_def = peek.defAt();

    // Null map.
    if (node.repetition == .optional and first_def < list_def) {
        a.consumeSubtree(node);
        return .null_;
    }
    // Empty map.
    if (first_def < element_floor_def) {
        a.consumeSubtree(node);
        return .{ .map = .{ .allocator = a.allocator, .entries = &.{} } };
    }

    var entries = std.ArrayList(Nested.MapEntry).empty;
    errdefer {
        for (entries.items) |*e| {
            e.key.deinit();
            e.value.deinit();
        }
        entries.deinit(a.allocator);
    }
    while (true) {
        var key = try assembleNode(a, key_node, element_floor_def, map_rep);
        errdefer key.deinit();
        var value = try assembleNode(a, value_node, element_floor_def, map_rep);
        errdefer value.deinit();
        try entries.append(a.allocator, .{ .key = key, .value = value });
        const pk = a.firstCursor(node);
        if (pk.atEnd()) break;
        if (pk.repAt() != map_rep) break;
    }
    const owned = try entries.toOwnedSlice(a.allocator);
    return .{ .map = .{ .allocator = a.allocator, .entries = owned } };
}

// ---------------------------------------------------------------------------
// Leaf value resolution — mirrors parquet.zig resolveKind scalar arms.
// ---------------------------------------------------------------------------

fn leafValueToNested(allocator: Allocator, c: *LeafCursor) AssemblyError!Nested {
    const node = c.node;
    const vi = c.value_idx;
    c.value_idx += 1;

    // Resolution precedence: logical_type → converted_type → physical.
    if (node.logical) |lt| switch (lt) {
        .integer => |it| {
            const v = try intValue(node, c, vi);
            if (it.is_signed) return .{ .int = v } else return .{ .uint = @bitCast(v) };
        },
        .date => return .{ .date = .{ .days = try i32Value(node, c, vi) } },
        .time => |tp| return .{ .time = .{ .value = try timeValue(node, c, vi), .unit = tp.unit, .utc = tp.is_adjusted_to_utc } },
        .timestamp => |tp| return .{ .timestamp = .{ .value = try i64Value(node, c, vi), .unit = tp.unit, .utc = tp.is_adjusted_to_utc, .origin = .int64 } },
        .decimal => |d| return decimalValue(node, c, vi, d.precision, d.scale),
        .string, .@"enum", .json => return stringValue(allocator, node, c, vi),
        .uuid => return uuidValue(node, c, vi),
        .float16 => return float16Value(node, c, vi),
        .bson => return bytesValue(allocator, node, c, vi),
        else => {},
    };
    if (node.converted) |ct| switch (ct) {
        .int_8, .int_16, .int_32, .int_64 => return .{ .int = try intValue(node, c, vi) },
        .uint_8, .uint_16, .uint_32, .uint_64 => return .{ .uint = @bitCast(try intValue(node, c, vi)) },
        .utf8, .@"enum", .json => return stringValue(allocator, node, c, vi),
        .bson => return bytesValue(allocator, node, c, vi),
        .date => return .{ .date = .{ .days = try i32Value(node, c, vi) } },
        .time_millis => return .{ .time = .{ .value = try timeValue(node, c, vi), .unit = .millis, .utc = true } },
        .time_micros => return .{ .time = .{ .value = try timeValue(node, c, vi), .unit = .micros, .utc = true } },
        .timestamp_millis => return .{ .timestamp = .{ .value = try i64Value(node, c, vi), .unit = .millis, .utc = true, .origin = .int64 } },
        .timestamp_micros => return .{ .timestamp = .{ .value = try i64Value(node, c, vi), .unit = .micros, .utc = true, .origin = .int64 } },
        .interval => return intervalValue(node, c, vi),
        .decimal => {
            const prec = node.precision orelse return error.UnexpectedPhysicalType;
            const scale = node.scale orelse 0;
            return decimalValue(node, c, vi, prec, scale);
        },
        else => {},
    };
    // Physical default.
    return switch (node.physical orelse return error.UnexpectedPhysicalType) {
        .boolean => .{ .boolean = (c.col.booleans orelse return error.UnexpectedPhysicalType)[vi] },
        .int32 => .{ .int = @as(i64, (c.col.int32s orelse return error.UnexpectedPhysicalType)[vi]) },
        .int64 => .{ .int = (c.col.int64s orelse return error.UnexpectedPhysicalType)[vi] },
        .float => .{ .float = @as(f64, (c.col.floats orelse return error.UnexpectedPhysicalType)[vi]) },
        .double => .{ .float = (c.col.doubles orelse return error.UnexpectedPhysicalType)[vi] },
        .byte_array => bytesValue(allocator, node, c, vi),
        .fixed_len_byte_array => bytesValue(allocator, node, c, vi),
        .int96 => int96Value(node, c, vi),
    };
}

fn intValue(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!i64 {
    _ = node;
    if (c.col.int32s) |v| return @as(i64, v[vi]);
    if (c.col.int64s) |v| return v[vi];
    return error.UnexpectedPhysicalType;
}

fn i32Value(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!i32 {
    _ = node;
    const v = c.col.int32s orelse return error.UnexpectedPhysicalType;
    return v[vi];
}

fn i64Value(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!i64 {
    _ = node;
    const v = c.col.int64s orelse return error.UnexpectedPhysicalType;
    return v[vi];
}

fn timeValue(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!i64 {
    _ = node;
    if (c.col.int32s) |v| return @as(i64, v[vi]);
    if (c.col.int64s) |v| return v[vi];
    return error.UnexpectedPhysicalType;
}

fn decimalValue(node: *const SchemaNode, c: *LeafCursor, vi: usize, precision_in: i32, scale_in: i32) AssemblyError!Nested {
    _ = node;
    const precision = std.math.cast(u8, precision_in) orelse return error.UnexpectedPhysicalType;
    const scale = std.math.cast(i8, scale_in) orelse return error.UnexpectedPhysicalType;
    if (c.col.int32s) |v| return .{ .decimal = .{ .unscaled = @as(i256, v[vi]), .precision = precision, .scale = scale } };
    if (c.col.int64s) |v| return .{ .decimal = .{ .unscaled = @as(i256, v[vi]), .precision = precision, .scale = scale } };
    if (c.col.byte_arrays) |v| return .{ .decimal = .{ .unscaled = try Decimal.fromBeBytes(v[vi]), .precision = precision, .scale = scale } };
    return error.UnexpectedPhysicalType;
}

fn stringValue(allocator: Allocator, node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    return .{ .string = try strings.String.fromSlice(allocator, v[vi]) };
}

fn bytesValue(allocator: Allocator, node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    return .{ .bytes = try Binary.fromSlice(allocator, v[vi]) };
}

fn uuidValue(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    if (v[vi].len != 16) return error.InvalidUuid;
    const bytes_ptr: *const [16]u8 = v[vi][0..16];
    return .{ .uuid = Uuid.fromBytes(bytes_ptr.*) };
}

fn intervalValue(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    if (v[vi].len != 12) return error.InvalidInterval;
    const bytes_ptr: *const [12]u8 = v[vi][0..12];
    return .{ .interval = Interval.fromLeBytes(bytes_ptr) };
}

fn float16Value(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    if (v[vi].len != 2) return error.InvalidFloat16;
    const bytes_ptr: *const [2]u8 = v[vi][0..2];
    const h: f16 = @bitCast(std.mem.readInt(u16, bytes_ptr, .little));
    return .{ .float = @as(f64, h) };
}

fn int96Value(node: *const SchemaNode, c: *LeafCursor, vi: usize) AssemblyError!Nested {
    _ = node;
    const v = c.col.byte_arrays orelse return error.UnexpectedPhysicalType;
    if (v[vi].len != 12) return error.InvalidInt96;
    const bytes_ptr: *const [12]u8 = v[vi][0..12];
    return .{ .timestamp = try Timestamp.fromInt96Bytes(bytes_ptr) };
}

// ===========================================================================
// Unit tests — hand-built level streams (pinned per slice .0 derivations).
// ===========================================================================

const testing = std.testing;

/// Build a heap leaf ParquetColumn with int64 values + def/rep streams. The
/// caller owns it via deinit. Mirrors the reader's nested-leaf layout.
fn makeI64Leaf(
    allocator: Allocator,
    name: []const u8,
    values: []const i64,
    defs: []const u16,
    reps: []const u16,
    is_optional: bool,
) !*ParquetColumn {
    const col = try allocator.create(ParquetColumn);
    col.* = ParquetColumn.initEmpty(allocator);
    col.name = try allocator.dupe(u8, name);
    col.physical_type = .int64;
    col.is_optional = is_optional;
    col.nested = true;
    col.num_rows = values.len;
    col.int64s = try allocator.dupe(i64, values);
    col.def_levels = try allocator.dupe(u16, defs);
    col.rep_levels = if (reps.len > 0) try allocator.dupe(u16, reps) else null;
    return col;
}

fn makeStringLeaf(
    allocator: Allocator,
    name: []const u8,
    values: []const []const u8,
    defs: []const u16,
    reps: []const u16,
) !*ParquetColumn {
    const col = try allocator.create(ParquetColumn);
    col.* = ParquetColumn.initEmpty(allocator);
    col.name = try allocator.dupe(u8, name);
    col.physical_type = .byte_array;
    col.converted_type = .utf8;
    col.is_optional = true;
    col.nested = true;
    col.num_rows = values.len;
    const arrs = try allocator.alloc([]const u8, values.len);
    for (values, 0..) |s, i| arrs[i] = try allocator.dupe(u8, s);
    col.byte_arrays = arrs;
    col.def_levels = try allocator.dupe(u16, defs);
    col.rep_levels = if (reps.len > 0) try allocator.dupe(u16, reps) else null;
    return col;
}

fn destroyLeaf(allocator: Allocator, col: *ParquetColumn) void {
    col.deinit();
    allocator.destroy(col);
}

fn freeRows(rows: []Nested, allocator: Allocator) void {
    for (rows) |*r| r.deinit();
    allocator.free(rows);
}

fn dupName(allocator: Allocator, s: []const u8) ![]u8 {
    return allocator.dupe(u8, s);
}

/// Helper: assemble + render each row to a string for assertion.
fn renderRow(allocator: Allocator, v: *const Nested, buf: []u8) ![]u8 {
    _ = allocator;
    return std.fmt.bufPrint(buf, "{f}", .{v.*});
}

test "assembleColumn: list<i64> rows {[1,2], null, []}" {
    const allocator = testing.allocator;
    // Schema: optional group l (LIST) { repeated group list { optional int64 element } }
    // max_def(element)=3, max_rep=1.  Pinned streams (slice .0):
    //   def {3,3,0,1}, rep {0,1,0,0}; values present (def==3) = {1,2}.
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

    const leaf = try makeI64Leaf(allocator, "element", &.{ 1, 2 }, &.{ 3, 3, 0, 1 }, &.{ 0, 1, 0, 0 }, true);
    defer destroyLeaf(allocator, leaf);
    const leaves = [_]*const ParquetColumn{leaf};

    const rows = try assembleColumn(allocator, &l, &leaves, 3);
    defer freeRows(rows, allocator);

    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("[1, 2]", try renderRow(allocator, &rows[0], &buf));
    try testing.expectEqualStrings("null", try renderRow(allocator, &rows[1], &buf));
    try testing.expectEqualStrings("[]", try renderRow(allocator, &rows[2], &buf));
}

test "assembleColumn: optional struct{required a, optional b}" {
    const allocator = testing.allocator;
    // Schema: optional group s { required int64 a; optional string b }
    //   a: max_def 1 (s optional). b: max_def 2 (s optional + b optional).
    // Rows: {a:1,b:"x"}, {a:2,b:null}, null.  Pinned streams (slice .0):
    //   a.def {1,1,0} (no values lost; a present when s present)... wait: a is
    //   required, so when s present a is present. s null => def 0.
    //   But a has values only when present: present iff def==max_def(a)=1.
    //   a.def {1,1,0}; a.values {1,2}.
    //   b.def {2,1,0}; b.values {"x"}.
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

    const la = try makeI64Leaf(allocator, "a", &.{ 1, 2 }, &.{ 1, 1, 0 }, &.{}, false);
    defer destroyLeaf(allocator, la);
    const lb = try makeStringLeaf(allocator, "b", &.{"x"}, &.{ 2, 1, 0 }, &.{});
    defer destroyLeaf(allocator, lb);
    const leaves = [_]*const ParquetColumn{ la, lb };

    const rows = try assembleColumn(allocator, &s, &leaves, 3);
    defer freeRows(rows, allocator);

    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("{1, \"x\"}", try renderRow(allocator, &rows[0], &buf));
    try testing.expectEqualStrings("{2, null}", try renderRow(allocator, &rows[1], &buf));
    try testing.expectEqualStrings("null", try renderRow(allocator, &rows[2], &buf));
}

test "assembleColumn: list<list<i64>> rows {[[1],[2,3]], [[]], null}" {
    const allocator = testing.allocator;
    // Schema (3-level nested twice):
    //   optional group ll (LIST) {                       def+1 -> defines ll
    //     repeated group list {                          def+1, rep+1 -> outer elem
    //       optional group element (LIST) {              def+1 -> inner list present
    //         repeated group list {                      def+1, rep+1 -> inner elem
    //           optional int64 element } } } }           def+1 -> value present
    //   element max_def = 5, max_rep = 2.
    //
    // DERIVATION of def/rep for rows {[[1],[2,3]], [[]], null}:
    //   levels by stop-point (1-based def-contributors):
    //     d0: ll null         (ll optional not present)
    //     d1: ll present, outer list empty
    //     d2: outer elem present, inner list (element) null
    //     d3: inner list present but empty
    //     d4: inner elem present, value null
    //     d5: value present
    //   rep: 0 new row; 1 continue outer list; 2 continue inner list.
    //
    //   row0 [[1],[2,3]]:
    //     [1]      -> value 1: def5 rep0  (new row)
    //     [2,3]    -> value 2: def5 rep1  (new outer element)
    //                 value 3: def5 rep2  (continue inner list)
    //   row1 [[]]:
    //     outer has one element which is an empty inner list:
    //                 def3 rep0  (ll present, outer present, inner list present-empty)
    //   row2 null:
    //                 def0 rep0
    //   concatenated: def {5,5,5,3,0}, rep {0,1,2,0,0}; present(def==5)={1,2,3}.
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

    const leaf = try makeI64Leaf(allocator, "element", &.{ 1, 2, 3 }, &.{ 5, 5, 5, 3, 0 }, &.{ 0, 1, 2, 0, 0 }, true);
    defer destroyLeaf(allocator, leaf);
    const leaves = [_]*const ParquetColumn{leaf};

    const rows = try assembleColumn(allocator, &ll, &leaves, 3);
    defer freeRows(rows, allocator);

    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("[[1], [2, 3]]", try renderRow(allocator, &rows[0], &buf));
    try testing.expectEqualStrings("[[]]", try renderRow(allocator, &rows[1], &buf));
    try testing.expectEqualStrings("null", try renderRow(allocator, &rows[2], &buf));
}

test "assembleColumn: map<string,i64> rows {{\"a\":1}, {}}" {
    const allocator = testing.allocator;
    // Schema: optional group m (MAP) {
    //   repeated group key_value { required string key; optional int64 value } }
    //   key:   max_def 2 (m optional + key_value repeated; key required)
    //   value: max_def 3 (+ value optional)
    // Rows: {"a":1}, {}.  Streams:
    //   key.def   {2,1}  rep {0,0}; key present(def==2)={"a"}
    //   value.def {3,1}  rep {0,0}; value present(def==3)={1}
    //   (empty map: m present, key_value absent => def 1 for both, key_value
    //    contributes its def at level 2 only when present.)
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

    const lk = try makeStringLeaf(allocator, "key", &.{"a"}, &.{ 2, 1 }, &.{ 0, 0 });
    defer destroyLeaf(allocator, lk);
    const lv = try makeI64Leaf(allocator, "value", &.{1}, &.{ 3, 1 }, &.{ 0, 0 }, true);
    defer destroyLeaf(allocator, lv);
    const leaves = [_]*const ParquetColumn{ lk, lv };

    const rows = try assembleColumn(allocator, &m, &leaves, 2);
    defer freeRows(rows, allocator);

    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings("{\"a\": 1}", try renderRow(allocator, &rows[0], &buf));
    try testing.expectEqualStrings("{}", try renderRow(allocator, &rows[1], &buf));
}

test "assembleColumn: corrupt levels (rep nonzero at row start) errors" {
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
    // First entry has rep 1 (invalid: a row must start at rep 0).
    const leaf = try makeI64Leaf(allocator, "element", &.{1}, &.{3}, &.{1}, true);
    defer destroyLeaf(allocator, leaf);
    const leaves = [_]*const ParquetColumn{leaf};
    try testing.expectError(error.CorruptLevels, assembleColumn(allocator, &l, &leaves, 1));
}

test "assembleColumn: sibling structs with duplicate leaf name bind positionally" {
    const allocator = testing.allocator;
    // Schema: optional group root {
    //   required group p { optional int64 x }   // leaf_index 0
    //   required group q { optional int64 x }   // leaf_index 1
    // }
    // root optional -> def+1; p/q required -> def+0; x optional -> def+1.
    // Each x: max_def 2, max_rep 0. Both leaves named "x" — a trailing-name
    // binding would alias them and mis-route q's values. Positional zip must
    // route p.x -> field0.field0, q.x -> field1.field0.
    // Rows: {p:{x:10}, q:{x:20}}, {p:{x:30}, q:{x:40}}.
    //   p.x def {2,2} values {10,30}; q.x def {2,2} values {20,40}.
    const px = SchemaNode{
        .name = try dupName(allocator, "x"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 2,
        .max_rep = 0,
        .leaf_index = 0,
    };
    var p_children = [_]SchemaNode{px};
    const p = SchemaNode{
        .name = try dupName(allocator, "p"),
        .repetition = .required,
        .max_def = 1,
        .max_rep = 0,
        .children = &p_children,
    };
    const qx = SchemaNode{
        .name = try dupName(allocator, "x"),
        .repetition = .optional,
        .physical = .int64,
        .max_def = 2,
        .max_rep = 0,
        .leaf_index = 1,
    };
    var q_children = [_]SchemaNode{qx};
    const q = SchemaNode{
        .name = try dupName(allocator, "q"),
        .repetition = .required,
        .max_def = 1,
        .max_rep = 0,
        .children = &q_children,
    };
    var root_children = [_]SchemaNode{ p, q };
    var root = SchemaNode{
        .name = try dupName(allocator, "root"),
        .repetition = .optional,
        .max_def = 1,
        .max_rep = 0,
        .children = &root_children,
    };
    defer {
        allocator.free(px.name);
        allocator.free(p.name);
        allocator.free(qx.name);
        allocator.free(q.name);
        allocator.free(root.name);
    }

    const lpx = try makeI64Leaf(allocator, "x", &.{ 10, 30 }, &.{ 2, 2 }, &.{}, true);
    defer destroyLeaf(allocator, lpx);
    const lqx = try makeI64Leaf(allocator, "x", &.{ 20, 40 }, &.{ 2, 2 }, &.{}, true);
    defer destroyLeaf(allocator, lqx);
    const leaves = [_]*const ParquetColumn{ lpx, lqx };

    const rows = try assembleColumn(allocator, &root, &leaves, 2);
    defer freeRows(rows, allocator);

    var buf: [64]u8 = undefined;
    // {p:{x:10}, q:{x:20}} -> {{10}, {20}}; {{30}, {40}}.
    try testing.expectEqualStrings("{{10}, {20}}", try renderRow(allocator, &rows[0], &buf));
    try testing.expectEqualStrings("{{30}, {40}}", try renderRow(allocator, &rows[1], &buf));
}
