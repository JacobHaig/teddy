const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const column_reader = @import("column_reader.zig");
const ThriftReader = @import("thrift_reader.zig").ThriftReader;

// ============================================================
// Parquet File Reader
// ============================================================

const MAGIC = "PAR1";

/// Read a Parquet file from an in-memory buffer and return decoded columns.
/// The buffer must contain the complete file contents.
/// Caller owns the returned ParquetResult and must call deinit().
pub fn readParquet(allocator: Allocator, file_data: []const u8) !types.ParquetResult {
    // 1. Validate minimum size and magic bytes
    if (file_data.len < 12) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, file_data[0..4], MAGIC)) return error.InvalidParquetFile;
    if (!std.mem.eql(u8, file_data[file_data.len - 4 ..], MAGIC)) return error.InvalidParquetFile;

    // 2. Read footer length (4 bytes LE, just before closing magic).
    // footer_len comes from a u32 (max ~4G); on a 64-bit usize `footer_len + 8`
    // cannot overflow, but use a checked add for clarity and 32-bit safety.
    const footer_len: usize = @intCast(std.mem.readInt(u32, file_data[file_data.len - 8 ..][0..4], .little));
    const footer_plus_magic = std.math.add(usize, footer_len, 8) catch return error.InvalidParquetFile;
    if (footer_plus_magic > file_data.len) return error.InvalidParquetFile;

    // 3. Decode FileMetaData from footer
    const footer_start = file_data.len - 8 - footer_len;
    const footer_data = file_data[footer_start .. footer_start + footer_len];

    var thrift_reader = ThriftReader.init(footer_data);
    var file_metadata = try metadata.FileMetaData.decode(&thrift_reader, allocator);
    defer file_metadata.deinit(allocator);

    // 4. Resolve schema: build the owned schema tree and collect every leaf
    //    (flat + nested) in column-chunk order with correct cumulative levels.
    var tree = try buildSchemaTree(allocator, file_metadata.schema);
    errdefer tree.deinit(allocator);

    const leaves = try collectLeaves(allocator, &tree);
    defer freeLeaves(allocator, leaves);

    // file-controlled i64; a negative row count is corrupt, not zero.
    const num_rows: usize = std.math.cast(usize, file_metadata.num_rows) orelse return error.CorruptFile;

    // 5. Read each leaf column, concatenating its chunks across every row
    //    group, then partition into flat `columns` (parent == root, max_rep 0)
    //    vs `nested_columns` (everything descending from a nested subtree).
    //    Flat files therefore behave EXACTLY as before; nested files now read
    //    their flat siblings at the correct column index instead of
    //    mis-indexing every column (the latent bug this slice fixes).
    var flat = std.ArrayList(types.ParquetColumn).empty;
    errdefer {
        for (flat.items) |*c| c.deinit();
        flat.deinit(allocator);
    }
    var nested = std.ArrayList(types.ParquetColumn).empty;
    errdefer {
        for (nested.items) |*c| c.deinit();
        nested.deinit(allocator);
    }

    for (leaves) |leaf| {
        var col = try readLeafConcat(allocator, file_data, file_metadata.row_groups, leaf, num_rows);
        errdefer col.deinit();
        if (leaf.nested) {
            try nested.append(allocator, col);
        } else {
            try flat.append(allocator, col);
        }
    }

    return .{
        .columns = try flat.toOwnedSlice(allocator),
        .num_rows = num_rows,
        .allocator = allocator,
        .schema_tree = tree,
        .nested_columns = try nested.toOwnedSlice(allocator),
    };
}

/// Read a single leaf column across all row groups, concatenating each chunk's
/// values (and validity) into one column of `total_rows` rows. A single-row-group
/// file is handled as the length-1 case of the loop.
fn readLeafConcat(
    allocator: Allocator,
    file_data: []const u8,
    row_groups: []const metadata.RowGroup,
    leaf: column_reader.LeafColumn,
    total_rows: usize,
) !types.ParquetColumn {
    var col = types.ParquetColumn.initEmpty(allocator);
    errdefer col.deinit();

    // Nested leaves surface under their dotted schema path; flat under the bare name.
    const src_name = leaf.dotted_path orelse leaf.schema_element.name;
    const name_copy = try allocator.alloc(u8, src_name.len);
    @memcpy(name_copy, src_name);
    col.name = name_copy;
    col.physical_type = leaf.schema_element.type_ orelse .byte_array;
    col.converted_type = leaf.schema_element.converted_type;
    col.logical_type = leaf.schema_element.logical_type;
    col.type_length = leaf.schema_element.type_length;
    col.scale = leaf.schema_element.scale;
    col.precision = leaf.schema_element.precision;
    col.is_optional = leaf.max_def_level > 0;
    col.num_rows = total_rows;
    col.nested = leaf.nested;
    col.root_child_index = leaf.root_child_index;

    // Per-type accumulators; only the one matching `physical_type` is filled.
    var booleans = std.ArrayList(bool).empty;
    var int32s = std.ArrayList(i32).empty;
    var int64s = std.ArrayList(i64).empty;
    var floats = std.ArrayList(f32).empty;
    var doubles = std.ArrayList(f64).empty;
    var byte_arrays = std.ArrayList([]const u8).empty;
    var validity = std.ArrayList(bool).empty;
    // Raw level streams, accumulated only for nested leaves. The first rep
    // level of every row group is 0 by definition, so cross-group
    // concatenation is safe (a new top-level record always starts each chunk).
    var def_levels = std.ArrayList(u16).empty;
    var rep_levels = std.ArrayList(u16).empty;
    errdefer {
        booleans.deinit(allocator);
        int32s.deinit(allocator);
        int64s.deinit(allocator);
        floats.deinit(allocator);
        doubles.deinit(allocator);
        for (byte_arrays.items) |b| allocator.free(b);
        byte_arrays.deinit(allocator);
        validity.deinit(allocator);
        def_levels.deinit(allocator);
        rep_levels.deinit(allocator);
    }

    for (row_groups) |rg| {
        if (leaf.column_index >= rg.columns.len) return error.ColumnIndexOutOfBounds;
        // file-controlled i64; negative → error.
        const rg_rows: usize = std.math.cast(usize, rg.num_rows) orelse return error.CorruptFile;

        var chunk = try column_reader.readColumnChunk(
            allocator,
            file_data,
            rg.columns[leaf.column_index],
            leaf,
            rg_rows,
        );
        // The chunk owns its buffers and frees them on deinit. Numeric/bool/
        // validity values are copied into the accumulators, so the chunk keeps
        // ownership of those. For byte arrays we transfer the owned inner slices
        // into `byte_arrays` and null the chunk's reference to avoid a double free.
        var bytes_moved = false;
        defer {
            if (bytes_moved) chunk.byte_arrays = null;
            chunk.deinit();
        }

        switch (col.physical_type) {
            .boolean => if (chunk.booleans) |v| try booleans.appendSlice(allocator, v),
            .int32 => if (chunk.int32s) |v| try int32s.appendSlice(allocator, v),
            .int64 => if (chunk.int64s) |v| try int64s.appendSlice(allocator, v),
            .float => if (chunk.floats) |v| try floats.appendSlice(allocator, v),
            .double => if (chunk.doubles) |v| try doubles.appendSlice(allocator, v),
            // byte_array, fixed_len_byte_array, and int96 are all stored as raw
            // owned byte slices; move ownership across row groups.
            .byte_array, .fixed_len_byte_array, .int96 => if (chunk.byte_arrays) |v| {
                try byte_arrays.appendSlice(allocator, v); // moves the inner slices
                bytes_moved = true;
                allocator.free(v); // free only the outer array
            },
        }

        if (leaf.nested) {
            // Concatenate the raw level streams across row groups.
            if (chunk.def_levels) |d| try def_levels.appendSlice(allocator, d);
            if (chunk.rep_levels) |r| try rep_levels.appendSlice(allocator, r);
        } else if (col.is_optional) {
            if (chunk.validity) |cv| {
                try validity.appendSlice(allocator, cv);
            } else {
                try validity.appendNTimes(allocator, true, rg_rows);
            }
        }
    }

    switch (col.physical_type) {
        .boolean => col.booleans = try booleans.toOwnedSlice(allocator),
        .int32 => col.int32s = try int32s.toOwnedSlice(allocator),
        .int64 => col.int64s = try int64s.toOwnedSlice(allocator),
        .float => col.floats = try floats.toOwnedSlice(allocator),
        .double => col.doubles = try doubles.toOwnedSlice(allocator),
        .byte_array, .fixed_len_byte_array, .int96 => col.byte_arrays = try byte_arrays.toOwnedSlice(allocator),
    }
    if (leaf.nested) {
        col.def_levels = try def_levels.toOwnedSlice(allocator);
        col.rep_levels = try rep_levels.toOwnedSlice(allocator);
    } else if (col.is_optional) {
        col.validity = try validity.toOwnedSlice(allocator);
    }

    return col;
}

// ============================================================
// Schema tree (owned SchemaNode) + leaf collection
// ============================================================

/// Build the owned schema tree from the flattened pre-order SchemaElement list
/// (root first; each group is followed by exactly its `num_children` subtrees).
/// Computes per-node cumulative levels via an ancestor walk and assigns each
/// leaf a `leaf_index` in pre-order (which is exactly parquet's column-chunk
/// ordering). Names are copied (owned by the tree). Caller owns the result and
/// must call `deinit`.
pub fn buildSchemaTree(allocator: Allocator, schema: []const metadata.SchemaElement) !types.SchemaNode {
    if (schema.len == 0) return error.EmptySchema;

    var cursor: usize = 0; // pre-order read position into `schema`
    var next_leaf: usize = 0; // running leaf index (column-chunk order)
    // The root's own repetition/levels do not contribute (it is the message
    // node): start the recursion with parent levels 0/0.
    const root = try buildNode(allocator, schema, &cursor, &next_leaf, 0, 0);
    return root;
}

/// Recursive descent over the pre-order list. `parent_def`/`parent_rep` are the
/// cumulative levels of THIS node's parent. Parquet level rules: def level
/// increments for every node that is optional OR repeated (NOT required); rep
/// level increments for every repeated node.
fn buildNode(
    allocator: Allocator,
    schema: []const metadata.SchemaElement,
    cursor: *usize,
    next_leaf: *usize,
    parent_def: u8,
    parent_rep: u8,
) !types.SchemaNode {
    if (cursor.* >= schema.len) return error.CorruptFile;
    const elem = schema[cursor.*];
    cursor.* += 1;

    const rep = elem.repetition_type orelse .required;
    // The root (cursor was 0) has no repetition contribution; for every other
    // node, optional/repeated each add one def level, repeated adds one rep.
    const is_root = (cursor.* == 1);
    const my_def: u8 = if (is_root) 0 else parent_def + @as(u8, if (rep == .required) 0 else 1);
    const my_rep: u8 = if (is_root) 0 else parent_rep + @as(u8, if (rep == .repeated) 1 else 0);

    const name_copy = try allocator.alloc(u8, elem.name.len);
    @memcpy(name_copy, elem.name);

    var node = types.SchemaNode{
        .name = name_copy,
        .repetition = rep,
    };
    // From here on, node.deinit owns name_copy — no separate errdefer for it
    // (a standalone free would double-free with node.deinit on error).
    errdefer node.deinit(allocator);

    const num_children: usize = if (elem.num_children) |nc|
        (std.math.cast(usize, nc) orelse return error.CorruptFile)
    else
        0;

    if (num_children == 0) {
        // Leaf: carry the physical/annotation payload and cumulative levels.
        node.physical = elem.type_;
        node.converted = elem.converted_type;
        node.logical = elem.logical_type;
        node.type_length = elem.type_length;
        node.scale = elem.scale;
        node.precision = elem.precision;
        node.max_def = my_def;
        node.max_rep = my_rep;
        node.leaf_index = next_leaf.*;
        next_leaf.* += 1;
        return node;
    }

    // Group node: recurse into exactly num_children subtrees.
    const children = try allocator.alloc(types.SchemaNode, num_children);
    var built: usize = 0;
    errdefer {
        for (0..built) |i| children[i].deinit(allocator);
        allocator.free(children);
    }
    for (0..num_children) |i| {
        children[i] = try buildNode(allocator, schema, cursor, next_leaf, my_def, my_rep);
        built += 1;
    }
    node.children = children;
    node.max_def = my_def;
    node.max_rep = my_rep;
    // Retain the group's converted/logical annotations (e.g. a LIST group's
    // converted_type=.list / modern .list logical, or MAP's). Reads classify by
    // STRUCTURE so this does not change read behavior, but it makes the
    // SchemaNode complete so flattenSchemaTree reproduces conformant LIST/MAP
    // annotations on write. Harmless on plain STRUCT groups (both null).
    node.converted = elem.converted_type;
    node.logical = elem.logical_type;
    node.type_length = elem.type_length;
    node.scale = elem.scale;
    node.precision = elem.precision;
    return node;
}

/// Inverse of `buildSchemaTree`: serialize a SchemaNode subtree into the flat,
/// PRE-ORDER `SchemaElement` list parquet stores (node first, then each child
/// recursively). Group nodes carry `num_children`; leaves carry the physical
/// type / annotations and a null `num_children`. Names are BORROWED from the
/// node (SchemaElement.name is `[]const u8`), so the produced slice must not
/// outlive `node`. The returned slice is allocator-owned (free with
/// `allocator.free`); the elements themselves hold no separate allocations.
pub fn flattenSchemaTree(allocator: Allocator, node: *const types.SchemaNode, out: *std.ArrayList(metadata.SchemaElement)) !void {
    const is_leaf = node.isLeaf();
    try out.append(allocator, .{
        .type_ = node.physical,
        .type_length = node.type_length,
        .repetition_type = node.repetition,
        .name = node.name,
        .num_children = if (is_leaf) null else @intCast(node.children.len),
        .converted_type = node.converted,
        .scale = node.scale,
        .precision = node.precision,
        .logical_type = node.logical,
    });
    for (node.children) |*child| {
        try flattenSchemaTree(allocator, child, out);
    }
}

/// Collect every leaf of the schema tree (flat + nested) in leaf_index order,
/// producing LeafColumns ready for `readColumnChunk`. A leaf is "flat" iff it
/// is a direct child of the root AND max_rep == 0; everything else is "nested"
/// (descends from a LIST/MAP/STRUCT subtree). Nested leaves carry an owned
/// dotted path (e.g. "l.list.element") and the index of the root child they
/// descend from. Caller must free via `freeLeaves`.
fn collectLeaves(allocator: Allocator, tree: *const types.SchemaNode) ![]column_reader.LeafColumn {
    var leaves = std.ArrayList(column_reader.LeafColumn).empty;
    errdefer {
        for (leaves.items) |*l| if (l.dotted_path) |p| allocator.free(p);
        leaves.deinit(allocator);
    }

    // Pre-build a slot array indexed by leaf_index so we surface leaves in
    // exact column-chunk order regardless of recursion order.
    for (tree.children, 0..) |*child, root_child| {
        try collectFromNode(allocator, child, root_child, child, &leaves);
    }

    // Sort by leaf_index (pre-order already yields this, but be explicit).
    std.sort.pdq(column_reader.LeafColumn, leaves.items, {}, struct {
        fn lt(_: void, a: column_reader.LeafColumn, b: column_reader.LeafColumn) bool {
            return a.column_index < b.column_index;
        }
    }.lt);

    return try leaves.toOwnedSlice(allocator);
}

fn collectFromNode(
    allocator: Allocator,
    node: *const types.SchemaNode,
    root_child: usize,
    root_child_node: *const types.SchemaNode,
    leaves: *std.ArrayList(column_reader.LeafColumn),
) !void {
    if (node.isLeaf()) {
        // Flat iff the leaf IS the root child itself (parent == root) and
        // never repeats. Otherwise it descends from a nested subtree.
        const is_flat = (node == root_child_node) and node.max_rep == 0;
        var dotted: ?[]const u8 = null;
        if (!is_flat) {
            dotted = try buildDottedPath(allocator, root_child_node, node);
        }
        errdefer if (dotted) |p| allocator.free(p);
        try leaves.append(allocator, .{
            .schema_element = .{
                .type_ = node.physical,
                .type_length = node.type_length,
                .repetition_type = node.repetition,
                .name = node.name,
                .converted_type = node.converted,
                .scale = node.scale,
                .precision = node.precision,
                .logical_type = node.logical,
            },
            .max_def_level = node.max_def,
            .max_rep_level = node.max_rep,
            .column_index = node.leaf_index.?,
            .nested = !is_flat,
            .root_child_index = root_child,
            .dotted_path = dotted,
        });
        return;
    }
    for (node.children) |*child| {
        try collectFromNode(allocator, child, root_child, root_child_node, leaves);
    }
}

/// Build the dotted path from a root child down to a leaf, e.g.
/// "l.list.element". Walks the subtree rooted at `root_child_node` to find
/// `target` (the tree is small; a path search is fine).
fn buildDottedPath(
    allocator: Allocator,
    root_child_node: *const types.SchemaNode,
    target: *const types.SchemaNode,
) ![]const u8 {
    var parts = std.ArrayList([]const u8).empty;
    defer parts.deinit(allocator);
    if (!findPath(root_child_node, target, allocator, &parts)) {
        return error.CorruptFile;
    }
    // parts is leaf-first; join in root→leaf order.
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(allocator);
    var i: usize = parts.items.len;
    while (i > 0) {
        i -= 1;
        if (buf.items.len > 0) try buf.append(allocator, '.');
        try buf.appendSlice(allocator, parts.items[i]);
    }
    return try buf.toOwnedSlice(allocator);
}

/// Depth-first search for `target`; on success `parts` holds the names from
/// `target` up to (and including) `node`, leaf-first.
fn findPath(
    node: *const types.SchemaNode,
    target: *const types.SchemaNode,
    allocator: Allocator,
    parts: *std.ArrayList([]const u8),
) bool {
    if (node == target) {
        parts.append(allocator, node.name) catch return false;
        return true;
    }
    for (node.children) |*child| {
        if (findPath(child, target, allocator, parts)) {
            parts.append(allocator, node.name) catch return false;
            return true;
        }
    }
    return false;
}

/// Free the owned dotted paths and the leaf slice returned by `collectLeaves`.
fn freeLeaves(allocator: Allocator, leaves: []column_reader.LeafColumn) void {
    for (leaves) |*l| if (l.dotted_path) |p| allocator.free(p);
    allocator.free(leaves);
}

/// Owned (tree, leaves) pair returned by `resolveSchema`. The leaves reference
/// names inside `tree`, so the tree must outlive them. `deinit` frees both.
const ResolvedSchema = struct {
    tree: types.SchemaNode,
    leaves: []column_reader.LeafColumn,
    allocator: Allocator,

    fn deinit(self: *ResolvedSchema) void {
        freeLeaves(self.allocator, self.leaves);
        self.tree.deinit(self.allocator);
    }
};

/// Build the schema tree and collect its leaves together. Used by unit tests;
/// production `readParquet` builds + partitions the pieces inline so it can
/// keep the tree in the result.
fn resolveSchema(allocator: Allocator, schema: []const metadata.SchemaElement) !ResolvedSchema {
    var tree = try buildSchemaTree(allocator, schema);
    errdefer tree.deinit(allocator);
    const leaves = try collectLeaves(allocator, &tree);
    return .{ .tree = tree, .leaves = leaves, .allocator = allocator };
}

// ============================================================
// Tests
// ============================================================

test "readParquet: logical_type and type_length surface on ParquetColumn" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    try std.testing.expect(result.columns[0].logical_type.? == .date);
    try std.testing.expect(result.columns[3].logical_type.? == .decimal);
    try std.testing.expect(result.columns[1].logical_type.? == .time);
    try std.testing.expect(result.columns[2].logical_type.? == .timestamp);

    // flba.parquet: FIXED_LEN_BYTE_ARRAY(4) → type_length must surface too
    const flba_data = try cwd.readFileAlloc(io, "data/flba.parquet", allocator, .unlimited);
    defer allocator.free(flba_data);
    var flba = try readParquet(allocator, flba_data);
    defer flba.deinit();
    try std.testing.expectEqual(@as(i32, 4), flba.columns[0].type_length.?);
}

test "readParquet: multi-row-group file concatenates all row groups" {
    const allocator = std.testing.allocator;

    // data/multi_rowgroup.parquet: 7 rows written in 3 row groups (sizes 3,3,1)
    // via pyarrow with columns id:int64, price:double, name:string, opt:int64?(nullable).
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/multi_rowgroup.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // All 3 row groups must be concatenated into 7 rows × 4 columns.
    try std.testing.expectEqual(@as(usize, 7), result.num_rows);
    try std.testing.expectEqual(@as(usize, 4), result.columns.len);

    try std.testing.expectEqualStrings("id", result.columns[0].name);
    try std.testing.expectEqualStrings("price", result.columns[1].name);
    try std.testing.expectEqualStrings("name", result.columns[2].name);
    try std.testing.expectEqualStrings("opt", result.columns[3].name);

    // int64 spanning all 3 row groups
    const ids = result.columns[0].int64s orelse return error.MissingData;
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3, 4, 5, 6, 7 }, ids);

    // double spanning all 3 row groups
    const prices = result.columns[1].doubles orelse return error.MissingData;
    try std.testing.expectEqualSlices(f64, &.{ 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5 }, prices);

    // byte_array (string) — ownership of inner slices is moved across groups
    const names = result.columns[2].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), names.len);
    try std.testing.expectEqualStrings("a", names[0]);
    try std.testing.expectEqualStrings("ccc", names[2]); // last row of group 1
    try std.testing.expectEqualStrings("dddd", names[3]); // first row of group 2
    try std.testing.expectEqualStrings("ggg", names[6]); // sole row of group 3

    // nullable int64 — validity must concatenate across groups too
    const opt = result.columns[3];
    try std.testing.expect(opt.is_optional);
    const valid = opt.validity orelse return error.MissingValidity;
    try std.testing.expectEqualSlices(bool, &.{ true, false, true, false, true, true, false }, valid);
    const opt_vals = opt.int64s orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), opt_vals.len);
    try std.testing.expectEqual(@as(i64, 10), opt_vals[0]);
    try std.testing.expectEqual(@as(i64, 30), opt_vals[2]);
    try std.testing.expectEqual(@as(i64, 50), opt_vals[4]);
    try std.testing.expectEqual(@as(i64, 60), opt_vals[5]);
}

test "readParquet: FIXED_LEN_BYTE_ARRAY reads raw fixed-width bytes" {
    const allocator = std.testing.allocator;

    // data/flba.parquet: one column "fb" of FIXED_LEN_BYTE_ARRAY(4): abcd/efgh/ijkl
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/flba.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    try std.testing.expectEqual(@as(usize, 1), result.columns.len);
    try std.testing.expectEqual(types.PhysicalType.fixed_len_byte_array, result.columns[0].physical_type);

    const vals = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 3), vals.len);
    try std.testing.expectEqualStrings("abcd", vals[0]);
    try std.testing.expectEqualStrings("efgh", vals[1]);
    try std.testing.expectEqualStrings("ijkl", vals[2]);
}

test "readParquet: INT96 column reads as raw 12-byte values" {
    const allocator = std.testing.allocator;

    // data/int96.parquet: one column "t" of deprecated INT96 timestamps (3 rows)
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.num_rows);
    try std.testing.expectEqual(@as(usize, 1), result.columns.len);
    try std.testing.expectEqual(types.PhysicalType.int96, result.columns[0].physical_type);

    // Raw preservation: every value is exactly 12 bytes (semantic decode is Phase 6d).
    const vals = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 3), vals.len);
    for (vals) |v| try std.testing.expectEqual(@as(usize, 12), v.len);
}

test "readParquet: addresses.parquet (uncompressed)" {
    const allocator = std.testing.allocator;

    // Read the file at runtime
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // addresses.parquet: 7 columns × 7 rows
    try std.testing.expectEqual(@as(usize, 7), result.num_rows);
    try std.testing.expectEqual(@as(usize, 7), result.columns.len);

    // Check column names
    try std.testing.expectEqualStrings("First Name", result.columns[0].name);
    try std.testing.expectEqualStrings("Last Name", result.columns[1].name);
    try std.testing.expectEqualStrings("Age", result.columns[2].name);
    try std.testing.expectEqualStrings("Address", result.columns[3].name);
    try std.testing.expectEqualStrings("City", result.columns[4].name);
    try std.testing.expectEqualStrings("State", result.columns[5].name);
    try std.testing.expectEqualStrings("Zip", result.columns[6].name);

    // Check first column values (First Name = byte_array)
    const first_names = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), first_names.len);
    try std.testing.expectEqualStrings("John", first_names[0]);
    try std.testing.expectEqualStrings("Jack", first_names[1]);

    // Check Age column (int64 as written by parquet-cpp-arrow)
    const ages = result.columns[2].int64s orelse return error.MissingData;
    try std.testing.expectEqual(@as(usize, 7), ages.len);
    try std.testing.expectEqual(@as(i64, 52), ages[0]); // John Doe
    try std.testing.expectEqual(@as(i64, 23), ages[1]); // Jack McGinnis
    try std.testing.expectEqual(@as(i64, 38), ages[2]); // John "Da Man" Repici
    try std.testing.expectEqual(@as(i64, 96), ages[3]); // Stephen Tyler

    // Check City column (byte_array)
    const cities = result.columns[4].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualStrings("Riverside", cities[0]);
    try std.testing.expectEqualStrings("Phila", cities[1]);
}

test "readParquet: addresses_snappy.parquet (snappy compressed)" {
    const allocator = std.testing.allocator;

    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses_snappy.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // Same data as uncompressed
    try std.testing.expectEqual(@as(usize, 7), result.num_rows);
    try std.testing.expectEqual(@as(usize, 7), result.columns.len);

    // Check first column
    try std.testing.expectEqualStrings("First Name", result.columns[0].name);
    const first_names = result.columns[0].byte_arrays orelse return error.MissingData;
    try std.testing.expectEqualStrings("John", first_names[0]);

    // Check Age column (int64)
    const ages = result.columns[2].int64s orelse return error.MissingData;
    try std.testing.expectEqual(@as(i64, 52), ages[0]);
}

test "resolveSchema: flat schema" {
    const allocator = std.testing.allocator;
    // Root + 2 leaf columns
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "col_a", .type_ = .int32, .repetition_type = .required },
        .{ .name = "col_b", .type_ = .byte_array, .repetition_type = .optional },
    };
    var resolved = try resolveSchema(allocator, &schema);
    defer resolved.deinit();
    const leaves = resolved.leaves;

    try std.testing.expectEqual(@as(usize, 2), leaves.len);
    try std.testing.expectEqualStrings("col_a", leaves[0].schema_element.name);
    try std.testing.expectEqual(@as(u8, 0), leaves[0].max_def_level);
    try std.testing.expectEqualStrings("col_b", leaves[1].schema_element.name);
    try std.testing.expectEqual(@as(u8, 1), leaves[1].max_def_level);
}

test "resolveSchema: empty schema returns error" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{};
    try std.testing.expectError(error.EmptySchema, resolveSchema(allocator, &schema));
}

test "resolveSchema: all required columns" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "id", .type_ = .int64, .repetition_type = .required },
        .{ .name = "value", .type_ = .double, .repetition_type = .required },
        .{ .name = "flag", .type_ = .boolean, .repetition_type = .required },
    };
    var resolved = try resolveSchema(allocator, &schema);
    defer resolved.deinit();
    const leaves = resolved.leaves;

    try std.testing.expectEqual(@as(usize, 3), leaves.len);
    for (leaves) |leaf| {
        try std.testing.expectEqual(@as(u8, 0), leaf.max_def_level);
    }
    try std.testing.expectEqual(@as(usize, 0), leaves[0].column_index);
    try std.testing.expectEqual(@as(usize, 1), leaves[1].column_index);
    try std.testing.expectEqual(@as(usize, 2), leaves[2].column_index);
}

test "resolveSchema: all optional columns" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "a", .type_ = .int32, .repetition_type = .optional },
        .{ .name = "b", .type_ = .byte_array, .repetition_type = .optional },
    };
    var resolved = try resolveSchema(allocator, &schema);
    defer resolved.deinit();
    const leaves = resolved.leaves;

    try std.testing.expectEqual(@as(usize, 2), leaves.len);
    try std.testing.expectEqual(@as(u8, 1), leaves[0].max_def_level);
    try std.testing.expectEqual(@as(u8, 1), leaves[1].max_def_level);
}

test "resolveSchema: single column" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "only_col", .type_ = .float, .repetition_type = .required },
    };
    var resolved = try resolveSchema(allocator, &schema);
    defer resolved.deinit();
    const leaves = resolved.leaves;

    try std.testing.expectEqual(@as(usize, 1), leaves.len);
    try std.testing.expectEqualStrings("only_col", leaves[0].schema_element.name);
}

// ============================================================
// Phase 6d-2b.0 — schema tree + level streams (nested groundwork)
// ============================================================

test "buildSchemaTree: flat schema → levels 0/1, max_rep 0, leaf_index order" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "col_a", .type_ = .int32, .repetition_type = .required },
        .{ .name = "col_b", .type_ = .byte_array, .repetition_type = .optional },
    };
    var tree = try buildSchemaTree(allocator, &schema);
    defer tree.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), tree.children.len);
    // col_a: required → max_def 0; col_b: optional → max_def 1; both flat.
    const a = tree.children[0];
    const b = tree.children[1];
    try std.testing.expect(a.isLeaf() and b.isLeaf());
    try std.testing.expectEqual(@as(u8, 0), a.max_def);
    try std.testing.expectEqual(@as(u8, 0), a.max_rep);
    try std.testing.expectEqual(@as(?usize, 0), a.leaf_index);
    try std.testing.expectEqual(@as(u8, 1), b.max_def);
    try std.testing.expectEqual(@as(u8, 0), b.max_rep);
    try std.testing.expectEqual(@as(?usize, 1), b.leaf_index);
    // fieldIndex lookup
    try std.testing.expectEqual(@as(?usize, 1), tree.fieldIndex("col_b"));
    try std.testing.expectEqual(@as(?usize, null), tree.fieldIndex("nope"));
}

test "buildSchemaTree: 3-level LIST → element max_def 3, max_rep 1" {
    const allocator = std.testing.allocator;
    // root → optional group l { repeated group list { optional int64 element } }
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "l", .repetition_type = .optional, .num_children = 1, .converted_type = .list },
        .{ .name = "list", .repetition_type = .repeated, .num_children = 1 },
        .{ .name = "element", .type_ = .int64, .repetition_type = .optional },
    };
    var tree = try buildSchemaTree(allocator, &schema);
    defer tree.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), tree.children.len);
    const l = tree.children[0];
    try std.testing.expect(!l.isLeaf());
    try std.testing.expectEqual(@as(u8, 1), l.max_def); // optional group
    try std.testing.expectEqual(@as(u8, 0), l.max_rep);
    const list = l.children[0];
    try std.testing.expectEqual(@as(u8, 2), list.max_def); // optional + repeated
    try std.testing.expectEqual(@as(u8, 1), list.max_rep); // repeated
    const element = list.children[0];
    try std.testing.expect(element.isLeaf());
    // def: l(optional)+1, list(repeated)+1, element(optional)+1 = 3.
    try std.testing.expectEqual(@as(u8, 3), element.max_def);
    // rep: only list is repeated = 1.
    try std.testing.expectEqual(@as(u8, 1), element.max_rep);
    try std.testing.expectEqual(@as(?usize, 0), element.leaf_index);
}

test "buildSchemaTree: STRUCT → a max_def 1, b max_def 2, both max_rep 0" {
    const allocator = std.testing.allocator;
    // root → optional group s { required int64 a; optional byte_array b }
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "s", .repetition_type = .optional, .num_children = 2 },
        .{ .name = "a", .type_ = .int64, .repetition_type = .required },
        .{ .name = "b", .type_ = .byte_array, .repetition_type = .optional },
    };
    var tree = try buildSchemaTree(allocator, &schema);
    defer tree.deinit(allocator);

    const s = tree.children[0];
    try std.testing.expect(!s.isLeaf());
    const a = s.children[0];
    const b = s.children[1];
    // a: s(optional)+1, a(required)+0 = 1.
    try std.testing.expectEqual(@as(u8, 1), a.max_def);
    try std.testing.expectEqual(@as(u8, 0), a.max_rep);
    // b: s(optional)+1, b(optional)+1 = 2.
    try std.testing.expectEqual(@as(u8, 2), b.max_def);
    try std.testing.expectEqual(@as(u8, 0), b.max_rep);
    try std.testing.expectEqual(@as(?usize, 0), a.leaf_index);
    try std.testing.expectEqual(@as(?usize, 1), b.leaf_index);
    try std.testing.expectEqual(@as(?usize, 1), s.fieldIndex("b"));
}

test "flattenSchemaTree: inverse of buildSchemaTree on a nested LIST schema" {
    const allocator = std.testing.allocator;
    // root → optional group l (list) { repeated group list { optional int64 element } }
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "l", .repetition_type = .optional, .num_children = 1, .converted_type = .list },
        .{ .name = "list", .repetition_type = .repeated, .num_children = 1 },
        .{ .name = "element", .type_ = .int64, .repetition_type = .optional },
    };
    var tree = try buildSchemaTree(allocator, &schema);
    defer tree.deinit(allocator);

    var out = std.ArrayList(metadata.SchemaElement).empty;
    defer out.deinit(allocator);
    try flattenSchemaTree(allocator, &tree, &out);

    try std.testing.expectEqual(schema.len, out.items.len);
    for (schema, 0..) |want, i| {
        const got = out.items[i];
        try std.testing.expectEqualStrings(want.name, got.name);
        try std.testing.expectEqual(want.type_, got.type_);
        // buildSchemaTree defaults a missing repetition_type to .required, so
        // the flattened root surfaces .required (input left it null).
        const want_rep = want.repetition_type orelse .required;
        try std.testing.expectEqual(want_rep, got.repetition_type.?);
        // num_children: groups carry it, leaves are null. The input root/groups
        // carry it; the input leaf carries null — matches.
        try std.testing.expectEqual(want.num_children, got.num_children);
    }
    // buildSchemaTree now retains converted_type/logical_type on GROUP nodes
    // too (Phase 13.1 read-side fix), so the LIST marker resurfaces on flatten —
    // this is what makes written nested files spec-conformant.
    try std.testing.expectEqual(types.ConvertedType.list, out.items[1].converted_type.?);
    // The leaf's own physical type round-trips:
    try std.testing.expectEqual(types.PhysicalType.int64, out.items[3].type_.?);
}

test "flattenSchemaTree: buildSchemaTree(flatten(tree)) reproduces the tree (STRUCT)" {
    const allocator = std.testing.allocator;
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "s", .repetition_type = .optional, .num_children = 2 },
        .{ .name = "a", .type_ = .int64, .repetition_type = .required },
        .{ .name = "b", .type_ = .byte_array, .repetition_type = .optional },
    };
    var tree = try buildSchemaTree(allocator, &schema);
    defer tree.deinit(allocator);

    var out = std.ArrayList(metadata.SchemaElement).empty;
    defer out.deinit(allocator);
    try flattenSchemaTree(allocator, &tree, &out);

    var tree2 = try buildSchemaTree(allocator, out.items);
    defer tree2.deinit(allocator);

    // Same shape and per-leaf levels.
    try std.testing.expectEqualStrings(tree.name, tree2.name);
    try std.testing.expectEqual(tree.children.len, tree2.children.len);
    const s1 = tree.children[0];
    const s2 = tree2.children[0];
    try std.testing.expectEqualStrings(s1.name, s2.name);
    try std.testing.expectEqual(s1.children.len, s2.children.len);
    try std.testing.expectEqual(s1.children[0].max_def, s2.children[0].max_def);
    try std.testing.expectEqual(s1.children[1].max_def, s2.children[1].max_def);
    try std.testing.expectEqual(s1.children[0].physical, s2.children[0].physical);
    try std.testing.expectEqual(s1.children[1].physical, s2.children[1].physical);
}

test "buildSchemaTree: num_children overrun → CorruptFile" {
    const allocator = std.testing.allocator;
    // root claims 2 children but only 1 follows.
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 2 },
        .{ .name = "only", .type_ = .int32, .repetition_type = .required },
    };
    try std.testing.expectError(error.CorruptFile, buildSchemaTree(allocator, &schema));
}

test "readParquet: nested_smoke.parquet — flat siblings read correctly, nested leaves carry level streams" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/nested_smoke.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.num_rows);

    // --- Flat columns: ONLY flat_before + flat_after, at the CORRECT indices.
    // This is the latent-bug regression: previously a file mixing nested and
    // flat columns mis-assigned column_index; now flat siblings decode right.
    try std.testing.expectEqual(@as(usize, 2), result.columns.len);
    try std.testing.expectEqualStrings("flat_before", result.columns[0].name);
    try std.testing.expectEqualStrings("flat_after", result.columns[1].name);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 3 }, result.columns[0].int64s.?);
    const fa = result.columns[1].byte_arrays.?;
    try std.testing.expectEqual(@as(usize, 3), fa.len);
    try std.testing.expectEqualStrings("p", fa[0]);
    try std.testing.expectEqualStrings("q", fa[1]);
    try std.testing.expectEqualStrings("r", fa[2]);

    // --- Schema tree: 4 top-level children with the right kinds.
    const tree = result.schema_tree.?;
    try std.testing.expectEqual(@as(usize, 4), tree.children.len);
    try std.testing.expectEqualStrings("flat_before", tree.children[0].name);
    try std.testing.expect(tree.children[0].isLeaf());
    try std.testing.expectEqualStrings("l", tree.children[1].name);
    try std.testing.expect(!tree.children[1].isLeaf()); // LIST group
    try std.testing.expectEqualStrings("s", tree.children[2].name);
    try std.testing.expect(!tree.children[2].isLeaf()); // STRUCT group
    try std.testing.expectEqual(@as(usize, 2), tree.children[2].children.len);
    try std.testing.expectEqualStrings("flat_after", tree.children[3].name);
    try std.testing.expect(tree.children[3].isLeaf());

    // --- Nested columns: the list/struct leaves, with raw def/rep streams.
    try std.testing.expectEqual(@as(usize, 3), result.nested_columns.len);

    // Locate by dotted name (collection order follows leaf_index).
    var el: ?types.ParquetColumn = null;
    var sa: ?types.ParquetColumn = null;
    var sb: ?types.ParquetColumn = null;
    for (result.nested_columns) |c| {
        if (std.mem.eql(u8, c.name, "l.list.element")) el = c;
        if (std.mem.eql(u8, c.name, "s.a")) sa = c;
        if (std.mem.eql(u8, c.name, "s.b")) sb = c;
    }

    // l.list.element — max_def 3, max_rep 1, root_child 1.
    // Rows: [1,2], None, [].  Pinned against pyarrow's actual streams:
    //   row0 [1,2]: elem0 rep0 def3 (new record, fully defined),
    //               elem1 rep1 def3 (same list, defined)            → {3,3}/{0,1}
    //   row1 None : null list → def0 rep0                            → {0}/{0}
    //   row2 []   : empty list, l present → def1 rep0                → {1}/{0}
    //   concatenated: def {3,3,0,1}, rep {0,1,0,0}; present (def==3)=2.
    const e = el.?;
    try std.testing.expectEqual(@as(usize, 1), e.root_child_index);
    try std.testing.expectEqualSlices(u16, &.{ 3, 3, 0, 1 }, e.def_levels.?);
    try std.testing.expectEqualSlices(u16, &.{ 0, 1, 0, 0 }, e.rep_levels.?);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, e.int64s.?);

    // s.a — max_def 2, max_rep 0, root_child 2.
    //   {a:1},{a:2},None → s present/a present=2, s present/a present=2, s null=0
    //   def {2,2,0}; rep stream empty (max_rep 0); present (def==2)=2.
    const a = sa.?;
    try std.testing.expectEqual(@as(usize, 2), a.root_child_index);
    try std.testing.expectEqualSlices(u16, &.{ 2, 2, 0 }, a.def_levels.?);
    try std.testing.expectEqual(@as(usize, 0), a.rep_levels.?.len);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, a.int64s.?);

    // s.b — max_def 2, max_rep 0, root_child 2.
    //   b="x", b=None(s present), None(s null) → 2, 1, 0
    //   def {2,1,0}; present (def==2)=1 → only "x".
    const b = sb.?;
    try std.testing.expectEqual(@as(usize, 2), b.root_child_index);
    try std.testing.expectEqualSlices(u16, &.{ 2, 1, 0 }, b.def_levels.?);
    try std.testing.expectEqual(@as(usize, 0), b.rep_levels.?.len);
    const bvals = b.byte_arrays.?;
    try std.testing.expectEqual(@as(usize, 1), bvals.len);
    try std.testing.expectEqualStrings("x", bvals[0]);
}

test "FileMetaData: logical_annotations.parquet parses LogicalType (field 10)" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    // Footer: [...data...][footer][4-byte LE len]["PAR1"]
    const footer_len: usize = @intCast(std.mem.readInt(u32, file_data[file_data.len - 8 ..][0..4], .little));
    if (footer_len + 8 > file_data.len) return error.TestUnexpectedResult;
    const footer_start = file_data.len - 8 - footer_len;
    var thrift_reader = ThriftReader.init(file_data[footer_start .. footer_start + footer_len]);
    var fmd = try metadata.FileMetaData.decode(&thrift_reader, allocator);
    defer fmd.deinit(allocator);

    // schema[0] is the root; leaves follow in column order d, t, ts, dec.
    try std.testing.expectEqual(@as(usize, 5), fmd.schema.len);
    try std.testing.expect(fmd.schema[1].logical_type.? == .date);
    try std.testing.expectEqualDeep(
        types.TimeParams{ .is_adjusted_to_utc = false, .unit = .micros },
        fmd.schema[2].logical_type.?.time,
    );
    try std.testing.expectEqualDeep(
        types.TimestampParams{ .is_adjusted_to_utc = true, .unit = .micros },
        fmd.schema[3].logical_type.?.timestamp,
    );
    try std.testing.expectEqualDeep(
        types.DecimalParams{ .scale = 2, .precision = 10 },
        fmd.schema[4].logical_type.?.decimal,
    );
}

test "readParquet: invalid magic returns error" {
    const allocator = std.testing.allocator;
    // Valid size (12 bytes) but wrong magic at start
    const data = [_]u8{ 'N', 'O', 'P', 'E', 0x00, 0x00, 0x00, 0x00, 'P', 'A', 'R', '1' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: wrong trailing magic returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 'P', 'A', 'R', '1', 0x00, 0x00, 0x00, 0x00, 'N', 'O', 'P', 'E' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: file too small returns error" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 'P', 'A', 'R', '1' };
    try std.testing.expectError(error.InvalidParquetFile, readParquet(allocator, &data));
}

test "readParquet: addresses.parquet all column types" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try readParquet(allocator, file_data);
    defer result.deinit();

    // Verify all string columns have correct physical type
    for ([_]usize{ 0, 1, 3, 4, 5 }) |col_idx| {
        try std.testing.expectEqual(types.PhysicalType.byte_array, result.columns[col_idx].physical_type);
        try std.testing.expect(result.columns[col_idx].byte_arrays != null);
    }

    // Age is int64
    try std.testing.expectEqual(types.PhysicalType.int64, result.columns[2].physical_type);
    try std.testing.expect(result.columns[2].int64s != null);

    // Zip is int64
    try std.testing.expectEqual(types.PhysicalType.int64, result.columns[6].physical_type);
    try std.testing.expect(result.columns[6].int64s != null);

    // Check all age values
    const ages = result.columns[2].int64s.?;
    try std.testing.expectEqual(@as(i64, 52), ages[0]);
    try std.testing.expectEqual(@as(i64, 23), ages[1]);
    try std.testing.expectEqual(@as(i64, 38), ages[2]);
    try std.testing.expectEqual(@as(i64, 96), ages[3]);
    try std.testing.expectEqual(@as(i64, 14), ages[4]);
    try std.testing.expectEqual(@as(i64, 56), ages[5]);
    try std.testing.expectEqual(@as(i64, 14), ages[6]);

    // Check all last names
    const last_names = result.columns[1].byte_arrays.?;
    try std.testing.expectEqualStrings("Doe", last_names[0]);
    try std.testing.expectEqualStrings("McGinnis", last_names[1]);
    try std.testing.expectEqualStrings("Repici", last_names[2]);
    try std.testing.expectEqualStrings("Tyler", last_names[3]);
    try std.testing.expectEqualStrings("Blankman", last_names[4]);
    try std.testing.expectEqualStrings("Jet", last_names[5]);
    try std.testing.expectEqualStrings("Blankman", last_names[6]);

    // Check special characters in address
    const addresses = result.columns[3].byte_arrays.?;
    try std.testing.expectEqualStrings("120 jefferson st.", addresses[0]);
    try std.testing.expectEqualStrings("220 hobo Av.", addresses[1]);
}

// ============================================================
// Phase 11 Unit B — malformed-file hardening (file level)
// ============================================================

const ThriftWriter = @import("thrift_writer.zig").ThriftWriter;
const column_writer = @import("column_writer.zig");
const parquet_writer = @import("parquet_writer.zig");

test "P2: corrupt v1 def-level length prefix returns CorruptPageData (not panic)" {
    const allocator = std.testing.allocator;

    // Write a valid OPTIONAL int64 column (uncompressed → def-level length prefix
    // is plaintext in the page body). DataPage v1 with max_rep_level==0: the page
    // body begins with the 4-byte LE def-level length prefix.
    const vals = [_]i64{ 10, 0, 30 };
    const valid = [_]bool{ true, false, true };
    const cols = [_]parquet_writer.WriteColumn{.{ .flat = .{
        .name = "x",
        .physical_type = .int64,
        .int64s = &vals,
        .num_values = 3,
        .validity = &valid,
    } }};
    const output = try parquet_writer.writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    // Sanity: it reads cleanly before corruption.
    {
        var ok = try readParquet(allocator, output);
        ok.deinit();
    }

    // Locate the column chunk's first page: decode the footer, take the chunk's
    // data_page_offset, then decode the page header to find where the body (and
    // thus the def-length prefix) begins.
    const footer_len: usize = @intCast(std.mem.readInt(u32, output[output.len - 8 ..][0..4], .little));
    const footer_start = output.len - 8 - footer_len;
    var tr = ThriftReader.init(output[footer_start .. footer_start + footer_len]);
    var fmd = try metadata.FileMetaData.decode(&tr, allocator);
    defer fmd.deinit(allocator);

    const cmd = fmd.row_groups[0].columns[0].meta_data.?;
    const page_off: usize = @intCast(cmd.data_page_offset);
    var ph_reader = ThriftReader.init(output[page_off..]);
    _ = try metadata.PageHeader.decode(&ph_reader);
    const body_off = page_off + ph_reader.pos; // first byte of the page body

    // Corrupt the 4-byte LE def-level length prefix to a huge value so the
    // def-level slice would run past the page → CorruptPageData, never a panic.
    const corrupt = try allocator.dupe(u8, output);
    defer allocator.free(corrupt);
    std.mem.writeInt(u32, corrupt[body_off..][0..4], 0xFFFF_FFFF, .little);

    try std.testing.expectError(error.CorruptPageData, readParquet(allocator, corrupt));
}

test "P4: negative num_rows in FileMetaData returns CorruptFile (not panic)" {
    const allocator = std.testing.allocator;

    // Build a minimal but structurally valid FileMetaData with num_rows = -1.
    // schema: root(num_children=1) + one required int32 leaf; one (empty) row group.
    const schema = [_]metadata.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "c", .type_ = .int32, .repetition_type = .required },
    };
    const fmd = metadata.FileMetaData{
        .version = 1,
        .schema = @constCast(schema[0..]),
        .num_rows = -1, // corrupt
        .row_groups = &.{},
    };

    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try fmd.encode(&w);
    const footer = w.written();

    // Assemble: PAR1 + footer + 4-byte LE footer_len + PAR1.
    var file = std.ArrayList(u8).empty;
    defer file.deinit(allocator);
    try file.appendSlice(allocator, "PAR1");
    try file.appendSlice(allocator, footer);
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer.len), .little);
    try file.appendSlice(allocator, &len_bytes);
    try file.appendSlice(allocator, "PAR1");

    try std.testing.expectError(error.CorruptFile, readParquet(allocator, file.items));
}

test "readParquet: snappy and uncompressed produce same data" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();

    const uncompressed_data = try cwd.readFileAlloc(io, "data/addresses.parquet", allocator, .unlimited);
    defer allocator.free(uncompressed_data);
    var result_uc = try readParquet(allocator, uncompressed_data);
    defer result_uc.deinit();

    const snappy_data = try cwd.readFileAlloc(io, "data/addresses_snappy.parquet", allocator, .unlimited);
    defer allocator.free(snappy_data);
    var result_sn = try readParquet(allocator, snappy_data);
    defer result_sn.deinit();

    // Same dimensions
    try std.testing.expectEqual(result_uc.num_rows, result_sn.num_rows);
    try std.testing.expectEqual(result_uc.columns.len, result_sn.columns.len);

    // Same column names and types
    for (0..result_uc.columns.len) |i| {
        try std.testing.expectEqualStrings(result_uc.columns[i].name, result_sn.columns[i].name);
        try std.testing.expectEqual(result_uc.columns[i].physical_type, result_sn.columns[i].physical_type);
    }

    // Same age values
    const ages_uc = result_uc.columns[2].int64s.?;
    const ages_sn = result_sn.columns[2].int64s.?;
    for (0..ages_uc.len) |i| {
        try std.testing.expectEqual(ages_uc[i], ages_sn[i]);
    }

    // Same first names
    const names_uc = result_uc.columns[0].byte_arrays.?;
    const names_sn = result_sn.columns[0].byte_arrays.?;
    for (0..names_uc.len) |i| {
        try std.testing.expectEqualStrings(names_uc[i], names_sn[i]);
    }
}
