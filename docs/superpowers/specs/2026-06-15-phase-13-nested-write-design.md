# Phase 13 — Nested Parquet WRITE (LIST / MAP / STRUCT)

**Status:** design + build (the deferred half of 6d-2b). Removes
`error.UnsupportedNestedWrite`. Drives to commit on green (no review gate
unless a design fork needs the user). The reader (6d-2b) defines the exact
on-wire contract this must produce — so correctness = "the existing reader
reads back what we write, identically."

## Goal & scope

teddy can WRITE nested LIST/MAP/STRUCT columns to parquet — completing
"read AND write almost anything." Headline: a nested file teddy read
round-trips back out identically (`parquet → df → parquet → df`).

**Scope decision (locked):** nested write requires the column to carry its
`SchemaNode` (`Series(Nested).meta.schema`). The round-trip case always has it
(it came from the read). A hand-built `Series(Nested)` WITHOUT a schema →
clean `error.NestedWriteRequiresSchema` (schema *synthesis* from bare values
is genuinely ambiguous — element nullability/names, empty-list element types —
and is deferred as its own follow-up, noted in the roadmap). Encoding: v1 data
pages, PLAIN values, RLE levels, no dictionary/v2.

## The inverse of assembly: shredding

Assembly (read) turned per-leaf `(values, def, rep)` streams into row trees.
Shredding (write) is the inverse: walk each row's `Nested` tree against the
`SchemaNode` and emit, per leaf, a `(rep, def, maybe-value)` sequence.

Level rules (same definitions as the reader — derive everything from these):
- `max_def` = count of optional|repeated nodes on the leaf's path.
- `max_rep` = count of repeated nodes on the leaf's path.
- A leaf entry has a value iff `def == max_def`.
- `rep` of the FIRST entry of a record is 0; deeper list continuations carry
  higher rep (the repeated-ancestor depth at which this entry continues).
- A null/absent ancestor or empty list emits ONE entry per descendant leaf
  recording the def at which definition stopped, no value, rep at the
  appropriate continuation depth (0 if it starts the record).

This is the precise mirror of `nested_assembly.assembleNode`'s shape dispatch
(list3 / list2 / map / strukt / leaf) — read it and invert each case.

## Implementation

### src/dataframe/nested_shred.zig (new)
```zig
pub const LeafStreams = struct {
    node: *const SchemaNode,   // the leaf's schema (physical/annotations)
    def: []u16, rep: []u16,    // owned; one entry per emitted slot
    // present values, in order, as a typed payload matching node.physical:
    values: ValuePayload,      // tagged: i32s/i64s/.../byte_arrays
};
pub fn shredColumn(allocator, node: *const SchemaNode, values: []const Nested,
                   num_rows: usize) ![]LeafStreams
```
Walk each of the `num_rows` `Nested` values against `node`, appending to each
descendant leaf's `(rep, def, value?)`. A `null_` top-level value (validity)
is the "null at the column root" case. Bound recursion (depth 64). Cross-
shape mismatch (value kind vs schema shape) → `error.NestedShredMismatch`
(defensive — for a teddy-read column this never happens). Leaf value payloads
carry the SEMANTIC value via the leaf's physical type (Date→i32, Timestamp→
i64 in its unit, Decimal→ its physical backing, String/Binary→ byte_arrays,
etc.) — reuse the read mapping in reverse (a `nestedScalarToLeaf` helper).

### Schema flattening (parquet_reader.zig or metadata.zig)
`flattenSchemaTree(allocator, node) -> []metadata.SchemaElement` — pre-order
(node, then children recursively), each carrying name/repetition/physical/
converted/logical/type_length/num_children. Inverse of `buildSchemaTree`.

### Level encoder generalization (encoding_writer.zig)
Generalize `encodeRleLevels` to a bit width: `encodeRleLevelsW(allocator,
bit_width: u8, levels: []const u32, out)`, writing each RLE run as
`varint(run_len<<1)` + the value in `ceil(bit_width/8)` bytes (LE). Keep the
existing `[]u1` entry as a thin wrapper (bit_width 1) so the flat path is
untouched. Mirror the reader's `levelBitWidth(max)`.

### ColumnData + writeColumn (column_writer.zig)
`ColumnData` gains: `rep_levels: ?[]const u32`, `def_levels: ?[]const u32`,
`max_rep: u8 = 0`, `max_def: u8 = 0`. When `def_levels != null` (a nested
leaf), `writeColumn` emits the body as `[4B rep_len][rep RLE @ bitWidth(max_rep)]
[4B def_len][def RLE @ bitWidth(max_def)][values]` (rep section only when
max_rep>0 — matches the reader's v1 read order), values = the present typed
values already filtered by shredding. The existing flat `validity` path is
unchanged (it's the max_def=1/max_rep=0 special case and stays as-is).

### Writer entry point (parquet_writer.zig)
Refactor `writeParquet` to accept top-level columns that may each expand to a
SCHEMA SUBTREE + MULTIPLE leaf chunks:
```zig
pub const WriteColumn = union(enum) {
    flat: ColumnData,                                   // one leaf
    nested: struct { schema: []metadata.SchemaElement,  // pre-order subtree
                     leaves: []ColumnData },             // leaf chunks, leaf order
};
pub fn writeParquet(allocator, cols: []const WriteColumn, options) ![]u8
```
Schema = `root{num_children = cols.len}` ++ (flat → its 1 element | nested →
its subtree, pre-order). RowGroup.columns = every leaf chunk in column order
(flat's 1, nested's many). `num_rows` = top-level row count. Keep a
back-compat path or update all call sites (writer.zig + tests) to the new
shape — flat columns wrap as `.{ .flat = cd }`.

### Adapter (dataframe/parquet.zig)
`boxedToColumnData` `.nested` arm: replace `error.UnsupportedNestedWrite` with
shredding — require `s.meta.schema` (else `error.NestedWriteRequiresSchema`);
`shredColumn` → build the `.nested` WriteColumn (flatten the schema subtree +
one ColumnData per leaf with its rep/def/values/max levels). `fromDataframe`
returns `[]WriteColumn` (flat arms wrap `.flat`). Thread through writer.zig.

## Tests
- shred unit tests mirroring the assembly pins: list<i64> rows
  `[1,2]/null/[]` → leaf def `{3,3,0,1}` rep `{0,1,0,0}` values `{1,2}`;
  struct, map, list<list>, list<struct> — the same fixtures, inverted.
- ROUND-TRIP (headline): read data/nested_kinds.parquet → df → writeParquet →
  readParquet → df2; assert df2 nested columns equal df (per-row asStringAt +
  isNull + typeName). Also nested_smoke.
- error: hand-built Series(Nested) without meta.schema → writeParquet →
  error.NestedWriteRequiresSchema.
- regression framework: add a parquet round-trip stage for the nested columns
  (read→df→parquet→df parity), and flip the JSON-nested-readback note where
  relevant (still a read gap, unchanged).
- full suite green; flat parquet writing unchanged (existing writer tests pin
  it); malformed battery still green.

## Slices
- **13.0** — infra: `encodeRleLevelsW`; `flattenSchemaTree`; ColumnData nested
  fields + writeColumn nested-leaf body; the `WriteColumn` union +
  `writeParquet` refactor (flat-only still works; nested path stubbed). Flat
  round-trip + existing tests stay green.
- **13.1** — `nested_shred.zig` + adapter wiring + the nested round-trip tests
  + regression parquet stage. Removes `error.UnsupportedNestedWrite`.

## Out of scope
Schema synthesis from bare `Nested` values (deferred); dictionary/v2 pages;
writing VARIANT/GEO (still Raw round-trip). Nested write performance.
