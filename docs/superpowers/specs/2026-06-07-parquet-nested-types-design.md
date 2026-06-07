# Phase 6d-2b — Nested Types (LIST / MAP / STRUCT)

**Status:** design locked by Claude under delegated authority ("complete all of
the phase six stuff"); flag any objection at bulk review. Completes Phase 6
(this work IS roadmap item 6e — the two entries described the same gap).

## Goal

Nested parquet files become READABLE end-to-end: LIST/MAP/STRUCT columns
surface as a dataframe column whose row values are owned, typed trees that
print/serialize sensibly and offer basic accessors. Today the reader *skips*
group schema nodes and *discards* repetition levels, so nested files decode
into silently-wrong flat columns — strictly worse than an error.

## Locked design decisions

1. **Scope = READ side.** Phase 6's headline is "Parquet reader: read (almost)
   anything." Nested READ (schema tree, repetition-level record assembly,
   typed row values) lands now; nested WRITE returns a clean
   `error.UnsupportedNestedWrite` and becomes its own roadmap item (it
   requires nested schema emission + def/rep generation — comparable in size
   to this whole phase). VARIANT/GEOMETRY/GEOGRAPHY stay `Raw` (they already
   read + round-trip bit-faithfully; decoding VARIANT's binary format is its
   own future spec).
2. **Row-value tree model, not Arrow-columnar.** A nested column is
   `Series(Nested)` where each row owns a typed value tree — consistent with
   teddy's per-row value architecture and the capability convention, at the
   cost of columnar memory efficiency (acceptable: correctness + usability
   first; revisit if perf becomes a goal).

   ```zig
   pub const Nested = union(enum) {
       null_,
       boolean: bool,
       int: i64,        // all signed widths normalize here
       uint: u64,       // all unsigned widths
       float: f64,      // f16/f32/f64 (lossless widening)
       date: Date, time: Time, timestamp: Timestamp,
       decimal: Decimal, uuid: Uuid, interval: Interval,
       string: strings.String,            // owned
       bytes: Binary,                     // owned (incl. FLBA/unannotated)
       list: []Nested,                    // owned
       strukt: []Nested,                  // owned; positional — names live in
                                          // the column schema (meta)
       map: []MapEntry,                   // owned; MapEntry = {key, value: Nested}
   };
   ```
   Scalar leaves inside nests reuse the 6d-2a value types, preserving logical
   semantics (a date inside a list is a `Date`).
3. **Column schema on `Series.meta` + ColumnMeta lifecycle capability.** The
   nested column's parquet subtree (names, kinds, optionality, leaf physical/
   annotations) is an owned `SchemaNode` tree stored in
   `Nested.ColumnMeta`. Because owned meta breaks the current copy-on-
   propagate convention, the capability convention gains two OPTIONAL hooks:
   if `ColumnMetaFor(T)` declares `deinit`/`clone`, `Series.deinit` calls
   `meta.deinit()` and the meta-propagating ops (`deepCopy`/`filterByIndices`/
   `fillNull`/`shift`) `clone()` instead of copying. Zero-size/POD metas are
   unaffected (no decls → old behavior).
4. **Record assembly is the Dremel algorithm**, driven per top-level nested
   root: each descendant leaf contributes (values, def_levels, rep_levels)
   streams; rows are assembled by advancing all leaf cursors in lockstep
   (rep == 0 starts a new row; def distinguishes null-ancestor / empty-list /
   null-element / present). LIST uses the standard 3-level encoding; MAP is
   `repeated group key_value {key, value}`; STRUCT adds def depth only.
   Legacy 2-level lists (bare `repeated` leaf) are decoded as LIST too.
5. **Rendering**: `Nested.format` emits JSON-ish text (lists `[..]`, structs
   `{name: ..}` with names resolved via a meta back-reference… names are NOT
   in the value, so format renders structs positionally `{..}` and the
   COLUMN-level pretty printers (asStringAt path) stay positional;
   field-by-name access goes through accessor APIs that take the series).
   JSON writer quotes nothing extra: nested values serialize as real JSON
   (lists/objects/numbers/strings/null) — struct field names from meta where
   available at the column level via a dedicated json arm… **simplification:**
   JSON output uses `format` (positional structs) for v1; documented.
6. **Accessors (v1)**: on `Nested`: `kind()`, `listLen()`, `listAt(i)`,
   `structAt(i)`, `mapLen()`/`mapAt(i)`; on the column/meta: field-name → index
   lookup (`SchemaNode.fieldIndex(name)`). Equality: `eql` deep-compares.
   Ordering: NONE (no `order` — argSort needs totality, so Nested gets a
   documented arbitrary-but-total order via... see risk note below).

## Risk notes
- `argSort`/`indicesWhere` instantiate for every variant: Nested must satisfy
  the ordering arm. Decision: implement `order` as a deterministic structural
  comparison (kind tag, then recursive lexicographic) — total, documented as
  storage ordering, mirrors Interval's precedent.
- GroupBy/join hashing: Nested declares `eql` but NOT `toSlice`; the
  GroupByContext else-arm hashes `asBytes` (pointers!) — Nested must be
  excluded: it is non-groupable (is_groupable unchanged — fine) and join KEYS
  on Nested hash wrongly → joinTyped must not be reachable... join dispatches
  on key column inline else: GroupByContext(Nested).hash via asBytes compiles
  but is semantically wrong for join-on-nested (not a use case; document) —
  give Nested a `hashBytes`-compatible path by declaring NOTHING and adding a
  GroupByContext arm: `hasMethod(T, "eql") and !hasMethod(T, "toSlice")` →
  hash via a new optional `hash(self) u64` capability that Nested implements
  (deep hash). Keeps join-on-nested correct-if-silly.

## Implementation slices
- **.0** Schema tree (owned `SchemaNode`, correct per-leaf max_def/max_rep via
  ancestor walk) + reader surfaces raw def+rep level streams per leaf +
  ColumnMeta lifecycle capability. Flat files: zero behavior change.
- **.1** `Nested` value type (+ capabilities incl. structural order + deep
  hash) + variant wiring + accessors + format.
- **.2** Dremel assembly (LIST/STRUCT/MAP incl. nesting combinations) +
  adapter wiring + pyarrow fixtures + write-side clean error + docs.

## Testing
pyarrow fixtures: list<i64> (incl. null list / empty list / null element),
struct{a,b}, struct<list>, list<struct>, list<list>, map<string,i64> (incl.
null value), nullable everything. Ground truth = expected per-row rendering
baked into tests (derived from pyarrow's own output). Flat-file regression:
entire existing suite must stay green. Malformed battery picks up the new
fixtures automatically.
