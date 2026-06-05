# Parquet 6d-2a.0 — Shared Infra Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared infrastructure for Parquet scalar logical types: the thrift `LogicalType` model (field 10 parse + encode), the `Series` comptime capability convention (replacing `String` special-cases), reader/writer resolution skeletons, and the `Raw` fallback type wired end-to-end.

**Architecture:** Parquet-side first (thrift model → schema element → fixture → column propagation → writer physical support), then dataframe-side (capability convention → comptime-safe BoxedSeries guards → `Raw` type → new variant → adapter wiring), finishing with an INT96 bit-faithful round-trip test. Spec: `docs/superpowers/specs/2026-06-04-parquet-scalar-logical-types-design.md` (§ "6d-2a.0 — Shared infra"). `WriteOptions.emit_int96` is deferred to slice 6d-2a.2 (it has no consumer until INT96→Timestamp decode exists).

**Tech Stack:** Zig 0.16.0, pyarrow 21 (fixtures), Thrift Compact Protocol (hand-rolled in `src/parquet/thrift_*.zig`).

**Commit policy (session norm — overrides per-task commits):** This entire plan lands as **one commit on `main`** after the user reviews the diff (same as 6b/6c/6d-1). Tasks below end in verification checkpoints, not commits. Run `zig build test` from the repo root (tests read `data/` fixtures from cwd). Baseline: 414/414 tests pass.

**Zig 0.16 gotchas (read before starting):**
- `@hasDecl` is a **compile error on primitive types** (`i32`, `f64`, `bool`). Always gate it behind a `@typeInfo` container check (the `hasMethod` helper in Task 6).
- Custom format methods use the Writergate interface: `pub fn format(self: @This(), writer: *std.Io.Writer) std.Io.Writer.Error!void`, invoked by the `{f}` specifier (NOT `{}` / `{s}`). If the compiler complains about the signature, check `std.Io.Writer` in the installed 0.16 — the parameter is a `*std.Io.Writer`.
- `std.mem.zeroes(T)` produces `false` for bool, `0` for ints/floats, zeroed fields for structs — used as the universal null placeholder.
- `std.testing.expectEqualDeep` compares tagged unions/structs deeply — use it for `LogicalType` round-trips.
- `BoxedSeries` `inline else` switches instantiate the called `Series(T)` method for **every** variant. Any method body that can't compile for `Raw` must be comptime-guarded **before** the `.raw` variant is added (Task 7), or the whole build breaks.

---

## File Map

| File | Change |
|---|---|
| `src/parquet/types.zig` | + `TimeUnit`, `LogicalType` (+ param structs); `ParquetColumn` gains `logical_type`, `type_length` |
| `src/parquet/metadata.zig` | + `decodeLogicalType`/`encodeLogicalType` (+ nested helpers); `SchemaElement`: `logical_type` field, decode field 10, encode fields 2 & 10; tests |
| `src/parquet/parquet.zig` | re-export `LogicalType`, `TimeUnit` |
| `src/parquet/encoding_writer.zig` | + `writeFixedByteArray` |
| `src/parquet/column_writer.zig` | `ColumnData` gains `logical_type`, `type_length`; `writeColumn` arms for FLBA + INT96 |
| `src/parquet/parquet_writer.zig` | schema elements carry `type_length` + `logical_type`; tests |
| `src/parquet/column_reader.zig` | `buildColumn` propagates `logical_type` + `type_length` |
| `src/parquet/parquet_reader.zig` | `readLeafConcat` propagates `logical_type` + `type_length`; footer fixture test |
| `src/dataframe/series.zig` | `hasMethod` helper; typeInfo-based `is_numeric`/`is_float` + `is_castable`/`is_groupable`; capability dispatch replacing `String` special-cases; `meta: ColumnMetaFor(T)` field; `Raw` arm in `toBoxedSeries` |
| `src/dataframe/group.zig` | `GroupByContext` capability arms (`toSlice` hash, `eql`) |
| `src/dataframe/boxed_series.zig` | comptime-safe numeric/cast/group guards; `raw` variant; `typeName`/`getType` arms; capability clone in `appendSeries` |
| `src/dataframe/raw.zig` | **NEW** — `Raw` value type + `ColumnMeta` |
| `src/dataframe/parquet.zig` | `ResolvedKind` + `resolveKind` (logical → converted → physical precedence); `addRawColumn`; `.raw` arm in `boxedToColumnData` |
| `src/dataframe/json_writer.zig` | `isStringSeries` gains `.raw => true` |
| `src/dataframe/series_test.zig` | `Blob` capability test type + tests |
| `src/dataframe/parquet_test.zig` | **NEW** — `resolveKind` unit tests + INT96 Raw round-trip |
| `src/dataframe/tests.zig` | register `parquet_test.zig` |
| `src_py/gen_fixtures.py` | + `logical_annotations()` fixture |
| `docs/cleanup-roadmap.md` | mark 6d-2a.0 done |

---

### Task 1: `TimeUnit` + `LogicalType` data model and thrift decode

**Files:**
- Modify: `src/parquet/types.zig` (after `ConvertedType`, ~line 42)
- Modify: `src/parquet/metadata.zig` (new functions + tests)

- [ ] **Step 1: Write the failing decode tests**

Append to `src/parquet/metadata.zig` tests section:

```zig
test "decodeLogicalType: DATE (empty variant)" {
    // union field 6 (DateType, empty struct), then union stop
    const data = [_]u8{ 0x6C, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expect(lt == .date);
}

test "decodeLogicalType: TIMESTAMP(utc=true, micros)" {
    // field 8 struct; inner: field 1 bool_true, field 2 TimeUnit union
    //   TimeUnit: field 2 (MICROS) empty struct
    const data = [_]u8{ 0x8C, 0x11, 0x1C, 0x2C, 0x00, 0x00, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(true, lt.timestamp.is_adjusted_to_utc);
    try std.testing.expectEqual(types.TimeUnit.micros, lt.timestamp.unit);
}

test "decodeLogicalType: DECIMAL(scale=2, precision=10)" {
    // field 5 struct; inner: field 1 scale zigzag(2)=4, field 2 precision zigzag(10)=20
    const data = [_]u8{ 0x5C, 0x15, 0x04, 0x15, 0x14, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(@as(i32, 2), lt.decimal.scale);
    try std.testing.expectEqual(@as(i32, 10), lt.decimal.precision);
}

test "decodeLogicalType: INTEGER(bit_width=16, signed)" {
    // field 10 struct; inner: field 1 i8 (raw byte 16), field 2 bool_true
    const data = [_]u8{ 0xAC, 0x13, 0x10, 0x11, 0x00, 0x00 };
    var reader = ThriftReader.init(&data);
    const lt = (try decodeLogicalType(&reader)).?;
    try std.testing.expectEqual(@as(i8, 16), lt.integer.bit_width);
    try std.testing.expectEqual(true, lt.integer.is_signed);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `zig build test 2>&1 | head -20`
Expected: compile error — `decodeLogicalType` / `types.TimeUnit` not defined.

- [ ] **Step 3: Add the data model to `src/parquet/types.zig`**

Insert after the `ConvertedType` enum:

```zig
// ============================================================
// Modern Logical Types (parquet.thrift LogicalType union, field 10
// of SchemaElement). ConvertedType above is the legacy 1:1 enum.
// ============================================================

pub const TimeUnit = enum { millis, micros, nanos };

pub const DecimalParams = struct { scale: i32, precision: i32 };
pub const TimeParams = struct { is_adjusted_to_utc: bool, unit: TimeUnit };
pub const TimestampParams = struct { is_adjusted_to_utc: bool, unit: TimeUnit };
pub const IntParams = struct { bit_width: i8, is_signed: bool };

/// One variant per parquet.thrift LogicalType union field. Parameterless
/// variants are empty thrift structs on the wire. VARIANT/GEOMETRY/GEOGRAPHY
/// carry optional fields we don't model yet — they decode by skipping the
/// payload and re-encode as empty structs (all their fields are optional).
pub const LogicalType = union(enum) {
    string, // 1
    map, // 2
    list, // 3
    @"enum", // 4
    decimal: DecimalParams, // 5
    date, // 6
    time: TimeParams, // 7
    timestamp: TimestampParams, // 8
    integer: IntParams, // 10
    unknown, // 11 (NullType)
    json, // 12
    bson, // 13
    uuid, // 14
    float16, // 15
    variant, // 16
    geometry, // 17
    geography, // 18
};
```

- [ ] **Step 4: Implement the decoders in `src/parquet/metadata.zig`**

Insert before `pub const SchemaElement`:

```zig
// ============================================================
// LogicalType (SchemaElement field 10) — thrift union decode/encode
// ============================================================

fn decodeTimeUnit(reader: *ThriftReader) !types.TimeUnit {
    var result: ?types.TimeUnit = null;
    reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => {
                try reader.skip(fh.field_type);
                result = .millis;
            },
            2 => {
                try reader.skip(fh.field_type);
                result = .micros;
            },
            3 => {
                try reader.skip(fh.field_type);
                result = .nanos;
            },
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result orelse error.InvalidLogicalType;
}

fn decodeDecimalType(reader: *ThriftReader) !types.DecimalParams {
    var result = types.DecimalParams{ .scale = 0, .precision = 0 };
    reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.scale = try reader.readZigZagI32(),
            2 => result.precision = try reader.readZigZagI32(),
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

fn decodeTimeType(reader: *ThriftReader) !types.TimeParams {
    var result = types.TimeParams{ .is_adjusted_to_utc = false, .unit = .millis };
    reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.is_adjusted_to_utc = fh.field_type == .boolean_true,
            2 => result.unit = try decodeTimeUnit(reader),
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

fn decodeTimestampType(reader: *ThriftReader) !types.TimestampParams {
    const t = try decodeTimeType(reader); // identical wire layout
    return .{ .is_adjusted_to_utc = t.is_adjusted_to_utc, .unit = t.unit };
}

fn decodeIntType(reader: *ThriftReader) !types.IntParams {
    var result = types.IntParams{ .bit_width = 0, .is_signed = false };
    reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => result.bit_width = @bitCast(try reader.readByte()),
            2 => result.is_signed = fh.field_type == .boolean_true,
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}

/// Decode the LogicalType thrift union: a struct with exactly one field set.
/// Returns null for union fields we don't recognize (future spec additions),
/// so callers fall back to converted_type / physical type.
pub fn decodeLogicalType(reader: *ThriftReader) !?types.LogicalType {
    var result: ?types.LogicalType = null;
    reader.pushStruct();
    while (true) {
        const fh = try reader.readFieldHeader();
        if (fh.field_type == .stop) break;
        switch (fh.field_id) {
            1 => {
                try reader.skip(fh.field_type);
                result = .string;
            },
            2 => {
                try reader.skip(fh.field_type);
                result = .map;
            },
            3 => {
                try reader.skip(fh.field_type);
                result = .list;
            },
            4 => {
                try reader.skip(fh.field_type);
                result = .@"enum";
            },
            5 => result = .{ .decimal = try decodeDecimalType(reader) },
            6 => {
                try reader.skip(fh.field_type);
                result = .date;
            },
            7 => result = .{ .time = try decodeTimeType(reader) },
            8 => result = .{ .timestamp = try decodeTimestampType(reader) },
            10 => result = .{ .integer = try decodeIntType(reader) },
            11 => {
                try reader.skip(fh.field_type);
                result = .unknown;
            },
            12 => {
                try reader.skip(fh.field_type);
                result = .json;
            },
            13 => {
                try reader.skip(fh.field_type);
                result = .bson;
            },
            14 => {
                try reader.skip(fh.field_type);
                result = .uuid;
            },
            15 => {
                try reader.skip(fh.field_type);
                result = .float16;
            },
            16 => {
                try reader.skip(fh.field_type);
                result = .variant;
            },
            17 => {
                try reader.skip(fh.field_type);
                result = .geometry;
            },
            18 => {
                try reader.skip(fh.field_type);
                result = .geography;
            },
            else => try reader.skip(fh.field_type),
        }
    }
    reader.popStruct();
    return result;
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `zig build test`
Expected: PASS (4 new tests; 413 baseline intact).

---

### Task 2: `encodeLogicalType` + all-variant round-trip

**Files:**
- Modify: `src/parquet/metadata.zig`

- [ ] **Step 1: Write the failing round-trip test**

```zig
test "LogicalType encode/decode round-trip: every variant" {
    const allocator = std.testing.allocator;
    const cases = [_]types.LogicalType{
        .string,                                                              .map,
        .list,                                                                .@"enum",
        .{ .decimal = .{ .scale = 2, .precision = 38 } },                     .date,
        .{ .time = .{ .is_adjusted_to_utc = false, .unit = .millis } },
        .{ .timestamp = .{ .is_adjusted_to_utc = true, .unit = .nanos } },
        .{ .integer = .{ .bit_width = 64, .is_signed = false } },             .unknown,
        .json,                                                                .bson,
        .uuid,                                                                .float16,
        .variant,                                                             .geometry,
        .geography,
    };
    for (cases) |orig| {
        var w = ThriftWriter.init(allocator);
        defer w.deinit();
        try encodeLogicalType(&w, orig);
        var r = ThriftReader.init(w.written());
        const decoded = (try decodeLogicalType(&r)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualDeep(orig, decoded);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `zig build test 2>&1 | head -10` → compile error: `encodeLogicalType` not defined.

- [ ] **Step 3: Implement the encoders** (below `decodeLogicalType`)

```zig
fn encodeEmptyVariant(w: *ThriftWriter, field_id: i16) !void {
    try w.writeFieldHeader(field_id, .@"struct");
    w.pushStruct();
    try w.writeFieldStop();
    w.popStruct();
}

fn encodeTimeUnit(w: *ThriftWriter, unit: types.TimeUnit) !void {
    w.pushStruct();
    const field_id: i16 = switch (unit) {
        .millis => 1,
        .micros => 2,
        .nanos => 3,
    };
    try encodeEmptyVariant(w, field_id);
    try w.writeFieldStop();
    w.popStruct();
}

fn encodeTimeFields(w: *ThriftWriter, is_utc: bool, unit: types.TimeUnit) !void {
    w.pushStruct();
    try w.writeBoolField(1, is_utc);
    try w.writeFieldHeader(2, .@"struct");
    try encodeTimeUnit(w, unit);
    try w.writeFieldStop();
    w.popStruct();
}

/// Encode the LogicalType thrift union (one field set, then stop).
pub fn encodeLogicalType(w: *ThriftWriter, lt: types.LogicalType) !void {
    w.pushStruct();
    switch (lt) {
        .string => try encodeEmptyVariant(w, 1),
        .map => try encodeEmptyVariant(w, 2),
        .list => try encodeEmptyVariant(w, 3),
        .@"enum" => try encodeEmptyVariant(w, 4),
        .decimal => |d| {
            try w.writeFieldHeader(5, .@"struct");
            w.pushStruct();
            try w.writeFieldHeader(1, .i32);
            try w.writeZigZagI32(d.scale);
            try w.writeFieldHeader(2, .i32);
            try w.writeZigZagI32(d.precision);
            try w.writeFieldStop();
            w.popStruct();
        },
        .date => try encodeEmptyVariant(w, 6),
        .time => |t| {
            try w.writeFieldHeader(7, .@"struct");
            try encodeTimeFields(w, t.is_adjusted_to_utc, t.unit);
        },
        .timestamp => |t| {
            try w.writeFieldHeader(8, .@"struct");
            try encodeTimeFields(w, t.is_adjusted_to_utc, t.unit);
        },
        .integer => |i| {
            try w.writeFieldHeader(10, .@"struct");
            w.pushStruct();
            try w.writeFieldHeader(1, .i8);
            try w.writeByte(@bitCast(i.bit_width));
            try w.writeBoolField(2, i.is_signed);
            try w.writeFieldStop();
            w.popStruct();
        },
        .unknown => try encodeEmptyVariant(w, 11),
        .json => try encodeEmptyVariant(w, 12),
        .bson => try encodeEmptyVariant(w, 13),
        .uuid => try encodeEmptyVariant(w, 14),
        .float16 => try encodeEmptyVariant(w, 15),
        .variant => try encodeEmptyVariant(w, 16),
        .geometry => try encodeEmptyVariant(w, 17),
        .geography => try encodeEmptyVariant(w, 18),
    }
    try w.writeFieldStop();
    w.popStruct();
}
```

- [ ] **Step 4: Run tests to verify they pass** — `zig build test` → PASS.

---

### Task 3: `SchemaElement` — `logical_type` field, decode field 10, encode fields 2 & 10

**Files:**
- Modify: `src/parquet/metadata.zig:45-103` (`SchemaElement`)

- [ ] **Step 1: Write the failing tests**

```zig
test "SchemaElement decode: field 10 logical_type (DATE)" {
    const data = [_]u8{
        0x15, 0x02, // field 1: type = INT32 (zigzag(1)=2)
        0x25, 0x00, // field 3: repetition = required
        0x18, 0x01, 'd', // field 4: name = "d"
        0x6C, // field 10 header (delta 6 from field 4, struct)
        0x6C, 0x00, 0x00, // LogicalType union: DATE variant + stop
        0x00, // SchemaElement stop
    };
    var reader = ThriftReader.init(&data);
    const elem = try SchemaElement.decode(&reader);
    try std.testing.expectEqualStrings("d", elem.name);
    try std.testing.expect(elem.logical_type.? == .date);
}

test "SchemaElement encode/decode round-trip: logical_type + type_length" {
    const allocator = std.testing.allocator;
    const orig = SchemaElement{
        .type_ = .fixed_len_byte_array,
        .type_length = 16,
        .repetition_type = .required,
        .name = "u",
        .logical_type = .uuid,
    };
    var w = ThriftWriter.init(allocator);
    defer w.deinit();
    try orig.encode(&w);
    var r = ThriftReader.init(w.written());
    const decoded = try SchemaElement.decode(&r);
    try std.testing.expectEqual(@as(i32, 16), decoded.type_length.?);
    try std.testing.expect(decoded.logical_type.? == .uuid);
    try std.testing.expectEqual(orig.type_.?, decoded.type_.?);
}
```

- [ ] **Step 2: Run to verify failure** — `zig build test 2>&1 | head -10` → compile error: no field `logical_type`.

- [ ] **Step 3: Implement**

Add to the `SchemaElement` struct fields:

```zig
    logical_type: ?types.LogicalType = null,
```

In `SchemaElement.decode`, replace the field-10 skip:

```zig
                10 => result.logical_type = try decodeLogicalType(reader),
```

In `SchemaElement.encode`, insert **type_length right after the field-1 block** (field IDs must ascend):

```zig
        if (self.type_length) |tl| {
            try w.writeFieldHeader(2, .i32);
            try w.writeZigZagI32(tl);
        }
```

and insert **after the converted_type block, before `writeFieldStop`**:

```zig
        if (self.logical_type) |lt| {
            try w.writeFieldHeader(10, .@"struct");
            try encodeLogicalType(w, lt);
        }
```

- [ ] **Step 4: Run tests to verify they pass** — `zig build test` → PASS (incl. all pre-existing SchemaElement tests).

---

### Task 4: pyarrow fixture + footer integration test

**Files:**
- Modify: `src_py/gen_fixtures.py`
- Modify: `src/parquet/parquet_reader.zig` (test only)

- [ ] **Step 1: Add the fixture generator**

In `src_py/gen_fixtures.py`, add `from decimal import Decimal as PyDecimal` to the imports, then:

```python
def logical_annotations():
    # Modern LogicalType annotations (SchemaElement field 10) on scalar columns.
    # 6d-2a.0 only asserts the footer parses these; value-level decode lands in
    # slices 6d-2a.1-.5. pyarrow also writes the legacy converted_type alongside.
    tbl = pa.table({
        "d":   pa.array([dt.date(2020, 1, 1), dt.date(2021, 6, 15)], type=pa.date32()),
        "t":   pa.array([dt.time(1, 2, 3), dt.time(4, 5, 6)], type=pa.time64("us")),
        "ts":  pa.array([dt.datetime(2020, 1, 1, 12, 0, 0),
                         dt.datetime(2021, 6, 15, 8, 30, 0)], type=pa.timestamp("us", tz="UTC")),
        "dec": pa.array([PyDecimal("12345678.90"), PyDecimal("-0.01")], type=pa.decimal128(10, 2)),
    })
    pq.write_table(tbl, "data/logical_annotations.parquet", compression=None)
    print("data/logical_annotations.parquet: DATE/TIME/TIMESTAMP/DECIMAL logical types, 2 rows")
```

Register it in `__main__` after `unsigned_ints()`:

```python
    logical_annotations()
```

- [ ] **Step 2: Generate** — Run: `python3 src_py/gen_fixtures.py`
Expected output includes `data/logical_annotations.parquet: ... 2 rows`. `git status` shows the new fixture (commit it with the phase).

- [ ] **Step 3: Write the footer-parse test** (in `src/parquet/parquet_reader.zig` tests; `metadata` and `ThriftReader` are already imported there)

```zig
test "FileMetaData: logical_annotations.parquet parses LogicalType (field 10)" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    // Footer: [...data...][footer][4-byte LE len]["PAR1"]
    const footer_len: usize = @intCast(std.mem.readInt(u32, file_data[file_data.len - 8 ..][0..4], .little));
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
```

- [ ] **Step 4: Run** — `zig build test` → PASS. (If pyarrow's `time64("us")` marks `isAdjustedToUTC` differently, the test failure will print the actual value — fix the expectation to match the real file, it's ground truth.)

---

### Task 5: Propagate `logical_type` + `type_length` to `ParquetColumn`

**Files:**
- Modify: `src/parquet/types.zig` (`ParquetColumn`)
- Modify: `src/parquet/column_reader.zig:244-247` (`buildColumn`)
- Modify: `src/parquet/parquet_reader.zig:82-84` (`readLeafConcat`)

- [ ] **Step 1: Write the failing test** (in `parquet_reader.zig`)

```zig
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

    // flba.parquet: FIXED_LEN_BYTE_ARRAY(4) → type_length must surface too
    const flba_data = try cwd.readFileAlloc(io, "data/flba.parquet", allocator, .unlimited);
    defer allocator.free(flba_data);
    var flba = try readParquet(allocator, flba_data);
    defer flba.deinit();
    try std.testing.expectEqual(@as(i32, 4), flba.columns[0].type_length.?);
}
```

- [ ] **Step 2: Run to verify failure** — compile error: no field `logical_type` on `ParquetColumn`.

- [ ] **Step 3: Implement**

`src/parquet/types.zig` — add to `ParquetColumn` after `converted_type`:

```zig
    logical_type: ?LogicalType,
    type_length: ?i32,
```

and to `initEmpty` after `.converted_type = null,`:

```zig
            .logical_type = null,
            .type_length = null,
```

`src/parquet/column_reader.zig` `buildColumn` — after `col.converted_type = ...`:

```zig
    col.logical_type = leaf.schema_element.logical_type;
    col.type_length = leaf.schema_element.type_length;
```

`src/parquet/parquet_reader.zig` `readLeafConcat` — after `col.converted_type = ...`:

```zig
    col.logical_type = leaf.schema_element.logical_type;
    col.type_length = leaf.schema_element.type_length;
```

- [ ] **Step 4: Run tests** — `zig build test` → PASS.

---

### Task 6: Writer physical support — FLBA + INT96 (+ `ColumnData` metadata)

**Files:**
- Modify: `src/parquet/encoding_writer.zig`
- Modify: `src/parquet/column_writer.zig`
- Modify: `src/parquet/parquet_writer.zig`
- Modify: `src/parquet/parquet.zig` (re-exports)

- [ ] **Step 1: Write the failing tests** (in `parquet_writer.zig`)

```zig
test "writeParquet: FIXED_LEN_BYTE_ARRAY round-trip with type_length" {
    const allocator = std.testing.allocator;
    const vals = [_][]const u8{ "abcd", "efgh" };
    const cols = [_]ColumnData{.{
        .name = "fb",
        .physical_type = .fixed_len_byte_array,
        .type_length = 4,
        .byte_arrays = &vals,
        .num_values = 2,
    }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(types.PhysicalType.fixed_len_byte_array, result.columns[0].physical_type);
    try std.testing.expectEqual(@as(i32, 4), result.columns[0].type_length.?);
    try std.testing.expectEqualStrings("abcd", result.columns[0].byte_arrays.?[0]);
    try std.testing.expectEqualStrings("efgh", result.columns[0].byte_arrays.?[1]);
}

test "writeParquet: INT96 round-trip preserves raw 12-byte values" {
    const allocator = std.testing.allocator;
    const v1 = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const v2 = [_]u8{ 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
    const vals = [_][]const u8{ &v1, &v2 };
    const cols = [_]ColumnData{.{
        .name = "t",
        .physical_type = .int96,
        .byte_arrays = &vals,
        .num_values = 2,
    }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(types.PhysicalType.int96, result.columns[0].physical_type);
    try std.testing.expectEqualSlices(u8, &v1, result.columns[0].byte_arrays.?[0]);
    try std.testing.expectEqualSlices(u8, &v2, result.columns[0].byte_arrays.?[1]);
}

test "writeParquet: FLBA value width mismatch errors" {
    const allocator = std.testing.allocator;
    const vals = [_][]const u8{"abc"}; // 3 bytes, width says 4
    const cols = [_]ColumnData{.{
        .name = "fb",
        .physical_type = .fixed_len_byte_array,
        .type_length = 4,
        .byte_arrays = &vals,
        .num_values = 1,
    }};
    try std.testing.expectError(error.FixedLengthMismatch, writeParquet(allocator, &cols, .{}));
}

test "writeParquet: logical_type lands in the schema and reads back" {
    const allocator = std.testing.allocator;
    const vals = [_]i32{ 18262, 18263 };
    const cols = [_]ColumnData{.{
        .name = "d",
        .physical_type = .int32,
        .converted_type = .date,
        .logical_type = .date,
        .int32s = &vals,
        .num_values = 2,
    }};
    const output = try writeParquet(allocator, &cols, .{});
    defer allocator.free(output);

    var result = try reader_mod.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expect(result.columns[0].logical_type.? == .date);
    try std.testing.expectEqual(types.ConvertedType.date, result.columns[0].converted_type.?);
}
```

- [ ] **Step 2: Run to verify failure** — compile error: no field `type_length` on `ColumnData`.

- [ ] **Step 3: Implement**

`src/parquet/encoding_writer.zig` — add after `writeByteArray`:

```zig
    /// Write a FIXED_LEN_BYTE_ARRAY / INT96 value: raw bytes, no length prefix.
    /// The caller is responsible for validating the value width.
    pub fn writeFixedByteArray(self: *PlainEncoder, data: []const u8) !void {
        try self.buf.appendSlice(self.allocator, data);
    }
```

`src/parquet/column_writer.zig` — add to `ColumnData` after `converted_type`:

```zig
    logical_type: ?types.LogicalType = null,
    type_length: ?i32 = null,
```

In `writeColumn`, replace `else => return error.UnsupportedType,` with:

```zig
        .fixed_len_byte_array => {
            const width: usize = @intCast(col.type_length orelse return error.MissingTypeLength);
            if (col.byte_arrays) |vals| {
                for (vals) |v| {
                    if (v.len != width) return error.FixedLengthMismatch;
                    try encoder.writeFixedByteArray(v);
                }
            }
        },
        .int96 => {
            if (col.byte_arrays) |vals| {
                for (vals) |v| {
                    if (v.len != 12) return error.FixedLengthMismatch;
                    try encoder.writeFixedByteArray(v);
                }
            }
        },
```

(The switch is now exhaustive over all 8 physical types — no `else` needed.)

`src/parquet/parquet_writer.zig` — in the schema-element loop (~line 45), replace the struct literal with:

```zig
        schema[i + 1] = .{
            .type_ = col.physical_type,
            .type_length = col.type_length,
            .repetition_type = .required,
            .name = col.name,
            .converted_type = col.converted_type,
            .logical_type = col.logical_type,
        };
```

`src/parquet/parquet.zig` — add to the type re-exports:

```zig
pub const LogicalType = types.LogicalType;
pub const TimeUnit = types.TimeUnit;
```

- [ ] **Step 4: Run tests** — `zig build test` → PASS.

**Parquet side done. Dataframe side follows.**

---

### Task 7: `Series` comptime capability convention

This replaces the `T == strings.String` special-cases with a convention any value type can opt into: `deinit()`, `clone()`, `eql()`, `toSlice()`, `format()`, `init(allocator)` (null placeholder), `type_name` (display name), `ColumnMeta` (column-level metadata struct). `String` already satisfies `deinit`/`clone`/`eql`/`toSlice`/`init` — its behavior must not change.

**Files:**
- Modify: `src/dataframe/series.zig`
- Modify: `src/dataframe/group.zig` (`GroupByContext`)
- Test: `src/dataframe/series_test.zig`

- [ ] **Step 1: Write the failing tests** (append to `series_test.zig`; it already imports `std`, `Series`, `String` — match its existing import style)

```zig
// Minimal owning value type exercising the capability convention
// (deinit/clone/eql/toSlice/format/init/type_name/ColumnMeta) without
// depending on the parquet module.
const Blob = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub const type_name = "Blob";

    pub const ColumnMeta = struct {
        tag: u32 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) !Blob {
        return .{ .allocator = allocator, .bytes = try allocator.alloc(u8, 0) };
    }

    pub fn fromSlice(allocator: std.mem.Allocator, data: []const u8) !Blob {
        return .{ .allocator = allocator, .bytes = try allocator.dupe(u8, data) };
    }

    pub fn deinit(self: *Blob) void {
        self.allocator.free(self.bytes);
    }

    pub fn clone(self: *const Blob) !Blob {
        return fromSlice(self.allocator, self.bytes);
    }

    pub fn eql(self: *const Blob, other: *const Blob) bool {
        return std.mem.eql(u8, self.bytes, other.bytes);
    }

    pub fn toSlice(self: *const Blob) []const u8 {
        return self.bytes;
    }

    pub fn format(self: Blob, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        for (self.bytes) |b| try writer.print("{x:0>2}", .{b});
    }
};

test "capability: Series(Blob) owns memory through deinit/dropRow/limit" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit(); // leak check via testing.allocator
    try s.rename("blobs");
    try s.append(try Blob.fromSlice(allocator, "aa"));
    try s.append(try Blob.fromSlice(allocator, "bb"));
    try s.append(try Blob.fromSlice(allocator, "cc"));
    s.dropRow(1); // frees "bb"
    try std.testing.expectEqual(@as(usize, 2), s.len());
    s.limit(1); // frees "cc"
    try std.testing.expectEqual(@as(usize, 1), s.len());
}

test "capability: deepCopy clones Blob values independently" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    try s.append(try Blob.fromSlice(allocator, "abc"));
    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expect(copy.values.items[0].eql(&s.values.items[0]));
    try std.testing.expect(copy.values.items[0].bytes.ptr != s.values.items[0].bytes.ptr);
}

test "capability: compareSeries uses eql; appendNull uses init placeholder" {
    const allocator = std.testing.allocator;
    var a = try Series(Blob).init(allocator);
    defer a.deinit();
    var b = try Series(Blob).init(allocator);
    defer b.deinit();
    try a.append(try Blob.fromSlice(allocator, "xy"));
    try b.append(try Blob.fromSlice(allocator, "xy"));
    try a.appendNull();
    try b.appendNull();
    try std.testing.expect(a.compareSeries(b));
    try std.testing.expect(a.isNull(1));
}

test "capability: filterByIndices, fillNull, shift clone Blob values" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    try s.append(try Blob.fromSlice(allocator, "a"));
    try s.appendNull();
    try s.append(try Blob.fromSlice(allocator, "c"));

    const filtered = try s.filterByIndices(&.{ 0, 2 });
    defer filtered.deinit();
    try std.testing.expectEqual(@as(usize, 2), filtered.len());

    var fill_value = try Blob.fromSlice(allocator, "z");
    defer fill_value.deinit();
    const filled = try s.fillNull(fill_value);
    defer filled.deinit();
    try std.testing.expect(filled.values.items[1].eql(&fill_value));

    const shifted = try s.shift(1);
    defer shifted.deinit();
    try std.testing.expect(shifted.isNull(0));
    try std.testing.expectEqualStrings("a", shifted.values.items[1].toSlice());
}

test "capability: asStringAt uses format; getTypeAsString uses type_name" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    try s.append(try Blob.fromSlice(allocator, &.{ 0xDE, 0xAD }));
    var str = try s.asStringAt(0);
    defer str.deinit();
    try std.testing.expectEqualStrings("dead", str.toSlice());
    var tn = try s.getTypeAsString();
    defer tn.deinit();
    try std.testing.expectEqualStrings("Blob", tn.toSlice());
}

test "capability: argSort and uniqueIndices work on Blob via toSlice/eql" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    // Distinct values for argSort: sortUnstable gives no order guarantee
    // between equal keys.
    try s.append(try Blob.fromSlice(allocator, "c"));
    try s.append(try Blob.fromSlice(allocator, "a"));
    try s.append(try Blob.fromSlice(allocator, "b"));

    var order = try s.argSort(allocator, true);
    defer order.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 0 }, order.items);

    // Duplicates are fine for uniqueIndices (first occurrence wins).
    try s.append(try Blob.fromSlice(allocator, "a"));
    var uniq = try s.uniqueIndices(allocator);
    defer uniq.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, uniq.items);
}

test "capability: ColumnMeta is stored on the series and propagated" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    s.meta = .{ .tag = 42 };
    try s.append(try Blob.fromSlice(allocator, "a"));

    const copy = try s.deepCopy();
    defer copy.deinit();
    try std.testing.expectEqual(@as(u32, 42), copy.meta.tag);

    const filtered = try s.filterByIndices(&.{0});
    defer filtered.deinit();
    try std.testing.expectEqual(@as(u32, 42), filtered.meta.tag);

    const shifted = try s.shift(0);
    defer shifted.deinit();
    try std.testing.expectEqual(@as(u32, 42), shifted.meta.tag);
}
```

- [ ] **Step 2: Run to verify failure** — `zig build test 2>&1 | head -20` → compile errors (e.g. `Series(Blob).deinit` calls `item.deinit()` only for String; `meta` field missing; `uniqueIndices` → `GroupByContext(Blob).eql` does `a == b` on a struct).

- [ ] **Step 3: Implement in `src/dataframe/series.zig`**

**(a)** Add near the top (after `canBeSlice`):

```zig
/// True if T declares `name`. @hasDecl is only legal on container types, so
/// primitives (i32, f64, bool, ...) safely report false. This drives the value-
/// type capability convention: deinit/clone/eql/toSlice/format/init/type_name/
/// ColumnMeta — String, Blob (tests), and Raw opt in by declaring them.
pub fn hasMethod(comptime T: type, comptime name: []const u8) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union", .@"enum", .@"opaque" => @hasDecl(T, name),
        else => false,
    };
}

/// Column-level metadata type for Series(T): value types may declare
/// `pub const ColumnMeta = struct { ... }` (all fields defaulted). Every other
/// element type gets a zero-size placeholder.
pub fn ColumnMetaFor(comptime T: type) type {
    if (hasMethod(T, "ColumnMeta")) return T.ColumnMeta;
    return struct {};
}
```

**(b)** Inside `Series(T)`'s struct, replace the existing `is_numeric`/`is_float` consts (line ~468) with:

```zig
        pub const is_numeric = switch (@typeInfo(T)) {
            .int, .float => true,
            else => false,
        };
        const is_float = switch (@typeInfo(T)) {
            .float => true,
            else => false,
        };
        /// cast/castSafe/castLossy participate only for these.
        pub const is_castable = is_numeric or T == bool or T == strings.String;
        /// groupBy keys: hashable + equatable today.
        pub const is_groupable = is_numeric or T == bool or T == strings.String;
        /// Display label for print/getTypeAsString.
        const type_label: []const u8 = if (hasMethod(T, "type_name")) T.type_name else @typeName(T);
```

**(c)** Add the `meta` field to the struct (after `validity`):

```zig
        /// Column-level metadata (e.g. Raw's preserved parquet types).
        /// Zero-size unless T declares ColumnMeta. Propagated by deepCopy/
        /// filterByIndices/fillNull/shift.
        meta: ColumnMetaFor(T) = .{},
```

and assign it in **both** `init` and `initWithCapacity` (fields are set manually after `allocator.create`, so the default above does not apply):

```zig
            ptr.meta = .{};
```

(in `initWithCapacity` the pointer variable is `series_ptr`.)

**(d)** Replace String special-cases with capability dispatch. Each rewrite below is the complete new body fragment:

`deinit`:

```zig
            if (comptime hasMethod(T, "deinit")) {
                for (self.values.items) |*item| {
                    item.deinit();
                }
            }
```

`appendNull` placeholder block (replaces the String/bool/else three-arm block):

```zig
            // Append default value as placeholder (value is irrelevant — validity marks it null).
            if (comptime hasMethod(T, "init")) {
                var empty = try T.init(self.allocator);
                errdefer empty.deinit();
                try self.values.append(self.allocator, empty);
            } else {
                try self.values.append(self.allocator, std.mem.zeroes(T));
            }
```

`dropRow`:

```zig
        pub fn dropRow(self: *Self, index: usize) void {
            if (comptime hasMethod(T, "deinit")) {
                var removed = self.values.orderedRemove(index);
                removed.deinit();
            } else {
                _ = self.values.orderedRemove(index);
            }
            if (self.validity) |*v| {
                _ = v.orderedRemove(index);
            }
        }
```

`deepCopy` value loop (and add `new_series.meta = self.meta;` right after the `rename` line):

```zig
            for (self.values.items) |*value| {
                if (comptime hasMethod(T, "clone")) {
                    var new_value = try value.clone();
                    errdefer new_value.deinit();
                    try new_series.values.append(self.allocator, new_value);
                } else {
                    try new_series.values.append(self.allocator, value.*);
                }
            }
```

`limit` cleanup block:

```zig
            if (comptime hasMethod(T, "deinit")) {
                for (n_limit..self.values.items.len) |i| {
                    self.values.items[i].deinit();
                }
            }
```

`compareSeries` comparison:

```zig
                if (comptime hasMethod(T, "eql")) {
                    if (!value.eql(&other.values.items[index])) return false;
                } else {
                    if (value != other.values.items[index]) return false;
                }
```

`filterByIndices` (add `new_series.meta = self.meta;` after `rename`; replace the String clone arm):

```zig
                if (comptime hasMethod(T, "clone")) {
                    var cloned = try self.values.items[idx].clone();
                    errdefer cloned.deinit();
                    try new_series.values.append(self.allocator, cloned);
                } else {
                    try new_series.values.append(self.allocator, self.values.items[idx]);
                }
```

`fillNull` (add `result.meta = self.meta;` after `rename`; full new loop body):

```zig
            for (self.values.items, 0..) |v, i| {
                if (self.isNull(i)) {
                    if (comptime hasMethod(T, "clone")) {
                        var cloned = try value.clone();
                        errdefer cloned.deinit();
                        try result.values.append(self.allocator, cloned);
                    } else {
                        try result.values.append(self.allocator, value);
                    }
                } else {
                    if (comptime hasMethod(T, "clone")) {
                        var cloned = try v.clone();
                        errdefer cloned.deinit();
                        try result.values.append(self.allocator, cloned);
                    } else {
                        try result.values.append(self.allocator, v);
                    }
                }
            }
```

`fillNullForward` null-fill branch:

```zig
                } else if (last_valid) |lv| {
                    if (comptime hasMethod(T, "clone")) {
                        v.deinit();
                        v.* = try lv.clone();
                    } else {
                        v.* = lv;
                    }
                    if (result.validity) |*bm| bm.items[i] = true;
                }
```

`fillNullBackward`: same transform on its String arm (`result.values.items[i].deinit(); result.values.items[i] = try nv.clone();` under `hasMethod(T, "clone")`).

`shift` (add `result.meta = self.meta;` after `rename`; both String clone arms → `hasMethod(T, "clone")`, same shape).

`replace` and `replaceSlice` match/replace logic:

```zig
                const matches = if (comptime hasMethod(T, "eql"))
                    v.eql(&old_value)
                else
                    v.* == old_value;
                if (matches) {
                    if (comptime hasMethod(T, "clone")) {
                        v.deinit();
                        v.* = try new_value.clone();
                    } else {
                        v.* = new_value;
                    }
                }
```

(`replaceSlice` analogously with `pair[0]` / `pair[1]`, keeping its `break`.)

`argSort` `Context.lessThan` String arm becomes:

```zig
                    if (comptime hasMethod(T, "toSlice")) {
                        const order = std.mem.order(u8, a.toSlice(), b.toSlice());
                        return if (ctx.asc) order == .lt else order == .gt;
                    } else if (comptime T == bool) {
```

`print` — rewrite the comptime switch as an if-chain so a capability arm fits (String keeps its `{s}` rendering, owning/format types use `{f}`):

```zig
        pub fn print(self: *Self) void {
            if (comptime T == strings.String) {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), "String" });
                for (self.values.items) |value| {
                    std.debug.print("{s}\n", .{value.toSlice()});
                }
                std.debug.print("\n", .{});
            } else if (comptime hasMethod(T, "format")) {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), type_label });
                for (self.values.items) |value| {
                    std.debug.print("{f}\n", .{value});
                }
                std.debug.print("\n", .{});
            } else if (comptime T == f32 or T == f64) {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), "Float" });
                for (self.values.items) |value| {
                    std.debug.print("{d}\n", .{value});
                }
                std.debug.print("\n", .{});
            } else {
                std.debug.print("{s}\n{s}\n--------\n", .{ self.name.toSlice(), @typeName(T) });
                for (self.values.items) |value| {
                    std.debug.print("{}\n", .{value});
                }
                std.debug.print("\n", .{});
            }
        }
```

`printAt` — same if-chain transform; the format arm is:

```zig
            } else if (comptime hasMethod(T, "format")) {
                std.debug.print("{f}", .{self.values.items[n]});
```

`asStringAt` slice selection — same transform; the format arm is:

```zig
                else if (comptime hasMethod(T, "format"))
                    try std.fmt.bufPrint(&buf, "{f}", .{self.values.items[n]})
```

(keep the existing String / `[]const u8` / float / else arms; values longer than the 128-byte buffer surface `error.NoSpaceLeft`, which is acceptable for 6d-2a.0).

`getTypeAsString` value selection:

```zig
            const value = if (comptime hasMethod(T, "type_name")) T.type_name else switch (T) {
                // ... existing switch unchanged ...
            };
```

`castValueStrict` and `castValueLossy` `From == To` arms (top of each function):

```zig
    if (comptime From == To) {
        if (comptime hasMethod(From, "clone")) return value.clone();
        return value;
    }
```

(in `castValueLossy` keep the `try`: `return try value.clone();`)

**(e)** `src/dataframe/group.zig` — replace the `GroupByContext` String arms with capability arms (import `hasMethod` at the top: `const hasMethod = @import("series.zig").hasMethod;`):

```zig
        pub fn hash(_: Self, key: T) u64 {
            if (comptime hasMethod(T, "toSlice")) {
                return std.hash.Wyhash.hash(0, key.toSlice());
            } else if (comptime T == f32) {
```

```zig
        pub fn eql(_: Self, a: T, b: T) bool {
            if (comptime hasMethod(T, "eql")) {
                return a.eql(&b);
            } else if (comptime T == f32 or T == f64) {
```

(String has both `toSlice` and `eql`, so its behavior is unchanged.)

- [ ] **Step 4: Run tests** — `zig build test` → all PASS (Blob tests + the full existing String regression suite; the testing allocator catches any leak introduced by a wrong capability arm).

---

### Task 8: Comptime-safe guards in `BoxedSeries`

These rewrites make every `inline else` arm compile for any future variant (the `.string, .bool => ...` enumerations would otherwise force-instantiate numeric methods for `Raw` and fail the build). Behavior for existing types is identical.

**Files:**
- Modify: `src/dataframe/boxed_series.zig`

- [ ] **Step 1: Rewrite the numeric-guard methods**

`mean` and `stdDev` (same shape):

```zig
    /// Returns the mean of non-null values as f64. Returns null if not a numeric type.
    pub fn mean(self: *Self) ?f64 {
        switch (self.*) {
            inline else => |s| {
                if (comptime @TypeOf(s.*).is_numeric) return s.mean();
                return null;
            },
        }
    }
```

`median` and `quantile` (same shape, propagating errors):

```zig
    /// Median of non-null values as f64. Returns null for non-numeric or all-null.
    pub fn median(self: *Self, allocator: std.mem.Allocator) !?f64 {
        switch (self.*) {
            inline else => |s| {
                if (comptime @TypeOf(s.*).is_numeric) return s.median(allocator);
                return null;
            },
        }
    }
```

(`quantile` passes `(allocator, q)`.)

`cumSum`, `cumMin`, `cumMax`, `cumProd`, `diff`, `diffLossy` (same shape):

```zig
    /// Running cumulative sum. Nulls propagate. Numeric columns only.
    pub fn cumSum(self: *Self) !BoxedSeries {
        switch (self.*) {
            inline else => |s| {
                if (comptime @TypeOf(s.*).is_numeric) return (try s.cumSum()).toBoxedSeries();
                return error.TypeNotNumeric;
            },
        }
    }
```

(`diff`/`diffLossy` pass `(n)`.)

- [ ] **Step 2: Guard the casts**

```zig
    /// Comptime-verified lossless cast. Compile error if the conversion could lose data.
    pub fn castSafe(self: *Self, comptime Target: type) !BoxedSeries {
        switch (self.*) {
            inline else => |s| {
                if (comptime @TypeOf(s.*).is_castable) return (try s.castSafe(Target)).toBoxedSeries();
                return error.TypeNotCastable;
            },
        }
    }
```

(`cast` and `castLossy` identically, calling `s.cast(Target)` / `s.castLossy(Target)`.)

- [ ] **Step 3: Guard `groupBy`** (keeps `GroupBy(Raw)` from ever instantiating, so `BoxedGroupBy` needs no new variant):

```zig
    pub fn groupBy(self: *Self, allocator: std.mem.Allocator, dataframe: *Dataframe) !BoxedGroupBy {
        switch (self.*) {
            inline else => |s| {
                if (comptime @TypeOf(s.*).is_groupable) {
                    const gb = try s.*.groupBy(allocator, dataframe);
                    return gb.toBoxedGroupBy();
                }
                return error.TypeNotGroupable;
            },
        }
    }
```

- [ ] **Step 4: Capability clone in `appendSeries`** — replace the String arm (import: `const hasMethod = @import("series.zig").hasMethod;` at the top of the file):

```zig
                    } else if (comptime hasMethod(@TypeOf(o.values.items[i]), "clone")) {
                        var cloned = try o.values.items[i].clone();
                        errdefer cloned.deinit();
                        try s.append(cloned);
                    } else {
```

- [ ] **Step 5: Run tests** — `zig build test` → all PASS (pure refactor; existing string/bool null-return tests in `series_test.zig`/`dataframe_test.zig` confirm semantics are unchanged).

---

### Task 9: The `Raw` value type

**Files:**
- Create: `src/dataframe/raw.zig`
- Modify: `src/dataframe/tests.zig` (register the file's tests via Task 11's `parquet_test.zig`; `raw.zig`'s own tests ride along via import below)

- [ ] **Step 1: Write the file with its tests**

```zig
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
```

- [ ] **Step 2: Run tests** — `zig build test` → file isn't imported anywhere yet, so its tests don't run; just confirm nothing broke. (They start running in Task 10 when `series.zig` imports `raw.zig`, and via `parquet_test.zig` in Task 11.)

---

### Task 10: `BoxedSeries.raw` variant + supporting arms

Adding the variant force-instantiates every `inline else` arm for `Series(Raw)` — Tasks 7–8 made that safe. The two exhaustive switches (`typeName`, `getType`) and `series.zig`'s `toBoxedSeries` need explicit arms; `boxedToColumnData` (dataframe adapter) and `json_writer` also get arms now so the build stays green in one step.

**Files:**
- Modify: `src/dataframe/boxed_series.zig`
- Modify: `src/dataframe/series.zig` (`toBoxedSeries`)
- Modify: `src/dataframe/parquet.zig` (`boxedToColumnData` `.raw` arm)
- Modify: `src/dataframe/json_writer.zig` (`isStringSeries`)

- [ ] **Step 1: Add the variant**

`boxed_series.zig` — import and union field:

```zig
const Raw = @import("raw.zig").Raw;
```

```zig
    string: *Series(String),
    raw: *Series(Raw),
```

`typeName`:

```zig
            .string => "String",
            .raw => "Raw",
```

`getType`:

```zig
            .string => return String,
            .raw => return Raw,
```

- [ ] **Step 2: `series.zig` `toBoxedSeries`** — import `const Raw = @import("raw.zig").Raw;` at the top, then add before the `else => @compileError(...)` arm:

```zig
                Raw => BoxedSeries{ .raw = self },
```

- [ ] **Step 3: `dataframe/parquet.zig` writer arm** — add to `boxedToColumnData`'s switch (alongside the `.string` arm; `Raw` import added in Task 11's wiring):

```zig
        .raw => |s| blk: {
            // Re-emit the preserved physical type + annotations bit-faithfully.
            const slices = try allocator.alloc([]const u8, s.values.items.len);
            for (s.values.items, 0..) |*r, j| {
                slices[j] = r.toSlice();
            }
            try string_bufs.append(allocator, slices);
            break :blk .{
                .name = s.name.toSlice(),
                .physical_type = s.meta.physical_type,
                .converted_type = s.meta.converted_type,
                .logical_type = s.meta.logical_type,
                .type_length = s.meta.type_length,
                .byte_arrays = slices,
                .num_values = s.values.items.len,
            };
        },
```

- [ ] **Step 4: `json_writer.zig`** — `isStringSeries` gains (hex payloads must be quoted to stay valid JSON):

```zig
        .raw => true,
```

- [ ] **Step 5: Run tests** — `zig build test` → all PASS. This is the moment the whole `Series(Raw)` surface compiles; any error here points at a missed guard from Tasks 7–8 (fix the guard, not the variant).

- [ ] **Step 6: Verify `join.zig` compiles and behaves with the new variant** (gap flagged by the 2026-06-04 project review): `join` dispatches `inline else` into `joinTyped`, instantiating `GroupByContext(Raw)` + `HashMap(Raw, …)` + the unmatched-cell default-value synthesis for the new variant. The Task 7 `GroupByContext` capability arms should make hashing compile; read `src/dataframe/join.zig` and check the default-value path (the code that appends `0`/`false`/`""` for unmatched rows) compiles for `Raw`. If it doesn't, gate the join dispatch with the same comptime pattern as Task 8 (`is_groupable` → `error.TypeNotJoinable`) rather than teaching join about Raw. Joining ON a Raw column is not a supported use case in 6d-2a.0.

---

### Task 11: Reader resolution skeleton + Raw fallback wiring

**Files:**
- Modify: `src/dataframe/parquet.zig`
- Create: `src/dataframe/parquet_test.zig`
- Modify: `src/dataframe/tests.zig`

- [ ] **Step 1: Write the failing tests** — create `src/dataframe/parquet_test.zig`:

```zig
const std = @import("std");
const parquet = @import("parquet");
const adapter = @import("parquet.zig");
const Raw = @import("raw.zig").Raw;

test "resolveKind: precedence logical -> converted -> physical" {
    var col = parquet.ParquetColumn.initEmpty(std.testing.allocator);

    // Bare physical
    col.physical_type = .int32;
    try std.testing.expectEqual(adapter.ResolvedKind.int32_, adapter.resolveKind(&col));

    // Legacy converted type wins over bare physical
    col.converted_type = .uint_32;
    try std.testing.expectEqual(adapter.ResolvedKind.uint32_, adapter.resolveKind(&col));

    // Modern logical type wins over converted
    col.logical_type = .{ .integer = .{ .bit_width = 8, .is_signed = true } };
    try std.testing.expectEqual(adapter.ResolvedKind.int8_, adapter.resolveKind(&col));

    // Not-yet-surfaced logical annotation falls through to the physical default
    col.converted_type = null;
    col.logical_type = .date;
    try std.testing.expectEqual(adapter.ResolvedKind.int32_, adapter.resolveKind(&col));

    // Deferred logical types -> raw
    col.physical_type = .byte_array;
    col.logical_type = .variant;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));
    col.logical_type = .geometry;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // INT96 physical -> raw
    col.logical_type = null;
    col.physical_type = .int96;
    try std.testing.expectEqual(adapter.ResolvedKind.raw, adapter.resolveKind(&col));

    // byte_array + utf8 -> string (unchanged behavior)
    col.physical_type = .byte_array;
    col.converted_type = .utf8;
    try std.testing.expectEqual(adapter.ResolvedKind.string, adapter.resolveKind(&col));
}

test "adapter: INT96 column loads as Raw and round-trips bit-faithfully" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/int96.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    const boxed = &df.series.items[0];
    try std.testing.expectEqualStrings("Raw", boxed.typeName());
    const s = boxed.raw;
    try std.testing.expectEqual(parquet.PhysicalType.int96, s.meta.physical_type);
    try std.testing.expectEqual(@as(usize, 3), s.len());
    for (s.values.items) |r| {
        try std.testing.expectEqual(@as(usize, 12), r.bytes.len);
    }

    // teddy write -> teddy read -> identical bytes and physical type
    var cols = try adapter.fromDataframe(allocator, df);
    defer cols.deinit();
    const output = try parquet.writeParquet(allocator, cols.columns, .{});
    defer allocator.free(output);

    var result2 = try parquet.readParquet(allocator, output);
    defer result2.deinit();
    try std.testing.expectEqual(parquet.PhysicalType.int96, result2.columns[0].physical_type);
    const orig = result.columns[0].byte_arrays.?;
    const rt = result2.columns[0].byte_arrays.?;
    try std.testing.expectEqual(orig.len, rt.len);
    for (orig, rt) |a, b| {
        try std.testing.expectEqualSlices(u8, a, b);
    }
}

test "adapter: logical_annotations.parquet still reads end-to-end" {
    // date/time/timestamp/decimal aren't surfaced yet (slices .1-.5); this
    // pins that they keep resolving to their physical defaults, not Raw.
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    const file_data = try cwd.readFileAlloc(io, "data/logical_annotations.parquet", allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    try std.testing.expectEqual(@as(usize, 4), df.width());
    try std.testing.expectEqualStrings("i32", df.series.items[0].typeName()); // date32 -> i32
    try std.testing.expectEqualStrings("i64", df.series.items[1].typeName()); // time64(us) -> i64
    try std.testing.expectEqualStrings("i64", df.series.items[2].typeName()); // timestamp(us) -> i64
    try std.testing.expectEqualStrings("String", df.series.items[3].typeName()); // decimal FLBA -> String (for now)
}
```

Register in `src/dataframe/tests.zig`:

```zig
    _ = @import("parquet_test.zig");
```

- [ ] **Step 2: Run to verify failure** — compile error: `adapter.ResolvedKind` not defined.

- [ ] **Step 3: Implement in `src/dataframe/parquet.zig`**

Add the import:

```zig
const Raw = @import("raw.zig").Raw;
```

Add above `addColumn`:

```zig
/// Dataframe-side type a parquet column resolves to.
pub const ResolvedKind = enum {
    boolean,
    int8_,
    int16_,
    int32_,
    int64_,
    uint8_,
    uint16_,
    uint32_,
    uint64_,
    float32_,
    float64_,
    string,
    raw,
};

/// Resolution precedence: modern logical_type (field 10) → legacy
/// converted_type (field 6) → bare physical type. Logical annotations not yet
/// surfaced as dataframe types (date/time/timestamp/decimal/uuid/float16/...)
/// fall through to the physical default; slices 6d-2a.1–.5 flip them one at a
/// time. Deferred types (VARIANT/GEOMETRY/GEOGRAPHY) and INT96 resolve to Raw.
pub fn resolveKind(col: *const parquet.ParquetColumn) ResolvedKind {
    if (col.logical_type) |lt| switch (lt) {
        .integer => |it| {
            if (it.is_signed) {
                switch (it.bit_width) {
                    8 => return .int8_,
                    16 => return .int16_,
                    32 => return .int32_,
                    64 => return .int64_,
                    else => {},
                }
            } else {
                switch (it.bit_width) {
                    8 => return .uint8_,
                    16 => return .uint16_,
                    32 => return .uint32_,
                    64 => return .uint64_,
                    else => {},
                }
            }
        },
        .string, .@"enum", .json => return .string,
        .variant, .geometry, .geography => return .raw,
        else => {},
    };
    if (col.converted_type) |ct| switch (ct) {
        .int_8 => return .int8_,
        .int_16 => return .int16_,
        .uint_8 => return .uint8_,
        .uint_16 => return .uint16_,
        .uint_32 => return .uint32_,
        .uint_64 => return .uint64_,
        .utf8 => return .string,
        else => {},
    };
    return switch (col.physical_type) {
        .boolean => .boolean,
        .int32 => .int32_,
        .int64 => .int64_,
        .float => .float32_,
        .double => .float64_,
        .byte_array, .fixed_len_byte_array => .string,
        .int96 => .raw,
    };
}
```

Rewrite `addColumn` to dispatch on it (full replacement — the per-kind bodies are the existing arms, relocated):

```zig
fn addColumn(allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    switch (resolveKind(col)) {
        .boolean => {
            var s = try df.createSeries(bool);
            try s.rename(col.name);
            const vals = col.booleans orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else false);
            }
        },
        .int8_ => return addNarrowIntColumn(i8, allocator, df, col),
        .int16_ => return addNarrowIntColumn(i16, allocator, df, col),
        .uint8_ => return addNarrowIntColumn(u8, allocator, df, col),
        .uint16_ => return addNarrowIntColumn(u16, allocator, df, col),
        // uint_32/uint_64 span the full unsigned range, so the signed bit
        // pattern must be reinterpreted (bitcast), not range-cast.
        .uint32_ => return addUintColumn(u32, i32, df, col, col.int32s),
        .uint64_ => return addUintColumn(u64, i64, df, col, col.int64s),
        .int32_ => {
            var s = try df.createSeries(i32);
            try s.rename(col.name);
            const vals = col.int32s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .int64_ => {
            var s = try df.createSeries(i64);
            try s.rename(col.name);
            const vals = col.int64s orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .float32_ => {
            var s = try df.createSeries(f32);
            try s.rename(col.name);
            const vals = col.floats orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .float64_ => {
            var s = try df.createSeries(f64);
            try s.rename(col.name);
            const vals = col.doubles orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                try s.append(if (valid and i < vals.len) vals[i] else 0);
            }
        },
        .string => {
            var s = try df.createSeries(String);
            try s.rename(col.name);
            const vals = col.byte_arrays orelse return;
            for (0..col.num_rows) |i| {
                const valid = if (col.validity) |v| v[i] else true;
                if (valid and i < vals.len) {
                    try s.append(try String.fromSlice(allocator, vals[i]));
                } else {
                    try s.append(try String.fromSlice(allocator, ""));
                }
            }
        },
        .raw => return addRawColumn(allocator, df, col),
    }
}

/// Fallback: preserve undecoded bytes + column metadata so the column can be
/// re-emitted bit-faithfully. Mirrors the String arm's null handling (invalid
/// rows become empty values — the adapter does not carry validity yet).
fn addRawColumn(allocator: Allocator, df: *dataframe.Dataframe, col: *const parquet.ParquetColumn) !void {
    var s = try df.createSeries(Raw);
    try s.rename(col.name);
    s.meta = .{
        .physical_type = col.physical_type,
        .converted_type = col.converted_type,
        .logical_type = col.logical_type,
        .type_length = col.type_length,
    };
    const vals = col.byte_arrays orelse return;
    for (0..col.num_rows) |i| {
        const valid = if (col.validity) |v| v[i] else true;
        if (valid and i < vals.len) {
            try s.append(try Raw.fromSlice(allocator, vals[i]));
        } else {
            try s.append(try Raw.fromSlice(allocator, ""));
        }
    }
}
```

(The old `error.UnsupportedParquetType` arm disappears: every physical type now resolves.)

- [ ] **Step 4: Run tests** — `zig build test` → all PASS, including the pre-existing unsigned/multi-rowgroup/addresses suites (the rewrite must not change their resolution — `unsigned.parquet` now resolves via the **logical** integer annotation instead of converted_type, with the identical u32/u64 outcome).

---

### Task 12: Docs, full verification, review, single commit

**Files:**
- Modify: `docs/cleanup-roadmap.md:61`

- [ ] **Step 1: Update the roadmap** — change the 6d-2a bullet's slice list to mark `.0` done:

In `docs/cleanup-roadmap.md`, replace the line:

```
  Implement as per-type vertical slices (6d-2a.0 infra → .1 Date → .2 Timestamp/Time
```

with:

```
  Implement as per-type vertical slices (6d-2a.0 infra ✅ → .1 Date → .2 Timestamp/Time
```

- [ ] **Step 2: Full verification**

Run: `zig build test`
Expected: 0 failures; total test count > 413 (new: 4 LogicalType decode, 1 encode round-trip, 2 SchemaElement, 1 footer fixture, 1 propagation, 4 writer, 8 capability, 2 Raw, 3 adapter ≈ 26 added).

Run: `git status`
Expected new/modified files match the File Map plus `data/logical_annotations.parquet`.

- [ ] **Step 3: USER REVIEW GATE** — present the diff summary to the user (session norm: user reviews each phase before commit). Do not commit until approved.

- [ ] **Step 4: Commit (after approval)**

```bash
git add src/parquet src/dataframe src_py/gen_fixtures.py data/logical_annotations.parquet docs/cleanup-roadmap.md docs/superpowers/plans/2026-06-04-parquet-6d-2a-0-shared-infra.md
git commit -m "feat(parquet): logical-type infra — thrift LogicalType, Series capabilities, Raw fallback (6d-2a.0)

- Model parquet.thrift LogicalType union (+ TimeUnit); parse + encode
  SchemaElement field 10 and type_length (field 2)
- Propagate logical_type/type_length through ParquetColumn; write
  FIXED_LEN_BYTE_ARRAY + INT96 physicals
- Series(T): comptime capability convention (deinit/clone/eql/toSlice/
  format/init/type_name/ColumnMeta via @hasDecl) replacing String
  special-cases; typeInfo-based is_numeric; comptime-safe BoxedSeries
  guards
- New Raw column type: INT96 + deferred logical types (VARIANT/GEO) now
  read end-to-end and round-trip bit-faithfully
- Fixture data/logical_annotations.parquet (gen_fixtures.py)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review Notes (spec §6d-2a.0 coverage)

| Spec item | Task |
|---|---|
| `TimeUnit` + `LogicalType` thrift model (parse + encode field 10, nested structs) | 1–3 |
| `Series` comptime capability convention replacing `String` special-cases | 7 |
| Reader resolution skeleton (logical → converted → physical precedence) | 11 |
| Writer resolution skeleton (Raw re-emit; FLBA/INT96 physical write; schema carries annotations) | 6, 10 |
| `Raw` type + variant + fallback wiring | 9–11 |
| `WriteOptions.emit_int96` | **deferred to slice 6d-2a.2** (no consumer until INT96→Timestamp decode) |
| FixedBytes "column width on the Series" groundwork | `ColumnMetaFor` capability (Task 7) is the mechanism slice .4 will reuse |

Known intentional behaviors (call out in review): nulls read from parquet are still materialized as placeholder values at the dataframe layer (pre-existing adapter limitation, unchanged); `bson` continues to resolve as String (only VARIANT/GEO/INT96 divert to Raw); `asStringAt` on values whose formatted form exceeds 128 bytes returns `error.NoSpaceLeft` (pre-existing buffer, unchanged).
