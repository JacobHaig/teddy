//! Phase 9.0 — Python↔Zig regression harness (read-stage parity).
//!
//! Loads data/validation/alltypes.parquet via the parquet reader + dataframe
//! adapter, loads data/validation/alltypes.golden.json (pyarrow ground truth),
//! and asserts that teddy's typed accessors reproduce the same SEMANTIC
//! canonical per cell (see validation/README.md for the protocol).
//!
//! Every mismatch is collected into a report (printed to stderr + written to
//! data/validation/divergence_report.txt) for visibility, then ENFORCED
//! against a committed allowlist (Phase 9.2): the read/transforms/sort
//! divergences must each appear in `known_divergences` (a NEW, unexpected one
//! FAILS the test = regression protection), and every allowlist entry must
//! still be observed (stale entries are flagged). The CSV/JSON round-trip
//! fidelity matrix is asserted against `expected_fidelity` (a committed
//! expected matrix — a future change to CSV/JSON typing is caught). See
//! docs/validation-divergences.md for the rationale of each justified entry.
//!
//! The test HARD-fails if the framework itself can't run (fixture missing, a
//! golden column absent from teddy, num_rows mismatch), on any unexpected /
//! stale divergence, on a fidelity-matrix change, on any tdf_roundtrip entry
//! (lossless contract), or if a Nested column's JSON output is not valid JSON.

const std = @import("std");
const parquet = @import("parquet");
const adapter = @import("parquet.zig");
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const Nested = @import("nested.zig").Nested;
const Dataframe = @import("dataframe.zig").Dataframe;
const native_format = @import("native_format.zig");
const csv_writer = @import("csv_writer.zig");
const csv_reader = @import("csv_reader.zig");
const json_writer = @import("json_writer.zig");
const json_reader = @import("json_reader.zig");

const FIXTURE = "data/validation/alltypes.parquet";
const GOLDEN = "data/validation/alltypes.golden.json";
const REPORT = "data/validation/divergence_report.txt";

/// Which end-to-end stage produced an entry. Read/transform/sort entries are
/// genuine divergences (teddy vs pyarrow); round-trip entries are CATALOG rows
/// (always recorded — a fidelity matrix of "what survives each format"), except
/// TDF which is a lossless contract (any tdf_roundtrip entry is a real bug).
const Stage = enum { read, transforms, sort, tdf_roundtrip, csv_roundtrip, json_roundtrip };

const DivergenceKind = enum {
    // read stage
    null_mismatch,
    type_mismatch,
    value_mismatch,
    missing_column,
    // transforms stage
    transform_sum,
    transform_mean,
    transform_min,
    transform_max,
    transform_missing, // teddy returned null where pyarrow produced a number
    // sort stage
    sort_order,
    // round-trip catalog
    tdf_roundtrip,
    csv_roundtrip,
    json_roundtrip,
};

const Divergence = struct {
    stage: Stage,
    column: []const u8, // borrowed from golden JSON (lives for the test)
    row: i64, // -1 for column-level (type/missing/transform/sort/round-trip)
    kind: DivergenceKind,
    arrow_repr: []u8, // owned — pyarrow / original side
    teddy_repr: []u8, // owned — teddy / re-read side
};

test "regression: read-stage parity vs pyarrow golden (soft report)" {
    const allocator = std.testing.allocator;
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();

    // --- load fixture -> dataframe ---
    const file_data = try cwd.readFileAlloc(io, FIXTURE, allocator, .unlimited);
    defer allocator.free(file_data);

    var result = try parquet.readParquet(allocator, file_data);
    defer result.deinit();
    var df = try adapter.toDataframe(allocator, &result);
    defer df.deinit();

    // --- load + parse golden ---
    const golden_bytes = try cwd.readFileAlloc(io, GOLDEN, allocator, .unlimited);
    defer allocator.free(golden_bytes);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, golden_bytes, .{});
    defer parsed.deinit();
    const root = parsed.value.object;

    const golden_num_rows: usize = @intCast(root.get("num_rows").?.integer);
    const golden_columns = root.get("columns").?.array;

    // --- divergence collection ---
    var divergences = std.ArrayList(Divergence).empty;
    defer {
        for (divergences.items) |d| {
            allocator.free(d.arrow_repr);
            allocator.free(d.teddy_repr);
        }
        divergences.deinit(allocator);
    }

    for (golden_columns.items) |col_val| {
        const col = col_val.object;
        const name = col.get("name").?.string;
        const expected_type = col.get("expected_teddy_type").?.string;
        const cells = col.get("cells").?.array;

        const boxed = df.getSeries(name) orelse {
            // HARD failure: the framework can't proceed without the column.
            std.debug.print("MISSING COLUMN: {s}\n", .{name});
            try writeReport(allocator, divergences.items);
            return error.GoldenColumnMissing;
        };

        // Type parity (collected, not hard-failed — a type-mapping divergence
        // is a legitimate triage item).
        const actual_type = boxed.typeName();
        if (!std.mem.eql(u8, actual_type, expected_type)) {
            try divergences.append(allocator, .{
                .stage = .read,
                .column = name,
                .row = -1,
                .kind = .type_mismatch,
                .arrow_repr = try allocator.dupe(u8, expected_type),
                .teddy_repr = try allocator.dupe(u8, actual_type),
            });
            // Continue: per-cell extraction below may still be meaningful, but
            // if the type diverged the semantic shapes likely differ too, so we
            // skip cells to avoid noise.
            continue;
        }

        // Row count parity per column is implied by num_rows; the cell loop
        // bounds on the golden cells and checks teddy length below.
        if (boxed.len() != golden_num_rows) {
            std.debug.print("ROW COUNT MISMATCH col={s} teddy={d} golden={d}\n", .{ name, boxed.len(), golden_num_rows });
            try writeReport(allocator, divergences.items);
            return error.RowCountMismatch;
        }

        for (cells.items, 0..) |cell_val, row| {
            const cell = cell_val.object;
            const golden_is_null = cell.get("null") != null;
            const teddy_is_null = boxed.isNull(row);

            if (golden_is_null != teddy_is_null) {
                try divergences.append(allocator, .{
                    .stage = .read,
                    .column = name,
                    .row = @intCast(row),
                    .kind = .null_mismatch,
                    .arrow_repr = try allocator.dupe(u8, if (golden_is_null) "null" else "non-null"),
                    .teddy_repr = try allocator.dupe(u8, if (teddy_is_null) "null" else "non-null"),
                });
                continue;
            }
            if (golden_is_null) continue; // both null — agree

            const golden_cell = cell.get("v").?;

            // Floats compare numerically with tolerance; everything else is a
            // canonical-string compare.
            if (isFloatColumn(boxed.*)) {
                const teddy_f = teddyFloat(boxed.*, row);
                const golden_f = jsonToF64(golden_cell);
                const tol: f64 = if (boxed.* == .float16) 1e-2 else 1e-6;
                if (!floatsClose(teddy_f, golden_f, tol)) {
                    try divergences.append(allocator, .{
                        .stage = .read,
                        .column = name,
                        .row = @intCast(row),
                        .kind = .value_mismatch,
                        .arrow_repr = try std.fmt.allocPrint(allocator, "{d}", .{golden_f}),
                        .teddy_repr = try std.fmt.allocPrint(allocator, "{d}", .{teddy_f}),
                    });
                }
                continue;
            }

            // String-canonical path.
            const teddy_sem = try teddySemantic(allocator, boxed.*, row);
            defer allocator.free(teddy_sem);
            const golden_sem = try goldenSemantic(allocator, golden_cell);
            defer allocator.free(golden_sem);

            if (!std.mem.eql(u8, teddy_sem, golden_sem)) {
                try divergences.append(allocator, .{
                    .stage = .read,
                    .column = name,
                    .row = @intCast(row),
                    .kind = .value_mismatch,
                    .arrow_repr = try allocator.dupe(u8, golden_sem),
                    .teddy_repr = try allocator.dupe(u8, teddy_sem),
                });
            }
        }
    }

    // ================= Stage 2 — transforms (pyarrow.compute reference) =====
    // For each numeric column the golden carries sum/mean/min/max (as f64) plus,
    // on the chosen sort column, the ascending sort_indices pyarrow produced.
    // teddy's BoxedSeries sum/mean/min/max return ?f64; we compare with relative
    // tolerance and CATALOG mismatches (never hard-fail).
    if (root.get("transforms")) |transforms_val| {
        var it = transforms_val.object.iterator();
        while (it.next()) |entry| {
            const col_name = entry.key_ptr.*;
            const spec = entry.value_ptr.*.object;

            const boxed = df.getSeries(col_name) orelse continue;
            const tol: f64 = if (boxed.* == .float16) 1e-2 else 1e-6;

            // sum: teddy's BoxedSeries.sum() accumulates in the column's NATIVE
            // int width and panics on overflow in safe builds (the c_u64 fixture
            // holds values near u64::MAX, so its sum is not representable). We
            // can't catch a panic, so we PRE-DETECT overflow by replaying teddy's
            // native-width accumulation with @addWithOverflow — mirroring the
            // error BoxedSeries.sumChecked() (the safe opt-in) would return.
            // Overflow here is a JUSTIFIED, allowlisted divergence: pyarrow
            // widens the accumulator; teddy keeps the native width (user
            // decision — sum() native width; sumChecked() is the checked path).
            switch (teddySumNativeWidth(boxed.*)) {
                .value => |teddy_sum| try checkAgg(allocator, &divergences, col_name, .transform_sum, teddy_sum, spec.get("sum"), tol),
                .overflow => {
                    if (spec.get("sum")) |golden_sum| {
                        try divergences.append(allocator, .{
                            .stage = .transforms,
                            .column = col_name,
                            .row = -1,
                            .kind = .transform_sum,
                            .arrow_repr = try std.fmt.allocPrint(allocator, "{d}", .{jsonToF64(golden_sum)}),
                            .teddy_repr = try allocator.dupe(u8, "OVERFLOW (native-width accumulator panics; pyarrow widens)"),
                        });
                    }
                },
                .not_numeric => {},
            }

            // mean / min / max
            try checkAgg(allocator, &divergences, col_name, .transform_mean, boxed.mean(), spec.get("mean"), tol);
            try checkAgg(allocator, &divergences, col_name, .transform_min, boxed.min(), spec.get("min"), tol);
            try checkAgg(allocator, &divergences, col_name, .transform_max, boxed.max(), spec.get("max"), tol);

            // ============= Stage 2b — ascending sort order =================
            // pyarrow places nulls LAST (null_placement="at_end"); as of Phase
            // 9.2 teddy's argSort matches this (nulls-last in both directions),
            // so we now EXPECT PARITY. Any recorded sort_order entry is an
            // unexpected divergence (not in the allowlist) and FAILS the test.
            if (spec.get("sort_asc_indices")) |golden_idx_val| {
                var idx = try boxed.argSort(allocator, true);
                defer idx.deinit(allocator);

                const golden_order = try formatGoldenIndices(allocator, golden_idx_val.array);
                defer allocator.free(golden_order);
                const teddy_order = try formatUsizeIndices(allocator, idx.items);
                defer allocator.free(teddy_order);

                if (!std.mem.eql(u8, golden_order, teddy_order)) {
                    try divergences.append(allocator, .{
                        .stage = .sort,
                        .column = col_name,
                        .row = -1,
                        .kind = .sort_order,
                        .arrow_repr = try allocator.dupe(u8, golden_order),
                        .teddy_repr = try allocator.dupe(u8, teddy_order),
                    });
                }
            }
        }
    }

    // ================= Stage 3 — format round-trips (teddy-internal) ========
    // These are CATALOG entries (always recorded), producing a fidelity matrix.
    // TDF is the one lossless contract: any tdf_roundtrip entry is a real bug.
    try runTdfRoundtrip(allocator, &divergences, df, golden_columns, golden_num_rows);
    try runCsvRoundtrip(allocator, &divergences, df);
    try runJsonRoundtrip(allocator, &divergences, df);

    // --- emit report (stderr + file) ---
    try writeReport(allocator, divergences.items);

    // Framework-ran sanity checks.
    try std.testing.expect(df.width() >= golden_columns.items.len);
    try std.testing.expectEqual(golden_num_rows, df.height());

    // === ENFORCEMENT (Phase 9.2) ============================================
    // 1. Every read/transforms/sort divergence must be in the allowlist.
    // 2. Every allowlist entry must still be observed (no stale entries).
    // 3. CSV/JSON fidelity matrix must match the committed expected matrix.
    // 4. tdf_roundtrip must be empty (lossless contract).
    // 5. Every Nested column's JSON write must be valid JSON.
    try enforceAllowlist(divergences.items);
    try enforceFidelityMatrix(allocator, divergences.items);
    try enforceNestedJsonValid(allocator, df);
}

// ---------------------------------------------------------------------------
// Phase 9.2 — committed allowlist of justified divergences.
//
// Each entry is a (stage, column, kind) tuple the user has signed off on (see
// docs/validation-divergences.md). The harness asserts the observed
// read/transforms/sort divergences are EXACTLY this set. Round-trip entries
// are a fidelity CATALOG (enforced separately by enforceFidelityMatrix), and
// tdf_roundtrip is a lossless contract (asserted empty), so neither appears
// here.
// ---------------------------------------------------------------------------

const KnownDivergence = struct {
    stage: Stage,
    column: []const u8,
    kind: DivergenceKind,
};

const known_divergences = [_]KnownDivergence{
    // mean over c_i64 (i64::MIN/MAX boundary values): teddy accumulates in f64,
    // pyarrow exactly. f64-precision divergence — JUSTIFIED.
    .{ .stage = .transforms, .column = "c_i64", .kind = .transform_mean },
    // sum over unsigned columns whose totals exceed the native width: teddy
    // keeps the native-width accumulator (user decision; sumChecked is the safe
    // opt-in), pyarrow widens. Overflow is detected (not panicked) and cataloged
    // — JUSTIFIED.
    .{ .stage = .transforms, .column = "c_u8", .kind = .transform_sum },
    .{ .stage = .transforms, .column = "c_u16", .kind = .transform_sum },
    .{ .stage = .transforms, .column = "c_u32", .kind = .transform_sum },
    .{ .stage = .transforms, .column = "c_u64", .kind = .transform_sum },
};

fn isAllowlistStage(stage: Stage) bool {
    return switch (stage) {
        .read, .transforms, .sort => true,
        .tdf_roundtrip, .csv_roundtrip, .json_roundtrip => false,
    };
}

fn enforceAllowlist(divs: []const Divergence) !void {
    var seen = [_]bool{false} ** known_divergences.len;

    // tdf_roundtrip is a lossless contract: any entry is a real bug.
    for (divs) |d| {
        if (d.stage == .tdf_roundtrip) {
            std.debug.print(
                "TDF ROUNDTRIP (lossless contract violated): col={s} arrow={s} teddy={s}\n",
                .{ d.column, d.arrow_repr, d.teddy_repr },
            );
            return error.TdfRoundtripRegression;
        }
    }

    // 1. Every read/transforms/sort divergence must be allowlisted.
    for (divs) |d| {
        if (!isAllowlistStage(d.stage)) continue;
        var matched = false;
        for (known_divergences, 0..) |k, i| {
            if (k.stage == d.stage and k.kind == d.kind and std.mem.eql(u8, k.column, d.column)) {
                seen[i] = true;
                matched = true;
                break;
            }
        }
        if (!matched) {
            std.debug.print(
                "UNEXPECTED DIVERGENCE (not in allowlist): stage={s} col={s} kind={s} arrow={s} teddy={s}\n",
                .{ @tagName(d.stage), d.column, @tagName(d.kind), d.arrow_repr, d.teddy_repr },
            );
            return error.UnexpectedDivergence;
        }
    }

    // 2. Every allowlist entry must still be observed.
    for (known_divergences, 0..) |k, i| {
        if (!seen[i]) {
            std.debug.print(
                "STALE ALLOWLIST ENTRY (no longer observed): stage={s} col={s} kind={s}\n",
                .{ @tagName(k.stage), k.column, @tagName(k.kind) },
            );
            return error.StaleAllowlistEntry;
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 9.2 — committed CSV/JSON fidelity matrix.
//
// Text formats carry no type metadata, so a column's re-read type is a
// documented function of the writer/reader inference. We pin (column, stage,
// reread_type) here; any change to CSV/JSON typing flips a cell and fails the
// test, referencing docs/validation-divergences.md. The `teddy_repr` cataloged
// by the round-trip stages is "<reread_type> (sample=\"...\")" — we compare the
// reread_type PREFIX (sample values are data, not contract).
// ---------------------------------------------------------------------------

const FidelityRow = struct {
    column: []const u8,
    csv_type: []const u8,
    json_type: []const u8,
};

const expected_fidelity = [_]FidelityRow{
    .{ .column = "c_bool", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_i8", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_i16", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_i32", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_i64", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_u8", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_u16", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_u32", .csv_type = "i64", .json_type = "i64" },
    .{ .column = "c_u64", .csv_type = "f64", .json_type = "f64" },
    .{ .column = "c_f32", .csv_type = "f64", .json_type = "f64" },
    .{ .column = "c_f64", .csv_type = "f64", .json_type = "f64" },
    .{ .column = "c_f16", .csv_type = "f64", .json_type = "f64" },
    .{ .column = "c_str", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_binary", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_fixed", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_date", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_time32", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_time64", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_ts_utc", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_ts_naive", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_ts_ns", .csv_type = "String", .json_type = "String" },
    .{ .column = "c_dec9", .csv_type = "f64", .json_type = "String" },
    .{ .column = "c_dec38", .csv_type = "f64", .json_type = "String" },
    .{ .column = "c_uuid", .csv_type = "String", .json_type = "String" },
    // Nested: CSV reads the rendered text back as a String. JSON write is valid
    // (verified by enforceNestedJsonValid) but the json_reader cannot
    // RECONSTRUCT a Nested column (Phase 7 documented gap), so the per-column
    // JSON round-trip parse fails — we pin that documented limitation here.
    .{ .column = "c_list", .csv_type = "String", .json_type = "parse failed: InvalidJson" },
    .{ .column = "c_struct", .csv_type = "String", .json_type = "parse failed: InvalidJson" },
    .{ .column = "c_map", .csv_type = "String", .json_type = "parse failed: InvalidJson" },
    .{ .column = "c_list_struct", .csv_type = "String", .json_type = "parse failed: InvalidJson" },
    .{ .column = "c_list_list", .csv_type = "String", .json_type = "parse failed: InvalidJson" },
};

/// The cataloged round-trip teddy_repr is either "<type> (sample=...)" or a
/// "<phase> failed: <err>" note. Return the contract-bearing prefix: the type
/// name before " (sample=" or the whole failure note.
fn fidelityPrefix(repr: []const u8) []const u8 {
    if (std.mem.indexOf(u8, repr, " (sample=")) |i| return repr[0..i];
    return repr;
}

fn enforceFidelityMatrix(allocator: std.mem.Allocator, divs: []const Divergence) !void {
    _ = allocator;
    inline for (.{ Stage.csv_roundtrip, Stage.json_roundtrip }) |stage| {
        for (expected_fidelity) |row| {
            const expected = if (stage == .csv_roundtrip) row.csv_type else row.json_type;
            var found = false;
            for (divs) |d| {
                if (d.stage != stage) continue;
                if (!std.mem.eql(u8, d.column, row.column)) continue;
                found = true;
                const actual = fidelityPrefix(d.teddy_repr);
                if (!std.mem.eql(u8, actual, expected)) {
                    std.debug.print(
                        "FIDELITY MATRIX CHANGED: stage={s} col={s} expected={s} actual={s} " ++
                            "(see docs/validation-divergences.md)\n",
                        .{ @tagName(stage), row.column, expected, actual },
                    );
                    return error.FidelityMatrixChanged;
                }
                break;
            }
            if (!found) {
                std.debug.print(
                    "FIDELITY MATRIX: missing {s} entry for col={s}\n",
                    .{ @tagName(stage), row.column },
                );
                return error.FidelityMatrixMissing;
            }
        }
    }
}

/// Assert every Nested column's JSON WRITE produces valid JSON (Phase 9.2: the
/// dedicated nested_json renderer names struct fields from the column schema
/// and emits parseable objects/arrays). Reconstruction on READ remains a
/// documented Phase 7 gap (pinned in the fidelity matrix above).
fn enforceNestedJsonValid(allocator: std.mem.Allocator, df: *Dataframe) !void {
    for (df.series.items) |*boxed| {
        if (boxed.* != .nested) continue;

        var one = try Dataframe.init(allocator);
        defer one.deinit();
        var copy = try boxed.deepCopy();
        one.addSeries(copy) catch |err| {
            copy.deinit();
            return err;
        };

        const text = try json_writer.writeToString(allocator, one, .rows);
        defer allocator.free(text);

        var parsed = std.json.parseFromSlice(std.json.Value, allocator, text, .{}) catch |err| {
            std.debug.print(
                "NESTED JSON INVALID: col={s} json_writer output did not parse: {s}\n  output: {s}\n",
                .{ boxed.name(), @errorName(err), text },
            );
            return error.NestedJsonInvalid;
        };
        parsed.deinit();
    }
}

// ---------------------------------------------------------------------------
// Stage 2 helpers — transforms + sort
// ---------------------------------------------------------------------------

/// Compare one teddy aggregation (?f64) against the golden JSON number with
/// relative tolerance. Records a divergence if they differ or if teddy returned
/// null where pyarrow produced a value.
fn checkAgg(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    col: []const u8,
    kind: DivergenceKind,
    teddy_opt: ?f64,
    golden_opt: ?std.json.Value,
    tol: f64,
) !void {
    const golden_val = golden_opt orelse return; // golden didn't carry this stat
    const golden_f = jsonToF64(golden_val);

    if (teddy_opt) |teddy_f| {
        if (!floatsClose(teddy_f, golden_f, tol)) {
            try divs.append(allocator, .{
                .stage = .transforms,
                .column = col,
                .row = -1,
                .kind = kind,
                .arrow_repr = try std.fmt.allocPrint(allocator, "{d}", .{golden_f}),
                .teddy_repr = try std.fmt.allocPrint(allocator, "{d}", .{teddy_f}),
            });
        }
    } else {
        try divs.append(allocator, .{
            .stage = .transforms,
            .column = col,
            .row = -1,
            .kind = .transform_missing,
            .arrow_repr = try std.fmt.allocPrint(allocator, "{d}", .{golden_f}),
            .teddy_repr = try allocator.dupe(u8, "null"),
        });
    }
}

const NativeSum = union(enum) { value: f64, overflow, not_numeric };

/// Replay BoxedSeries.sum()'s native-width integer accumulation, but with
/// @addWithOverflow so we DETECT (rather than panic on) the overflow teddy's
/// real sum() would hit. Floats accumulate in their own width (no overflow).
/// Returns the f64 of the native-width result, .overflow, or .not_numeric.
fn teddySumNativeWidth(b: BoxedSeries) NativeSum {
    switch (b) {
        inline .uint8, .uint16, .uint32, .uint64, .uint128, .usize, .int8, .int16, .int32, .int64, .int128, .isize => |s| {
            const T = @TypeOf(s.values.items[0]);
            var total: T = 0;
            for (s.values.items, 0..) |v, i| {
                if (s.isNull(i)) continue;
                const r, const ov = @addWithOverflow(total, v);
                if (ov != 0) return .overflow;
                total = r;
            }
            return .{ .value = @floatFromInt(total) };
        },
        inline .float16, .float32 => |s| {
            var total: f64 = 0;
            for (s.values.items, 0..) |v, i| {
                if (!s.isNull(i)) total += @as(f64, v);
            }
            return .{ .value = total };
        },
        .float64 => |s| {
            var total: f64 = 0;
            for (s.values.items, 0..) |v, i| {
                if (!s.isNull(i)) total += v;
            }
            return .{ .value = total };
        },
        else => return .not_numeric,
    }
}

fn formatGoldenIndices(allocator: std.mem.Allocator, arr: std.json.Array) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    const w = &aw.writer;
    try w.writeByte('[');
    for (arr.items, 0..) |item, i| {
        if (i > 0) try w.writeAll(", ");
        try w.print("{d}", .{item.integer});
    }
    try w.writeByte(']');
    return aw.toOwnedSlice();
}

fn formatUsizeIndices(allocator: std.mem.Allocator, items: []const usize) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    const w = &aw.writer;
    try w.writeByte('[');
    for (items, 0..) |item, i| {
        if (i > 0) try w.writeAll(", ");
        try w.print("{d}", .{item});
    }
    try w.writeByte(']');
    return aw.toOwnedSlice();
}

// ---------------------------------------------------------------------------
// Stage 3 helpers — format round-trips
// ---------------------------------------------------------------------------

/// TDF is a lossless contract: write → parse → every column must still match the
/// read-stage golden semantic values + types. Any mismatch is a REAL bug.
fn runTdfRoundtrip(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    df: *Dataframe,
    golden_columns: std.json.Array,
    golden_num_rows: usize,
) !void {
    const bytes = native_format.writeToString(allocator, df) catch |err| {
        try divs.append(allocator, .{
            .stage = .tdf_roundtrip,
            .column = "<frame>",
            .row = -1,
            .kind = .tdf_roundtrip,
            .arrow_repr = try allocator.dupe(u8, "write"),
            .teddy_repr = try std.fmt.allocPrint(allocator, "writeToString failed: {s}", .{@errorName(err)}),
        });
        return;
    };
    defer allocator.free(bytes);

    var rt = native_format.parse(allocator, bytes) catch |err| {
        try divs.append(allocator, .{
            .stage = .tdf_roundtrip,
            .column = "<frame>",
            .row = -1,
            .kind = .tdf_roundtrip,
            .arrow_repr = try std.fmt.allocPrint(allocator, "parse {d} bytes", .{bytes.len}),
            .teddy_repr = try std.fmt.allocPrint(allocator, "parse failed: {s}", .{@errorName(err)}),
        });
        return;
    };
    defer rt.deinit();

    for (golden_columns.items) |col_val| {
        const col = col_val.object;
        const name = col.get("name").?.string;
        const expected_type = col.get("expected_teddy_type").?.string;
        const cells = col.get("cells").?.array;

        const boxed = rt.getSeries(name) orelse {
            try divs.append(allocator, .{
                .stage = .tdf_roundtrip,
                .column = name,
                .row = -1,
                .kind = .tdf_roundtrip,
                .arrow_repr = try allocator.dupe(u8, "present"),
                .teddy_repr = try allocator.dupe(u8, "MISSING after round-trip"),
            });
            continue;
        };

        const actual_type = boxed.typeName();
        if (!std.mem.eql(u8, actual_type, expected_type)) {
            try divs.append(allocator, .{
                .stage = .tdf_roundtrip,
                .column = name,
                .row = -1,
                .kind = .tdf_roundtrip,
                .arrow_repr = try allocator.dupe(u8, expected_type),
                .teddy_repr = try allocator.dupe(u8, actual_type),
            });
            continue;
        }
        if (boxed.len() != golden_num_rows) continue;

        for (cells.items, 0..) |cell_val, row| {
            const cell = cell_val.object;
            const golden_is_null = cell.get("null") != null;
            const teddy_is_null = boxed.isNull(row);
            if (golden_is_null != teddy_is_null) {
                try divs.append(allocator, .{
                    .stage = .tdf_roundtrip,
                    .column = name,
                    .row = @intCast(row),
                    .kind = .tdf_roundtrip,
                    .arrow_repr = try allocator.dupe(u8, if (golden_is_null) "null" else "non-null"),
                    .teddy_repr = try allocator.dupe(u8, if (teddy_is_null) "null" else "non-null"),
                });
                continue;
            }
            if (golden_is_null) continue;

            const golden_cell = cell.get("v").?;
            if (isFloatColumn(boxed.*)) {
                const teddy_f = teddyFloat(boxed.*, row);
                const golden_f = jsonToF64(golden_cell);
                const tol: f64 = if (boxed.* == .float16) 1e-2 else 1e-6;
                if (!floatsClose(teddy_f, golden_f, tol)) {
                    try divs.append(allocator, .{
                        .stage = .tdf_roundtrip,
                        .column = name,
                        .row = @intCast(row),
                        .kind = .tdf_roundtrip,
                        .arrow_repr = try std.fmt.allocPrint(allocator, "{d}", .{golden_f}),
                        .teddy_repr = try std.fmt.allocPrint(allocator, "{d}", .{teddy_f}),
                    });
                }
                continue;
            }

            const teddy_sem = try teddySemantic(allocator, boxed.*, row);
            defer allocator.free(teddy_sem);
            const golden_sem = try goldenSemantic(allocator, golden_cell);
            defer allocator.free(golden_sem);
            if (!std.mem.eql(u8, teddy_sem, golden_sem)) {
                try divs.append(allocator, .{
                    .stage = .tdf_roundtrip,
                    .column = name,
                    .row = @intCast(row),
                    .kind = .tdf_roundtrip,
                    .arrow_repr = try allocator.dupe(u8, golden_sem),
                    .teddy_repr = try allocator.dupe(u8, teddy_sem),
                });
            }
        }
    }
}

/// CSV is lossy / re-typing. We do NOT assert equality; instead we CATALOG what
/// each original column round-trips to: original teddy type -> re-read teddy
/// type + a sample value of the re-read column (row 0). This yields the
/// "what survives CSV" table the user wants.
fn runCsvRoundtrip(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    df: *Dataframe,
) !void {
    // CSV survives the full multi-column frame round-trip (each cell is a field),
    // so we catalog against the re-read whole frame.
    const text = csv_writer.writeToString(allocator, df, .{}) catch |err| {
        try appendRoundtripNote(allocator, divs, .csv_roundtrip, .csv_roundtrip, "<frame>", "frame", "write", err);
        return;
    };
    defer allocator.free(text);

    var rt = csv_reader.parse(allocator, text, .{}) catch |err| {
        try appendRoundtripNote(allocator, divs, .csv_roundtrip, .csv_roundtrip, "<frame>", "frame", "parse", err);
        return;
    };
    defer rt.deinit();

    try catalogWholeFrame(allocator, divs, df, rt, .csv_roundtrip, .csv_roundtrip);
}

/// Catalog every original column against an already-round-tripped whole frame.
fn catalogWholeFrame(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    orig: *Dataframe,
    rt: *Dataframe,
    stage: Stage,
    kind: DivergenceKind,
) !void {
    for (orig.series.items) |*orig_boxed| {
        const col_name = orig_boxed.name();
        const orig_type = orig_boxed.typeName();

        const rt_boxed = rt.getSeries(col_name) orelse {
            try divs.append(allocator, .{
                .stage = stage,
                .column = col_name,
                .row = -1,
                .kind = kind,
                .arrow_repr = try allocator.dupe(u8, orig_type),
                .teddy_repr = try allocator.dupe(u8, "MISSING after round-trip"),
            });
            continue;
        };

        const rt_type = rt_boxed.typeName();
        const sample = try sampleValue(allocator, rt_boxed, 0);
        defer allocator.free(sample);

        try divs.append(allocator, .{
            .stage = stage,
            .column = col_name,
            .row = -1,
            .kind = kind,
            .arrow_repr = try allocator.dupe(u8, orig_type),
            .teddy_repr = try std.fmt.allocPrint(allocator, "{s} (sample=\"{s}\")", .{ rt_type, sample }),
        });
    }
}

/// JSON round-trip (rows format). Preserves more than CSV (numbers stay numbers,
/// null stays null) but dates/decimals/nested degrade to strings/numbers — and
/// some renderings (e.g. struct -> `{1, "x"}`) are not valid JSON, so those
/// columns fail to re-parse. Cataloged per-column the same way as CSV.
fn runJsonRoundtrip(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    df: *Dataframe,
) !void {
    try catalogPerColumn(allocator, divs, df, .json_roundtrip);
}

/// Round-trip EACH original column on its own (a single-column dataframe), so
/// one column that fails to re-parse (a real fidelity finding) does not blank
/// out the catalog for the rest. Records, per column:
///   arrow_repr = "<orig_type>"
///   teddy_repr = "<reread_type> (sample=\"<v>\")"  OR  "<phase> failed: <err>"
/// Always recorded (catalog, not pass/fail).
fn catalogPerColumn(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    df: *Dataframe,
    stage: Stage,
) !void {
    const kind: DivergenceKind = switch (stage) {
        .csv_roundtrip => .csv_roundtrip,
        .json_roundtrip => .json_roundtrip,
        else => unreachable,
    };

    for (df.series.items) |*orig_boxed| {
        const col_name = orig_boxed.name();
        const orig_type = orig_boxed.typeName();

        // Build a one-column frame (deep copy so ownership is clean).
        var one = try Dataframe.init(allocator);
        defer one.deinit();
        var copy = try orig_boxed.deepCopy();
        one.addSeries(copy) catch |err| {
            copy.deinit();
            return err;
        };

        const text = switch (stage) {
            .csv_roundtrip => csv_writer.writeToString(allocator, one, .{}),
            .json_roundtrip => json_writer.writeToString(allocator, one, .rows),
            else => unreachable,
        } catch |err| {
            try appendRoundtripNote(allocator, divs, stage, kind, col_name, orig_type, "write", err);
            continue;
        };
        defer allocator.free(text);

        var rt = switch (stage) {
            .csv_roundtrip => csv_reader.parse(allocator, text, .{}),
            .json_roundtrip => json_reader.parse(allocator, text, .{}),
            else => unreachable,
        } catch |err| {
            try appendRoundtripNote(allocator, divs, stage, kind, col_name, orig_type, "parse", err);
            continue;
        };
        defer rt.deinit();

        const rt_boxed = rt.getSeries(col_name) orelse {
            try divs.append(allocator, .{
                .stage = stage,
                .column = col_name,
                .row = -1,
                .kind = kind,
                .arrow_repr = try allocator.dupe(u8, orig_type),
                .teddy_repr = try allocator.dupe(u8, "MISSING after round-trip"),
            });
            continue;
        };

        const rt_type = rt_boxed.typeName();
        const sample = try sampleValue(allocator, rt_boxed, 0);
        defer allocator.free(sample);

        try divs.append(allocator, .{
            .stage = stage,
            .column = col_name,
            .row = -1,
            .kind = kind,
            .arrow_repr = try allocator.dupe(u8, orig_type),
            .teddy_repr = try std.fmt.allocPrint(allocator, "{s} (sample=\"{s}\")", .{ rt_type, sample }),
        });
    }
}

fn appendRoundtripNote(
    allocator: std.mem.Allocator,
    divs: *std.ArrayList(Divergence),
    stage: Stage,
    kind: DivergenceKind,
    col_name: []const u8,
    orig_type: []const u8,
    phase: []const u8,
    err: anyerror,
) !void {
    try divs.append(allocator, .{
        .stage = stage,
        .column = col_name,
        .row = -1,
        .kind = kind,
        .arrow_repr = try allocator.dupe(u8, orig_type),
        .teddy_repr = try std.fmt.allocPrint(allocator, "{s} failed: {s}", .{ phase, @errorName(err) }),
    });
}

/// A best-effort textual sample of a re-read cell (row 0). Uses the boxed
/// asStringAt rendering; null renders as "<null>". Catalog-only — never compared.
fn sampleValue(allocator: std.mem.Allocator, boxed: *BoxedSeries, row: usize) ![]u8 {
    if (boxed.len() <= row) return allocator.dupe(u8, "<empty>");
    if (boxed.isNull(row)) return allocator.dupe(u8, "<null>");
    var s = boxed.asStringAt(row) catch return allocator.dupe(u8, "<unrenderable>");
    defer s.deinit();
    return allocator.dupe(u8, s.toSlice());
}

// ---------------------------------------------------------------------------
// Report writer (stderr + data/validation/divergence_report.txt)
// ---------------------------------------------------------------------------

fn countStage(divs: []const Divergence, stage: Stage) usize {
    var n: usize = 0;
    for (divs) |d| {
        if (d.stage == stage) n += 1;
    }
    return n;
}

/// Emit ONE report section: a header line, then every entry whose stage matches.
/// `note` describes the expectation (e.g. "0 expected" for read/TDF).
fn writeSection(
    w: *std.Io.Writer,
    divs: []const Divergence,
    stage: Stage,
    header: []const u8,
    note: []const u8,
) !void {
    const n = countStage(divs, stage);
    try w.print("== {s} == ({d}) {s}\n", .{ header, n, note });
    for (divs) |d| {
        if (d.stage != stage) continue;
        if (d.row >= 0) {
            try w.print(
                "  col={s} row={d} kind={s} arrow={s} teddy={s}\n",
                .{ d.column, d.row, @tagName(d.kind), d.arrow_repr, d.teddy_repr },
            );
        } else {
            try w.print(
                "  col={s} kind={s} arrow={s} teddy={s}\n",
                .{ d.column, @tagName(d.kind), d.arrow_repr, d.teddy_repr },
            );
        }
    }
}

fn writeReport(allocator: std.mem.Allocator, divs: []const Divergence) !void {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    const w = &aw.writer;

    try w.print("REGRESSION REPORT — {d} cataloged entr(ies) across all stages\n", .{divs.len});
    try w.writeAll("(read/transforms/sort = teddy-vs-pyarrow divergences; round-trips = fidelity catalog)\n\n");

    try writeSection(w, divs, .read, "READ", "0 divergences expected");
    try w.writeByte('\n');
    try writeSection(w, divs, .transforms, "TRANSFORMS", "sum/mean/min/max mismatches");
    try w.writeByte('\n');
    try writeSection(w, divs, .sort, "SORT", "nulls-last parity expected (teddy == pyarrow); any entry is a regression");
    try w.writeByte('\n');
    try writeSection(w, divs, .tdf_roundtrip, "TDF_ROUNDTRIP", "lossless contract — should be 0; any entry is a REAL BUG");
    try w.writeByte('\n');
    try writeSection(w, divs, .csv_roundtrip, "CSV_ROUNDTRIP", "per-column: original_type -> reread_type (sample)");
    try w.writeByte('\n');
    try writeSection(w, divs, .json_roundtrip, "JSON_ROUNDTRIP", "per-column: original_type -> reread_type (sample)");

    const report = aw.written();
    // stderr
    std.debug.print("{s}", .{report});

    // file (best-effort; do not fail the test on a write error)
    const cwd = std.Io.Dir.cwd();
    const io = std.Io.Threaded.global_single_threaded.io();
    cwd.writeFile(io, .{ .sub_path = REPORT, .data = report }) catch |err| {
        std.debug.print("(could not write {s}: {s})\n", .{ REPORT, @errorName(err) });
    };
}

// ---------------------------------------------------------------------------
// Float helpers
// ---------------------------------------------------------------------------

fn isFloatColumn(b: BoxedSeries) bool {
    return switch (b) {
        .float16, .float32, .float64 => true,
        else => false,
    };
}

fn teddyFloat(b: BoxedSeries, row: usize) f64 {
    return switch (b) {
        .float16 => |s| @floatCast(s.values.items[row]),
        .float32 => |s| @floatCast(s.values.items[row]),
        .float64 => |s| s.values.items[row],
        else => unreachable,
    };
}

fn jsonToF64(v: std.json.Value) f64 {
    return switch (v) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        .number_string => |s| std.fmt.parseFloat(f64, s) catch std.math.nan(f64),
        else => std.math.nan(f64),
    };
}

fn floatsClose(a: f64, b: f64, tol: f64) bool {
    if (std.math.isNan(a) and std.math.isNan(b)) return true;
    if (a == b) return true;
    const diff = @abs(a - b);
    const scale = @max(@abs(a), @abs(b));
    return diff <= tol * @max(scale, 1.0);
}

// ---------------------------------------------------------------------------
// Golden cell -> canonical string (non-float path). Strings come through
// directly; nested values arrive as JSON arrays which we render to the SAME
// canonical shape teddySemantic produces (see nestedToCanonical).
// ---------------------------------------------------------------------------

fn goldenSemantic(allocator: std.mem.Allocator, v: std.json.Value) ![]u8 {
    switch (v) {
        .string => |s| return allocator.dupe(u8, s),
        .array => {
            var aw: std.Io.Writer.Allocating = .init(allocator);
            errdefer aw.deinit();
            try goldenNestedInto(&aw.writer, v);
            return aw.toOwnedSlice();
        },
        else => return allocator.dupe(u8, "<non-canonical-golden>"),
    }
}

/// Render a golden nested JSON value into the canonical text shape:
///   list/struct/list-of-* -> [a, b, ...]
///   map                    -> [[k, v], ...]
///   scalar string          -> verbatim
///   JSON null              -> null
///   JSON number (float leaf)-> {d}
fn goldenNestedInto(w: *std.Io.Writer, v: std.json.Value) !void {
    switch (v) {
        .null => try w.writeAll("null"),
        .string => |s| try w.writeAll(s),
        .integer => |i| try w.print("{d}", .{i}),
        .float => |f| try w.print("{d}", .{f}),
        .number_string => |s| try w.writeAll(s),
        .bool => |b| try w.writeAll(if (b) "1" else "0"),
        .array => |arr| {
            try w.writeByte('[');
            for (arr.items, 0..) |item, i| {
                if (i > 0) try w.writeAll(", ");
                try goldenNestedInto(w, item);
            }
            try w.writeByte(']');
        },
        else => try w.writeAll("<?>"),
    }
}

// ---------------------------------------------------------------------------
// teddy boxed value -> canonical string (non-float path).
// ---------------------------------------------------------------------------

fn teddySemantic(allocator: std.mem.Allocator, b: BoxedSeries, row: usize) ![]u8 {
    var aw: std.Io.Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    const w = &aw.writer;

    switch (b) {
        .bool => |s| try w.writeAll(if (s.values.items[row]) "1" else "0"),
        .uint8 => |s| try w.print("{d}", .{s.values.items[row]}),
        .uint16 => |s| try w.print("{d}", .{s.values.items[row]}),
        .uint32 => |s| try w.print("{d}", .{s.values.items[row]}),
        .uint64 => |s| try w.print("{d}", .{s.values.items[row]}),
        .uint128 => |s| try w.print("{d}", .{s.values.items[row]}),
        .usize => |s| try w.print("{d}", .{s.values.items[row]}),
        .int8 => |s| try w.print("{d}", .{s.values.items[row]}),
        .int16 => |s| try w.print("{d}", .{s.values.items[row]}),
        .int32 => |s| try w.print("{d}", .{s.values.items[row]}),
        .int64 => |s| try w.print("{d}", .{s.values.items[row]}),
        .int128 => |s| try w.print("{d}", .{s.values.items[row]}),
        .isize => |s| try w.print("{d}", .{s.values.items[row]}),
        .string => |s| try w.writeAll(s.values.items[row].toSlice()),
        .raw => |s| try bytesHex(w, s.values.items[row].toSlice()),
        .binary => |s| try bytesHex(w, s.values.items[row].toSlice()),
        .fixed_bytes => |s| try bytesHex(w, s.values.items[row].toSlice()),
        .uuid => |s| try bytesHex(w, &s.values.items[row].bytes),
        .date => |s| try w.print("{d}", .{s.values.items[row].days}),
        .time => |s| try w.print("{d}", .{s.values.items[row].toNanos()}),
        .timestamp => |s| {
            const ts = s.values.items[row];
            try w.print("{d}:{d}", .{ ts.toNanos(), @intFromBool(ts.utc) });
        },
        .decimal => |s| {
            const d = s.values.items[row];
            try w.print("{d}:{d}", .{ d.unscaled, d.scale });
        },
        .interval => |s| {
            const iv = s.values.items[row];
            try w.print("{d}:{d}:{d}", .{ iv.months, iv.days, iv.millis });
        },
        .nested => |s| try nestedToCanonical(w, &s.values.items[row]),
        // Float arms never reach here (handled by the numeric path).
        .float16, .float32, .float64 => unreachable,
    }

    return aw.toOwnedSlice();
}

fn bytesHex(w: *std.Io.Writer, bytes: []const u8) !void {
    for (bytes) |byte| try w.print("{x:0>2}", .{byte});
}

/// Render a Nested tree into the SAME canonical text the golden produces:
///   list/struct -> [elem, elem, ...] (struct positional)
///   map         -> [[k, v], ...]
///   scalar leaves match the per-type protocol (ints {d}, bool 0/1, hex bytes,
///   float {d}, etc.); null_ -> null.
fn nestedToCanonical(w: *std.Io.Writer, n: *const Nested) !void {
    switch (n.*) {
        .null_ => try w.writeAll("null"),
        .boolean => |v| try w.writeAll(if (v) "1" else "0"),
        .int => |v| try w.print("{d}", .{v}),
        .uint => |v| try w.print("{d}", .{v}),
        .float => |v| try w.print("{d}", .{v}),
        .date => |v| try w.print("{d}", .{v.days}),
        .time => |v| try w.print("{d}", .{v.toNanos()}),
        .timestamp => |v| try w.print("{d}:{d}", .{ v.toNanos(), @intFromBool(v.utc) }),
        .decimal => |v| try w.print("{d}:{d}", .{ v.unscaled, v.scale }),
        .uuid => |v| try bytesHex(w, &v.bytes),
        .interval => |v| try w.print("{d}:{d}:{d}", .{ v.months, v.days, v.millis }),
        .string => |*v| try w.writeAll(v.toSlice()),
        .bytes => |*v| try bytesHex(w, v.toSlice()),
        .list => |*l| {
            try w.writeByte('[');
            for (l.items, 0..) |*item, i| {
                if (i > 0) try w.writeAll(", ");
                try nestedToCanonical(w, item);
            }
            try w.writeByte(']');
        },
        .strukt => |*st| {
            try w.writeByte('[');
            for (st.fields, 0..) |*f, i| {
                if (i > 0) try w.writeAll(", ");
                try nestedToCanonical(w, f);
            }
            try w.writeByte(']');
        },
        .map => |*m| {
            try w.writeByte('[');
            for (m.entries, 0..) |*e, i| {
                if (i > 0) try w.writeAll(", ");
                try w.writeByte('[');
                try nestedToCanonical(w, &e.key);
                try w.writeAll(", ");
                try nestedToCanonical(w, &e.value);
                try w.writeByte(']');
            }
            try w.writeByte(']');
        },
    }
}
