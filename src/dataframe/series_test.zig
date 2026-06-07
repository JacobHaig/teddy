const std = @import("std");
const Series = @import("series.zig").Series;
const strings = @import("strings.zig");
const String = strings.String;
const Dataframe = @import("dataframe.zig").Dataframe;
const json_writer = @import("json_writer.zig");

// --- filterByIndices Tests ---

test "Series: filterByIndices basic subset" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.rename("vals");
    try s.append(10);
    try s.append(20);
    try s.append(30);
    try s.append(40);

    var filtered = try s.filterByIndices(&[_]usize{ 1, 3 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.len());
    try std.testing.expectEqual(@as(i32, 20), filtered.values.items[0]);
    try std.testing.expectEqual(@as(i32, 40), filtered.values.items[1]);
    try std.testing.expectEqualStrings("vals", filtered.name.toSlice());
}

test "Series: filterByIndices empty indices" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);

    var filtered = try s.filterByIndices(&[_]usize{});
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 0), filtered.len());
}

test "Series: filterByIndices with strings deep copies" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    try s.tryAppend("hello");
    try s.tryAppend("world");

    var filtered = try s.filterByIndices(&[_]usize{1});
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 1), filtered.len());
    try std.testing.expectEqualStrings("world", filtered.values.items[0].toSlice());
}

test "Series: filterByIndices out-of-order indices" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);

    var filtered = try s.filterByIndices(&[_]usize{ 2, 0 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(i32, 30), filtered.values.items[0]);
    try std.testing.expectEqual(@as(i32, 10), filtered.values.items[1]);
}

test "Series: sum" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);

    try std.testing.expectEqual(@as(i32, 60), s.sum());
}

test "Series: min and max" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(30);
    try s.append(10);
    try s.append(20);

    try std.testing.expectEqual(@as(i32, 10), s.min().?);
    try std.testing.expectEqual(@as(i32, 30), s.max().?);
}

test "Series: min empty returns null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();

    try std.testing.expect(s.min() == null);
}

test "Series: mean" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);

    try std.testing.expectEqual(@as(f64, 15.0), s.mean());
}

test "Series: stdDev" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);

    // mean=15, stddev = sqrt(((10-15)^2 + (20-15)^2) / 2) = sqrt(25) = 5
    try std.testing.expectEqual(@as(f64, 5.0), s.stdDev());
}

test "Series: float mean" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.0);
    try s.append(3.0);

    try std.testing.expectEqual(@as(f64, 2.0), s.mean());
}

test "Series: appendNull and isNull" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);

    try std.testing.expectEqual(@as(usize, 3), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(s.isNull(1));
    try std.testing.expect(!s.isNull(2));
    try std.testing.expectEqual(@as(usize, 1), s.nullCount());
    try std.testing.expect(s.hasNulls());
}

test "Series: no nulls by default" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    try std.testing.expect(!s.isNull(0));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
    try std.testing.expect(!s.hasNulls());
}

test "Series: appendNull with strings" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    try s.tryAppend("hello");
    try s.appendNull();

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(s.isNull(1));
}

test "Series: filterByIndices preserves validity" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);

    var filtered = try s.filterByIndices(&[_]usize{ 0, 1 });
    defer filtered.deinit();

    try std.testing.expect(!filtered.isNull(0));
    try std.testing.expect(filtered.isNull(1));
}

// --- getAt Tests ---

test "Series: getAt returns value when not null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(42);
    try std.testing.expectEqual(@as(?i32, 42), s.getAt(0));
}

test "Series: getAt returns null for null slot" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 1), s.getAt(0));
    try std.testing.expectEqual(@as(?i32, null), s.getAt(1));
    try std.testing.expectEqual(@as(?i32, 3), s.getAt(2));
}

// --- asStringAt null Tests ---

test "Series: asStringAt returns 'null' for null slot" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();

    var v0 = try s.asStringAt(0);
    defer v0.deinit();
    var v1 = try s.asStringAt(1);
    defer v1.deinit();

    try std.testing.expectEqualStrings("10", v0.toSlice());
    try std.testing.expectEqualStrings("null", v1.toSlice());
}

// --- dropRow validity Tests ---

test "Series: dropRow removes validity entry" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull(); // index 1
    try s.append(3);

    s.dropRow(1); // remove the null

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expect(!s.isNull(1));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
}

test "Series: dropRow keeping null slot updates indices correctly" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull(); // index 1
    try s.append(3);

    s.dropRow(0); // remove the first valid value

    try std.testing.expectEqual(@as(usize, 2), s.len());
    try std.testing.expect(s.isNull(0)); // null moved to index 0
    try std.testing.expect(!s.isNull(1));
}

// --- limit validity Tests ---

test "Series: limit truncates validity bitmap" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    s.limit(1);

    try std.testing.expectEqual(@as(usize, 1), s.len());
    try std.testing.expect(!s.isNull(0));
    try std.testing.expectEqual(@as(usize, 0), s.nullCount());
}

// --- appendSlice validity Tests ---

test "Series: appendSlice marks entries valid in existing bitmap" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull(); // triggers bitmap init
    try s.appendSlice(&[_]i32{ 10, 20 });

    try std.testing.expectEqual(@as(usize, 3), s.len());
    try std.testing.expect(s.isNull(0));
    try std.testing.expect(!s.isNull(1));
    try std.testing.expect(!s.isNull(2));
}

// --- compareSeries null-aware Tests ---

test "Series: compareSeries null == null" {
    const allocator = std.testing.allocator;
    var a = try Series(i32).init(allocator);
    defer a.deinit();
    var b = try Series(i32).init(allocator);
    defer b.deinit();
    try a.appendNull();
    try b.appendNull();
    try std.testing.expect(a.compareSeries(b));
}

test "Series: compareSeries null != value" {
    const allocator = std.testing.allocator;
    var a = try Series(i32).init(allocator);
    defer a.deinit();
    var b = try Series(i32).init(allocator);
    defer b.deinit();
    try a.appendNull();
    try b.append(0); // same placeholder, different nullness
    try std.testing.expect(!a.compareSeries(b));
}

// --- Aggregation null-skipping Tests ---

test "Series: sum skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);
    try std.testing.expectEqual(@as(i32, 40), s.sum());
}

test "Series: min skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(5);
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 3), s.min());
}

test "Series: max skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.appendNull();
    try s.append(3);
    try std.testing.expectEqual(@as(?i32, 5), s.max());
}

test "Series: min all nulls returns null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.appendNull();
    try std.testing.expectEqual(@as(?i32, null), s.min());
}

test "Series: mean skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);
    // mean of [10, 30] = 20, not mean of [10, 0, 30] = 13.33
    try std.testing.expectEqual(@as(f64, 20.0), s.mean());
}

test "Series: stdDev skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(20);
    // stddev of [10, 20] = 5
    try std.testing.expectEqual(@as(f64, 5.0), s.stdDev());
}

// --- fillNull Tests ---

test "Series: fillNull replaces nulls with value" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    var filled = try s.fillNull(99);
    defer filled.deinit();

    try std.testing.expectEqual(@as(usize, 3), filled.len());
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 99), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 3), filled.getAt(2));
    try std.testing.expect(!filled.hasNulls());
}

test "Series: fillNull with no nulls is a copy" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    var filled = try s.fillNull(0);
    defer filled.deinit();

    try std.testing.expectEqual(@as(usize, 2), filled.len());
    try std.testing.expect(!filled.hasNulls());
}

// --- fillNullForward Tests ---

test "Series: fillNullForward carries last valid value forward" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.appendNull();
    try s.append(4);

    var filled = try s.fillNullForward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(2));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(3));
}

test "Series: fillNullForward leaves leading nulls alone" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(5);

    var filled = try s.fillNullForward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, null), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 5), filled.getAt(1));
}

// --- fillNullBackward Tests ---

test "Series: fillNullBackward carries next valid value backward" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.appendNull();
    try s.append(4);

    var filled = try s.fillNullBackward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 1), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(1));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(2));
    try std.testing.expectEqual(@as(?i32, 4), filled.getAt(3));
}

test "Series: fillNullBackward leaves trailing nulls alone" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.appendNull();

    var filled = try s.fillNullBackward();
    defer filled.deinit();

    try std.testing.expectEqual(@as(?i32, 5), filled.getAt(0));
    try std.testing.expectEqual(@as(?i32, null), filled.getAt(1));
}

// --- dropNulls Tests ---

test "Series: dropNulls removes null rows" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);

    var dropped = try s.dropNulls();
    defer dropped.deinit();

    try std.testing.expectEqual(@as(usize, 2), dropped.len());
    try std.testing.expectEqual(@as(?i32, 1), dropped.getAt(0));
    try std.testing.expectEqual(@as(?i32, 3), dropped.getAt(1));
    try std.testing.expect(!dropped.hasNulls());
}

test "Series: dropNulls with no nulls returns full copy" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);

    var dropped = try s.dropNulls();
    defer dropped.deinit();

    try std.testing.expectEqual(@as(usize, 2), dropped.len());
}

// --- cast Tests ---

test "Series.cast: i32 to f64" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.rename("x");
    try s.append(1);
    try s.append(2);
    try s.append(3);

    var casted = try s.cast(f64);
    defer casted.deinit();

    try std.testing.expectEqual(@as(usize, 3), casted.len());
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), casted.values.items[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), casted.values.items[1], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), casted.values.items[2], 1e-9);
    try std.testing.expectEqualStrings("x", casted.name.toSlice());
}

test "Series.cast: f64 to i32 fails on fractional value" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.9);
    try std.testing.expectError(error.LossyCast, s.cast(i32));
}

test "Series.cast: f64 to i32 succeeds for exact integer" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(3.0);
    try s.append(-2.0);

    var casted = try s.cast(i32);
    defer casted.deinit();

    try std.testing.expectEqual(@as(i32, 3), casted.values.items[0]);
    try std.testing.expectEqual(@as(i32, -2), casted.values.items[1]);
}

test "Series.castLossy: f64 to i32 truncates" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.9);
    try s.append(-2.7);

    var casted = try s.castLossy(i32);
    defer casted.deinit();

    try std.testing.expectEqual(@as(i32, 1), casted.values.items[0]);
    try std.testing.expectEqual(@as(i32, -2), casted.values.items[1]);
}

test "Series.castLossy: overflow becomes null" {
    const allocator = std.testing.allocator;
    var s = try Series(i64).init(allocator);
    defer s.deinit();
    try s.append(300); // overflows i8
    try s.append(1);

    var casted = try s.castLossy(i8);
    defer casted.deinit();

    try std.testing.expect(casted.isNull(0));
    try std.testing.expect(!casted.isNull(1));
    try std.testing.expectEqual(@as(i8, 1), casted.values.items[1]);
}

test "Series.castLossy: bad String becomes null" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    var good = try strings.String.fromSlice(allocator, "42");
    defer good.deinit();
    var bad = try strings.String.fromSlice(allocator, "not_a_number");
    defer bad.deinit();
    try s.append(try good.clone());
    try s.append(try bad.clone());

    var casted = try s.castLossy(i32);
    defer casted.deinit();

    try std.testing.expect(!casted.isNull(0));
    try std.testing.expectEqual(@as(i32, 42), casted.values.items[0]);
    try std.testing.expect(casted.isNull(1));
}

test "Series.cast: strict int overflow returns error" {
    const allocator = std.testing.allocator;
    var s = try Series(i64).init(allocator);
    defer s.deinit();
    try s.append(300); // doesn't fit in i8
    try std.testing.expectError(error.Overflow, s.cast(i8));
}

test "Series.cast: i32 to String" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(42);
    try s.append(-7);

    var casted = try s.cast(strings.String);
    defer casted.deinit();

    try std.testing.expectEqualStrings("42", casted.values.items[0].toSlice());
    try std.testing.expectEqualStrings("-7", casted.values.items[1].toSlice());
}

test "Series.cast: String to i64" {
    const allocator = std.testing.allocator;
    var s = try Series(strings.String).init(allocator);
    defer s.deinit();
    var a = try strings.String.fromSlice(allocator, "100");
    defer a.deinit();
    var b = try strings.String.fromSlice(allocator, "-50");
    defer b.deinit();
    try s.append(try a.clone());
    try s.append(try b.clone());

    var casted = try s.cast(i64);
    defer casted.deinit();

    try std.testing.expectEqual(@as(i64, 100), casted.values.items[0]);
    try std.testing.expectEqual(@as(i64, -50), casted.values.items[1]);
}

test "Series.cast: preserves nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.appendNull();
    try s.append(30);

    var casted = try s.cast(f64);
    defer casted.deinit();

    try std.testing.expectEqual(@as(usize, 3), casted.len());
    try std.testing.expect(!casted.isNull(0));
    try std.testing.expect(casted.isNull(1));
    try std.testing.expect(!casted.isNull(2));
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), casted.values.items[0], 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), casted.values.items[2], 1e-9);
}

test "Series.cast: bool to i32" {
    const allocator = std.testing.allocator;
    var s = try Series(bool).init(allocator);
    defer s.deinit();
    try s.append(true);
    try s.append(false);

    var casted = try s.cast(i32);
    defer casted.deinit();

    try std.testing.expectEqual(@as(i32, 1), casted.values.items[0]);
    try std.testing.expectEqual(@as(i32, 0), casted.values.items[1]);
}

// --- sumChecked Tests ---
test "Series.sumChecked: normal sum" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(3);
    try std.testing.expectEqual(@as(i32, 6), try s.sumChecked());
}

test "Series.sumChecked: overflow returns error" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(std.math.maxInt(i32));
    try s.append(1);
    try std.testing.expectError(error.Overflow, s.sumChecked());
}

// --- prod Tests ---
test "Series.prod: basic product" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(2);
    try s.append(3);
    try s.append(4);
    try std.testing.expectEqual(@as(i32, 24), s.prod());
}

test "Series.prod: skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(2);
    try s.appendNull();
    try s.append(5);
    try std.testing.expectEqual(@as(i32, 10), s.prod());
}

test "Series.prodChecked: overflow returns error" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(std.math.maxInt(i32));
    try s.append(2);
    try std.testing.expectError(error.Overflow, s.prodChecked());
}

// --- first / last Tests ---
test "Series.first: returns first non-null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(10);
    try s.append(20);
    try std.testing.expectEqual(@as(?i32, 10), s.first());
}

test "Series.last: returns last non-null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.appendNull();
    try std.testing.expectEqual(@as(?i32, 20), s.last());
}

test "Series.first: all null returns null" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try std.testing.expectEqual(@as(?i32, null), s.first());
}

// --- median / quantile Tests ---
test "Series.median: odd count" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(3);
    try s.append(1);
    try s.append(2);
    const m = try s.median(allocator);
    try std.testing.expectEqual(@as(?f64, 2.0), m);
}

test "Series.median: even count averages middle two" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(3);
    try s.append(4);
    const m = try s.median(allocator);
    try std.testing.expectEqual(@as(?f64, 2.5), m);
}

test "Series.median: skips nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);
    const m = try s.median(allocator);
    try std.testing.expectEqual(@as(?f64, 2.0), m);
}

test "Series.quantile: q=0 is min, q=1 is max" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.0);
    try s.append(2.0);
    try s.append(3.0);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), (try s.quantile(allocator, 0.0)).?, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), (try s.quantile(allocator, 1.0)).?, 1e-9);
}

test "Series.quantile: invalid q returns error" {
    const allocator = std.testing.allocator;
    var s = try Series(f64).init(allocator);
    defer s.deinit();
    try s.append(1.0);
    try std.testing.expectError(error.InvalidQuantile, s.quantile(allocator, 1.5));
}

// --- nunique Tests ---
test "Series.nunique: counts distinct values" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(1);
    try s.append(3);
    try std.testing.expectEqual(@as(usize, 3), try s.nunique(allocator));
}

// --- cumSum / cumMin / cumMax / cumProd Tests ---
test "Series.cumSum: basic" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(3);
    var r = try s.cumSum();
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 1), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 3), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 6), r.values.items[2]);
}

test "Series.cumSum: null propagates" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.appendNull();
    try s.append(3);
    var r = try s.cumSum();
    defer r.deinit();
    try std.testing.expect(!r.isNull(0));
    try std.testing.expect(r.isNull(1));
    try std.testing.expect(!r.isNull(2));
    try std.testing.expectEqual(@as(i32, 4), r.values.items[2]);
}

test "Series.cumMin: basic" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(3);
    try s.append(1);
    try s.append(2);
    var r = try s.cumMin();
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 3), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 1), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 1), r.values.items[2]);
}

test "Series.cumMax: basic" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(3);
    try s.append(2);
    var r = try s.cumMax();
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 1), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 3), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 3), r.values.items[2]);
}

test "Series.cumProd: basic" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(3);
    var r = try s.cumProd();
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 1), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 2), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 6), r.values.items[2]);
}

// --- shift Tests ---
test "Series.shift: positive shift prepends nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);
    var r = try s.shift(1);
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 3), r.len());
    try std.testing.expect(r.isNull(0));
    try std.testing.expectEqual(@as(i32, 10), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 20), r.values.items[2]);
}

test "Series.shift: negative shift appends nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(20);
    try s.append(30);
    var r = try s.shift(-1);
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 20), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), r.values.items[1]);
    try std.testing.expect(r.isNull(2));
}

// --- diff Tests ---
test "Series.diff: basic difference" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(10);
    try s.append(13);
    try s.append(11);
    var r = try s.diff(1);
    defer r.deinit();
    try std.testing.expect(r.isNull(0));
    try std.testing.expectEqual(@as(i32, 3), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, -2), r.values.items[2]);
}

test "Series.diff: unsigned underflow returns error" {
    const allocator = std.testing.allocator;
    var s = try Series(u32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.append(3); // 3 - 5 = underflow
    try std.testing.expectError(error.Underflow, s.diff(1));
}

test "Series.diffLossy: underflow becomes null" {
    const allocator = std.testing.allocator;
    var s = try Series(u32).init(allocator);
    defer s.deinit();
    try s.append(5);
    try s.append(3);
    var r = try s.diffLossy(1);
    defer r.deinit();
    try std.testing.expect(r.isNull(0));
    try std.testing.expect(r.isNull(1));
}

// --- clip Tests ---
test "Series.clip: clamps values" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(-5);
    try s.append(3);
    try s.append(15);
    var r = try s.clip(0, 10);
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 0), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 3), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 10), r.values.items[2]);
}

test "Series.clip: preserves nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(5);
    var r = try s.clip(0, 10);
    defer r.deinit();
    try std.testing.expect(r.isNull(0));
    try std.testing.expectEqual(@as(i32, 5), r.values.items[1]);
}

// --- replace / replaceSlice Tests ---
test "Series.replace: replaces matching values" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(1);
    var r = try s.replace(1, 99);
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 99), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 2), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 99), r.values.items[2]);
}

test "Series.replace: does not match nulls" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.appendNull();
    try s.append(1);
    var r = try s.replace(0, 99);
    defer r.deinit();
    try std.testing.expect(r.isNull(0));
    try std.testing.expectEqual(@as(i32, 1), r.values.items[1]);
}

test "Series.replaceSlice: applies multiple replacements" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.append(1);
    try s.append(2);
    try s.append(3);
    var r = try s.replaceSlice(&.{ .{ 1, 10 }, .{ 3, 30 } });
    defer r.deinit();
    try std.testing.expectEqual(@as(i32, 10), r.values.items[0]);
    try std.testing.expectEqual(@as(i32, 2), r.values.items[1]);
    try std.testing.expectEqual(@as(i32, 30), r.values.items[2]);
}

// --- Capability convention tests (Phase 6d-2a.0, plan Task 7) ---

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

test "capability: replace on owning type deinits old and clones new" {
    const allocator = std.testing.allocator;
    var s = try Series(Blob).init(allocator);
    defer s.deinit();
    try s.append(try Blob.fromSlice(allocator, "old"));
    try s.append(try Blob.fromSlice(allocator, "keep"));

    var old_v = try Blob.fromSlice(allocator, "old");
    defer old_v.deinit();
    var new_v = try Blob.fromSlice(allocator, "new");
    defer new_v.deinit();

    const replaced = try s.replace(old_v, new_v);
    defer replaced.deinit();
    try std.testing.expectEqualStrings("new", replaced.values.items[0].toSlice());
    try std.testing.expectEqualStrings("keep", replaced.values.items[1].toSlice());
    // independence: replacing didn't alias new_v's buffer
    try std.testing.expect(replaced.values.items[0].bytes.ptr != new_v.bytes.ptr);
}

// ---------------------------------------------------------------------------
// Date capability tests (Phase 6d-2a.1)
// ---------------------------------------------------------------------------

const Date = @import("date.zig").Date;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;
const CompareOp = @import("boxed_series.zig").CompareOp;

test "Date: argSort on Series(Date) ascending" {
    const allocator = std.testing.allocator;
    var s = try Series(Date).init(allocator);
    defer s.deinit();
    // Append out of order: 2021-06-15, 2020-01-01, 2020-02-29
    try s.append(Date.fromCivil(.{ .year = 2021, .month = 6, .day = 15 }));
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 2, .day = 29 }));

    var sorted = try s.argSort(allocator, true);
    defer sorted.deinit(allocator);

    // Ascending: 2020-01-01 (idx 1) < 2020-02-29 (idx 2) < 2021-06-15 (idx 0)
    try std.testing.expectEqualSlices(usize, &.{ 1, 2, 0 }, sorted.items);
}

test "Date: argSort on Series(Date) descending" {
    const allocator = std.testing.allocator;
    var s = try Series(Date).init(allocator);
    defer s.deinit();
    try s.append(Date.fromCivil(.{ .year = 2021, .month = 6, .day = 15 }));
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 2, .day = 29 }));

    var sorted = try s.argSort(allocator, false);
    defer sorted.deinit(allocator);

    // Descending: 2021-06-15 (idx 0) > 2020-02-29 (idx 2) > 2020-01-01 (idx 1)
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 1 }, sorted.items);
}

test "Date: indicesWhere .lt via BoxedSeries" {
    const allocator = std.testing.allocator;
    var s = try Series(Date).init(allocator);
    defer s.deinit();
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));
    try s.append(Date.fromCivil(.{ .year = 2021, .month = 6, .day = 15 }));
    try s.append(Date.fromCivil(.{ .year = 2019, .month = 12, .day = 31 }));

    var boxed = s.toBoxedSeries();
    // Note: do NOT call boxed.deinit() — s owns the memory.

    const cutoff = Date.fromCivil(.{ .year = 2020, .month = 6, .day = 1 });
    var indices = try boxed.indicesWhere(Date, allocator, .lt, cutoff);
    defer indices.deinit(allocator);

    // Only idx 0 (2020-01-01) and idx 2 (2019-12-31) are < 2020-06-01
    try std.testing.expectEqual(@as(usize, 2), indices.items.len);
    try std.testing.expectEqual(@as(usize, 0), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
}

test "Date: indicesWhere .eq via BoxedSeries" {
    const allocator = std.testing.allocator;
    var s = try Series(Date).init(allocator);
    defer s.deinit();
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));
    try s.append(Date.fromCivil(.{ .year = 2021, .month = 6, .day = 15 }));
    try s.append(Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 }));

    var boxed = s.toBoxedSeries();

    const target = Date.fromCivil(.{ .year = 2020, .month = 1, .day = 1 });
    var indices = try boxed.indicesWhere(Date, allocator, .eq, target);
    defer indices.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), indices.items.len);
    try std.testing.expectEqual(@as(usize, 0), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
}

// --- Float16 (f16) Tests ---

test "Series(f16): sum/mean/min/max through BoxedSeries" {
    const allocator = std.testing.allocator;
    var s = try Series(f16).init(allocator);
    defer s.deinit();
    try s.rename("vals");
    try s.append(1.5);
    try s.append(2.5);
    try s.append(3.0);

    var bs = s.toBoxedSeries();
    try std.testing.expectEqual(@as(f64, 7.0), bs.sum().?);
    try std.testing.expectEqual(@as(f64, 7.0 / 3.0), bs.mean().?);
    try std.testing.expectEqual(@as(f64, 1.5), bs.min().?);
    try std.testing.expectEqual(@as(f64, 3.0), bs.max().?);
}

test "BoxedSeries: cumSum works on f16" {
    const allocator = std.testing.allocator;
    var s = try Series(f16).init(allocator);
    defer s.deinit();
    try s.rename("vals");
    try s.append(1.5);
    try s.append(2.5);
    try s.append(3.0);

    var bs = s.toBoxedSeries();
    var cum = try bs.cumSum();
    defer cum.deinit();

    const out = cum.float16;
    try std.testing.expectEqual(@as(f16, 1.5), out.values.items[0]);
    try std.testing.expectEqual(@as(f16, 4.0), out.values.items[1]);
    try std.testing.expectEqual(@as(f16, 7.0), out.values.items[2]);
}

test "Series(f16): argSort" {
    const allocator = std.testing.allocator;
    var s = try Series(f16).init(allocator);
    defer s.deinit();
    try s.rename("vals");
    try s.append(3.0);
    try s.append(1.5);
    try s.append(2.5);

    var indices = try s.argSort(allocator, true);
    defer indices.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
    try std.testing.expectEqual(@as(usize, 0), indices.items[2]);
}

test "Series(f16): getTypeAsString is Float16" {
    const allocator = std.testing.allocator;
    var s = try Series(f16).init(allocator);
    defer s.deinit();
    var ts = try s.getTypeAsString();
    defer ts.deinit();
    try std.testing.expectEqualStrings("Float16", ts.toSlice());
}

test "Series(f16): asStringAt renders decimal" {
    const allocator = std.testing.allocator;
    var s = try Series(f16).init(allocator);
    defer s.deinit();
    try s.append(1.5);
    var v = try s.asStringAt(0);
    defer v.deinit();
    try std.testing.expectEqualStrings("1.5", v.toSlice());
}

test "GroupBy: f16 key column count" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var key = try df.createSeries(f16);
    try key.rename("k");
    try key.append(1.5);
    try key.append(2.5);
    try key.append(1.5);
    try key.append(2.5);
    try key.append(1.5);

    var vals = try df.createSeries(i64);
    try vals.rename("v");
    try vals.append(10);
    try vals.append(20);
    try vals.append(30);
    try vals.append(40);
    try vals.append(50);

    var gb = try df.groupBy("k");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    try std.testing.expectEqual(@as(usize, 2), counts.height());
    const count_series = counts.getSeries("count") orelse return error.DoesNotExist;
    const a = count_series.usize.values.items[0];
    const b = count_series.usize.values.items[1];
    try std.testing.expect((a == 3 and b == 2) or (a == 2 and b == 3));
}

test "json_writer: f16 column writes unquoted numbers" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(f16);
    try col.rename("x");
    try col.append(1.5);
    try col.append(2.0);

    const output = try json_writer.writeToString(allocator, df, .rows);
    defer allocator.free(output);

    // numeric, not string: contains :1.5 not :"1.5"
    try std.testing.expect(std.mem.indexOf(u8, output, ":1.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "\"1.5\"") == null);
}

test "nulls: indicesWhere never matches null rows" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.rename("x");
    try s.append(0);
    try s.appendNull(); // index 1: null
    try s.append(5);

    var boxed = s.toBoxedSeries();

    // eq 0 matches index 0 only — null at index 1 must not match even though placeholder is 0
    var idx_eq = try boxed.indicesWhere(i32, allocator, .eq, 0);
    defer idx_eq.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &[_]usize{0}, idx_eq.items);

    // neq 0 matches index 2 only — null matches NOTHING, not even neq
    var idx_neq = try boxed.indicesWhere(i32, allocator, .neq, 0);
    defer idx_neq.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &[_]usize{2}, idx_neq.items);

    // lt 10 matches indices 0 and 2
    var idx_lt = try boxed.indicesWhere(i32, allocator, .lt, 10);
    defer idx_lt.deinit(allocator);
    try std.testing.expectEqualSlices(usize, &[_]usize{ 0, 2 }, idx_lt.items);
}

test "nulls: applyInplace skips null slots" {
    const allocator = std.testing.allocator;
    var s = try Series(i32).init(allocator);
    defer s.deinit();
    try s.rename("x");
    try s.append(1);
    try s.appendNull(); // index 1: null with placeholder 0
    try s.append(3);

    const add_one = struct {
        fn f(x: i32) i32 {
            return x + 1;
        }
    }.f;

    s.applyInplace(add_one);

    // index 0: 1 -> 2
    try std.testing.expectEqual(@as(i32, 2), s.values.items[0]);
    // index 1: null slot — still null, placeholder still 0
    try std.testing.expect(s.isNull(1));
    try std.testing.expectEqual(@as(i32, 0), s.values.items[1]);
    // index 2: 3 -> 4
    try std.testing.expectEqual(@as(i32, 4), s.values.items[2]);
}
