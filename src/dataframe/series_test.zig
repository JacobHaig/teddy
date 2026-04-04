const std = @import("std");
const Series = @import("series.zig").Series;
const strings = @import("strings.zig");
const String = strings.String;

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
