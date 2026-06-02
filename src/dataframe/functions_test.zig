const std = @import("std");
const funcs = @import("functions.zig");
const Dataframe = @import("dataframe.zig").Dataframe;

test "identity returns input unchanged" {
    const f = funcs.identity(i32);
    try std.testing.expectEqual(@as(i32, 0), f(0));
    try std.testing.expectEqual(@as(i32, -7), f(-7));
    try std.testing.expectEqual(@as(i32, 42), f(42));
}

test "negate flips sign (signed int and float)" {
    const fi = funcs.negate(i32);
    try std.testing.expectEqual(@as(i32, -5), fi(5));
    try std.testing.expectEqual(@as(i32, 5), fi(-5));

    const ff = funcs.negate(f64);
    try std.testing.expectEqual(@as(f64, -2.5), ff(2.5));
}

test "negate wraps minInt without trapping" {
    const f = funcs.negate(i8);
    try std.testing.expectEqual(@as(i8, -128), f(-128));
}

test "abs: signed int, unsigned int, float" {
    const fi = funcs.abs(i32);
    try std.testing.expectEqual(@as(i32, 9), fi(-9));
    try std.testing.expectEqual(@as(i32, 9), fi(9));

    const fu = funcs.abs(u32);
    try std.testing.expectEqual(@as(u32, 9), fu(9));

    const ff = funcs.abs(f64);
    try std.testing.expectEqual(@as(f64, 3.5), ff(-3.5));
}

test "signum: -1 / 0 / +1, unsigned never negative" {
    const fi = funcs.signum(i32);
    try std.testing.expectEqual(@as(i32, -1), fi(-100));
    try std.testing.expectEqual(@as(i32, 0), fi(0));
    try std.testing.expectEqual(@as(i32, 1), fi(100));

    const fu = funcs.signum(u8);
    try std.testing.expectEqual(@as(u8, 0), fu(0));
    try std.testing.expectEqual(@as(u8, 1), fu(7));
}

test "square and cube" {
    const sq = funcs.square(i64);
    try std.testing.expectEqual(@as(i64, 49), sq(7));
    try std.testing.expectEqual(@as(i64, 49), sq(-7));

    const cb = funcs.cube(i64);
    try std.testing.expectEqual(@as(i64, 27), cb(3));
    try std.testing.expectEqual(@as(i64, -27), cb(-3));
}

test "reciprocal (float only)" {
    const f = funcs.reciprocal(f64);
    try std.testing.expectEqual(@as(f64, 0.25), f(4.0));
}

test "floor / ceil / round / trunc" {
    const fl = funcs.floor(f64);
    const ce = funcs.ceil(f64);
    const ro = funcs.round(f64);
    const tr = funcs.trunc(f64);

    try std.testing.expectEqual(@as(f64, 2.0), fl(2.7));
    try std.testing.expectEqual(@as(f64, -3.0), fl(-2.3));
    try std.testing.expectEqual(@as(f64, 3.0), ce(2.3));
    try std.testing.expectEqual(@as(f64, 3.0), ro(2.5));
    try std.testing.expectEqual(@as(f64, -3.0), ro(-2.5));
    try std.testing.expectEqual(@as(f64, 2.0), tr(2.9));
    try std.testing.expectEqual(@as(f64, -2.0), tr(-2.9));
}

test "integration: applyInplace with a functions.zig factory" {
    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.createSeries(i32);
    try series.rename("v");
    try series.append(-3);
    try series.append(4);
    try series.append(-5);

    df.applyInplace("v", i32, funcs.abs(i32));

    try std.testing.expectEqual(@as(usize, 3), series.len());
    try std.testing.expectEqual(@as(i32, 3), series.toSlice()[0]);
    try std.testing.expectEqual(@as(i32, 4), series.toSlice()[1]);
    try std.testing.expectEqual(@as(i32, 5), series.toSlice()[2]);
}
