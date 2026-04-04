const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const Series = @import("series.zig").Series;
const BoxedSeries = @import("boxed_series.zig").BoxedSeries;

test "basic manipulations" {
    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.createSeries(String);
    try series.rename("Name");
    try series.append(try String.fromSlice(std.testing.allocator, "Alice"));
    try series.tryAppend(try String.fromSlice(std.testing.allocator, "Gary"));
    try series.tryAppend("Bob");

    var series2 = try df.createSeries(f32);
    try series2.rename("Salary");
    try series2.append(15000);
    try series2.append(75000.0);
    try series2.append(110000.0);

    df.applyInplace("Salary", f32, struct {
        fn call(x: f32) f32 {
            return x / 52 / 40;
        }
    }.call);

    var series3 = try df.createSeries(i32);
    try series3.rename("Age");
    try series3.append(15);
    try series3.append(20);
    try series3.append(30);
    // series3.print();

    const add_five = struct {
        fn call(x: i32) i32 {
            return x + 5;
        }
    }.call;
    df.applyInplace("Age", i32, add_five);

    df.applyInplace("Age", i32, struct {
        fn call(x: i32) i32 {
            return x + 10;
        }
    }.call);

    var df2 = try df.deepCopy();
    defer df2.deinit();

    df.dropSeries("Age");
    df.limit(2);

    try std.testing.expectEqual(2, df.height());
    try std.testing.expectEqual(2, df.width());
    try std.testing.expectEqual(3, df2.height());
    try std.testing.expectEqual(3, df2.width());
}

test "Dataframe: init, width, height, createSeries, addSeries, dropSeries, dropRow" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expect(df.width() == 0);
    try std.testing.expect(df.height() == 0);

    var s = try Series(i32).init(allocator);
    try s.rename("col1");
    try s.append(1);
    try s.append(2);
    try s.append(3);
    try df.addSeries(s.toBoxedSeries());
    // Do NOT deinit s after ownership is transferred
    try std.testing.expect(df.width() == 1);
    try std.testing.expect(df.height() == 3);

    var s2 = try Series(i32).init(allocator);
    try s2.rename("col2");
    try s2.append(10);
    try s2.append(20);
    try s2.append(30);
    try df.addSeries(s2.toBoxedSeries());
    // Do NOT deinit s2 after ownership is transferred
    try std.testing.expect(df.width() == 2);
    try std.testing.expect(df.height() == 3);

    df.dropSeries("col1");
    try std.testing.expect(df.width() == 1);
    df.dropRow(1);
    try std.testing.expect(df.height() == 2);
}

test "Dataframe: compareDataframe equality and inequality" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var s1 = try Series(i32).init(allocator);
    try s1.rename("col");
    try s1.append(1);
    try s1.append(2);
    try df1.addSeries(s1.toBoxedSeries());
    // Do NOT deinit s1 after ownership is transferred

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var s2 = try Series(i32).init(allocator);
    try s2.rename("col");
    try s2.append(1);
    try s2.append(2);
    try df2.addSeries(s2.toBoxedSeries());
    // Do NOT deinit s2 after ownership is transferred

    try std.testing.expect(try df1.compareDataframe(df2));
    // Add a new value to s2 (not owned by df2, so this is safe)
    var s3 = try Series(i32).init(allocator);
    try s3.rename("col");
    try s3.append(1);
    try s3.append(2);
    try s3.append(3);
    var df3 = try Dataframe.init(allocator);
    defer df3.deinit();
    try df3.addSeries(s3.toBoxedSeries());
    try std.testing.expect(!(try df1.compareDataframe(df3)));
}

test "External Function Test: add5a" {
    const f = @import("functions.zig");

    var df = try Dataframe.init(std.testing.allocator);
    defer df.deinit();

    var series = try df.createSeries(i32);
    try series.rename("Salary");
    try series.append(15000);
    try series.append(75000);

    df.applyInplace("Salary", i32, f.add5a);

    try std.testing.expect(series.len() == 2);
    try std.testing.expect(series.toSlice()[0] == 15005);
    try std.testing.expect(series.toSlice()[1] == 75005);
}

test "String re-export: can create and use String from top-level API" {
    const allocator = std.testing.allocator;
    var s = try String.init(allocator);
    defer s.deinit();
    try s.append('a');
    try s.append('b');
    try std.testing.expect(s.len() == 2);
    try std.testing.expect(s.toSlice()[0] == 'a');
    try std.testing.expect(s.toSlice()[1] == 'b');
}

test "memory management and ownership" {
    const series_mod = @import("series.zig");
    const csv_mod = @import("csv_reader.zig");
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    var had_leak = false;
    defer {
        if (debug_allocator.deinit() == .leak) {
            std.debug.print("Memory leaks detected!\n", .{});
            had_leak = true;
        }
    }

    // Test Series allocation and deallocation
    var s = try series_mod.Series(String).init(allocator);
    defer s.deinit();
    try s.rename("Test Series");
    try s.tryAppend("Hello");
    try s.tryAppend("World");

    // Test deepCopy and ownership
    var s2 = try s.deepCopy();
    defer s2.deinit();
    try s2.rename("Copy");

    // Test CSV parsing allocation and deallocation
    const content =
        "A,B\n1,2\n3,4\n";

    // Test Dataframe allocation and deallocation
    var df = try csv_mod.parse(allocator, content, .{});
    defer df.deinit();
    try std.testing.expect(!had_leak);
}

test "dataframe series ownership" {
    const series_mod = @import("series.zig");
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    const allocator = debug_allocator.allocator();
    var had_leak = false;
    defer {
        if (debug_allocator.deinit() == .leak) {
            std.debug.print("Memory leaks detected!\n", .{});
            had_leak = true;
        }
    }

    // Create a dataframe and add a series to it, let dataframe own the series
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var s = try series_mod.Series(String).init(allocator);
    try s.rename("Test Series");
    try s.tryAppend("A");
    try s.tryAppend("B");
    try df.addSeries(s.toBoxedSeries());
    // Do NOT deinit s, dataframe owns it now

    try std.testing.expect(!had_leak);
}

test "groupBy: count groups" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);
    try category.append(2);
    try category.append(1);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    try std.testing.expect(counts.height() == 2);
    try std.testing.expect(counts.width() == 2);
    const count_series = counts.getSeries("count") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(usize, 3), count_series.usize.values.items[0]);
    try std.testing.expectEqual(@as(usize, 2), count_series.usize.values.items[1]);
}

test "groupBy: sum by group" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);
    try category.append(2);

    var values = try df.createSeries(i32);
    try values.rename("values");
    try values.append(10);
    try values.append(20);
    try values.append(15);
    try values.append(25);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var sum_result = try gb.sum("values");
    defer sum_result.deinit();

    try std.testing.expect(sum_result.height() == 2);
    const sum_col = sum_result.getSeries("values") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 25), sum_col.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 45), sum_col.int32.values.items[1]);
}

test "groupBy: mean by group" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var category = try df.createSeries(i32);
    try category.rename("category");
    try category.append(1);
    try category.append(2);
    try category.append(1);

    var values = try df.createSeries(i32);
    try values.rename("values");
    try values.append(10);
    try values.append(20);
    try values.append(20);

    var gb = try df.groupBy("category");
    defer gb.deinit();

    var mean_result = try gb.mean("values");
    defer mean_result.deinit();

    try std.testing.expect(mean_result.height() == 2);
    // mean of [10, 20] for group 1 = 15.0, mean of [20] for group 2 = 20.0
    const mean_col = mean_result.getSeries("values") orelse return error.DoesNotExist;
    try std.testing.expectEqual(15.0, mean_col.float64.values.items[0]);
    try std.testing.expectEqual(20.0, mean_col.float64.values.items[1]);
}

test "Dataframe: filterByIndices creates correct subset" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col1 = try df.createSeries(i32);
    try col1.rename("a");
    try col1.append(10);
    try col1.append(20);
    try col1.append(30);
    try col1.append(40);

    var col2 = try df.createSeries(i32);
    try col2.rename("b");
    try col2.append(1);
    try col2.append(2);
    try col2.append(3);
    try col2.append(4);

    var filtered = try df.filterByIndices(&[_]usize{ 0, 2 });
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
    try std.testing.expectEqual(@as(usize, 2), filtered.width());

    const a = filtered.getSeries("a") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), a.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), a.int32.values.items[1]);

    const b = filtered.getSeries("b") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 1), b.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 3), b.int32.values.items[1]);
}

test "Dataframe: select picks named columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try df.createSeries(i32);
    try a.rename("a");
    try a.append(1);

    var b = try df.createSeries(i32);
    try b.rename("b");
    try b.append(2);

    var c = try df.createSeries(i32);
    try c.rename("c");
    try c.append(3);

    var selected = try df.select(&[_][]const u8{ "a", "c" });
    defer selected.deinit();

    try std.testing.expectEqual(@as(usize, 2), selected.width());
    try std.testing.expect(selected.getSeries("a") != null);
    try std.testing.expect(selected.getSeries("b") == null);
    try std.testing.expect(selected.getSeries("c") != null);
}

test "Dataframe: select missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try df.createSeries(i32);
    try a.rename("a");
    try a.append(1);

    try std.testing.expectError(error.ColumnNotFound, df.select(&[_][]const u8{"nope"}));
}

test "Dataframe: head returns first n rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);

    var h = try df.head(2);
    defer h.deinit();

    try std.testing.expectEqual(@as(usize, 2), h.height());
    const s = h.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
}

test "Dataframe: tail returns last n rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);

    var t = try df.tail(2);
    defer t.deinit();

    try std.testing.expectEqual(@as(usize, 2), t.height());
    const s = t.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[1]);
}

test "Dataframe: slice returns row range" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);
    try col.append(40);

    var s = try df.slice(1, 3);
    defer s.deinit();

    try std.testing.expectEqual(@as(usize, 2), s.height());
    const xs = s.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), xs.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), xs.int32.values.items[1]);
}

test "Dataframe: head with n > height returns all rows" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    var h = try df.head(100);
    defer h.deinit();

    try std.testing.expectEqual(@as(usize, 1), h.height());
}

test "Dataframe: sort ascending" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(30);
    try col.append(10);
    try col.append(20);

    var sorted = try df.sort("x", true);
    defer sorted.deinit();

    const s = sorted.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[2]);
}

test "Dataframe: sort descending" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(30);
    try col.append(20);

    var sorted = try df.sort("x", false);
    defer sorted.deinit();

    const s = sorted.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 10), s.int32.values.items[2]);
}

test "Dataframe: sort preserves other columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var ages = try df.createSeries(i32);
    try ages.rename("age");
    try ages.append(30);
    try ages.append(10);
    try ages.append(20);

    var names = try df.createSeries(String);
    try names.rename("name");
    try names.tryAppend("Alice");
    try names.tryAppend("Bob");
    try names.tryAppend("Carol");

    var sorted = try df.sort("age", true);
    defer sorted.deinit();

    const n = sorted.getSeries("name") orelse return error.DoesNotExist;
    try std.testing.expectEqualStrings("Bob", n.string.values.items[0].toSlice());
    try std.testing.expectEqualStrings("Carol", n.string.values.items[1].toSlice());
    try std.testing.expectEqualStrings("Alice", n.string.values.items[2].toSlice());
}

test "Dataframe: filter gt" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);
    try col.append(30);
    try col.append(5);

    var filtered = try df.filter("x", i32, .gt, 15);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
    const s = filtered.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 20), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 30), s.int32.values.items[1]);
}

test "Dataframe: filter eq" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);

    var filtered = try df.filter("x", i32, .eq, 1);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 2), filtered.height());
}

test "Dataframe: filter no matches returns empty" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    var filtered = try df.filter("x", i32, .gt, 100);
    defer filtered.deinit();

    try std.testing.expectEqual(@as(usize, 0), filtered.height());
}

test "Dataframe: filter missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);

    try std.testing.expectError(error.ColumnNotFound, df.filter("nope", i32, .eq, 1));
}

test "Dataframe: unique removes duplicates" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);
    try col.append(3);
    try col.append(2);

    var u = try df.unique("x");
    defer u.deinit();

    try std.testing.expectEqual(@as(usize, 3), u.height());
}

test "Dataframe: valueCounts returns key and count" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try col.append(1);

    var vc = try df.valueCounts("x");
    defer vc.deinit();

    try std.testing.expectEqual(@as(usize, 2), vc.width());
    try std.testing.expectEqual(@as(usize, 2), vc.height());
    try std.testing.expect(vc.getSeries("x") != null);
    try std.testing.expect(vc.getSeries("count") != null);
}

test "Dataframe: concat stacks rows" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var a1 = try df1.createSeries(i32);
    try a1.rename("x");
    try a1.append(1);
    try a1.append(2);

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var a2 = try df2.createSeries(i32);
    try a2.rename("x");
    try a2.append(3);

    var combined = try df1.concat(df2);
    defer combined.deinit();

    try std.testing.expectEqual(@as(usize, 3), combined.height());
    const s = combined.getSeries("x") orelse return error.DoesNotExist;
    try std.testing.expectEqual(@as(i32, 1), s.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 2), s.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 3), s.int32.values.items[2]);
}

test "Dataframe: concat missing column returns error" {
    const allocator = std.testing.allocator;
    var df1 = try Dataframe.init(allocator);
    defer df1.deinit();
    var a1 = try df1.createSeries(i32);
    try a1.rename("x");
    try a1.append(1);

    var df2 = try Dataframe.init(allocator);
    defer df2.deinit();
    var a2 = try df2.createSeries(i32);
    try a2.rename("y");
    try a2.append(1);

    try std.testing.expectError(error.ColumnNotFound, df1.concat(df2));
}

test "Dataframe: describe returns summary statistics" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try df.createSeries(i32);
    try col.rename("x");
    try col.append(10);
    try col.append(20);

    var str_col = try df.createSeries(String);
    try str_col.rename("name");
    try str_col.tryAppend("a");
    try str_col.tryAppend("b");

    var desc = try df.describe();
    defer desc.deinit();

    // Should have "stat" + "x" columns (string column skipped)
    try std.testing.expectEqual(@as(usize, 2), desc.width());
    try std.testing.expectEqual(@as(usize, 5), desc.height());

    const x_col = desc.getSeries("x") orelse return error.DoesNotExist;
    // count=2, mean=15, std=5, min=10, max=20
    try std.testing.expectEqual(@as(f64, 2.0), x_col.float64.values.items[0]);
    try std.testing.expectEqual(@as(f64, 15.0), x_col.float64.values.items[1]);
    try std.testing.expectEqual(@as(f64, 5.0), x_col.float64.values.items[2]);
    try std.testing.expectEqual(@as(f64, 10.0), x_col.float64.values.items[3]);
    try std.testing.expectEqual(@as(f64, 20.0), x_col.float64.values.items[4]);
}

test "Dataframe: groupByMultiple composite key" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var state = try df.createSeries(String);
    try state.rename("state");
    try state.tryAppend("NJ");
    try state.tryAppend("PA");
    try state.tryAppend("NJ");

    var city = try df.createSeries(String);
    try city.rename("city");
    try city.tryAppend("Riverside");
    try city.tryAppend("Phila");
    try city.tryAppend("Riverside");

    var val = try df.createSeries(i32);
    try val.rename("val");
    try val.append(10);
    try val.append(20);
    try val.append(30);

    var gb = try df.groupByMultiple(&[_][]const u8{ "state", "city" });
    defer gb.deinit();

    var counts = try gb.count();
    defer counts.deinit();

    // NJ|Riverside=2, PA|Phila=1
    try std.testing.expectEqual(@as(usize, 2), counts.height());
}

// --- Nullable DataFrame Tests ---

test "Dataframe: dropNulls removes rows where column is null" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.appendNull();
    try col.append(3);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.dropNulls("x");
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
    const s = result.getSeries("x").?;
    for (0..result.height()) |i| try std.testing.expect(!s.isNull(i));
}

test "Dataframe: dropNulls unknown column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.dropNulls("nope"));
}

test "Dataframe: dropNullsAny removes rows with any null" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i64).init(allocator);
    try a.rename("a");
    try a.append(1);
    try a.appendNull(); // row 1 has null in "a"
    try a.append(3);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i64).init(allocator);
    try b.rename("b");
    try b.append(10);
    try b.append(20);
    try b.appendNull(); // row 2 has null in "b"
    try df.addSeries(b.toBoxedSeries());

    var result = try df.dropNullsAny();
    defer result.deinit();

    // Only row 0 has no nulls in either column.
    try std.testing.expectEqual(@as(usize, 1), result.height());
}

test "Dataframe: dropNullsAny with no nulls returns full copy" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.dropNullsAny();
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.height());
}

test "Dataframe: fillNull replaces nulls in target column only" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i64).init(allocator);
    try a.rename("a");
    try a.append(1);
    try a.appendNull();
    try a.append(3);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i64).init(allocator);
    try b.rename("b");
    try b.appendNull();
    try b.append(20);
    try b.append(30);
    try df.addSeries(b.toBoxedSeries());

    var result = try df.fillNull("a", i64, 99);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 3), result.height());

    // Column "a": null at index 1 should be filled.
    const ra = result.getSeries("a").?;
    try std.testing.expect(!ra.isNull(0));
    try std.testing.expect(!ra.isNull(1));
    try std.testing.expect(!ra.isNull(2));
    var v = try ra.asStringAt(1);
    defer v.deinit();
    try std.testing.expectEqualStrings("99", v.toSlice());

    // Column "b": still has its original null at index 0.
    const rb = result.getSeries("b").?;
    try std.testing.expect(rb.isNull(0));
}

test "Dataframe: fillNull unknown column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.fillNull("nope", i64, 0));
}

test "Dataframe: fillNull type mismatch returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i64).init(allocator);
    try col.rename("x");
    try col.appendNull();
    try df.addSeries(col.toBoxedSeries());

    try std.testing.expectError(error.TypeMismatch, df.fillNull("x", f64, 0.0));
}

test "Dataframe: cast i32 column to f64" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var col = try Series(i32).init(allocator);
    try col.rename("val");
    try col.append(1);
    try col.append(2);
    try col.append(3);
    try df.addSeries(col.toBoxedSeries());

    var result = try df.cast("val", f64);
    defer result.deinit();

    const s = result.getSeries("val") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("f64", s.typeName());
    try std.testing.expectEqual(@as(usize, 3), s.len());
}

test "Dataframe: cast preserves other columns" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var a = try Series(i32).init(allocator);
    try a.rename("a");
    try a.append(10);
    try df.addSeries(a.toBoxedSeries());

    var b = try Series(i32).init(allocator);
    try b.rename("b");
    try b.append(20);
    try df.addSeries(b.toBoxedSeries());

    var result = try df.cast("a", f64);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.width());
    const sa = result.getSeries("a") orelse return error.TestFailed;
    const sb = result.getSeries("b") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("f64", sa.typeName());
    try std.testing.expectEqualStrings("i32", sb.typeName());
}

test "Dataframe: cast missing column returns error" {
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();
    try std.testing.expectError(error.ColumnNotFound, df.cast("nope", f64));
}

// ---------------------------------------------------------------------------
// Large pipeline integration test — composes multiple Priority 1 operations
// ---------------------------------------------------------------------------

test "Dataframe: pipeline — nulls, cast, shift, diff, clip, replace, groupby, cumsum" {
    // Schema: category(i32), units(i32 with nulls), price(i64 cents)
    //   row 0:  cat=1, units=10,  price=1500
    //   row 1:  cat=2, units=null,price=2000
    //   row 2:  cat=1, units=30,  price=500
    //   row 3:  cat=2, units=200, price=3000   ← units will be clipped to 100
    //   row 4:  cat=1, units=50,  price=1000
    //   row 5:  cat=3, units=5,   price=800
    const allocator = std.testing.allocator;
    var df = try Dataframe.init(allocator);
    defer df.deinit();

    var cat = try Series(i32).init(allocator);
    try cat.rename("cat");
    try cat.append(1); try cat.append(2); try cat.append(1);
    try cat.append(2); try cat.append(1); try cat.append(3);

    var units = try Series(i32).init(allocator);
    try units.rename("units");
    try units.append(10); try units.appendNull(); try units.append(30);
    try units.append(200); try units.append(50); try units.append(5);

    var price = try Series(i64).init(allocator);
    try price.rename("price");
    try price.append(1500); try price.append(2000); try price.append(500);
    try price.append(3000); try price.append(1000); try price.append(800);

    try df.addSeries(cat.toBoxedSeries());
    try df.addSeries(units.toBoxedSeries());
    try df.addSeries(price.toBoxedSeries());

    // 1. Verify null is present
    try std.testing.expectEqual(@as(usize, 1), df.getSeries("units").?.nullCount());

    // 2. Fill null units with 0
    var df2 = try df.fillNull("units", i32, 0);
    defer df2.deinit();
    try std.testing.expectEqual(@as(usize, 0), df2.getSeries("units").?.nullCount());
    try std.testing.expectEqual(@as(i32, 0), df2.getSeries("units").?.int32.values.items[1]);

    // 3. Clip units to [0, 100] — row 3 was 200, should become 100
    var df3 = try df2.clip("units", i32, 0, 100);
    defer df3.deinit();
    try std.testing.expectEqual(@as(i32, 100), df3.getSeries("units").?.int32.values.items[3]);
    try std.testing.expectEqual(@as(i32, 10),  df3.getSeries("units").?.int32.values.items[0]);

    // 4. Cast price from i64 to f64
    var df4 = try df3.cast("price", f64);
    defer df4.deinit();
    try std.testing.expectEqualStrings("f64", df4.getSeries("price").?.typeName());
    try std.testing.expectApproxEqAbs(@as(f64, 1500.0), df4.getSeries("price").?.float64.values.items[0], 1e-9);

    // 5. Replace cat 3 with cat 99
    var df5 = try df3.replace("cat", i32, 3, 99);
    defer df5.deinit();
    try std.testing.expectEqual(@as(i32, 99), df5.getSeries("cat").?.int32.values.items[5]);
    try std.testing.expectEqual(@as(i32, 1),  df5.getSeries("cat").?.int32.values.items[0]);

    // 6. Shift units down by 1 (prepend null, drop last)
    var df6 = try df3.shift("units", 1);
    defer df6.deinit();
    try std.testing.expectEqual(@as(usize, 6), df6.height());
    try std.testing.expect(df6.getSeries("units").?.isNull(0));
    try std.testing.expectEqual(@as(i32, 10), df6.getSeries("units").?.int32.values.items[1]);

    // 7. Diff units by lag 1 (strict — all values are signed so no underflow)
    var df7 = try df3.diff("units", 1);
    defer df7.deinit();
    try std.testing.expect(df7.getSeries("units").?.isNull(0)); // first row always null
    // row 2: 30 - 0 = 30
    try std.testing.expectEqual(@as(i32, 30), df7.getSeries("units").?.int32.values.items[2]);

    // 8. Cumulative sum of units
    var df8 = try df3.cumSum("units");
    defer df8.deinit();
    // values: 10, 0, 30, 100, 50, 5  → cumsum: 10, 10, 40, 140, 190, 195
    try std.testing.expectEqual(@as(i32, 10),  df8.getSeries("units").?.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 10),  df8.getSeries("units").?.int32.values.items[1]);
    try std.testing.expectEqual(@as(i32, 40),  df8.getSeries("units").?.int32.values.items[2]);
    try std.testing.expectEqual(@as(i32, 140), df8.getSeries("units").?.int32.values.items[3]);
    try std.testing.expectEqual(@as(i32, 195), df8.getSeries("units").?.int32.values.items[5]);

    // 9. GroupBy cat → sum units, median price
    var gb = try df3.groupBy("cat");
    defer gb.deinit();

    var gb_sum = try gb.sum("units");
    defer gb_sum.deinit();
    try std.testing.expectEqual(@as(usize, 3), gb_sum.height()); // 3 categories

    var gb_med = try gb.median("price");
    defer gb_med.deinit();
    try std.testing.expectEqual(@as(usize, 3), gb_med.height());
    // Category 1 prices: 1500, 500, 1000 → median = 1000.0
    const med_col = gb_med.getSeries("price") orelse return error.MissingColumn;
    var cat1_med: ?f64 = null;
    const cat_col = gb_sum.getSeries("cat") orelse return error.MissingColumn;
    for (cat_col.int32.values.items, 0..) |c, i| {
        if (c == 1) cat1_med = med_col.float64.values.items[i];
    }
    try std.testing.expectApproxEqAbs(@as(f64, 1000.0), cat1_med.?, 1e-9);

    // 10. describe() — verify count reflects non-null rows, not total rows
    //     (df3 has no nulls after fillNull+clip, so count should equal height=6)
    var desc = try df3.describe();
    defer desc.deinit();
    // "stat" column + numeric columns
    try std.testing.expect(desc.width() >= 2);
    // count row is row 0; for units (6 non-null rows) count should be 6
    const units_stats = desc.getSeries("units") orelse return error.MissingColumn;
    try std.testing.expectApproxEqAbs(@as(f64, 6.0), units_stats.float64.values.items[0], 1e-9);

    // 11. Filter then sort
    var filtered = try df3.filter("cat", i32, .eq, 1);
    defer filtered.deinit();
    try std.testing.expectEqual(@as(usize, 3), filtered.height());
    var sorted = try filtered.sort("units", true);
    defer sorted.deinit();
    // sorted units for cat=1: 10, 30, 50
    try std.testing.expectEqual(@as(i32, 10), sorted.getSeries("units").?.int32.values.items[0]);
    try std.testing.expectEqual(@as(i32, 50), sorted.getSeries("units").?.int32.values.items[2]);

    // 12. Quantile on the units series directly
    const units_series = df3.getSeries("units").?;
    const q25 = try units_series.int32.quantile(allocator, 0.25);
    try std.testing.expect(q25 != null);
    const q75 = try units_series.int32.quantile(allocator, 0.75);
    try std.testing.expect(q75.? >= q25.?);

    // 13. nunique on categories
    try std.testing.expectEqual(@as(usize, 3), try df3.getSeries("cat").?.int32.nunique(allocator));
}
