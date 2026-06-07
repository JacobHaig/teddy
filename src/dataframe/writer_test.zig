const std = @import("std");
const Dataframe = @import("dataframe.zig").Dataframe;
const String = @import("strings.zig").String;
const Writer = @import("writer.zig").Writer;
const Reader = @import("reader.zig").Reader;
const parquet = @import("parquet");

test "writer: CSV toString" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.csv);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expect(output.len > 0);
}

test "writer: JSON toString rows" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.rows);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("[{\"x\":1}]", output);
}

test "writer: JSON toString columns" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i32).init(allocator);
    try col.rename("x");
    try col.append(1);
    try col.append(2);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.json).withJsonFormat(.columns);

    const output = try w.toString(df);
    defer allocator.free(output);
    try std.testing.expectEqualStrings("{\"x\":[1,2]}", output);
}

test "writer: Parquet toString and read back" {
    const allocator = std.testing.allocator;
    const Series = @import("series.zig").Series;

    var df = try Dataframe.init(allocator);
    defer df.deinit();
    var col = try Series(i64).init(allocator);
    try col.rename("val");
    try col.append(10);
    try col.append(20);
    try df.addSeries(col.toBoxedSeries());

    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.parquet);

    const output = try w.toString(df);
    defer allocator.free(output);

    // Read it back
    var result = try parquet.readParquet(allocator, output);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.num_rows);
    try std.testing.expectEqual(@as(i64, 10), result.columns[0].int64s.?[0]);
    try std.testing.expectEqual(@as(i64, 20), result.columns[0].int64s.?[1]);
}

test "writer: builder pattern chaining" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    _ = w.withFileType(.csv).withDelimiter(';').withHeader(false);
    try std.testing.expectEqual(@as(u8, ';'), w.delimiter);
    try std.testing.expectEqual(false, w.include_header);
}

test "writer: withEmitInt96 builder sets field" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();
    try std.testing.expectEqual(false, w.emit_int96);
    _ = w.withEmitInt96(true);
    try std.testing.expectEqual(true, w.emit_int96);
}

// ---- B4 regression tests (Phase 12) ----

test "writer B4: withPath happy path — path is stored" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();

    _ = w.withPath("/tmp/test.csv");
    try std.testing.expect(!w.path_alloc_failed);
    try std.testing.expect(w.path != null);
    try std.testing.expectEqualStrings("/tmp/test.csv", w.path.?);
}

test "writer B4: double withPath keeps the second path" {
    const allocator = std.testing.allocator;
    var w = try Writer.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer w.deinit();

    _ = w.withPath("/tmp/first.csv");
    _ = w.withPath("/tmp/second.csv");
    try std.testing.expect(!w.path_alloc_failed);
    try std.testing.expectEqualStrings("/tmp/second.csv", w.path.?);
}

test "reader B4: withPath happy path — path is stored" {
    const allocator = std.testing.allocator;
    var r = try Reader.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer r.deinit();

    _ = r.withPath("/tmp/test.csv");
    try std.testing.expect(!r.path_alloc_failed);
    try std.testing.expect(r.path != null);
    try std.testing.expectEqualStrings("/tmp/test.csv", r.path.?);
}

test "reader B4: double withPath keeps the second path" {
    const allocator = std.testing.allocator;
    var r = try Reader.init(allocator, std.Io.Threaded.global_single_threaded.io());
    defer r.deinit();

    _ = r.withPath("/tmp/first.csv");
    _ = r.withPath("/tmp/second.csv");
    try std.testing.expect(!r.path_alloc_failed);
    try std.testing.expectEqualStrings("/tmp/second.csv", r.path.?);
}

test "reader B4: FailingAllocator on path dupe — load returns OutOfMemory" {
    // Simulate OOM on the path alloc inside withPath.
    var fa = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const fa_alloc = fa.allocator();

    var r = try Reader.init(fa_alloc, std.Io.Threaded.global_single_threaded.io());
    defer r.allocator.destroy(r);

    _ = r.withPath("/tmp/test.csv"); // fails → path_alloc_failed = true
    try std.testing.expect(r.path_alloc_failed);
    try std.testing.expectError(error.OutOfMemory, r.load());
}

test "writer B4: FailingAllocator on path dupe — save returns OutOfMemory" {
    // Use FailingAllocator to simulate OOM on the path duplication inside
    // withPath. The writer must record the failure and save() must surface
    // error.OutOfMemory rather than error.InvalidFilePath.
    const Series = @import("series.zig").Series;

    // Build a minimal dataframe to pass to save().
    const base_alloc = std.testing.allocator;
    var df = try Dataframe.init(base_alloc);
    defer df.deinit();
    var col = try Series(i32).init(base_alloc);
    try col.rename("x");
    try col.append(1);
    try df.addSeries(col.toBoxedSeries());

    // Writer itself is allocated from a failing allocator that is configured
    // to succeed for the struct allocation but fail on the path dupe.
    // We use a FailingAllocator that fails after N successful allocations.
    // Writer.init needs 1 allocation (for the Writer struct), so fail on the
    // 2nd allocation (index 1 = the path dupe in withPath).
    var fa = std.testing.FailingAllocator.init(base_alloc, .{ .fail_index = 1 });
    const fa_alloc = fa.allocator();

    var w = try Writer.init(fa_alloc, std.Io.Threaded.global_single_threaded.io());
    // deinit must not free path (it's null due to failed alloc)
    defer w.allocator.destroy(w);

    _ = w.withPath("/tmp/test.csv"); // fails → sets path_alloc_failed
    try std.testing.expect(w.path_alloc_failed);
    try std.testing.expect(w.path == null);

    try std.testing.expectError(error.OutOfMemory, w.save(df));
}
