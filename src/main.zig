const std = @import("std");
const teddy = @import("teddy");
const Dataframe = teddy.Dataframe;
const Series = teddy.Series;
const String = teddy.String;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const sep = "═" ** 60;

    // ── 1. Load addresses from Parquet ────────────────────────────
    std.debug.print("\n{s}\n  Addresses (Parquet)\n{s}\n", .{ sep, sep });

    var reader = try teddy.Reader.init(allocator, io);
    defer reader.deinit();

    var addr = try reader
        .withFileType(.parquet)
        .withPath("data/addresses.parquet")
        .withDelimiter(',')
        .withHeaders(true)
        .withSkipRows(0)
        .load();
    defer addr.deinit();

    try addr.print();

    // ── 2. Describe numeric columns ────────────────────────────────
    std.debug.print("\n{s}\n  Describe\n{s}\n", .{ sep, sep });

    const stats = try addr.describe();
    defer stats.deinit();
    try stats.print();

    // ── 3. Filter — adults under 60 ───────────────────────────────
    std.debug.print("\n{s}\n  Age >= 18 and Age < 60\n{s}\n", .{ sep, sep });

    const adults = try addr.filter("Age", i64, .gte, 18);
    defer adults.deinit();
    const under60 = try adults.filter("Age", i64, .lt, 60);
    defer under60.deinit();
    try under60.print();

    // ── 4. Sort by Age descending ──────────────────────────────────
    std.debug.print("\n{s}\n  Sorted by Age (desc)\n{s}\n", .{ sep, sep });

    const by_age = try addr.sort("Age", false);
    defer by_age.deinit();
    try by_age.print();

    // ── 5. GroupBy City — count and mean age ──────────────────────
    std.debug.print("\n{s}\n  GroupBy City — count\n{s}\n", .{ sep, sep });

    var by_city = try addr.groupBy("City");
    defer by_city.deinit();

    const city_count = try by_city.count();
    defer city_count.deinit();
    try city_count.print();

    std.debug.print("\n{s}\n  GroupBy City — mean Age\n{s}\n", .{ sep, sep });

    const city_mean_age = try by_city.mean("Age");
    defer city_mean_age.deinit();
    try city_mean_age.print();

    // ── 6. Select columns ─────────────────────────────────────────
    std.debug.print("\n{s}\n  Selected: First Name, City, Age\n{s}\n", .{ sep, sep });

    const slim = try addr.select(&.{ "First Name", "City", "Age" });
    defer slim.deinit();
    try slim.print();

    // ── 7. Value counts on State ───────────────────────────────────
    std.debug.print("\n{s}\n  Value counts — State\n{s}\n", .{ sep, sep });

    const state_counts = try addr.valueCounts("State");
    defer state_counts.deinit();
    try state_counts.print();

    // ── 8. Write to JSON (rows) ────────────────────────────────────
    std.debug.print("\n{s}\n  Round-trip: Parquet → JSON\n{s}\n", .{ sep, sep });

    var writer = try teddy.Writer.init(allocator, io);
    defer writer.deinit();

    const json_bytes = try writer
        .withFileType(.json)
        .withJsonFormat(.rows)
        .toString(addr);
    defer allocator.free(json_bytes);

    std.debug.print("JSON output ({d} bytes):\n{s}\n", .{ json_bytes.len, json_bytes });

    // ── 9. Write addresses to CSV ─────────────────────────────────
    std.debug.print("\n{s}\n  Write addresses → CSV\n{s}\n", .{ sep, sep });

    var csv_writer = try teddy.Writer.init(allocator, io);
    defer csv_writer.deinit();

    try csv_writer
        .withFileType(.csv)
        .withPath("data/addresses_out.csv")
        .save(addr);

    std.debug.print("Saved to data/addresses_out.csv\n", .{});

    // ── 10. Load stock data from CSV ──────────────────────────────
    std.debug.print("\n{s}\n  Stock Data (CSV) — head 10\n{s}\n", .{ sep, sep });

    var stock_reader = try teddy.Reader.init(allocator, io);
    defer stock_reader.deinit();

    var stock = try stock_reader
        .withFileType(.csv)
        .withPath("data/stock_apple.csv")
        .load();
    defer stock.deinit();

    const stock_head = try stock.head(10);
    defer stock_head.deinit();
    try stock_head.print();

    // ── 11. Cumulative sum and diff on Close price ─────────────────
    std.debug.print("\n{s}\n  Close: cumSum, diff, clip [0.25, 2.0]\n{s}\n", .{ sep, sep });

    const close_cumsum = try stock.cumSum("Close");
    defer close_cumsum.deinit();

    const close_diff = try stock.diff("Close", 1);
    defer close_diff.deinit();

    const close_clipped = try stock.clip("Close", f64, 0.25, 2.0);
    defer close_clipped.deinit();

    std.debug.print("cumSum head:\n", .{});
    const cs_head = try close_cumsum.head(5);
    defer cs_head.deinit();
    try cs_head.print();

    std.debug.print("\ndiff(1) head:\n", .{});
    const diff_head = try close_diff.head(6);
    defer diff_head.deinit();
    try diff_head.print();

    std.debug.print("\nclip [0.25, 2.0] tail:\n", .{});
    const clip_tail = try close_clipped.tail(5);
    defer clip_tail.deinit();
    try clip_tail.print();

    // ── 12. Describe stock numerics ───────────────────────────────
    std.debug.print("\n{s}\n  Stock Describe\n{s}\n", .{ sep, sep });

    const stock_stats = try stock.describe();
    defer stock_stats.deinit();
    try stock_stats.print();

    // ── 13. Manual DataFrame — join example ───────────────────────
    std.debug.print("\n{s}\n  Manual join: people × scores\n{s}\n", .{ sep, sep });

    var people = try Dataframe.init(allocator);
    defer people.deinit();

    var names = try Series(String).init(allocator);
    try names.rename("name");
    try names.tryAppend("Alice");
    try names.tryAppend("Bob");
    try names.tryAppend("Carol");
    try names.tryAppend("Dave");
    try people.addSeries(names.toBoxedSeries());

    var depts = try Series(String).init(allocator);
    try depts.rename("dept");
    try depts.tryAppend("Eng");
    try depts.tryAppend("Eng");
    try depts.tryAppend("Sales");
    try depts.tryAppend("Sales");
    try people.addSeries(depts.toBoxedSeries());

    var scores_df = try Dataframe.init(allocator);
    defer scores_df.deinit();

    var score_depts = try Series(String).init(allocator);
    try score_depts.rename("dept");
    try score_depts.tryAppend("Eng");
    try score_depts.tryAppend("Sales");
    try scores_df.addSeries(score_depts.toBoxedSeries());

    var avg_scores = try Series(f64).init(allocator);
    try avg_scores.rename("avg_score");
    try avg_scores.append(91.5);
    try avg_scores.append(78.3);
    try scores_df.addSeries(avg_scores.toBoxedSeries());

    const joined = try people.join(scores_df, "dept", .left);
    defer joined.deinit();
    try joined.print();

    // ── 14. Null handling demo ────────────────────────────────────
    std.debug.print("\n{s}\n  Null handling\n{s}\n", .{ sep, sep });

    var nullable_df = try Dataframe.init(allocator);
    defer nullable_df.deinit();

    var temps = try Series(f64).init(allocator);
    try temps.rename("temp_c");
    try temps.append(22.1);
    try temps.appendNull();
    try temps.append(19.8);
    try temps.appendNull();
    try temps.append(25.0);
    try nullable_df.addSeries(temps.toBoxedSeries());

    std.debug.print("Raw (with nulls):\n", .{});
    try nullable_df.print();

    const filled = try nullable_df.fillNull("temp_c", f64, 0.0);
    defer filled.deinit();
    std.debug.print("\nfillNull(0.0):\n", .{});
    try filled.print();

    const dropped = try nullable_df.dropNulls("temp_c");
    defer dropped.deinit();
    std.debug.print("\ndropNulls:\n", .{});
    try dropped.print();

    std.debug.print("\nDone.\n", .{});
}
