// const std = @import("std");

// pub fn read_parquet1() void {
//     var gpa = std.heap.DebugAllocator(.{}){};
//     const allocator = gpa.allocator();

//     const file = std.fs.cwd().openFile("./data/addresses.parquet", .{}) catch unreachable;
//     const buffer = file.readToEndAlloc(allocator, 10_000_000) catch unreachable;

//     const b_len = buffer.len;

//     const first_m_number = buffer[0..4];
//     const last_m_number = buffer[b_len - 4 .. b_len];

//     const footer_len_slice = buffer[b_len - 8 .. b_len - 4];
//     const footer_len = std.mem.readVarInt(u32, footer_len_slice, .little);

//     const footer_slice = buffer[b_len - footer_len - 8 .. b_len - 8];

//     std.debug.print("{s} - {s}\n", .{ first_m_number, last_m_number });

//     std.debug.print("{any}\n", .{footer_len_slice});
//     std.debug.print("{d}\n", .{footer_len});

//     std.debug.print("{any}\n", .{footer_slice});
// }

// pub fn read_parquet2() void {
//     var gpa = std.heap.DebugAllocator(.{}){};
//     const allocator = gpa.allocator();

//     const file = std.fs.cwd().openFile("./data/addresses.parquet", .{}) catch unreachable;
//     const buffer = file.readToEndAlloc(allocator, 10_000_000) catch unreachable;

//     const b_len = buffer.len;

//     const first_m_number = buffer[0..4];
//     const last_m_number = buffer[buffer.len - 4 ..];

//     const footer_len_slice = buffer[b_len - 8 .. b_len - 4];
//     const footer_len = std.mem.readVarInt(u32, footer_len_slice, .little);

//     const data_and_footer = buffer[4 .. buffer.len - 8];
//     _ = data_and_footer[0 .. data_and_footer.len - footer_len];
//     const footer = data_and_footer[data_and_footer.len - footer_len ..];

//     // meta data
//     const version_slice = footer[0..4];
//     const version = std.mem.readVarInt(i32, version_slice, .little);

//     std.debug.print("{s} - {s}\n", .{ first_m_number, last_m_number });

//     std.debug.print("{any}\n", .{footer_len_slice});
//     std.debug.print("{d}\n", .{footer_len});

//     std.debug.print("version {any}\n", .{version_slice});
//     std.debug.print("version {d}\n", .{version});
// }
