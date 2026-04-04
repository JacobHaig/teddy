const std = @import("std");
const String = @import("strings.zig").String;

test "String: init, append, eql, clear, remove, deinit" {
    const allocator = std.testing.allocator;
    var s = try String.init(allocator);
    defer s.deinit();
    try s.append('a');
    try s.append('b');
    try s.append('c');
    try std.testing.expect(s.len() == 3);
    try std.testing.expect(s.toSlice()[0] == 'a');
    try std.testing.expect(s.toSlice()[1] == 'b');
    try std.testing.expect(s.toSlice()[2] == 'c');
    s.clear();
    try std.testing.expect(s.len() == 0);
    try s.append('x');
    try std.testing.expect(s.toSlice()[0] == 'x');
    _ = s.remove(0);
    try std.testing.expect(s.len() == 0);
}

test "String: fromSlice, eql, eqlSlice, clone" {
    const allocator = std.testing.allocator;
    var s1 = try String.fromSlice(allocator, "hello");
    defer s1.deinit();
    var s2 = try String.fromSlice(allocator, "hello");
    defer s2.deinit();
    try std.testing.expect(s1.eql(&s2));
    try std.testing.expect(s1.eqlSlice("hello"));
    var s3 = try s1.clone();
    defer s3.deinit();
    try std.testing.expect(s3.eql(&s1));
}
