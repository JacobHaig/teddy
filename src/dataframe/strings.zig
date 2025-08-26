const std = @import("std");

pub const String = std.ArrayList(u8);

pub fn createString(allocator: std.mem.Allocator) !String {
    var name = try String.initCapacity(allocator, 0);
    errdefer name.deinit(allocator);

    return name;
}

pub fn createStringFromArray(allocator: std.mem.Allocator, str: []const u8) !String {
    var name = try String.initCapacity(allocator, str.len);
    errdefer name.deinit(allocator);

    name.appendSliceAssumeCapacity(str);
    return name;
}
