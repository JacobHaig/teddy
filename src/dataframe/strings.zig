const std = @import("std");

pub const String = struct {
    allocator: std.mem.Allocator,
    list: std.ArrayList(u8),

    /// Create an empty String
    pub fn init(allocator: std.mem.Allocator) !String {
        return String{
            .allocator = allocator,
            .list = try std.ArrayList(u8).initCapacity(allocator, 0),
        };
    }

    /// Create a String from a slice
    pub fn fromSlice(allocator: std.mem.Allocator, str: []const u8) !String {
        var s = try String.init(allocator);
        try s.appendSlice(str);
        return s;
    }

    /// Free all memory used by this String
    pub fn deinit(self: *String) void {
        self.list.deinit(self.allocator);
        // No need to free self, as this is a value type
    }

    /// Clone this String (deep copy)
    pub fn clone(self: *const String) !String {
        return String.fromSlice(self.allocator, self.toSlice());
    }

    /// Remove all contents, keep capacity
    pub fn clear(self: *String) void {
        self.list.clearRetainingCapacity();
    }

    /// Append a single byte
    pub fn append(self: *String, c: u8) !void {
        try self.list.append(self.allocator, c);
    }

    /// Append a slice
    pub fn appendSlice(self: *String, s: []const u8) !void {
        try self.list.appendSlice(self.allocator, s);
    }

    /// Get the string as a slice
    pub fn toSlice(self: *const String) []const u8 {
        return self.list.items;
    }

    /// Compare with another String
    pub fn eql(self: *const String, other: *const String) bool {
        return std.mem.eql(u8, self.toSlice(), other.toSlice());
    }

    /// Compare with a slice
    pub fn eqlSlice(self: *const String, other: []const u8) bool {
        return std.mem.eql(u8, self.toSlice(), other);
    }

    /// Reserve capacity
    pub fn ensureCapacity(self: *String, n: usize) !void {
        try self.list.ensureTotalCapacity(self.allocator, n);
    }

    /// Remove a character at index
    pub fn remove(self: *String, idx: usize) u8 {
        return self.list.orderedRemove(idx);
    }

    /// Length
    pub fn len(self: *const String) usize {
        return self.list.items.len;
    }
};

/// Deprecated: use String.init
pub fn createString(allocator: std.mem.Allocator) !String {
    return String.init(allocator);
}

/// Deprecated: use String.fromSlice
pub fn createStringFromSlice(allocator: std.mem.Allocator, str: []const u8) !String {
    return String.fromSlice(allocator, str);
}

