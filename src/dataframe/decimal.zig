//! Decimal column type (Phase 6d-2a.3): parquet DECIMAL annotation.
//! Fixed-point: the logical value is unscaled * 10^(-scale). i256 unscaled
//! covers precision <= 76 (Arrow decimal256 parity); precision > 76 reads as
//! Raw. Arithmetic is strict: overflow and lossy rescales error rather than
//! silently truncating (project strictness convention).

const std = @import("std");

pub const Decimal = struct {
    unscaled: i256,
    precision: u8,
    scale: i8,

    pub const type_name = "Decimal";

    /// Largest representable magnitude: 10^76 - 1. i256 actually holds a hair
    /// more (2^255 - 1 has 77 digits) but the 76-digit cap is the contract.
    const max_digits: u16 = 76;

    /// 10^n as i512 (n in [0, 154]); i512 headroom means scale alignment of
    /// two i256 values can never overflow the intermediate.
    fn pow10(n: u16) i512 {
        var r: i512 = 1;
        var i: u16 = 0;
        while (i < n) : (i += 1) r *= 10;
        return r;
    }

    /// |value| <= 10^76 - 1. The single fit-check gating every constructed
    /// result: it is the tighter of the two bounds (10^76 - 1 < 2^255 - 1), so
    /// satisfying it implies the value fits i256.
    fn fitsCap(value: i512) bool {
        const cap = pow10(max_digits) - 1;
        return value >= -cap and value <= cap;
    }

    /// Narrow an i512 result that has passed fitsCap down to i256.
    fn toI256(value: i512) !i256 {
        if (!fitsCap(value)) return error.Overflow;
        return @intCast(value);
    }

    /// Value normalized to `target_scale` ticks, in i512.
    /// Caller guarantees target_scale >= self.scale.
    fn rescaledTo(self: Decimal, target_scale: i8) i512 {
        const diff: u16 = @intCast(@as(i16, target_scale) - @as(i16, self.scale));
        return @as(i512, self.unscaled) * pow10(diff);
    }

    fn maxScale(a: i8, b: i8) i8 {
        return if (a >= b) a else b;
    }

    /// Scale-aligned addition. Result scale = max(scale); precision capped at
    /// 76; error.Overflow if the result exceeds the 76-digit cap.
    pub fn add(self: Decimal, other: Decimal) !Decimal {
        const target = maxScale(self.scale, other.scale);
        const sum = self.rescaledTo(target) + other.rescaledTo(target);
        return .{ .unscaled = try toI256(sum), .precision = max_digits, .scale = target };
    }

    /// Scale-aligned subtraction. Done in i512 throughout so the i256 minInt
    /// edge (whose negation overflows i256) never arises.
    pub fn sub(self: Decimal, other: Decimal) !Decimal {
        const target = maxScale(self.scale, other.scale);
        const diff = self.rescaledTo(target) - other.rescaledTo(target);
        return .{ .unscaled = try toI256(diff), .precision = max_digits, .scale = target };
    }

    /// Multiplication: scales add; result precision is pinned to 76 (the cap
    /// is what's enforced — values past 76 digits error with Overflow).
    pub fn mul(self: Decimal, other: Decimal) !Decimal {
        const prod = @as(i512, self.unscaled) * @as(i512, other.unscaled);
        const new_scale: i8 = @intCast(@as(i16, self.scale) + @as(i16, other.scale));
        return .{ .unscaled = try toI256(prod), .precision = max_digits, .scale = new_scale };
    }

    /// Truncating division (toward zero) producing `quot_scale`:
    ///   result_unscaled = (self * 10^(quot_scale + other.scale - self.scale))
    ///                      / other.unscaled
    /// error.DivisionByZero when the divisor is zero; error.LossyRescale when
    /// the required power of ten is negative (quot_scale too small to compute
    /// without pre-truncating the dividend); error.Overflow on cap exceed.
    pub fn div(self: Decimal, other: Decimal, quot_scale: i8) !Decimal {
        if (other.unscaled == 0) return error.DivisionByZero;
        const exp: i16 = @as(i16, quot_scale) + @as(i16, other.scale) - @as(i16, self.scale);
        if (exp < 0) return error.LossyRescale;
        const dividend = @as(i512, self.unscaled) * pow10(@intCast(exp));
        const quot = @divTrunc(dividend, @as(i512, other.unscaled));
        return .{ .unscaled = try toI256(quot), .precision = max_digits, .scale = quot_scale };
    }

    /// Change scale. Widening multiplies by 10^diff (Overflow past the cap);
    /// narrowing divides and errors with LossyRescale if any digit is lost.
    pub fn rescale(self: Decimal, new_scale: i8) !Decimal {
        if (new_scale == self.scale) return self;
        if (new_scale > self.scale) {
            const diff: u16 = @intCast(@as(i16, new_scale) - @as(i16, self.scale));
            const widened = @as(i512, self.unscaled) * pow10(diff);
            return .{ .unscaled = try toI256(widened), .precision = self.precision, .scale = new_scale };
        }
        const diff: u16 = @intCast(@as(i16, self.scale) - @as(i16, new_scale));
        const divisor = pow10(diff);
        const v = @as(i512, self.unscaled);
        if (@rem(v, divisor) != 0) return error.LossyRescale;
        return .{ .unscaled = @intCast(@divExact(v, divisor)), .precision = self.precision, .scale = new_scale };
    }

    /// f64 carries only ~15-16 significant digits, so float conversions route
    /// through i128 intermediates: this LLVM/aarch64 backend can't lower
    /// float<->{i256,i512} conversions, and i128 (38 digits) loses no f64
    /// precision. Magnitudes beyond i128 are pre-reduced by powers of ten
    /// (those digits are unrepresentable in f64 regardless).
    pub fn toF64(self: Decimal) f64 {
        // Reduce unscaled into i128 range, tracking how many digits we dropped.
        var v: i256 = self.unscaled;
        var dropped: i32 = 0;
        const i128_lim: i256 = std.math.maxInt(i128);
        while (v > i128_lim or v < -i128_lim) {
            v = @divTrunc(v, 10);
            dropped += 1;
        }
        const u: f64 = @floatFromInt(@as(i128, @intCast(v)));
        const exp: f64 = @floatFromInt(@as(i32, dropped) - @as(i32, self.scale));
        return u * std.math.pow(f64, 10.0, exp);
    }

    /// Strict-ish: NaN/Inf error; otherwise rounds to nearest tick (ties away
    /// from zero via @round) and checks the 76-digit cap. The rounded scaled
    /// value passes through i128 (sufficient for any f64-representable value).
    pub fn fromF64(v: f64, precision: u8, scale: i8) !Decimal {
        if (std.math.isNan(v) or std.math.isInf(v)) return error.InvalidFloat;
        const mult = std.math.pow(f64, 10.0, @floatFromInt(scale));
        const scaled = @round(v * mult);
        // 2^127 = upper edge of i128; comptime maxInt(i128) is not exactly
        // f64-representable, so compare against the power-of-two bound.
        const i128_edge: f64 = 0x1p127;
        if (std.math.isInf(scaled) or scaled >= i128_edge or scaled < -i128_edge) return error.Overflow;
        const as_i128: i128 = @intFromFloat(scaled);
        return .{ .unscaled = try toI256(@as(i512, as_i128)), .precision = precision, .scale = scale };
    }

    pub fn eql(self: *const Decimal, other: *const Decimal) bool {
        return self.order(other) == .eq;
    }

    pub fn order(self: *const Decimal, other: *const Decimal) std.math.Order {
        const target = maxScale(self.scale, other.scale);
        const a = self.rescaledTo(target);
        const b = other.rescaledTo(target);
        return std.math.order(a, b);
    }

    /// Sign + integer + '.' + zero-padded fraction (scale digits). scale <= 0:
    /// no point; negative scale appends -scale trailing zeros.
    pub fn format(self: Decimal, writer: *std.Io.Writer) std.Io.Writer.Error!void {
        // Magnitude of unscaled as decimal digits, via i512 to dodge the i256
        // minInt negation edge.
        const v: i512 = self.unscaled;
        const neg = v < 0;
        const mag: u512 = @intCast(if (neg) -v else v);

        // u512 has <= 155 decimal digits; 160 is comfortable headroom.
        var buf: [160]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{mag}) catch unreachable;

        if (neg) try writer.writeByte('-');

        if (self.scale <= 0) {
            // No fractional part; negative scale tacks on trailing zeros.
            try writer.writeAll(digits);
            var z: i16 = -@as(i16, self.scale);
            while (z > 0) : (z -= 1) try writer.writeByte('0');
            return;
        }

        const scale_u: usize = @intCast(self.scale);
        if (digits.len > scale_u) {
            // Integer part present.
            const int_len = digits.len - scale_u;
            try writer.writeAll(digits[0..int_len]);
            try writer.writeByte('.');
            try writer.writeAll(digits[int_len..]);
        } else {
            // |value| < 1: "0." then leading zeros then digits.
            try writer.writeAll("0.");
            var pad = scale_u - digits.len;
            while (pad > 0) : (pad -= 1) try writer.writeByte('0');
            try writer.writeAll(digits);
        }
    }

    // ---- wire codec (used by the adapter in Task B) ----

    /// Two's-complement BIG-endian, 1..32 bytes, sign-extended to i256.
    pub fn fromBeBytes(bytes: []const u8) !i256 {
        if (bytes.len == 0 or bytes.len > 32) return error.DecimalTooWide;
        // Build the bit pattern in u256 with sign extension, then bitcast.
        const negative = (bytes[0] & 0x80) != 0;
        var u: u256 = if (negative) std.math.maxInt(u256) else 0;
        for (bytes) |b| {
            u = (u << 8) | @as(u256, b);
        }
        return @bitCast(u);
    }

    /// Write `value` as two's-complement BE into out[0..out.len]. Errors with
    /// error.DecimalOverflow if `value` needs more bytes than out provides
    /// (i.e. the emitted bytes would not sign-extend back to `value`).
    pub fn toBeBytes(value: i256, out: []u8) !void {
        if (out.len == 0 or out.len > 32) return error.DecimalTooWide;
        const u: u256 = @bitCast(value);
        var i: usize = 0;
        while (i < out.len) : (i += 1) {
            const shift: u8 = @intCast((out.len - 1 - i) * 8);
            out[i] = @truncate(u >> shift);
        }
        // Fit-check: the emitted bytes must sign-extend back to the original.
        const round = try fromBeBytes(out);
        if (round != value) return error.DecimalOverflow;
    }

    /// Smallest byte width whose signed range holds `p` decimal digits:
    /// minimal w with 10^p - 1 <= 2^(8w-1) - 1.
    pub fn minBytesForPrecision(p: u8) u8 {
        const target = pow10(p) - 1; // i512: max p-digit magnitude
        var w: u8 = 1;
        while (w < 32) : (w += 1) {
            // signed max for w bytes = 2^(8w-1) - 1
            const bits: u16 = @as(u16, w) * 8 - 1;
            const signed_max = (@as(i512, 1) << @intCast(bits)) - 1;
            if (target <= signed_max) return w;
        }
        return 32;
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const D = Decimal;

test "Decimal: minBytesForPrecision canonical table" {
    try std.testing.expectEqual(@as(u8, 1), D.minBytesForPrecision(1));
    try std.testing.expectEqual(@as(u8, 1), D.minBytesForPrecision(2));
    try std.testing.expectEqual(@as(u8, 2), D.minBytesForPrecision(3));
    try std.testing.expectEqual(@as(u8, 2), D.minBytesForPrecision(4));
    try std.testing.expectEqual(@as(u8, 4), D.minBytesForPrecision(9));
    try std.testing.expectEqual(@as(u8, 5), D.minBytesForPrecision(10));
    try std.testing.expectEqual(@as(u8, 8), D.minBytesForPrecision(18));
    try std.testing.expectEqual(@as(u8, 16), D.minBytesForPrecision(38));
    try std.testing.expectEqual(@as(u8, 32), D.minBytesForPrecision(76));
}

test "Decimal: codec round-trip small values" {
    const vals = [_]i256{ 0, 1, -1, 127, -128, 128, -129, 255, 256, -256, 65535, -65536 };
    for (vals) |v| {
        // Width 4 is plenty for these.
        var buf: [4]u8 = undefined;
        try D.toBeBytes(v, &buf);
        const back = try D.fromBeBytes(&buf);
        try std.testing.expectEqual(v, back);
    }
}

test "Decimal: codec 128 needs 2 bytes" {
    var one: [1]u8 = undefined;
    try std.testing.expectError(error.DecimalOverflow, D.toBeBytes(128, &one));
    var two: [2]u8 = undefined;
    try D.toBeBytes(128, &two);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x80 }, &two);
    try std.testing.expectEqual(@as(i256, 128), try D.fromBeBytes(&two));
}

test "Decimal: codec -128 fits 1 byte" {
    var one: [1]u8 = undefined;
    try D.toBeBytes(-128, &one);
    try std.testing.expectEqualSlices(u8, &[_]u8{0x80}, &one);
    try std.testing.expectEqual(@as(i256, -128), try D.fromBeBytes(&one));
}

test "Decimal: codec all widths 1..32 value -1 (all 0xFF) and 0" {
    var w: usize = 1;
    while (w <= 32) : (w += 1) {
        var buf: [32]u8 = undefined;
        const slice = buf[0..w];

        try D.toBeBytes(-1, slice);
        for (slice) |b| try std.testing.expectEqual(@as(u8, 0xFF), b);
        try std.testing.expectEqual(@as(i256, -1), try D.fromBeBytes(slice));

        try D.toBeBytes(0, slice);
        for (slice) |b| try std.testing.expectEqual(@as(u8, 0x00), b);
        try std.testing.expectEqual(@as(i256, 0), try D.fromBeBytes(slice));
    }
}

test "Decimal: codec known vector 1234567890 = 0x499602D2" {
    // sign bit of 0x49 is 0, so 4 bytes suffice for the VALUE.
    var four: [4]u8 = undefined;
    try D.toBeBytes(1_234_567_890, &four);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x49, 0x96, 0x02, 0xD2 }, &four);
    // File width set by PRECISION (10 -> 5 bytes); toBeBytes pads with 0x00.
    var five: [5]u8 = undefined;
    try D.toBeBytes(1_234_567_890, &five);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x49, 0x96, 0x02, 0xD2 }, &five);
    try std.testing.expectEqual(@as(i256, 1_234_567_890), try D.fromBeBytes(&five));
}

test "Decimal: codec 16-byte negative round-trip" {
    const v: i256 = -1234567890123456789;
    var buf: [16]u8 = undefined;
    try D.toBeBytes(v, &buf);
    try std.testing.expectEqual(v, try D.fromBeBytes(&buf));
    // Top byte sign-extends -> 0xFF prefix.
    try std.testing.expectEqual(@as(u8, 0xFF), buf[0]);
}

test "Decimal: codec i256 extreme magnitudes round-trip in 32 bytes" {
    const cap = D.pow10(76) - 1;
    const big: i256 = @intCast(cap);
    var buf: [32]u8 = undefined;
    try D.toBeBytes(big, &buf);
    try std.testing.expectEqual(big, try D.fromBeBytes(&buf));
    try D.toBeBytes(-big, &buf);
    try std.testing.expectEqual(-big, try D.fromBeBytes(&buf));
}

test "Decimal: codec errors on bad width" {
    try std.testing.expectError(error.DecimalTooWide, D.fromBeBytes(&[_]u8{}));
    var big: [33]u8 = undefined;
    try std.testing.expectError(error.DecimalTooWide, D.toBeBytes(0, &big));
}

test "Decimal: add scale alignment 1.23 + 4.5 = 5.73" {
    const a = D{ .unscaled = 123, .precision = 3, .scale = 2 }; // 1.23
    const b = D{ .unscaled = 45, .precision = 2, .scale = 1 }; // 4.5
    const r = try a.add(b);
    try std.testing.expectEqual(@as(i8, 2), r.scale);
    try std.testing.expectEqual(@as(i256, 573), r.unscaled);
}

test "Decimal: sub crossing zero" {
    const a = D{ .unscaled = 100, .precision = 5, .scale = 2 }; // 1.00
    const b = D{ .unscaled = 250, .precision = 5, .scale = 2 }; // 2.50
    const r = try a.sub(b);
    try std.testing.expectEqual(@as(i8, 2), r.scale);
    try std.testing.expectEqual(@as(i256, -150), r.unscaled); // -1.50
}

test "Decimal: sub does not overflow at i256 minInt" {
    const min = std.math.minInt(i256);
    const a = D{ .unscaled = min, .precision = 76, .scale = 0 };
    const b = D{ .unscaled = 1, .precision = 1, .scale = 0 };
    // min - 1 exceeds the cap -> Overflow (not a panic).
    try std.testing.expectError(error.Overflow, a.sub(b));
    // min itself exceeds 76-digit cap too, but the i512 math must not trap.
    const c = D{ .unscaled = 0, .precision = 1, .scale = 0 };
    try std.testing.expectError(error.Overflow, a.add(c));
}

test "Decimal: mul 1.5 * 2.5 = 3.75" {
    const a = D{ .unscaled = 15, .precision = 2, .scale = 1 };
    const b = D{ .unscaled = 25, .precision = 2, .scale = 1 };
    const r = try a.mul(b);
    try std.testing.expectEqual(@as(i8, 2), r.scale);
    try std.testing.expectEqual(@as(i256, 375), r.unscaled);
}

test "Decimal: div 1.00 / 3.00 quot_scale 4 = 0.3333 (truncates toward zero)" {
    const a = D{ .unscaled = 100, .precision = 5, .scale = 2 };
    const b = D{ .unscaled = 300, .precision = 5, .scale = 2 };
    const r = try a.div(b, 4);
    try std.testing.expectEqual(@as(i8, 4), r.scale);
    try std.testing.expectEqual(@as(i256, 3333), r.unscaled);
}

test "Decimal: div by zero errors" {
    const a = D{ .unscaled = 1, .precision = 1, .scale = 0 };
    const z = D{ .unscaled = 0, .precision = 1, .scale = 0 };
    try std.testing.expectError(error.DivisionByZero, a.div(z, 2));
}

test "Decimal: div negative quot power errors LossyRescale" {
    // self.scale large, quot_scale + other.scale - self.scale < 0
    const a = D{ .unscaled = 100, .precision = 5, .scale = 5 };
    const b = D{ .unscaled = 3, .precision = 1, .scale = 0 };
    try std.testing.expectError(error.LossyRescale, a.div(b, 0));
}

test "Decimal: rescale widen 1.5 -> scale 1 noop, 1.50" {
    const a = D{ .unscaled = 15, .precision = 2, .scale = 1 }; // 1.5
    const same = try a.rescale(1);
    try std.testing.expectEqual(@as(i256, 15), same.unscaled);
    const wider = try a.rescale(3);
    try std.testing.expectEqual(@as(i8, 3), wider.scale);
    try std.testing.expectEqual(@as(i256, 1500), wider.unscaled);
}

test "Decimal: rescale narrow lossless and lossy" {
    const a = D{ .unscaled = 150, .precision = 4, .scale = 2 }; // 1.50
    const n = try a.rescale(1);
    try std.testing.expectEqual(@as(i256, 15), n.unscaled);

    const b = D{ .unscaled = 155, .precision = 4, .scale = 2 }; // 1.55
    try std.testing.expectError(error.LossyRescale, b.rescale(1));
}

test "Decimal: add overflow at 76-digit cap" {
    const cap_i256: i256 = @intCast(D.pow10(76) - 1);
    const a = D{ .unscaled = cap_i256, .precision = 76, .scale = 0 };
    const one = D{ .unscaled = 1, .precision = 1, .scale = 0 };
    try std.testing.expectError(error.Overflow, a.add(one));
}

test "Decimal: eql/order across scales" {
    const a = D{ .unscaled = 15, .precision = 2, .scale = 1 }; // 1.5
    const b = D{ .unscaled = 150, .precision = 4, .scale = 2 }; // 1.50
    try std.testing.expect(a.eql(&b));
    try std.testing.expectEqual(std.math.Order.eq, a.order(&b));

    const neg_small = D{ .unscaled = -200, .precision = 4, .scale = 2 }; // -2.00
    const neg_big = D{ .unscaled = -100, .precision = 4, .scale = 2 }; // -1.00
    try std.testing.expectEqual(std.math.Order.lt, neg_small.order(&neg_big));
    try std.testing.expectEqual(std.math.Order.gt, neg_big.order(&neg_small));
}

test "Decimal: toF64 / fromF64 1.25" {
    const a = D{ .unscaled = 125, .precision = 3, .scale = 2 };
    try std.testing.expectApproxEqAbs(@as(f64, 1.25), a.toF64(), 1e-9);

    const b = try D.fromF64(1.25, 3, 2);
    try std.testing.expectEqual(@as(i256, 125), b.unscaled);
    try std.testing.expectEqual(@as(i8, 2), b.scale);
}

test "Decimal: fromF64 NaN / Inf error" {
    try std.testing.expectError(error.InvalidFloat, D.fromF64(std.math.nan(f64), 3, 2));
    try std.testing.expectError(error.InvalidFloat, D.fromF64(std.math.inf(f64), 3, 2));
}

test "Decimal: format edge cases" {
    const cases = [_]struct { u: i256, scale: i8, want: []const u8 }{
        .{ .u = 0, .scale = 0, .want = "0" },
        .{ .u = 0, .scale = 2, .want = "0.00" },
        .{ .u = 15, .scale = 1, .want = "1.5" },
        .{ .u = -5, .scale = 2, .want = "-0.05" },
        .{ .u = 1234567890, .scale = 2, .want = "12345678.90" },
        .{ .u = 12, .scale = -2, .want = "1200" },
        .{ .u = -1, .scale = 0, .want = "-1" },
        .{ .u = 100, .scale = 2, .want = "1.00" },
    };
    for (cases) |c| {
        const d = D{ .unscaled = c.u, .precision = 76, .scale = c.scale };
        var buf: [128]u8 = undefined;
        const out = try std.fmt.bufPrint(&buf, "{f}", .{d});
        try std.testing.expectEqualStrings(c.want, out);
    }
}

test "Decimal: format precision-76 extreme value scale 0" {
    const cap_i256: i256 = @intCast(D.pow10(76) - 1);
    const d = D{ .unscaled = cap_i256, .precision = 76, .scale = 0 };
    var buf: [128]u8 = undefined;
    const out = try std.fmt.bufPrint(&buf, "{f}", .{d});
    // 76 nines.
    try std.testing.expectEqual(@as(usize, 76), out.len);
    for (out) |ch| try std.testing.expectEqual(@as(u8, '9'), ch);
}

// ---------------------------------------------------------------------------
// Capability smoke tests: Series(Decimal) argSort + asStringAt
// ---------------------------------------------------------------------------

test "Decimal: Series(Decimal) argSort across mixed scales" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Decimal).init(allocator);
    defer s.deinit();
    try s.rename("dec");

    // 3.0 (scale 1), 1.00 (scale 2), 2.5 (scale 1) -> sorted indices [1,2,0]
    try s.append(.{ .unscaled = 30, .precision = 2, .scale = 1 });
    try s.append(.{ .unscaled = 100, .precision = 3, .scale = 2 });
    try s.append(.{ .unscaled = 25, .precision = 2, .scale = 1 });

    var indices = try s.argSort(allocator, true);
    defer indices.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), indices.items[0]);
    try std.testing.expectEqual(@as(usize, 2), indices.items[1]);
    try std.testing.expectEqual(@as(usize, 0), indices.items[2]);
}

test "Decimal: Series(Decimal) asStringAt via format" {
    const Series = @import("series.zig").Series;
    const allocator = std.testing.allocator;

    var s = try Series(Decimal).init(allocator);
    defer s.deinit();
    try s.rename("dec");
    try s.append(.{ .unscaled = 1234567890, .precision = 10, .scale = 2 });

    var str = try s.asStringAt(0);
    defer str.deinit();
    try std.testing.expectEqualStrings("12345678.90", str.toSlice());
}
