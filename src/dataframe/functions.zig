//! General-purpose element-wise functions for use with `Dataframe.applyInplace`,
//! `Dataframe.applyNew`, and `Series.applyInplace`.
//!
//! Those `apply*` entry points take a `comptime func: fn (x: T) T` — a unary
//! transform mapping a value of type `T` back to `T`. Each function here is a
//! *factory*: call it with the column's element type to get a concrete
//! `fn (T) T` to hand to `apply*`.
//!
//!     df.applyInplace("Salary", i32, funcs.negate(i32));
//!     try df.applyNew("Area", "Radius", f64, funcs.square(f64));
//!
//! Functions that need extra parameters (clip bounds, replacement values, ...)
//! already exist as dedicated `Dataframe`/`Series` methods; this module covers
//! the parameter-free building blocks `apply*` expects.

const std = @import("std");

fn assertNumeric(comptime T: type) void {
    switch (@typeInfo(T)) {
        .int, .float, .comptime_int, .comptime_float => {},
        else => @compileError("expected a numeric type, found " ++ @typeName(T)),
    }
}

fn assertFloat(comptime T: type) void {
    switch (@typeInfo(T)) {
        .float, .comptime_float => {},
        else => @compileError("expected a floating-point type, found " ++ @typeName(T)),
    }
}

/// Returns the value unchanged. Useful as a default / no-op transform.
pub fn identity(comptime T: type) fn (T) T {
    return struct {
        fn f(x: T) T {
            return x;
        }
    }.f;
}

/// Arithmetic negation. Requires a signed integer or floating-point type;
/// integer negation wraps (so `minInt` maps to itself rather than trapping).
pub fn negate(comptime T: type) fn (T) T {
    switch (@typeInfo(T)) {
        .float, .comptime_float, .comptime_int => {},
        .int => |i| if (i.signedness == .unsigned)
            @compileError("negate requires a signed type, found " ++ @typeName(T)),
        else => @compileError("negate requires a numeric type, found " ++ @typeName(T)),
    }
    return struct {
        fn f(x: T) T {
            return switch (@typeInfo(T)) {
                .int, .comptime_int => 0 -% x,
                else => -x,
            };
        }
    }.f;
}

/// Absolute value. Unsigned integers are returned unchanged; signed integer
/// magnitude wraps for `minInt` (consistent with two's-complement).
pub fn abs(comptime T: type) fn (T) T {
    assertNumeric(T);
    return struct {
        fn f(x: T) T {
            return switch (@typeInfo(T)) {
                .float, .comptime_float => @abs(x),
                .int => |i| if (i.signedness == .unsigned) x else (if (x < 0) 0 -% x else x),
                else => if (x < 0) -x else x,
            };
        }
    }.f;
}

/// Sign of the value: -1, 0, or +1 (as `T`). Unsigned types never return -1.
pub fn signum(comptime T: type) fn (T) T {
    assertNumeric(T);
    return struct {
        fn f(x: T) T {
            const zero: T = 0;
            const one: T = 1;
            if (x > zero) return one;
            switch (@typeInfo(T)) {
                .int => |i| if (i.signedness == .unsigned) return zero,
                else => {},
            }
            if (x < zero) return zero - one;
            return zero;
        }
    }.f;
}

/// `x * x`.
pub fn square(comptime T: type) fn (T) T {
    assertNumeric(T);
    return struct {
        fn f(x: T) T {
            return x * x;
        }
    }.f;
}

/// `x * x * x`.
pub fn cube(comptime T: type) fn (T) T {
    assertNumeric(T);
    return struct {
        fn f(x: T) T {
            return x * x * x;
        }
    }.f;
}

/// `1 / x`. Floating-point only (integer reciprocal would truncate to 0/1).
pub fn reciprocal(comptime T: type) fn (T) T {
    assertFloat(T);
    return struct {
        fn f(x: T) T {
            return 1.0 / x;
        }
    }.f;
}

/// Largest integer value not greater than `x`. Floating-point only.
pub fn floor(comptime T: type) fn (T) T {
    assertFloat(T);
    return struct {
        fn f(x: T) T {
            return @floor(x);
        }
    }.f;
}

/// Smallest integer value not less than `x`. Floating-point only.
pub fn ceil(comptime T: type) fn (T) T {
    assertFloat(T);
    return struct {
        fn f(x: T) T {
            return @ceil(x);
        }
    }.f;
}

/// Round to the nearest integer, ties away from zero. Floating-point only.
pub fn round(comptime T: type) fn (T) T {
    assertFloat(T);
    return struct {
        fn f(x: T) T {
            return @round(x);
        }
    }.f;
}

/// Truncate toward zero. Floating-point only.
pub fn trunc(comptime T: type) fn (T) T {
    assertFloat(T);
    return struct {
        fn f(x: T) T {
            return @trunc(x);
        }
    }.f;
}
