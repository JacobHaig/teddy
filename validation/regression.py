"""Phase 9 regression-framework generator (read + transforms stages).

Reference engine: pyarrow (NOT pandas). Writes a comprehensive fixture
covering one column per teddy type, plus a golden manifest carrying the
implementation-neutral SEMANTIC canonical for every cell. The committed Zig
test (src/dataframe/regression_test.zig) loads both and asserts teddy's typed
accessors reproduce the same canonical per cell.

The golden's top-level "transforms" object (slice 9.1) carries, per NUMERIC
column, sum/mean/min/max computed with pyarrow.compute over the non-null values
(all stored as JSON numbers — teddy aggregations return f64), plus an ascending
sort_indices ("sort_asc_indices") on c_i32 (default null_placement = nulls
LAST). The Zig harness compares these and the format round-trips; see the
"transforms"/"round-trips" notes in validation/README.md.

Run from the repo root:  python3 validation/regression.py

Outputs:
  data/validation/alltypes.parquet      one column per teddy type, with nulls
  data/validation/alltypes.golden.json  pyarrow ground truth (semantic + type)
  data/validation/int96.parquet         tiny INT96 fixture (bonus)
  data/validation/int96.golden.json     its golden

Semantic protocol (must stay in lockstep with teddySemantic() in the harness):
  bool            -> "0"/"1"
  int/uint        -> decimal string
  f16/f32/f64     -> JSON number (compared with tolerance)
  String          -> raw UTF-8 string
  Binary/Fixed/Uuid -> lowercase hex of the bytes
  Date            -> str(days since epoch)
  Time            -> str(nanos since midnight)
  Timestamp       -> "<epoch_nanos>:<utc 0|1>"
  Decimal         -> "<unscaled_int>:<scale>"
  Nested          -> JSON value: lists as arrays, structs as POSITIONAL arrays,
                     maps as arrays of [key_v, value_v] pairs; recursive.
  null            -> {"null": true}
"""

import json
from decimal import Decimal as PyDecimal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

OUT_PARQUET = "data/validation/alltypes.parquet"
OUT_GOLDEN = "data/validation/alltypes.golden.json"

NROWS = 6  # row 1 (index 1) is the canonical null row across nullable columns


# ---------------------------------------------------------------------------
# Per-arrow-type semantic encoders. Each returns the canonical for ONE non-null
# cell value (the as_py() / scalar value), given the arrow type for context.
# ---------------------------------------------------------------------------

_UNIT_TO_NS = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


def _hex(b: bytes) -> str:
    return b.hex()


def _decimal_canonical(value: PyDecimal, scale: int) -> str:
    # Unscaled mantissa = value * 10**scale, computed EXACTLY from the Decimal
    # tuple. (scaleb/to_integral_value go through the active decimal context,
    # which defaults to 28 significant digits and silently rounds 38-digit
    # values — so we never use them here.)
    sign, digits, exponent = value.as_tuple()
    mantissa = int(("-" if sign else "") + "".join(str(d) for d in digits))
    shift = exponent + scale  # value = mantissa * 10**exponent; unscaled = value * 10**scale
    if shift >= 0:
        unscaled = mantissa * (10 ** shift)
    else:
        # value has more fractional digits than `scale` — would lose data.
        q, r = divmod(mantissa, 10 ** (-shift))
        if r != 0:
            raise ValueError(f"decimal {value} does not fit scale {scale} exactly")
        unscaled = q
    return f"{unscaled}:{scale}"


def _timestamp_canonical(raw_value: int, unit: str, utc: bool) -> str:
    epoch_nanos = raw_value * _UNIT_TO_NS[unit]
    return f"{epoch_nanos}:{1 if utc else 0}"


def encode_cell(arrow_type, scalar):
    """Encode a single pyarrow scalar to its semantic JSON cell.

    Returns either {"null": true} or {"v": <semantic>} where <semantic> is a
    string for most types and a JSON number for floats / a JSON value for
    Nested.
    """
    if not scalar.is_valid:
        return {"null": True}

    t = arrow_type
    # Temporal types: derive from the underlying integer (as_py() can raise on
    # ns timestamps that overflow datetime, and date/time as_py() lose the raw
    # epoch unit we want).
    if pa.types.is_date32(t):
        # date32 underlying int32 IS days since epoch.
        return {"v": str(scalar.cast(pa.int32()).as_py())}
    if pa.types.is_time(t):
        unit = t.unit  # "s"/"ms"/"us"/"ns"
        int_t = pa.int32() if unit in ("s", "ms") else pa.int64()
        raw = scalar.cast(int_t).as_py()
        return {"v": str(raw * _UNIT_TO_NS[unit])}
    if pa.types.is_timestamp(t):
        raw = scalar.cast(pa.int64()).as_py()
        utc = t.tz is not None
        return {"v": _timestamp_canonical(raw, t.unit, utc)}

    v = scalar.as_py()
    if pa.types.is_boolean(t):
        return {"v": "1" if v else "0"}
    if pa.types.is_integer(t):
        return {"v": str(v)}
    if pa.types.is_floating(t):
        # f16/f32/f64 -> JSON number (compared with tolerance on the Zig side).
        return {"v": float(v)}
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return {"v": v}
    if pa.types.is_binary(t) or pa.types.is_fixed_size_binary(t):
        return {"v": _hex(v)}
    if pa.types.is_decimal(t):
        return {"v": _decimal_canonical(v, t.scale)}
    if _is_uuid(t):
        # as_py() -> uuid.UUID; .bytes is the 16-byte big-endian layout.
        return {"v": _hex(v.bytes)}
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        elem_t = t.value_type
        return {"v": [_encode_nested(elem_t, e) for e in v]}
    if pa.types.is_struct(t):
        # POSITIONAL: encode each field in declared order (matches teddy).
        return {"v": [_encode_nested(t.field(i).type, v[t.field(i).name]) for i in range(t.num_fields)]}
    if pa.types.is_map(t):
        kt, vt = t.key_type, t.item_type
        return {"v": [[_encode_nested(kt, k), _encode_nested(vt, val)] for (k, val) in v]}
    raise ValueError(f"no encoder for arrow type {t}")


def _is_uuid(t) -> bool:
    # pyarrow exposes uuid as an extension type; guard for older builds.
    try:
        return isinstance(t, pa.UuidType)
    except AttributeError:
        return False


def _encode_nested(elem_type, py_value):
    """Encode a nested element from its python value (as produced by to_pylist).

    Returns None for nulls (JSON null inside the array), matching teddy's
    `.null_` arm which the harness renders as JSON null.
    """
    if py_value is None:
        return None
    t = elem_type
    if pa.types.is_boolean(t):
        return "1" if py_value else "0"
    if pa.types.is_integer(t):
        return str(py_value)
    if pa.types.is_floating(t):
        return float(py_value)
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return py_value
    if pa.types.is_binary(t) or pa.types.is_fixed_size_binary(t):
        return _hex(py_value)
    if pa.types.is_decimal(t):
        return _decimal_canonical(py_value, t.scale)
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return [_encode_nested(t.value_type, e) for e in py_value]
    if pa.types.is_struct(t):
        return [_encode_nested(t.field(i).type, py_value[t.field(i).name]) for i in range(t.num_fields)]
    if pa.types.is_map(t):
        return [[_encode_nested(t.key_type, k), _encode_nested(t.item_type, val)] for (k, val) in py_value]
    raise ValueError(f"no nested encoder for arrow type {t}")


# ---------------------------------------------------------------------------
# Column specs: (name, arrow_array, expected_teddy_type)
# ---------------------------------------------------------------------------

def build_columns():
    struct_ab = pa.struct([("a", pa.int64()), ("b", pa.string())])
    struct_x = pa.struct([("x", pa.int64())])

    cols = [
        ("c_bool", pa.array([True, None, False, True, False, True], type=pa.bool_()), "bool"),
        ("c_i8", pa.array([1, None, -128, 127, 0, -1], type=pa.int8()), "i8"),
        ("c_i16", pa.array([1, None, -32768, 32767, 0, -1], type=pa.int16()), "i16"),
        ("c_i32", pa.array([1, None, -2147483648, 2147483647, 0, -1], type=pa.int32()), "i32"),
        ("c_i64", pa.array([42, None, -9223372036854775808, 9223372036854775807, 0, -1], type=pa.int64()), "i64"),
        ("c_u8", pa.array([1, None, 0, 255, 128, 7], type=pa.uint8()), "u8"),
        ("c_u16", pa.array([1, None, 0, 65535, 256, 7], type=pa.uint16()), "u16"),
        ("c_u32", pa.array([1, None, 0, 4294967295, 4000000000, 7], type=pa.uint32()), "u32"),
        ("c_u64", pa.array([1, None, 0, 18446744073709551615, 18000000000000000000, 7], type=pa.uint64()), "u64"),
        ("c_f32", pa.array([1.5, None, -0.25, 3.5, 0.0, 100.125], type=pa.float32()), "f32"),
        ("c_f64", pa.array([1.5, None, -0.25, 3.140625, 0.0, 1e10], type=pa.float64()), "f64"),
        ("c_f16", pa.array([1.5, None, -0.25, 0.5, 0.0, 2.0], type=pa.float16()), "f16"),
        ("c_str", pa.array(["alpha", None, "", "héllo", "tab\there", "z"], type=pa.string()), "String"),
        ("c_binary", pa.array([b"\x00\x01\xff", None, b"", b"\xde\xad\xbe\xef", b"\xff\x00", b"ab"], type=pa.binary()), "Binary"),
        ("c_fixed", pa.array([b"abcd", None, b"\x00\x00\x00\x00", b"\xff\xff\xff\xff", b"wxyz", b"0123"], type=pa.binary(4)), "FixedBytes"),
        ("c_date", pa.array([0, None, 1, -1, 18262, 19000], type=pa.int32()).cast(pa.date32()), "Date"),
        ("c_time32", pa.array([1, None, 0, 1000, 86399999, 3600000], type=pa.int32()).cast(pa.time32("ms")), "Time"),
        ("c_time64", pa.array([1, None, 0, 1000, 86399999999, 3600000000], type=pa.int64()).cast(pa.time64("us")), "Time"),
        ("c_ts_utc", pa.array([1, None, 0, -1, 1600000000000000, 1700000000000000], type=pa.int64()).cast(pa.timestamp("us", tz="UTC")), "Timestamp"),
        ("c_ts_naive", pa.array([1, None, 0, -1, 1600000000000, 1700000000000], type=pa.int64()).cast(pa.timestamp("ms")), "Timestamp"),
        ("c_ts_ns", pa.array([1, None, 0, -1, 1600000000000000000, 1700000000000000000], type=pa.int64()).cast(pa.timestamp("ns", tz="UTC")), "Timestamp"),
        ("c_dec9", pa.array([PyDecimal("1234567.89"), None, PyDecimal("-0.01"), PyDecimal("0.00"), PyDecimal("9999999.99"), PyDecimal("-9999999.99")], type=pa.decimal128(9, 2)), "Decimal"),
        ("c_dec38", pa.array([PyDecimal("1234567890123456789012345678.0123456789"), None, PyDecimal("-0.0000000001"), PyDecimal("0E-10"), PyDecimal("1.0000000000"), PyDecimal("-1.0000000000")], type=pa.decimal128(38, 10)), "Decimal"),
        ("c_uuid", pa.array([
            b"\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef",
            None,
            b"\x00" * 16,
            b"\xff" * 16,
            b"\x11" * 16,
            b"\xaa" * 16,
        ], type=pa.uuid()), "Uuid"),
        ("c_list", pa.array([[1, 2], None, [], [3], [4, 5, 6], [None, 7]], type=pa.list_(pa.int64())), "Nested"),
        ("c_struct", pa.array([{"a": 1, "b": "x"}, None, {"a": 2, "b": None}, {"a": -3, "b": "z"}, {"a": 0, "b": ""}, {"a": 9, "b": "q"}], type=struct_ab), "Nested"),
        ("c_map", pa.array([[("a", 1), ("b", 2)], None, [], [("c", None)], [("d", 4)], [("e", 5), ("f", 6)]], type=pa.map_(pa.string(), pa.int64())), "Nested"),
        ("c_list_struct", pa.array([[{"x": 1}, {"x": 2}], None, [], [{"x": 3}], [{"x": None}], [{"x": 9}]], type=pa.list_(struct_x)), "Nested"),
        ("c_list_list", pa.array([[[1], [2, 3]], None, [[]], [[4]], [None, [5]], [[6, 7]]], type=pa.list_(pa.list_(pa.int64()))), "Nested"),
    ]
    return cols


# ---------------------------------------------------------------------------
# Stage 2 — transforms (pyarrow.compute reference).
#
# For each NUMERIC column we compute sum/mean/min/max over the NON-NULL values.
# teddy's BoxedSeries.sum/mean/min/max all return ?f64, so we store all four as
# JSON numbers (floats) and the Zig side compares with relative tolerance.
# NOTE: pyarrow's pc.sum on an int column returns an int (possibly widened);
# we coerce to float here so both sides speak f64. Teddy aggregations are f64.
#
# We also pick ONE column with a null (c_i32) and store the ascending
# sort_indices pyarrow produces. By default pyarrow's pc.sort_indices places
# NULLS LAST (null_placement="at_end"). As of Phase 9.2 teddy's argSort matches
# this convention (nulls sort last in BOTH directions), so the harness now
# EXPECTS sort PARITY — any SORT divergence is a regression.
# ---------------------------------------------------------------------------

# teddy treats these column names as numeric (int/uint/float).
_NUMERIC_COLS = {
    "c_i8", "c_i16", "c_i32", "c_i64",
    "c_u8", "c_u16", "c_u32", "c_u64",
    "c_f32", "c_f64", "c_f16",
}

_SORT_COL = "c_i32"  # has a null at row index 1 -> exercises null ordering


def build_transforms(cols):
    transforms = {}
    for name, arr, _expected in cols:
        if name not in _NUMERIC_COLS:
            continue
        try:
            if pa.types.is_float16(arr.type):
                # pyarrow.compute has no aggregation kernel for halffloat, so
                # compute over the (already widened to python float) non-null
                # values. teddy widens f16 -> f64 the same way for aggregations.
                vals = [v for v in arr.to_pylist() if v is not None]
                entry = {
                    "sum": float(sum(vals)),
                    "mean": float(sum(vals) / len(vals)),
                    "min": float(min(vals)),
                    "max": float(max(vals)),
                }
            else:
                entry = {
                    "sum": float(pc.sum(arr).as_py()),
                    "mean": float(pc.mean(arr).as_py()),
                    "min": float(pc.min(arr).as_py()),
                    "max": float(pc.max(arr).as_py()),
                }
        except Exception as exc:  # noqa: BLE001 — note + skip per spec
            print(f"  (transforms SKIP {name}: {exc})")
            continue

        if name == _SORT_COL:
            # default null_placement="at_end" -> nulls sort LAST.
            entry["sort_asc_indices"] = [
                int(i) for i in pc.sort_indices(arr, null_placement="at_end").to_pylist()
            ]
        transforms[name] = entry
    return transforms


def build_golden(cols):
    columns_json = []
    for name, arr, expected in cols:
        cells = [encode_cell(arr.type, arr[i]) for i in range(len(arr))]
        columns_json.append({
            "name": name,
            "arrow_type": str(arr.type),
            "expected_teddy_type": expected,
            "cells": cells,
        })
    return {
        "fixture": OUT_PARQUET,
        "num_rows": NROWS,
        "columns": columns_json,
        "transforms": build_transforms(cols),
    }


def write_int96_bonus():
    """Tiny INT96 fixture + golden (INT96 can't sit alongside others in one
    table because use_deprecated_int96_timestamps applies file-wide)."""
    import datetime as dt
    vals = [
        dt.datetime(2021, 1, 1, 0, 0, 0),
        dt.datetime(2021, 6, 15, 12, 30, 0),
        dt.datetime(2022, 3, 3, 3, 3, 3),
    ]
    arr = pa.array(vals, type=pa.timestamp("ns"))
    tbl = pa.table({"c_ts_int96": arr})
    pq.write_table(tbl, "data/validation/int96.parquet",
                   use_deprecated_int96_timestamps=True, compression=None)
    # INT96 reads back as Timestamp origin=int96, unit=nanos, utc=false.
    # The semantic canonical is "<epoch_nanos>:0" (teddy treats INT96 as non-utc).
    cells = []
    raw = arr.cast(pa.int64())  # ns since epoch
    for i in range(len(arr)):
        cells.append({"v": f"{raw[i].as_py()}:0"})
    golden = {
        "fixture": "data/validation/int96.parquet",
        "num_rows": len(arr),
        "columns": [{
            "name": "c_ts_int96",
            "arrow_type": "timestamp[ns] (INT96 on wire)",
            "expected_teddy_type": "Timestamp",
            "cells": cells,
        }],
        "transforms": {},
    }
    with open("data/validation/int96.golden.json", "w") as f:
        json.dump(golden, f, indent=2, sort_keys=False)
        f.write("\n")
    print("data/validation/int96.parquet + int96.golden.json: INT96, 3 rows")


def main():
    cols = build_columns()
    tbl = pa.table({name: arr for (name, arr, _exp) in cols})
    pq.write_table(tbl, OUT_PARQUET, compression=None)

    golden = build_golden(cols)
    with open(OUT_GOLDEN, "w") as f:
        json.dump(golden, f, indent=2, sort_keys=False)
        f.write("\n")

    print(f"{OUT_PARQUET}: {len(cols)} columns, {NROWS} rows")
    for name, arr, expected in cols:
        print(f"  {name:<14} {str(arr.type):<28} -> {expected}")
    print(f"{OUT_GOLDEN}: golden manifest written")

    write_int96_bonus()


if __name__ == "__main__":
    main()
