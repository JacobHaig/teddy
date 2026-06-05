"""Generate Parquet test fixtures used by the Zig test suite.

Requires pyarrow:  python3 -m pip install pyarrow
Run from the repo root:  python3 src_py/gen_fixtures.py

Regenerates the pyarrow-generated fixtures under data/. Note that
data/addresses.parquet and data/addresses_snappy.parquet are externally
sourced (parquet-cpp-arrow) and have NO generator here — do not delete them.
(Phase 9 will fold this into a proper Python<->Zig regression framework.)
"""

import datetime as dt
from decimal import Decimal as PyDecimal

import pyarrow as pa
import pyarrow.parquet as pq


def multi_rowgroup():
    # 7 rows written in row groups of 3 -> groups of sizes 3, 3, 1.
    # Exercises the reader's cross-row-group concatenation for numeric, double,
    # string (owned byte slices), and nullable (validity) columns.
    tbl = pa.table({
        "id":    pa.array([1, 2, 3, 4, 5, 6, 7], type=pa.int64()),
        "price": pa.array([1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5], type=pa.float64()),
        "name":  pa.array(["a", "bb", "ccc", "dddd", "e", "ff", "ggg"], type=pa.string()),
        "opt":   pa.array([10, None, 30, None, 50, 60, None], type=pa.int64()),
    })
    # compression=None mirrors the existing addresses.parquet fixture.
    pq.write_table(tbl, "data/multi_rowgroup.parquet", row_group_size=3, compression=None)
    pf = pq.ParquetFile("data/multi_rowgroup.parquet")
    print(f"data/multi_rowgroup.parquet: {pf.num_row_groups} row groups, {pf.metadata.num_rows} rows")


def fixed_len_byte_array():
    # FIXED_LEN_BYTE_ARRAY via fixed-size binary(4). Exercises the reader's
    # type_length-driven decode (raw bytes preserved; semantic typing is later).
    tbl = pa.table({"fb": pa.array([b"abcd", b"efgh", b"ijkl"], type=pa.binary(4))})
    pq.write_table(tbl, "data/flba.parquet", compression=None)
    print("data/flba.parquet: FIXED_LEN_BYTE_ARRAY(4), 3 rows")


def int96_timestamps():
    # Deprecated INT96 timestamps (Impala/Spark legacy). Reader preserves the
    # raw 12 bytes; decoding to a timestamp is deferred to the logical-type layer.
    tbl = pa.table({"t": pa.array(
        [dt.datetime(2021, 1, 1, 0, 0, 0),
         dt.datetime(2021, 6, 15, 12, 30, 0),
         dt.datetime(2022, 3, 3, 3, 3, 3)],
        type=pa.timestamp("ns"))})
    pq.write_table(tbl, "data/int96.parquet", use_deprecated_int96_timestamps=True, compression=None)
    print("data/int96.parquet: INT96, 3 rows")


def unsigned_ints():
    # Unsigned ints whose values exceed the signed range of their physical type
    # (UINT_32 over INT32, UINT_64 over INT64) — exercises bit-reinterpretation.
    tbl = pa.table({
        "u32": pa.array([1, 2, 4000000000], type=pa.uint32()),
        "u64": pa.array([1, 2, 18000000000000000000], type=pa.uint64()),
    })
    pq.write_table(tbl, "data/unsigned.parquet", compression=None)
    print("data/unsigned.parquet: UINT_32 + UINT_64, 3 rows")


def logical_annotations():
    # Modern LogicalType annotations (SchemaElement field 10) on scalar columns.
    # 6d-2a.0 only asserts the footer parses these; value-level decode lands in
    # slices 6d-2a.1-.5. pyarrow also writes the legacy converted_type alongside.
    tbl = pa.table({
        "d":   pa.array([dt.date(2020, 1, 1), dt.date(2021, 6, 15)], type=pa.date32()),
        # time without tz -> is_adjusted_to_utc=false
        "t":   pa.array([dt.time(1, 2, 3), dt.time(4, 5, 6)], type=pa.time64("us")),
        "ts":  pa.array([dt.datetime(2020, 1, 1, 12, 0, 0),
                         dt.datetime(2021, 6, 15, 8, 30, 0)], type=pa.timestamp("us", tz="UTC")),
        "dec": pa.array([PyDecimal("12345678.90"), PyDecimal("-0.01")], type=pa.decimal128(10, 2)),
    })
    pq.write_table(tbl, "data/logical_annotations.parquet", compression=None)
    print("data/logical_annotations.parquet: DATE/TIME/TIMESTAMP/DECIMAL logical types, 2 rows")


def time_units():
    # Unit/utc breadth for TIME/TIMESTAMP resolution: time32(ms) is INT32-backed;
    # timestamp without tz -> isAdjustedToUTC=false.
    tbl = pa.table({
        "t_ms":     pa.array([dt.time(1, 2, 3), dt.time(4, 5, 6)], type=pa.time32("ms")),
        "ts_ms":    pa.array([dt.datetime(2020, 1, 1, 12, 0, 0)] * 2, type=pa.timestamp("ms", tz="UTC")),
        "ts_ns":    pa.array([dt.datetime(2021, 6, 15, 8, 30, 0)] * 2, type=pa.timestamp("ns", tz="UTC")),
        "ts_local": pa.array([dt.datetime(2022, 3, 3, 3, 3, 3)] * 2, type=pa.timestamp("us")),
    })
    pq.write_table(tbl, "data/time_units.parquet", compression=None)
    print("data/time_units.parquet: TIME(ms)/TIMESTAMP(ms,ns,local us), 2 rows")


if __name__ == "__main__":
    multi_rowgroup()
    fixed_len_byte_array()
    int96_timestamps()
    unsigned_ints()
    logical_annotations()
    time_units()
