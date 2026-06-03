"""Generate Parquet test fixtures used by the Zig test suite.

Requires pyarrow:  python3 -m pip install pyarrow
Run from the repo root:  python3 src_py/gen_fixtures.py

(Phase 9 will fold this into a proper Python<->Zig regression framework; for now
it just regenerates the committed fixtures under data/.)
"""

import datetime as dt

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


if __name__ == "__main__":
    multi_rowgroup()
    fixed_len_byte_array()
    int96_timestamps()
