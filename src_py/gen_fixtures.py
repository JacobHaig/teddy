"""Generate Parquet test fixtures used by the Zig test suite.

Requires pyarrow:  python3 -m pip install pyarrow
Run from the repo root:  python3 src_py/gen_fixtures.py

(Phase 9 will fold this into a proper Python<->Zig regression framework; for now
it just regenerates the committed fixtures under data/.)
"""

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


if __name__ == "__main__":
    multi_rowgroup()
