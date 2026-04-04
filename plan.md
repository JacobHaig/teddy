# Teddy — Feature Roadmap

Comparison against pandas and polars to identify high-value missing features,
ordered by priority (highest impact / lowest complexity first).

---

## What We Have Today

- DataFrame with typed columns (all numeric types, bool, String)
- Series with aggregations: sum, min, max, mean, std
- Filter, sort, select, head, tail, slice, limit
- GroupBy with count, sum, mean, min, max, stdDev
- Joins: inner, left, right, outer
- Concat (vertical), unique, valueCounts
- describe(), applyInplace, applyNew
- CSV read/write, JSON read/write, Parquet read/write
- Null tracking via validity bitmap
- Missing value operations: dropNulls, dropNullsAny, fillNull, fillNullForward, fillNullBackward (Series + DataFrame)
- Type casting: castSafe / cast / castLossy (Series, BoxedSeries, DataFrame)
- Cumulative ops: cumSum, cumMin, cumMax, cumProd (Series + DataFrame)
- Shift / diff / diffLossy (Series + DataFrame)
- Additional aggregations: median, quantile, nunique, prod/prodChecked, sumChecked, first, last (Series + BoxedSeries + GroupBy)
- Clip, replace, replaceSlice (Series + DataFrame)

---

## Priority 1 — Core Gaps (High Impact, Relatively Contained)

### 1.1 Missing Value Operations ✓ COMPLETE
pandas: `fillna`, `dropna`, `isna`, `notna`
polars: `fill_null`, `drop_nulls`, `is_null`, `is_not_null`

- `df.dropNulls(column)` ✓
- `df.dropNullsAny()` ✓
- `df.fillNull(column, T, value)` ✓
- `series.fillNull(value)` ✓
- `series.fillNullForward()` ✓
- `series.fillNullBackward()` ✓
- `series.dropNulls()` ✓
- `series.isNull(i)`, `series.nullCount()`, `series.hasNulls()`, `series.appendNull()` ✓
- `boxed.isNull(i)`, `boxed.nullCount()`, `boxed.hasNulls()`, `boxed.appendNull()` ✓

---

### 1.2 Type Casting ✓ COMPLETE
pandas: `series.astype(dtype)`
polars: `series.cast(dtype)`

Three tiers matching Zig's own philosophy — see **API Design: Strictness Levels** below.

- `series.castSafe(Target)` ✓ — comptime-verified lossless widening; compile error if unsafe
- `series.cast(Target)` ✓ — strict runtime; errors on overflow / fractional float→int / parse failure
- `series.castLossy(Target)` ✓ — permissive; failures become null, float→int truncates
- Same three tiers exposed on BoxedSeries and DataFrame (`df.castSafe/cast/castLossy(col, Target)`)
- Numeric ↔ numeric, numeric ↔ String, bool ↔ numeric all supported

---

### 1.3 Cumulative Operations ✓ COMPLETE
pandas: `cumsum`, `cumprod`, `cummin`, `cummax`
polars: `cum_sum`, `cum_prod`, `cum_min`, `cum_max`

- `series.cumSum()` / `cumMin()` / `cumMax()` / `cumProd()` ✓ — nulls propagate
- `df.cumSum/cumMin/cumMax/cumProd(column)` ✓ — returns new DataFrame
- BoxedSeries dispatch for all four ✓

---

### 1.4 Shift / Diff ✓ COMPLETE
pandas: `series.shift(n)`, `series.diff(n)`
polars: `series.shift(n)`, `series.diff(n)`

- `series.shift(n)` ✓ — positive shifts down (prepends nulls), negative shifts up; all column types
- `series.diff(n)` ✓ — strict: underflow on unsigned → error.Underflow
- `series.diffLossy(n)` ✓ — underflow → null (strictness-level convention)
- `df.shift/diff/diffLossy(column, n)` ✓ — DataFrame-level wrappers

---

### 1.5 Additional Aggregations ✓ COMPLETE
pandas: `median`, `quantile`, `var`, `count`, `nunique`, `prod`
polars: same + `first`, `last`

- `series.median(alloc)` ✓ / `series.quantile(alloc, q)` ✓ — returns `!?f64`
- `series.nunique(alloc)` ✓ — distinct non-null count
- `series.prod()` / `series.prodChecked()` ✓ — wrapping and overflow-checked
- `series.sumChecked()` ✓ — overflow-checked sum
- `series.first()` / `series.last()` ✓ — first/last non-null value
- GroupBy: `median`, `nunique`, `prod`, `first`, `last` ✓
- BoxedSeries: `prod`, `first`, `last`, `median`, `quantile`, `nunique`, `sumChecked` ✓
- `describe()` null bug fixed ✓ — count now reflects non-null rows; min/max show null for all-null columns

---

### 1.6 Clip ✓ COMPLETE
pandas: `series.clip(lower, upper)`
polars: `series.clip(lower, upper)`

- `series.clip(lower, upper)` ✓ — nulls preserved; numeric only
- `df.clip(column, T, lower, upper)` ✓

---

### 1.7 Replace / Map Values ✓ COMPLETE
pandas: `series.replace(old, new)`, `series.map(dict)`
polars: `series.replace(old, new)`

- `series.replace(old_value, new_value)` ✓ — nulls never matched; String compared by content
- `series.replaceSlice(pairs)` ✓ — multiple replacements, first match wins
- `df.replace(column, T, old, new)` ✓

---

## Priority 2 — Reshaping & Advanced Selection

### 2.1 Horizontal Concatenation
pandas: `pd.concat([df1, df2], axis=1)`
polars: `pl.concat([df1, df2], how="horizontal")`

`concat` currently only does vertical stacking. Horizontal concat (adding
columns side by side) is critical for feature engineering.

- `df.hconcat(other)` — join columns from two dataframes of equal height
- Validate heights match; error otherwise

---

### 2.2 Column Expression / withColumn
pandas: `df["new_col"] = df["a"] + df["b"]`
polars: `df.with_column(pl.col("a") + pl.col("b"))`

No way to create a new column derived from arithmetic between existing columns.

- `df.withColumn(name, column_a, column_b, func)` — apply binary function across two columns
- `df.withColumnScalar(name, source_column, scalar, func)` — apply scalar operation
- This unlocks: `price * quantity`, `(a - b) / c`, etc.

---

### 2.3 Melt (Unpivot)
pandas: `df.melt(id_vars, value_vars)`
polars: `df.melt(id_vars, value_vars)`

Converts wide format to long format. Common for analytics and plotting.

- `df.melt(id_vars[], value_vars[])` — fold value columns into rows with
  a "variable" column and a "value" column

---

### 2.4 Pivot
pandas: `df.pivot(index, columns, values)`
polars: `df.pivot(values, index, columns)`

Converts long format to wide format (inverse of melt).

- `df.pivot(index_col, column_col, value_col)` — spread unique values of
  `column_col` into separate columns

---

### 2.5 Sample
pandas: `df.sample(n)`, `df.sample(frac=0.1)`
polars: `df.sample(n)`, `df.sample(fraction=0.1)`

Random row sampling — essential for ML workflows.

- `df.sample(n, seed)` — sample n rows without replacement
- `df.sampleFraction(frac, seed)` — sample a fraction of rows

---

### 2.6 Rank
pandas: `series.rank(method)`
polars: `series.rank(method)`

Assign rank to each value. Used in statistical analysis and competition scoring.

- `series.rank()` — dense ranking by default
- Options: dense, ordinal, min, max, average methods

---

## Priority 3 — String Operations

pandas has `.str` accessor; polars has `pl.col("x").str.*`.
Currently teddy has no string transformation operations on String columns.

### 3.1 String Accessor Methods
- `series.strToUpper()` / `series.strToLower()`
- `series.strTrim()` / `series.strTrimStart()` / `series.strTrimEnd()`
- `series.strContains(substr)` → `Series(bool)`
- `series.strStartsWith(prefix)` → `Series(bool)`
- `series.strEndsWith(suffix)` → `Series(bool)`
- `series.strReplace(old, new)` → `Series(String)`
- `series.strLen()` → `Series(usize)` — character count per element
- `series.strSplit(delimiter)` — split into list (requires list column type)
- `series.strSlice(start, end)` → `Series(String)` — substring extraction

These should be grouped under a `StringSeries` wrapper or namespace so the
API doesn't pollute the generic `Series(T)`.

---

## Priority 4 — Window / Rolling Operations

pandas: `series.rolling(n).mean()`, `.rolling(n).sum()`, etc.
polars: `series.rolling_mean(n)`, `series.rolling_sum(n)`, etc.

Critical for time-series smoothing and financial analysis.

- `series.rollingMean(window)` — sliding window mean
- `series.rollingSum(window)` — sliding window sum
- `series.rollingMin(window)` / `series.rollingMax(window)`
- `series.rollingStd(window)` — sliding window std dev
- `series.ewm(alpha)` — exponentially weighted mean

---

## Priority 5 — Additional Join Types & Cross-Frame Ops

### 5.1 Semi / Anti Joins
polars: `how="semi"`, `how="anti"`

- `df.semiJoin(other, on)` — keep left rows that have a match in right (no columns added)
- `df.antiJoin(other, on)` — keep left rows that have NO match in right

### 5.2 Cross Join
pandas: `df.merge(other, how="cross")`
polars: `df.join(other, how="cross")`

- `df.crossJoin(other)` — cartesian product of all rows

### 5.3 Join on Multiple Keys
Currently join only supports a single `on` column name.

- `df.joinOn(other, on_keys[], join_type)` — join on multiple columns simultaneously

---

## Priority 6 — I/O Expansion

### 6.1 Parquet Multi-Row-Group (Known TODO)
The reader currently only processes the first row group. Large Parquet files
written by Spark or DuckDB often have many row groups.

- Complete the TODO in `parquet_reader.zig` to concatenate all row groups

### 6.2 NDJSON (Newline-Delimited JSON)
Common format from APIs and log pipelines.

- `parse` variant that reads one JSON object per line

### 6.3 Fixed-Width / Whitespace-Delimited
Common in scientific data.

- Reader option: `delimiter = .whitespace` — split on any run of whitespace

### 6.4 Write to File Convenience
Currently `Writer.save()` exists but requires builder setup. A direct shorthand:

- `df.toCsvFile(path, options)` 
- `df.toJsonFile(path, format)`
- `df.toParquetFile(path, options)`

---

## Priority 7 — Correlation & Statistical Analysis

pandas: `df.corr()`, `df.cov()`, `series.corr(other)`
polars: `df.pearson_corr()`, `series.pearson_corr(other)`

- `series.corr(other)` — Pearson correlation coefficient between two series
- `series.cov(other)` — covariance
- `df.corr()` — correlation matrix of all numeric columns (returns a new DataFrame)

---

## Priority 8 — Lazy / Query Optimization (Longer Term)

polars' key differentiator is its lazy API that optimizes query plans before
execution (predicate pushdown, projection pushdown, etc.).

This is a significant architectural investment but enables:
- Queries that compose without materializing intermediate dataframes
- Automatic parallelism
- Memory efficiency on large datasets

Sketch of API:
```
var lf = df.lazy();
var result = try lf
    .filter("age", .gt, 18)
    .select(&.{ "name", "age", "score" })
    .sort("score", false)
    .collect(allocator);
```

This would require a query plan IR and an execution engine — treat as a
separate milestone once the eager API is more complete.

---

## Priority 9 — DateTime Support

pandas: `pd.to_datetime()`, `.dt` accessor
polars: `pl.Date`, `pl.Datetime`, `.dt` accessor

Currently no date/time type exists. Required for time-series work.

- Add `Date` (days since epoch) and `Datetime` (microseconds since epoch) types
- Parsing from strings: `Series.strToDate(format)`
- `.dt` accessor: `year()`, `month()`, `day()`, `hour()`, `minute()`, `second()`
- Date arithmetic: `date_a - date_b` → duration in days
- Resampling / truncation: `dt.truncate(.day)`, `.truncate(.month)`

---

## API Design Principles to Maintain

1. **Caller owns all returned pointers** — consistent `deinit` contract
2. **Comptime types where possible** — keep type safety at the call site
3. **No hidden allocations** — every allocation-producing call takes an allocator
4. **Return `*Dataframe` from transformations** — enables chaining via temp vars
5. **Errors over panics** — return `!T` rather than asserting

---

## API Design: Strictness Levels

For any operation that could silently lose data or produce surprising output, offer
three tiers. Apply this consistently to new features going forward.

| Tier | When to use | Failure behavior |
|---|---|---|
| `*Safe` | Types are comptime-provably compatible (widening, lossless) | Compile error if not safe |
| `*` (plain) | Data *should* be representable; bad data is a bug | Returns `error.*` |
| `*Lossy` | Best-effort ETL; bad/unconvertible values should be skipped | Failures become `null` |

**Examples following this:**
- `castSafe` / `cast` / `castLossy` ✓
- `diff` / `diffLossy` ✓ (underflow on unsigned: strict errors, lossy nulls)
- `sum` / `sumChecked` ✓ (wrapping vs overflow-checked)
- `prod` / `prodChecked` ✓

**Apply to upcoming features where relevant:**
- **withColumn (2.2):** Column arithmetic overflow → strict / lossy variants.

The rule: if a function can silently discard or corrupt a value at runtime, it needs at
minimum a strict variant that fails loudly. The lossy variant is optional but recommended
whenever "skip bad rows" is a plausible use case.

---

## Summary Table

| Feature Area | pandas | polars | teddy today | Priority |
|---|---|---|---|---|
| Null fill/drop | ✓ | ✓ | ✓ | 1 |
| Type casting | ✓ | ✓ | ✓ | 1 |
| Cumulative ops | ✓ | ✓ | ✓ | 1 |
| Shift / diff | ✓ | ✓ | ✓ | 1 |
| Median / quantile | ✓ | ✓ | ✓ | 1 |
| Clip | ✓ | ✓ | ✓ | 1 |
| Replace | ✓ | ✓ | ✓ | 1 |
| Horizontal concat | ✓ | ✓ | ✗ | 2 |
| withColumn / derived cols | ✓ | ✓ | ✗ | 2 |
| Melt | ✓ | ✓ | ✗ | 2 |
| Pivot | ✓ | ✓ | ✗ | 2 |
| Sample | ✓ | ✓ | ✗ | 2 |
| Rank | ✓ | ✓ | ✗ | 2 |
| String operations | ✓ | ✓ | ✗ | 3 |
| Rolling / window | ✓ | ✓ | ✗ | 4 |
| Semi/anti joins | partial | ✓ | ✗ | 5 |
| Multi-key join | ✓ | ✓ | ✗ | 5 |
| Cross join | ✓ | ✓ | ✗ | 5 |
| NDJSON / fixed-width | ✓ | ✓ | ✗ | 6 |
| Multi-row-group Parquet | ✓ | ✓ | partial | 6 |
| Correlation / covariance | ✓ | ✓ | ✗ | 7 |
| Lazy evaluation | ✗ | ✓ | ✗ | 8 |
| DateTime type | ✓ | ✓ | ✗ | 9 |
