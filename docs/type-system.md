# Teddy Type System

## Type Erasure Pattern

Teddy uses compile-time generics wrapped in runtime unions to support multiple column types efficiently.

```mermaid
flowchart TB
    subgraph Generic["Compile-Time Generic"]
        S_bool["Series(bool)"]
        S_i8["Series(i8)"]
        S_i16["Series(i16)"]
        S_i32["Series(i32)"]
        S_i64["Series(i64)"]
        S_f32["Series(f32)"]
        S_f64["Series(f64)"]
        S_str["Series(String)"]
        S_u8["Series(u8)"]
        S_u16["Series(u16)"]
        S_u32["Series(u32)"]
        S_u64["Series(u64)"]
    end

    subgraph Boxed["Runtime Union (BoxedSeries)"]
        BS["BoxedSeries
        ────────────
        .bool
        .int8 / .int16 / .int32 / .int64
        .uint8 / .uint16 / .uint32 / .uint64
        .float32 / .float64
        .string
        .int128 / .uint128 / .usize"]
    end

    subgraph DF["DataFrame"]
        COLS["ArrayList&lt;BoxedSeries&gt;
        ────────────
        [col0] [col1] [col2] ..."]
    end

    S_bool --> BS
    S_i32 --> BS
    S_i64 --> BS
    S_f64 --> BS
    S_str --> BS
    S_u8 --> BS
    BS --> COLS
```

## Series(T) Internal Structure

```mermaid
classDiagram
    class `Series~T~` {
        +allocator: Allocator
        +values: ArrayList~T~
        +name: String
        +validity: ?ArrayList~bool~
        --
        +append(value: T)
        +appendNull()
        +len() usize
        +rename(name)
        +deepCopy() *Series~T~
        +filterByIndices(indices) *Series~T~
        +argSort(ascending) ArrayList~usize~
        +uniqueIndices() ArrayList~usize~
        +seriesSum() T
        +seriesMean() f64
        +seriesStdDev() f64
        +minVal() ?T
        +maxVal() ?T
        +toBoxedSeries() BoxedSeries
    }

    class String {
        +data: ArrayList~u8~
        +allocator: Allocator
        --
        +fromSlice(str) String
        +toSlice() []const u8
        +append(char)
        +appendSlice(str)
        +eql(other) bool
        +clone() String
        +len() usize
    }

    `Series~T~` --> String : name field
    `Series~T~` --> String : when T = String
```

## Supported Type Mappings

```
┌──────────────┬───────────────┬───────────────┬────────────────┐
│  Zig Type    │  BoxedSeries  │  Parquet      │  Display Name  │
├──────────────┼───────────────┼───────────────┼────────────────┤
│  bool        │  .bool        │  BOOLEAN      │  Bool          │
│  i8          │  .int8        │  INT32+int_8  │  Int8          │
│  i16         │  .int16       │  INT32+int_16 │  Int16         │
│  i32         │  .int32       │  INT32        │  Int32         │
│  i64         │  .int64       │  INT64        │  Int64         │
│  u8          │  .uint8       │  INT32+uint_8 │  UInt8         │
│  u16         │  .uint16      │  INT32+uint16 │  UInt16        │
│  u32         │  .uint32      │  -            │  UInt32        │
│  u64         │  .uint64      │  -            │  UInt64        │
│  f32         │  .float32     │  FLOAT        │  Float32       │
│  f64         │  .float64     │  DOUBLE       │  Float64       │
│  String      │  .string      │  BYTE_ARRAY   │  String        │
└──────────────┴───────────────┴───────────────┴────────────────┘
```
