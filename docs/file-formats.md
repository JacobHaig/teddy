# File Format Support

## Format Comparison

```mermaid
flowchart LR
    subgraph CSV["CSV"]
        direction TB
        CSV_R["csv_reader.zig
        ─────────────
        Scanner → Tokens
        Type inference
        Field parsing"]
        CSV_W["csv_writer.zig
        ─────────────
        RFC 4180
        Field quoting
        Escape handling"]
    end

    subgraph JSON["JSON"]
        direction TB
        JSON_R["json_reader.zig
        ─────────────
        Tokenizer
        Auto-detect format
        Type inference"]
        JSON_W["json_writer.zig
        ─────────────
        Row format
        Column format
        String escaping"]
    end

    subgraph Parquet["Parquet"]
        direction TB
        PQ_R["parquet_reader.zig
        ─────────────
        Thrift metadata
        Column chunks
        Dictionary pages
        Snappy decompress"]
        PQ_W["parquet_writer.zig
        ─────────────
        Thrift metadata
        PLAIN encoding
        Snappy compress
        Full file assembly"]
    end

    DF["DataFrame"] --> CSV_W
    DF --> JSON_W
    DF --> PQ_W
    CSV_R --> DF
    JSON_R --> DF
    PQ_R --> DF
```

## CSV Format

### Reading

```
Options:
  delimiter:  u8    (default: ',')
  has_header: bool  (default: true)
  skip_rows:  usize (default: 0)

Type Inference Pipeline:
  Raw bytes → Scanner → Tokens → Field values → Type detection → Series

Type Detection (per column):
  1. Try parse as integer → i64
  2. Try parse as float   → f64
  3. Fallback             → String
```

### Writing

```
Options:
  delimiter:      u8   (default: ',')
  include_header: bool (default: true)

Quoting Rules:
  - Fields containing delimiter, quote, or newline are quoted
  - Quotes inside fields are escaped: " → ""
```

### Example

```
Input CSV:                          DataFrame:
┌──────────────────────────┐        ┌──────┬─────┬────────┐
│ Name,Age,City            │   →    │ Name │ Age │ City   │
│ Alice,30,NYC             │        ├──────┼─────┼────────┤
│ Bob,25,LA                │        │Alice │  30 │ NYC    │
└──────────────────────────┘        │Bob   │  25 │ LA     │
                                    └──────┴─────┴────────┘
                                    String   i64   String
```

## JSON Format

### Two Layouts

```
ROW FORMAT (array of objects):        COLUMN FORMAT (object of arrays):
┌─────────────────────────────┐       ┌──────────────────────────────┐
│ [                           │       │ {                            │
│   {"Name":"Alice","Age":30},│       │   "Name": ["Alice", "Bob"],  │
│   {"Name":"Bob","Age":25}   │       │   "Age": [30, 25]            │
│ ]                           │       │ }                            │
└─────────────────────────────┘       └──────────────────────────────┘
        ↕                                       ↕
    ┌──────┬─────┐                        ┌──────┬─────┐
    │ Name │ Age │                        │ Name │ Age │
    ├──────┼─────┤  Same DataFrame  ←→    ├──────┼─────┤
    │Alice │  30 │                        │Alice │  30 │
    │Bob   │  25 │                        │Bob   │  25 │
    └──────┴─────┘                        └──────┴─────┘
```

### Type Inference

```
JSON value     →  Series type
─────────────────────────────
integer (42)   →  i64
float (3.14)   →  f64
string ("abc") →  String
true/false     →  bool
null           →  default (0 / false / "")

Mixed column rules:
  int + float  →  f64 (promote)
  any + string →  String (fallback)
```

## Parquet Format

### File Layout

```
┌────────────────────────────────────────┐
│ "PAR1"  (4 bytes magic)                │
├────────────────────────────────────────┤
│ Column Chunk 0                         │
│   ┌─ Page Header (Thrift encoded) ───┐ │
│   │  page_type: DATA_PAGE            │ │
│   │  uncompressed_size               │ │
│   │  compressed_size                 │ │
│   │  DataPageHeader:                 │ │
│   │    num_values, encoding          │ │
│   └──────────────────────────────────┘ │
│   ┌─ Page Data ──────────────────────┐ │
│   │  PLAIN encoded values            │ │
│   │  (optionally Snappy compressed)  │ │
│   └──────────────────────────────────┘ │
├────────────────────────────────────────┤
│ Column Chunk 1                         │
│   (same structure)                     │
├────────────────────────────────────────┤
│ ...                                    │
├────────────────────────────────────────┤
│ Footer (Thrift encoded FileMetaData)   │
│   version: 2                           │
│   schema: [root, col0, col1, ...]      │
│   num_rows                             │
│   row_groups: [{columns, sizes}]       │
│   created_by: "teddy (Zig)"            │
├────────────────────────────────────────┤
│ Footer Length (4 bytes LE u32)         │
├────────────────────────────────────────┤
│ "PAR1"  (4 bytes magic)                │
└────────────────────────────────────────┘
```

### Parquet Read/Write Stack

```mermaid
flowchart TB
    subgraph Read["Read Path"]
        direction TB
        R1["parquet_reader.zig
        Validate PAR1 magic
        Read footer length
        Parse FileMetaData"] --> R2["column_reader.zig
        Read page headers
        Decode dictionary pages
        Decode data pages"]
        R2 --> R3["encoding_reader.zig
        PlainDecoder
        RleBitPackedDecoder"]
        R2 --> R4["snappy.zig
        decompress()"]
        R1 --> R5["thrift_reader.zig
        ThriftReader"]
    end

    subgraph Write["Write Path"]
        direction TB
        W1["parquet_writer.zig
        Assemble PAR1 file
        Encode FileMetaData
        Write footer"] --> W2["column_writer.zig
        Build page headers
        Encode values"]
        W2 --> W3["encoding_writer.zig
        PlainEncoder"]
        W2 --> W4["snappy.zig
        compress()"]
        W1 --> W5["thrift_writer.zig
        ThriftWriter"]
    end

    subgraph Shared["Shared"]
        META["metadata.zig
        SchemaElement
        PageHeader
        ColumnMetaData
        FileMetaData
        (decode + encode)"]
        TYPES["types.zig
        PhysicalType
        CompressionCodec
        Encoding
        ParquetColumn
        ParquetResult"]
    end

    R5 --> META
    W5 --> META
    R1 --> TYPES
    W1 --> TYPES
```

### Compression Support

```
Codec          Read    Write   Notes
───────────────────────────────────────
Uncompressed   Yes     Yes     Default
Snappy         Yes     Yes     Literal-only compression
GZIP           No      No      Future
LZ4            No      No      Future
ZSTD           No      No      Future
```
