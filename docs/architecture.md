# Teddy Architecture Overview

## High-Level Data Flow

```mermaid
flowchart LR
    subgraph Input["Input Sources"]
        CSV_F["CSV File"]
        JSON_F["JSON File"]
        PAR_F["Parquet File"]
    end

    subgraph Reader["Reader (Builder Pattern)"]
        R["Reader.init()
        .withFileType()
        .withPath()
        .load()"]
    end

    subgraph Core["In-Memory DataFrame"]
        DF["DataFrame
        ─────────────
        ArrayList&lt;BoxedSeries&gt;"]
    end

    subgraph Ops["Operations"]
        FILTER["filter()"]
        SORT["sort()"]
        SELECT["select()"]
        JOIN["join()"]
        GROUP["groupBy()"]
        HEAD["head/tail/slice()"]
        UNIQUE["unique()"]
        CONCAT["concat()"]
        DESC["describe()"]
    end

    subgraph Writer["Writer (Builder Pattern)"]
        W["Writer.init()
        .withFileType()
        .withPath()
        .save()"]
    end

    subgraph Output["Output Targets"]
        CSV_O["CSV File"]
        JSON_O["JSON File"]
        PAR_O["Parquet File"]
        STR_O["In-Memory []u8"]
    end

    CSV_F --> R
    JSON_F --> R
    PAR_F --> R
    R --> DF
    DF --> FILTER --> DF
    DF --> SORT --> DF
    DF --> SELECT --> DF
    DF --> JOIN --> DF
    DF --> GROUP --> DF
    DF --> HEAD --> DF
    DF --> UNIQUE --> DF
    DF --> CONCAT --> DF
    DF --> DESC --> DF
    DF --> W
    W --> CSV_O
    W --> JSON_O
    W --> PAR_O
    W --> STR_O
```

## Module Dependency Graph

```mermaid
flowchart TB
    subgraph App["Application"]
        MAIN["main.zig"]
    end

    subgraph DF_MOD["dataframe module"]
        DATAFRAME["dataframe.zig"]
        SERIES["series.zig"]
        BOXED["boxed_series.zig"]
        GROUP["group.zig"]
        BGROUP["boxed_groupby.zig"]
        READER["reader.zig"]
        WRITER["writer.zig"]
        CSV_R["csv_reader.zig"]
        CSV_W["csv_writer.zig"]
        JSON_R["json_reader.zig"]
        JSON_W["json_writer.zig"]
        JOIN["join.zig"]
        STRINGS["strings.zig"]
        PQ_ADAPT["parquet.zig"]
    end

    subgraph PQ_MOD["parquet module"]
        PQ_API["parquet.zig"]
        PQ_READ["parquet_reader.zig"]
        PQ_WRITE["parquet_writer.zig"]
        COL_R["column_reader.zig"]
        COL_W["column_writer.zig"]
        META["metadata.zig"]
        TYPES["types.zig"]
        ENC_R["encoding_reader.zig"]
        ENC_W["encoding_writer.zig"]
        THR_R["thrift_reader.zig"]
        THR_W["thrift_writer.zig"]
        SNAPPY["snappy.zig"]
    end

    MAIN --> DATAFRAME
    DATAFRAME --> SERIES
    DATAFRAME --> BOXED
    DATAFRAME --> GROUP
    DATAFRAME --> BGROUP
    DATAFRAME --> READER
    DATAFRAME --> WRITER
    DATAFRAME --> JOIN
    SERIES --> STRINGS
    BOXED --> SERIES
    READER --> CSV_R
    READER --> JSON_R
    READER --> PQ_ADAPT
    WRITER --> CSV_W
    WRITER --> JSON_W
    WRITER --> PQ_ADAPT
    PQ_ADAPT --> PQ_API

    PQ_API --> PQ_READ
    PQ_API --> PQ_WRITE
    PQ_API --> COL_W
    PQ_READ --> COL_R
    PQ_READ --> META
    PQ_READ --> THR_R
    PQ_WRITE --> COL_W
    PQ_WRITE --> META
    PQ_WRITE --> THR_W
    COL_R --> ENC_R
    COL_R --> SNAPPY
    COL_R --> THR_R
    COL_W --> ENC_W
    COL_W --> SNAPPY
    COL_W --> THR_W
    META --> THR_R
    META --> THR_W
    ENC_W --> ENC_R
    THR_W --> THR_R

    style PQ_MOD fill:#1a1a2e,stroke:#e94560,color:#eee
    style DF_MOD fill:#1a1a2e,stroke:#0f3460,color:#eee
    style App fill:#1a1a2e,stroke:#16213e,color:#eee
```

## Project File Structure

```
teddy/
├── build.zig                    # Build configuration
├── data/                        # Sample data files
│   ├── addresses.csv
│   └── addresses.parquet
├── src/
│   ├── main.zig                 # Entry point
│   │
│   ├── dataframe/               # Core DataFrame module
│   │   ├── dataframe.zig        # DataFrame struct & operations
│   │   ├── series.zig           # Series(T) generic column
│   │   ├── boxed_series.zig     # Type-erased BoxedSeries union
│   │   ├── strings.zig          # Custom String type
│   │   ├── group.zig            # GroupBy(T) typed impl
│   │   ├── boxed_groupby.zig    # Type-erased BoxedGroupBy union
│   │   ├── join.zig             # Join operations
│   │   ├── reader.zig           # Unified file reader
│   │   ├── writer.zig           # Unified file writer
│   │   ├── csv_reader.zig       # CSV parser
│   │   ├── csv_writer.zig       # CSV serializer
│   │   ├── json_reader.zig      # JSON parser
│   │   ├── json_writer.zig      # JSON serializer
│   │   └── parquet.zig          # Parquet <-> DataFrame adapter
│   │
│   └── parquet/                 # Native Parquet implementation
│       ├── parquet.zig          # Module public API
│       ├── parquet_reader.zig   # Parquet file reader
│       ├── parquet_writer.zig   # Parquet file writer
│       ├── column_reader.zig    # Column chunk decoder
│       ├── column_writer.zig    # Column chunk encoder
│       ├── metadata.zig         # Thrift metadata structs
│       ├── types.zig            # Parquet type definitions
│       ├── encoding_reader.zig  # PLAIN/RLE decoders
│       ├── encoding_writer.zig  # PLAIN encoder
│       ├── thrift_reader.zig    # Thrift Compact decoder
│       ├── thrift_writer.zig    # Thrift Compact encoder
│       └── snappy.zig           # Snappy compress/decompress
└── docs/
    └── (this file)
```
