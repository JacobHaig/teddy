# Zeddy

Zeddy is a data manipulation and analysis library for Zig, designed to provide efficient and type-safe operations on structured data.

## Features

- **Type-safe Series**: Strongly typed series with generics supporting various data types
- **DataFrame Structure**: Combine multiple series into a tabular data structure
- **CSV Parsing**: Built-in support for parsing CSV data
- **Memory Safety**: Full control over memory allocation and deallocation
- **Performance**: Designed for high performance data operations
- **Data Transformation**: Apply functions to manipulate data in-place

## Status

This project is in early development. The API is not yet stable and is subject to change.

## Building

Zeddy uses the standard Zig build system. To build the project:

```
zig build
```

To run tests:

```
zig build test
```

To run the example application:

```
zig build run
```

## Project Structure

- `src/dataframe/`: Core dataframe implementation
  - `dataframe.zig`: Main dataframe structure
  - `series.zig`: Generic series implementation
  - `variant_series.zig`: Type-safe variant wrapper for series
  - `csv.zig`: CSV parsing utilities
  - `reader.zig`: File reading and loading utilities

## Future Plans

- Complete CSV reader integration
- Add data filtering capabilities
- Implement aggregation functions
- Add sorting and grouping operations
- Support for additional file formats (JSON, Parquet)
- Improve performance with SIMD operations where applicable

## License

TBD

## Contributing

This project is still in the early development phase. Contribution guidelines will be added once the API is more stable.