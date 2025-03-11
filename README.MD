# Aubuchon Discount Append Tool

A Rust-based utility for processing the Aubuchon database, calculating discounted prices, and synchronizing with database systems.

## Overview

This tool automates the process of reading propane product data from CSV files, applying discount calculations, and uploading the processed data to a database. It's designed to support inventory management and sales analysis for propane-related items.

## Features

- CSV file parsing with flexible input handling
- Automatic discount price calculation
- Concurrent database updates with controlled throughput
- Comprehensive error handling and logging
- Support for propane product inventory tracking

## Prerequisites

- Rust 1.85.0 or newer
- Database access (configured via environment variables)

## Installation

Clone the repository and build the project:

```shell script
git clone https://github.com/Mardens-Inc/aubuchon_discount_append_tool.git
cd aubuchon_discount_append_tool
cargo build --release
```

## Usage

Run the tool with the path to your CSV file:

```shell script
cargo run --release -- path/to/your/file.csv
```

Or use the compiled binary directly:

```shell script
./target/release/aubuchon_discount_append_tool path/to/your/file.csv
```

## Configuration

The application can be configured using environment variables:

```
DATABASE_URL=postgres://username:password@localhost/database_name
LOG_LEVEL=info  # Options: trace, debug, info, warn, error
```

## Project Structure

- `src/` - Source code files
- `file.csv` - Sample propane product data file

## Dependencies

- `tokio` - Asynchronous runtime
- `sqlx` - Database operations
- `serde` - Data serialization/deserialization
- `csv` - CSV file processing
- `anyhow` - Error handling
- `log` and `env_logger` - Logging functionality

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the project
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Contact

Drew Chase - drew.chase@mardens.com