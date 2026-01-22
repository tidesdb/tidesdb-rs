# tidesdb-rs

tidesdb-rs is the official Rust binding for TidesDB.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This Rust binding provides a safe, idiomatic Rust interface to TidesDB with full support for all features.

## Features

- MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
- Column families (isolated key-value stores with independent configuration)
- Bidirectional iterators with forward/backward traversal and seek support
- TTL (time to live) support with automatic key expiration
- LZ4, LZ4 Fast, ZSTD, Snappy, or no compression
- Bloom filters with configurable false positive rates
- Global block CLOCK cache for hot blocks
- Savepoints for partial transaction rollback
- Six built-in comparators plus custom registration

For Rust usage you can go to the TidesDB Rust Reference [here](https://tidesdb.com/reference/rust/).

## License

Multiple licenses apply:

- Mozilla Public License Version 2.0 (TidesDB)
- BSD 3-Clause (Snappy)
- BSD 2-Clause (LZ4)
- BSD 2-Clause (xxHash - Yann Collet)
- BSD (Zstandard)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

- [Discord](https://discord.gg/tWEmjR66cy)
- [GitHub Issues](https://github.com/tidesdb/tidesdb-rs/issues)
