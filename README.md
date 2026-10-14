# tidesdb-rs

tidesdb-rs is the official Rust binding for TidesDB.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This Rust binding provides a safe, idiomatic Rust interface to TidesDB with full support for all features.

For the Rust reference see [doc/rust.md](doc/rust.md) or the [TidesDB Rust Reference](https://tidesdb.com/docs/v10/bindings/rust/reference/).

## Versioning

The crate's major version maps to a TidesDB major: crate N.x binds TidesDB (N+9).x.

| Crate | TidesDB |
|-------|---------|
| 0.x | 9.x |
| 1.x | 10.x |
| 2.x | 11.x |

This release bundles TidesDB 10.2.1. The build ignores a system TidesDB of any other major and says so in a build warning. TidesDB 10 cannot open TidesDB 9 data and there is no migration tool; see [Upgrading from TidesDB 9](doc/rust.md#upgrading-from-tidesdb-9-crate-0x).

## License

Multiple licenses apply:

- Mozilla Public License Version 2.0 (TidesDB)
- BSD 3-Clause (Snappy)
- BSD 2-Clause (LZ4)
- BSD 2-Clause (xxHash - Yann Collet)
- BSD (Zstandard)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
