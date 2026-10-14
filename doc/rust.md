---
title: TidesDB Rust API Reference
description: Rust API reference for TidesDB
---

<div class="no-print">

If you want to download the source of this document, you can find it [here](https://github.com/tidesdb/tidesdb-rs/blob/master/doc/rust.md).

<hr/>

</div>

## Getting Started

The `tidesdb` crate provides safe Rust bindings for TidesDB, an embeddable key-value storage engine based on a log-structured merge-tree. This release (crate 1.0.0) binds and bundles **TidesDB 10.2.1**.

### Versioning

The crate's major version maps to a TidesDB major version: crate **N.x binds TidesDB (N+9).x**.

| Crate | TidesDB |
|-------|---------|
| 0.x | 9.x |
| 1.x | 10.x |
| 2.x | 11.x (when released) |

Each TidesDB major gets a new crate major; crate minor and patch releases follow changes to the bindings and to the bundled TidesDB release within that major. Pick the crate major that matches the TidesDB major you want:

```toml
[dependencies]
tidesdb = "1" # TidesDB 10.x
```

Two constants report what you got:

```rust
// the release the bindings were written against and the bundled source is pinned to
assert_eq!(tidesdb::TIDESDB_VERSION, "10.2.1");
// the release actually linked: the bundled one, or a system install (see below)
println!("linked TidesDB {}", tidesdb::TIDESDB_LINKED_VERSION);
```

The build script enforces the mapping. A system TidesDB of another major (or older than 10.2.1) is never linked: the build prints a warning saying so and builds the bundled release instead, for example:

```text
warning: tidesdb@1.0.0: Ignoring the system TidesDB 11.0.0 found via pkg-config: tidesdb crate 1.0.0
binds TidesDB 10.x (at least 10.2.1). Crate 1.x maps to TidesDB 10.x, crate 2.x to TidesDB 11.x.
Building the bundled TidesDB 10.2.1 instead.
```

### Prerequisites

The crate needs Rust 1.85 or newer (it uses the 2024 edition; `rust-version` in its `Cargo.toml` says so). The TidesDB C source is bundled with the crate, so beyond Rust you only need a C compiler and CMake. No system libraries are required by default.

**Debian/Ubuntu**
```bash
sudo apt install build-essential cmake
```

**macOS**
```bash
xcode-select --install
brew install cmake
```

**Windows**

Install Visual Studio Build Tools (or another C toolchain) and CMake.

### Installing from crates.io

```toml
[dependencies]
tidesdb = "1"
```

Or using `cargo add`:

```bash
cargo add tidesdb
```

### Compression Features

TidesDB needs no compression library, and the default build has none. Each codec is opt-in through a Cargo feature, and each one links its system library:

| Feature | Enables | System library |
|---------|---------|----------------|
| `snappy` | `CompressionAlgorithm::Snappy` | snappy |
| `lz4` | `CompressionAlgorithm::Lz4`, `CompressionAlgorithm::Lz4Fast` | liblz4 |
| `zstd` | `CompressionAlgorithm::Zstd` | libzstd |
| `compression` | all three of the above | all three |

```toml
[dependencies]
tidesdb = { version = "1", features = ["lz4", "zstd"] }
```

Install the libraries for the features you enable:

- Debian/Ubuntu · `sudo apt install libsnappy-dev liblz4-dev libzstd-dev`
- macOS · `brew install snappy lz4 zstd`
- Windows · `vcpkg install snappy:x64-windows lz4:x64-windows zstd:x64-windows`

The build script looks for each enabled library with `pkg-config` and falls back to linking it by name, printing a warning with the install commands above if it was not found.

The features choose the codecs of the bundled build. A system TidesDB linked through pkg-config has whichever codecs it was built with, whatever the features say (the build prints a warning if you enable them anyway). Either way, `CompressionAlgorithm::is_available` tells you at runtime what the linked library supports (see [Compression](#compression)).

### Allocator Features

TidesDB can run on an alternative memory allocator, matching its `TIDESDB_WITH_JEMALLOC`, `TIDESDB_WITH_MIMALLOC` and `TIDESDB_WITH_TCMALLOC` CMake options. Enable at most one; enabling two fails the build.

| Feature | Allocator | Debian/Ubuntu | macOS | Windows |
|---------|-----------|---------------|-------|---------|
| `jemalloc` | jemalloc | `libjemalloc-dev` | `brew install jemalloc` | not supported |
| `mimalloc` | mimalloc | `libmimalloc-dev` | `brew install mimalloc` | `vcpkg install mimalloc:x64-windows` |
| `tcmalloc` | tcmalloc (gperftools) | `libgoogle-perftools-dev` | `brew install gperftools` | not supported |

```toml
[dependencies]
tidesdb = { version = "1", features = ["jemalloc"] }
```

The allocator is linked into your binary as a shared library and replaces `malloc` and `free` for the **whole process**: TidesDB, Rust's default (system) allocator, and any other C code. That is how TidesDB's own CMake options work too; TidesDB has no allocator-specific code. If you only want Rust allocations on jemalloc or mimalloc, use a Rust allocator crate with `#[global_allocator]` instead; it does not affect TidesDB. The library is found with `pkg-config` (for tcmalloc, `libtcmalloc` then `libtcmalloc_minimal`), falling back to linking it by name with a warning. The allocator feature works with both the bundled build and a system TidesDB.

### How the Build System Works

The build script (`build.rs`) links one TidesDB 10.x release:

1. **Version mapping check** · The crate major and the pinned TidesDB release must agree (crate 1.x, TidesDB 10.x), and the bundled source must be the pinned release (10.2.1); otherwise the build stops with an error naming the mismatch.
2. **pkg-config check** · If `pkg-config` finds a TidesDB installed on the system that is **10.x and at least 10.2.1**, it is linked and nothing is built. A newer 10.x is linked with a warning that the bindings were written against 10.2.1. Any other version is ignored with the warning shown under [Versioning](#versioning). TidesDB's own CMake install does not write a `tidesdb.pc` file, so this applies only when a package (or you) provides one. Set the `TIDESDB_NO_PKG_CONFIG` environment variable to skip this check and always build the bundled source.
3. **Bundled source** · Otherwise the TidesDB source in the crate's `tidesdb/` directory (a git submodule pinned to the 10.2.1 release, shipped inside the published crate) is built with CMake as a static library and linked into your binary. The `snappy`, `lz4` and `zstd` features are passed to the CMake build explicitly.

```bash
# Always build the bundled source, even if a matching TidesDB is installed
TIDESDB_NO_PKG_CONFIG=1 cargo build
```

The crate declares `links = "tidesdb"`, so only one version of it can be linked into a build.

### Building from GitHub

The TidesDB source is a git submodule, so a checkout needs it initialised before building:

```bash
git clone https://github.com/tidesdb/tidesdb-rs.git
cd tidesdb-rs
git submodule update --init
cargo build --release
cargo test
```

If the submodule is missing, the build fails with a message telling you to run `git submodule update --init` (or to install TidesDB 10.x with a pkg-config file). If the submodule is checked out at a different release, the build fails and tells you to check out `v10.2.1`.

**Using directly from GitHub in your Cargo.toml**

```toml
[dependencies]
tidesdb = { git = "https://github.com/tidesdb/tidesdb-rs.git" }
```

**Using an installed TidesDB through pkg-config**

TidesDB's `cmake --install` installs the library and headers but no pkg-config file. If your package does not ship one, write a minimal `tidesdb.pc` next to the library:

```text
prefix=/custom/path
libdir=${prefix}/lib
includedir=${prefix}/include

Name: tidesdb
Description: TidesDB storage engine
Version: 10.2.1
Libs: -L${libdir} -ltidesdb -lpthread
Cflags: -I${includedir}
```

```bash
# The installed library must be TidesDB 10.x, at least 10.2.1
export PKG_CONFIG_PATH="/custom/path/lib/pkgconfig:$PKG_CONFIG_PATH"
cargo build
```

A TidesDB built with compression links its codec libraries too; add them to `Libs` (for example `-lzstd -llz4 -lsnappy`). When the library is a shared one outside the loader's search path, set `LD_LIBRARY_PATH` (or `DYLD_LIBRARY_PATH` on macOS) when running.

### Cross-Compiling and Embedded Targets

The bundled TidesDB is built for the Cargo target, not the build machine, so cross-compiling works like any other C-backed crate. Nothing is downloaded at build time: the C source ships inside the crate.

TidesDB needs an operating system with a filesystem and threads. Embedded Linux on ARM, AArch64, RISC-V, MIPS or PowerPC is supported; bare-metal (`no_std`) targets are not.

Point the build at your cross toolchain the usual way; the `cmake` crate passes the C compiler on to TidesDB's CMake build:

```bash
rustup target add armv7-unknown-linux-gnueabihf
export CC_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-gcc
export CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc
cargo build --release --target armv7-unknown-linux-gnueabihf
```

A CMake toolchain file (for Yocto, Buildroot or a vendor SDK) can be given with `CMAKE_TOOLCHAIN_FILE`, or per target with `CMAKE_TOOLCHAIN_FILE_<target>`.

The build script links the system libraries the static TidesDB needs for the **target**, following TidesDB's own CMake recipe:

| Target | Linked |
|--------|--------|
| Linux and other Unix | `pthread`, `m`; `atomic` on 32-bit targets (ARMv7, PowerPC, MIPS, x86); `stdc++` with the `snappy` feature |
| macOS, iOS, FreeBSD, OpenBSD, NetBSD, DragonFly, Solaris, illumos | `m`, `pthread` |
| Android | `m` (Bionic has pthreads in libc) |
| Windows, GNU (MinGW) | `pthread` (winpthreads) |
| Windows, MSVC | nothing (native Win32 threads) |

A 32-bit target's toolchain must provide `libatomic` (GCC cross toolchains do).

Package-manager library directories (Homebrew, `/usr/local/lib`, MSYS2, pkgsrc) are added to the link search path only for native builds. When cross-compiling, the compression and allocator libraries must be built for the target. Make them visible through pkg-config, which the build uses only when cross-compiling is allowed (`PKG_CONFIG_ALLOW_CROSS=1`, with `PKG_CONFIG_SYSROOT_DIR` and `PKG_CONFIG_PATH` pointing into the target sysroot), or with `RUSTFLAGS="-L /path/to/target/libs"`. The same applies to a system TidesDB: when cross-compiling, pkg-config only finds one if it is configured for the target, and otherwise the bundled source is built.

The CI cross-builds and tests `armv7-unknown-linux-gnueabihf` and `aarch64-unknown-linux-gnu` under QEMU, and cross-builds `x86_64-pc-windows-gnu` from Linux.

## Upgrading from TidesDB 9 (crate 0.x)

TidesDB 10 is a new major version. It cannot open a database written by TidesDB 9, and there is no migration tool: to keep your data, read it out with a program built against crate 0.x (TidesDB 9) and write it into a new database with a program built against crate 1.x (TidesDB 10).

Both crates link a C library exporting the same `tidesdb_*` symbols, so they cannot be linked into one binary. Use two programs, for example one that exports every key and value to a file and one that imports that file:

```rust
// Importer, built against tidesdb = "1"
use tidesdb::{TidesDB, Config, ColumnFamilyConfig};

fn import(pairs: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) -> tidesdb::Result<()> {
    let db = TidesDB::open(Config::new("./mydb-v10"))?;
    db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
    let cf = db.get_column_family("my_cf")?;

    let mut txn = db.begin_transaction()?;
    for (key, value) in pairs {
        txn.put(&cf, &key, &value)?;
    }
    txn.commit()?;
    Ok(())
}
```

For large data sets, commit in batches rather than in one transaction. Crate 0.x cannot read a key's expiry back, so keys written with a TTL come across as permanent unless your application knows their lifetimes and writes them with `put_with_ttl`.

The API changed with the engine as well; this reference describes the 1.x API in full. TidesDB 10 itself no longer has an object store (S3), replica mode, custom comparators or INI configuration files, so the crate has none either.

## Usage

### Opening and Closing a Database

```rust
use tidesdb::{TidesDB, Config, LogLevel, SyncMode};

fn main() -> tidesdb::Result<()> {
    let config = Config::new("./mydb")
        .num_flush_threads(2)
        .num_compaction_threads(2)
        .log_level(LogLevel::Warn)
        .block_cache_size(64 * 1024 * 1024)
        .max_open_sstables(256)
        .memtable_write_buffer_size(64 * 1024 * 1024)
        .memtable_sync_mode(SyncMode::Interval)
        .memtable_sync_interval_us(100_000);

    let db = TidesDB::open(config)?;
    println!("Database opened");

    // The database closes when `db` is dropped.
    Ok(())
}
```

`Config::new(path)` fills every other field from the C library's defaults (`tidesdb_default_config()`), so the binding tracks the engine's defaults automatically. See the [Database Configuration Reference](#database-configuration-reference) for every field.

A database directory can be open in only one handle at a time. Opening it again, from this process or another, fails with `ErrorCode::Locked`:

```rust
use tidesdb::{TidesDB, Config, ErrorCode};

let db = TidesDB::open(Config::new("./mydb"))?;
match TidesDB::open(Config::new("./mydb")) {
    Err(e) if e.code() == Some(ErrorCode::Locked) => println!("already open"),
    Err(e) => return Err(e),
    Ok(_) => unreachable!(),
}
```

There is no explicit `close`: drop the `TidesDB`. Every `ColumnFamily`, `Transaction`, `Snapshot` and `Iterator` borrows the database, so the compiler guarantees they are all gone before it closes.

### Column Families

Column families are isolated key spaces with their own configuration. They share the database's memtable, write-ahead log, value log and block cache.

#### Creating and Getting

```rust
use tidesdb::{TidesDB, Config, ColumnFamilyConfig, IsolationLevel};

fn main() -> tidesdb::Result<()> {
    let db = TidesDB::open(Config::new("./mydb"))?;

    db.create_column_family("users", ColumnFamilyConfig::default())?;

    let config = ColumnFamilyConfig::new()
        .level_size_ratio(10)
        .enable_bloom_filter(true)
        .bloom_fpr(0.01)
        .default_isolation_level(IsolationLevel::Snapshot);
    db.create_column_family("orders", config)?;

    let users = db.get_column_family("users")?;
    println!("handle for {}", users.name());

    Ok(())
}
```

`create_column_family` returns `ErrorCode::Exists` if the name is taken. `get_column_family` returns `ErrorCode::NotFound` if it does not exist.

`ColumnFamily` handles are cheap to clone, `Send + Sync`, and borrow the `TidesDB` they came from. Using a handle with a transaction from a different database is rejected with `ErrorCode::InvalidArgs`.

#### Listing

```rust
for name in db.list_column_families()? {
    println!("{name}");
}
```

#### Renaming

```rust
db.rename_column_family("old_name", "new_name")?;
```

The family is renamed in place, so existing handles stay valid and keep referring to it, but `ColumnFamily::name()` keeps reporting the name the handle was looked up by. `cf.get_stats()?.name` reports the current name.

#### Cloning

```rust
db.clone_column_family("users", "users_copy")?;
let copy = db.get_column_family("users_copy")?;
```

The clone is a point-in-time copy: everything written to the source before the call is included; later writes to the source are not. Cloning a missing source returns `NotFound`; cloning onto an existing name returns `Exists`. The clone flushes the memtable first and returns `Locked` if the flush queue does not drain in time; retry it. A commit hook on the source is not copied.

#### Dropping

Dropping a column family deletes all of its data and cannot be undone. `drop_column_family` takes `&mut self`, so every handle to the family must be gone first:

```rust
use tidesdb::{TidesDB, Config, ColumnFamilyConfig};

fn main() -> tidesdb::Result<()> {
    let mut db = TidesDB::open(Config::new("./mydb"))?;
    db.create_column_family("scratch", ColumnFamilyConfig::default())?;
    {
        let cf = db.get_column_family("scratch")?;
        // ... use cf ...
    } // cf dropped here

    db.drop_column_family("scratch")?;
    Ok(())
}
```

When the database is shared (for example behind an `Arc` or across `std::thread::scope` threads) you cannot get `&mut TidesDB`. Use the unchecked variant and uphold its contract yourself:

```rust
// SAFETY: no ColumnFamily handle for "scratch", and no transaction or iterator
// that has used one, is used after this call.
unsafe { db.drop_column_family_unchecked("scratch")? };
```

### Reading and Writing

All reads and writes go through a transaction. Writes are buffered until `commit`; dropping an uncommitted transaction rolls it back. `put`, `delete` and the other write methods take `&self`; `commit`, `rollback`, `reset`, `set_timeout` and the two-phase-commit methods take `&mut self`.

#### Writing Data

```rust
use tidesdb::{TidesDB, Config, ColumnFamilyConfig};

fn main() -> tidesdb::Result<()> {
    let db = TidesDB::open(Config::new("./mydb"))?;
    db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
    let cf = db.get_column_family("my_cf")?;

    let mut txn = db.begin_transaction()?;
    txn.put(&cf, b"key", b"value")?;
    txn.put(&cf, b"empty", b"")?; // empty values are allowed
    txn.commit()?;

    Ok(())
}
```

`put` writes a key that never expires. Keys must be non-empty (an empty key is rejected with `InvalidArgs`). An empty value is allowed and is distinct from an absent key: reads return it as an empty `Vec`.

#### Writing with TTL

```rust
use std::time::Duration;

let mut txn = db.begin_transaction()?;
txn.put_with_ttl(&cf, b"session:abc", b"token", Duration::from_secs(60))?;
txn.commit()?;
```

The lifetime is relative. It is rounded up to whole seconds and converted to an absolute deadline when the write is buffered (so `Duration::from_millis(1)` lives about one second). A zero `Duration` is rejected with `ErrorCode::InvalidArgs`. Once the deadline passes, the committed key reads as absent. (A transaction that outlives the deadline of its own uncommitted write still reads that write back with `get`; `get_expiry` reports it absent.)

#### Reading Data

```rust
let txn = db.begin_transaction()?;

let value = txn.get(&cf, b"key")?;
println!("{}", String::from_utf8_lossy(&value));

match txn.get(&cf, b"missing") {
    Ok(v) => println!("found {v:?}"),
    Err(e) if e.is_not_found() => println!("not found"),
    Err(e) => return Err(e),
}
```

`get` returns an error with `ErrorCode::NotFound` when the key is absent, deleted or expired. A transaction reads its own uncommitted writes, including its own deletes and range deletes.

| Method | Returns | Conflict tracking |
|--------|---------|-------------------|
| `get(&cf, key)` | `Result<Vec<u8>>` | Records the read for conflict detection |
| `get_notrack(&cf, key)` | `Result<Vec<u8>>` | Does not record the read |
| `contains(&cf, key)` | `Result<bool>` (`Ok(false)` when absent) | Does not record the read |
| `get_expiry(&cf, key)` | `Result<Option<i64>>`: absolute expiry in Unix seconds; `None` = never expires; `NotFound` if the key is absent | Records the read for conflict detection |

Reads are only recorded at `RepeatableRead` and `Serializable`; at weaker levels nothing is tracked.

`get_notrack` and `contains` are meant for existence probes, such as uniqueness checks, that should not add to the transaction's read set (and so cannot cause a conflict at `RepeatableRead` or `Serializable`).

```rust
let txn = db.begin_transaction()?;
if !txn.contains(&cf, b"user:42")? {
    println!("free");
}
if let Some(expires_at) = txn.get_expiry(&cf, b"session:abc")? {
    println!("expires at unix time {expires_at}");
}
```

#### Deleting Data

```rust
let mut txn = db.begin_transaction()?;
txn.delete(&cf, b"key")?;
txn.commit()?;
```

#### Single-Delete

`single_delete` writes a tombstone that reads the same as `delete`, but lets compaction drop the put and the tombstone together:

```rust
let mut txn = db.begin_transaction()?;
txn.single_delete(&cf, b"once")?;
txn.commit()?;
```

:::caution[Contract]
You promise the key was put **at most once** since its previous single-delete (or since it was created). The engine does not verify this; breaking the contract can leave older puts visible. When in doubt, use `delete`.
:::

#### Range and Prefix Deletes

```rust
let mut txn = db.begin_transaction()?;

// every key in [b"b", b"d")
txn.delete_range(&cf, b"b", Some(b"d"))?;

// every key from b"x" to the end of the family
txn.delete_range(&cf, b"x", None)?;

// every key beginning with b"user:"
txn.delete_prefix(&cf, b"user:")?;

txn.commit()?;
```

A range or prefix delete costs one entry however many keys it covers. It covers keys written before it, not merely the keys present when it is called, and a newer write inside the range survives it. The lower bound (or prefix) must be non-empty, each bound is at most 256 bytes, and an upper bound must be non-empty and greater than the lower bound; otherwise the call fails with `InvalidArgs`. Use `None` for no upper bound.

#### Multi-Column-Family Transactions

A transaction can write to several column families and commits them atomically:

```rust
let users = db.get_column_family("users")?;
let orders = db.get_column_family("orders")?;

let mut txn = db.begin_transaction()?;
txn.put(&users, b"u1", b"alice")?;
txn.put(&orders, b"o1", b"u1")?;
txn.commit()?;
```

#### Rollback

```rust
let mut txn = db.begin_transaction()?;
txn.put(&cf, b"k", b"v")?;
txn.rollback()?; // nothing is written
```

Dropping a transaction without committing also rolls it back.

### Transactions

#### Isolation Levels

```rust
use tidesdb::IsolationLevel;

let txn = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
```

| Level | Behavior |
|-------|----------|
| `IsolationLevel::ReadUncommitted` | Every version is visible, including sequences still in progress |
| `IsolationLevel::ReadCommitted` | The newest committed version, re-read on every operation (default) |
| `IsolationLevel::RepeatableRead` | Frozen snapshot; commit validates that every key read is unchanged and that no key appeared inside a scanned range |
| `IsolationLevel::Snapshot` | Frozen snapshot; commit claims each written key first-committer-wins |
| `IsolationLevel::Serializable` | Both of the checks above |

`begin_transaction()` uses `ReadCommitted`. `begin_transaction_cf(&cf)` uses the column family's configured `default_isolation_level`:

```rust
use tidesdb::{ColumnFamilyConfig, IsolationLevel};

db.create_column_family(
    "rr",
    ColumnFamilyConfig::default().default_isolation_level(IsolationLevel::RepeatableRead),
)?;
let cf = db.get_column_family("rr")?;
let txn = db.begin_transaction_cf(&cf)?; // RepeatableRead
```

#### Handling Conflicts

`commit` returns `ErrorCode::Conflict` when another transaction committed a conflicting write first. Even at `ReadCommitted`, a commit is refused with `Conflict` if it writes a key that a prepared but not yet resolved two-phase transaction read. Retry the whole transaction:

```rust
use tidesdb::IsolationLevel;

loop {
    let mut txn = db.begin_transaction_with_isolation(IsolationLevel::Snapshot)?;
    let n: u64 = match txn.get(&cf, b"counter") {
        Ok(v) => u64::from_be_bytes(v.try_into().unwrap()),
        Err(e) if e.is_not_found() => 0,
        Err(e) => return Err(e),
    };
    txn.put(&cf, b"counter", &(n + 1).to_be_bytes())?;
    match txn.commit() {
        Ok(()) => break,
        Err(e) if e.is_conflict() => continue,
        Err(e) => return Err(e),
    }
}
```

`DbStats::txn_conflicts` counts commits and prepares refused with a conflict.

#### Savepoints

Savepoints allow partial rollback within a transaction:

```rust
let mut txn = db.begin_transaction()?;
txn.put(&cf, b"key1", b"value1")?;

txn.savepoint("sp1")?;
txn.put(&cf, b"key2", b"value2")?;

// key2 is discarded, key1 remains
txn.rollback_to_savepoint("sp1")?;

txn.savepoint("sp2")?;
txn.release_savepoint("sp2")?; // forget it without rolling back

txn.put(&cf, b"key3", b"value3")?;
txn.commit()?; // writes key1 and key3
```

Rolling back to or releasing a savepoint that does not exist returns `ErrorCode::NotFound`. Rolling back to a savepoint keeps that savepoint and discards any created after it.

#### Transaction Reset

`reset` discards a transaction's state, committed or not, and begins it again at a new isolation level and a fresh snapshot. It also restores the database's default timeout and clears any pending abort request. The `Transaction` value is reused, but the engine allocates a new transaction internally, so this is a convenience, not an optimisation:

```rust
use tidesdb::IsolationLevel;

let mut txn = db.begin_transaction()?;
txn.put(&cf, b"a", b"1")?;
txn.commit()?;

txn.reset(IsolationLevel::Serializable)?;
txn.put(&cf, b"b", b"2")?;
txn.commit()?;
```

#### Transaction State

```rust
use tidesdb::TxnState;

let mut txn = db.begin_transaction()?;
assert_eq!(txn.state()?, TxnState::Active);
txn.put(&cf, b"k", b"v")?;
txn.commit()?;
assert_eq!(txn.state()?, TxnState::Committed);
```

| `TxnState` | Meaning |
|------------|---------|
| `Active` | Buffering writes, not yet resolved |
| `Prepared` | Durably prepared under an xid, awaiting commit or rollback |
| `Committed` | Committed and applied |
| `Aborted` | Rolled back, aborted or expired |

#### Timeouts

A transaction can be bounded in time, either for every transaction with `Config::txn_timeout_seconds`, or for one transaction with `set_timeout`, which overrides the database default:

```rust
use std::time::Duration;
use tidesdb::ErrorCode;

let mut txn = db.begin_transaction()?;
txn.set_timeout(Some(Duration::from_secs(5)))?; // measured from this call
txn.put(&cf, b"k", b"v")?;

// ... later ...
match txn.commit() {
    Err(e) if e.code() == Some(ErrorCode::TxnExpired) => println!("took too long"),
    other => other?,
}

// `None` clears the timeout
let mut txn = db.begin_transaction()?;
txn.set_timeout(None)?;
```

The transaction is not aborted in the background: its next operation notices the deadline and fails with `ErrorCode::TxnExpired` (until then `state()` still reports `Active`). Sub-second durations round up to one second. `set_timeout(None)` removes the timeout altogether; it does not fall back to `Config::txn_timeout_seconds`.

:::note[Timing]
Deadlines are checked against the engine's one-second clock, so expiry can land up to about two seconds after the nominal deadline.
:::

#### Aborting from Another Thread

A `Transaction` may be moved to another thread but not shared between threads. To abort it from elsewhere, take an `AbortHandle`. The handle is `Send + Sync` and `Clone`, and may outlive the transaction, but it borrows the database, so it cannot be passed to `std::thread::spawn` (which needs `'static`). Use `std::thread::scope`:

```rust
use tidesdb::ErrorCode;

let mut txn = db.begin_transaction()?;
txn.put(&cf, b"k", b"v")?;

let handle = txn.abort_handle();
std::thread::scope(|s| {
    s.spawn(move || handle.request_abort());
});

let err = txn.commit().unwrap_err();
assert_eq!(err.code(), Some(ErrorCode::TxnAborted));
```

`request_abort` only sets a flag: the owning thread's next operation on the transaction (including `commit`) aborts it and returns `ErrorCode::TxnAborted`, which is distinct from the engine's own `Conflict`. A transaction that has already prepared is not affected, and aborting a committed transaction does nothing.

### Snapshots and Point-in-Time Reads

#### Snapshots

A `Snapshot` names a point in time that transactions can read at:

```rust
let snap = db.snapshot()?;
println!("snapshot at sequence {}", snap.seq());

// ... more writes happen ...

let old = db.begin_transaction_at_snapshot(&snap)?;
let value = old.get(&cf, b"k")?; // value as of the snapshot
```

The transaction runs at `RepeatableRead` with its snapshot pinned to the snapshot's sequence. It is not read-only: writes are accepted, and its commit validates its reads against that older point in time, so it is likely to conflict if anything it read has changed since. The transaction borrows the snapshot, which must outlive it. A snapshot is `Send + Sync`, so several threads can begin transactions at the same one:

```rust
let snap = db.snapshot()?;
std::thread::scope(|s| {
    for _ in 0..4 {
        s.spawn(|| {
            let txn = db.begin_transaction_at_snapshot(&snap).unwrap();
            let _ = txn.get(&cf, b"k");
        });
    }
});
```

:::caution[Cost]
While a snapshot lives, the versions it can see are protected from compaction, which costs the same as a long-running transaction. Drop it as soon as the point in time is no longer needed.
:::

#### Reading at a Sequence Number

Every transaction reads below a sequence ceiling, returned by `read_snapshot`:

- `u64::MAX` for `ReadUncommitted`
- the current sequence for `ReadCommitted`
- the sequence frozen at begin for `RepeatableRead` and stronger

That sequence can be used later to begin a transaction that reads at the same point (a `RepeatableRead` transaction, like one begun at a `Snapshot`):

```rust
use tidesdb::{ErrorCode, IsolationLevel};

// hold the point in time open with a repeatable read transaction
let holder = db.begin_transaction_with_isolation(IsolationLevel::RepeatableRead)?;
let seq = holder.read_snapshot();

// ... more writes happen ...

assert!(db.oldest_readable_seq() <= seq);
let at = db.begin_transaction_at_seq(seq)?;
let value = at.get(&cf, b"k")?;
```

`begin_transaction_at_seq` returns `ErrorCode::TooOld` once compaction has discarded the versions that sequence resolves to. A sequence stays readable only while an open transaction or a `Snapshot` holds it. `oldest_readable_seq()` is the oldest sequence it still accepts; it only rises while the database is open. It is not persisted: after a reopen it starts again from 0, so do not keep sequence numbers across a restart and expect `TooOld` to protect you. A sequence above the current one is treated as the current one.

### Two-Phase Commit

`prepare` runs the commit's conflict checks and durably logs the write batch under a transaction id (`xid`), leaving the writes invisible until `commit_prepared` or `rollback_prepared`:

```rust
use tidesdb::TxnState;

let mut txn = db.begin_transaction()?;
txn.put(&cf, b"k", b"v")?;

txn.prepare(b"xid-1")?;
assert_eq!(txn.state()?, TxnState::Prepared);

// ... coordinator decides ...
txn.commit_prepared()?;     // or: txn.rollback_prepared()?;
```

`commit_prepared` on a transaction that was not prepared returns `ErrorCode::InvalidArgs`. A transient I/O failure in `commit_prepared` leaves the transaction prepared so the call can be retried. The xid must be non-empty.

#### Recovering Prepared Transactions

Dropping a prepared transaction without resolving it leaves it in doubt on disk. After the next open, `recover_prepared` lists every such transaction as a `PreparedTransaction { xid, txn }`, with `txn` in the `Prepared` state:

```rust
let db = TidesDB::open(Config::new("./mydb"))?;

// `should_commit` stands for your coordinator's decision for this xid
for mut p in db.recover_prepared()? {
    if should_commit(&p.xid) {
        p.txn.commit_prepared()?;
    } else {
        p.txn.rollback_prepared()?;
    }
}
```

:::caution[Resolve each entry once]
The set is fixed when the database opens: calling `recover_prepared` again before reopening lists the same transactions, including ones already resolved. Resolve each entry once; committing a stale entry applies its batch again, over any newer writes.
:::

### Iterating Over Data

Iterators are bidirectional cursors over a column family at the transaction's snapshot. Positioning calls (`seek_to_first`, `seek_to_last`, `seek`, `seek_for_prev`, `next`, `prev`) succeed when they run off either end; check `is_valid()` to see whether the iterator is on an entry. `seek` and `seek_for_prev` with an empty key fail with `InvalidArgs`.

#### Forward Iteration

```rust
let txn = db.begin_transaction()?;
let mut iter = txn.new_iterator(&cf)?;

iter.seek_to_first()?;
while iter.is_valid() {
    let (key, value) = iter.key_value()?;
    println!("{:?} = {:?}", key, value);
    iter.next()?;
}
```

#### Backward Iteration

```rust
let mut iter = txn.new_iterator(&cf)?;
iter.seek_to_last()?;
while iter.is_valid() {
    let key = iter.key()?;
    println!("{:?}", key);
    iter.prev()?;
}
```

#### Seek Operations

```rust
let mut iter = txn.new_iterator(&cf)?;

// first key >= b"user:"
iter.seek(b"user:")?;
while iter.is_valid() {
    let key = iter.key()?;
    if !key.starts_with(b"user:") {
        break;
    }
    println!("{}", String::from_utf8_lossy(&key));
    iter.next()?;
}

// last key <= b"m"
iter.seek_for_prev(b"m")?;
if iter.is_valid() {
    println!("{:?}", iter.key()?);
}
```

#### Iterator Methods

| Method | Description |
|--------|-------------|
| `seek_to_first()` | Position at the first key |
| `seek_to_last()` | Position at the last key |
| `seek(key)` | Position at the first key >= `key` |
| `seek_for_prev(key)` | Position at the last key <= `key` |
| `next()` / `prev()` | Move to the next / previous entry |
| `is_valid()` | Whether the iterator is on an entry |
| `key()` | `Result<Vec<u8>>`, the current key |
| `value()` | `Result<Vec<u8>>`, the current value |
| `key_value()` | `Result<(Vec<u8>, Vec<u8>)>`, both in one call |
| `expires_at()` | `Result<Option<i64>>`, the current entry's absolute expiry in Unix seconds, `None` if it never expires |

Calling `key()`, `value()` or the others when the iterator is not valid returns an error (`NotFound`).

#### Range Iterators

`new_range_iterator(&cf, lower, upper)` only opens the sstables overlapping `[lower, upper]` (both bounds inclusive; the memtables are always included), which is much cheaper than `new_iterator` for a narrow scan of a large family:

```rust
let txn = db.begin_transaction()?;
let mut iter = txn.new_range_iterator(&cf, b"k010", b"k019")?;

iter.seek(b"k010")?;
while iter.is_valid() {
    let key = iter.key()?;
    if key.as_slice() > b"k019".as_slice() {
        break; // stop at the upper bound yourself
    }
    println!("{}", String::from_utf8_lossy(&key));
    iter.next()?;
}
```

:::caution[A promise, not a fence]
The range is a promise, not a fence: results are only defined inside it, and seeking or stepping outside it may report a key that exists as absent. Seek inside the range and stop at the bound yourself.
:::

#### Iterators and Commit

An iterator borrows its transaction, and `commit` takes `&mut self`, so the iterator must be dropped before the transaction commits. Scope it:

```rust
let mut txn = db.begin_transaction()?;
{
    let mut iter = txn.new_iterator(&cf)?;
    iter.seek_to_first()?;
    while iter.is_valid() {
        // ... inspect entries ...
        iter.next()?;
    }
} // iter dropped
txn.put(&cf, b"summary", b"done")?;
txn.commit()?;
```

At `RepeatableRead` and `Serializable`, a scan is also what records the scanned range for conflict detection. The range is recorded when the iterator is dropped, which is one more reason to drop it before committing.

### Commit Hooks (Change Data Capture)

A commit hook is a callback that receives every committed batch for a column family:

```rust
use std::sync::{Arc, Mutex};
use tidesdb::CommitOp;

let log: Arc<Mutex<Vec<CommitOp>>> = Arc::default();
let sink = Arc::clone(&log);

cf.set_commit_hook(move |ops: &[CommitOp], commit_seq: u64| {
    for op in ops {
        if op.is_delete {
            println!("[{commit_seq}] delete {:?}", op.key);
        } else {
            println!("[{commit_seq}] put {:?} = {:?} (expires {:?})", op.key, op.value, op.expires_at);
        }
    }
    sink.lock().unwrap().extend_from_slice(ops);
    0 // non-zero reports a failure
})?;

// ... commits ...

cf.clear_commit_hook()?;
```

`CommitOp` fields:

| Field | Type | Description |
|-------|------|-------------|
| `key` | `Vec<u8>` | The key, or the inclusive lower bound of a range delete |
| `value` | `Option<Vec<u8>>` | The value; `None` for every kind of delete |
| `range_end` | `Option<Vec<u8>>` | The exclusive upper bound of a range or prefix delete (every key in `[key, range_end)` was deleted); `None` otherwise |
| `expires_at` | `Option<i64>` | Absolute expiry in Unix seconds as the engine stored it (not the `Duration` passed to `put_with_ttl`); `None` if it never expires |
| `is_delete` | `bool` | Whether this is a delete (point, range or prefix) |

How each write arrives:

| Write | `key` | `value` | `range_end` | `is_delete` |
|-------|-------|---------|-------------|-------------|
| `put`, `put_with_ttl` | the key | `Some(value)` | `None` | `false` |
| `delete`, `single_delete` | the key | `None` | `None` | `true` |
| `delete_range(lo, Some(hi))` | `lo` | `None` | `Some(hi)` | `true` |
| `delete_prefix(p)` | `p` | `None` | `Some(first key past p)`, e.g. `b"user;"` for `b"user:"` | `true` |
| `delete_range(lo, None)` | `lo` | `None` | `None` | `true` |

A consumer replicating the hook's ops applies a range delete as "delete every key in `[key, range_end)`".

Behavior:

- The hook fires synchronously on the committing thread after every commit (and every `commit_prepared`) that touches the family, with the family's whole batch, once the commit is logged, applied and visible. It is not a durability signal: under `SyncMode::None` or `SyncMode::Interval` the write-ahead log may not be on disk yet.
- Each commit gets a larger `commit_seq` than the one before it, but commits on different threads may run the hook concurrently, so calls can arrive out of `commit_seq` order. The closure must be `Fn + Send + Sync + 'static`.
- A range delete with no upper bound (and a prefix delete of a prefix made only of `0xFF` bytes, which has no key past it) arrives exactly like a point delete of its lower bound: the C library passes nothing that tells them apart. A `single_delete` arrives like a `delete`.
- A non-zero return is logged by the engine but does not undo the commit. A panic is caught and counts as a failure.
- The hook belongs to the column family, not the handle: it stays installed after the handle is dropped, until it is replaced, cleared, or the database closes. `set_commit_hook` replaces any existing hook; replaced hooks are kept alive until close.

## Maintenance

### Memtable Flush

The memtable is shared by every column family, so flushing is a database operation:

```rust
db.flush_memtable()?; // rotate and flush; waits up to 60 s for the queue to drain
if db.is_flushing() {
    println!("a flush is in progress");
}
```

`flush_memtable` returns `ErrorCode::Locked` if the flush queue has not drained after 60 seconds; the flush itself carries on. `is_flushing` is true while an immutable memtable is queued or being flushed.

### Compaction

```rust
use tidesdb::ErrorCode;

// one forced compaction pass, merging even when no trigger is due
match cf.compact() {
    Ok(()) => {}
    Err(e) if e.is_locked() => println!("a compaction is already running"),
    Err(e) => return Err(e),
}

// compact every sstable overlapping [b"a", b"n")
cf.compact_range(Some(b"a"), Some(b"n"))?;

// from b"m" to the end
cf.compact_range(Some(b"m"), None)?;

if cf.is_compacting() {
    println!("compaction in progress");
}
```

Both calls run synchronously, and both return `ErrorCode::Locked` without waiting if the family is already compacting or being reconfigured or renamed; retry later. `compact_range` also returns `Locked` if the sstable layout changes while it plans, and does nothing (successfully) when fewer than two sstables overlap the range. `compact_range(None, None)` is rejected with `InvalidArgs`; use `compact()` instead. `is_compacting` reports the same claim, so it is also true during a runtime configuration update or a rename.

### WAL Sync

```rust
db.sync_wal()?;
```

Forces an fsync of the write-ahead log, for explicit durability control under `SyncMode::None` or `SyncMode::Interval`.

### Checkpoint

```rust
db.checkpoint()?;
```

`checkpoint` is a durability barrier: it flushes the memtable to L1, then forces the value log, write-ahead log and manifest to disk regardless of the configured sync mode. It does not write a copy anywhere; use `backup` for that.

### Backup

```rust
db.backup("./mydb-backup")?;

// the backup is a regular database directory
let restored = TidesDB::open(Config::new("./mydb-backup"))?;
```

Writes a consistent, directly openable copy of the database into the directory, which must be absent or empty. A non-empty directory is refused with `ErrorCode::Io`; the parent directory must already exist.

### Runtime Configuration Updates

```rust
use tidesdb::ColumnFamilyConfig;

let updated = ColumnFamilyConfig::default()
    .l1_file_count_trigger(8)
    .bloom_fpr(0.005);
cf.update_runtime_config(&updated, true)?; // true = persist to the manifest
```

Every `ColumnFamilyConfig` field can be changed at runtime, since keys are always ordered byte-wise and sstables stay mergeable. With `persist_to_disk = true` the configuration is written to the manifest; with `false` it lasts until the database is reopened, unless a later `rename_column_family` writes the family's manifest entry, which then saves the in-memory configuration too. Start from `cf.get_stats()?.config` to change one field and keep the rest.

The update returns `ErrorCode::Locked` without waiting if the family is compacting or under another reconfiguration or rename. The new configuration is validated as on create; `InvalidArgs` means one of: `level_size_ratio` below 2, `min_levels` of 0 or less, `bloom_fpr` outside `(0, 1)` with bloom filters enabled, `tombstone_density_trigger` outside `[0, 1]`, or a codec this build does not support.

### Open File Limit

```rust
let ceiling = tidesdb::raise_open_file_limit(8192);
println!("open-file ceiling is now {ceiling}");
let db = TidesDB::open(Config::new("./mydb").max_open_sstables(4096))?;
```

TidesDB reads the process's open-file ceiling at open and lowers `max_open_sstables` to fit it. `raise_open_file_limit(desired)` raises the ceiling toward `desired` and returns the ceiling in effect afterwards; call it before `TidesDB::open`. Passing `0` or a negative value only reports the current ceiling. A failed or partial raise is not an error.

## Statistics

### Column Family Statistics

```rust
let stats = cf.get_stats()?;

println!("levels: {}", stats.num_levels);
println!("keys: {}  (unflushed: {})", stats.total_keys, stats.unflushed_key_count);
println!("data size: {} bytes", stats.total_data_size);
println!("read amp: {:.2}", stats.read_amp);
println!("tombstone ratio: {:.2}", stats.tombstone_ratio);

for (i, size) in stats.level_sizes.iter().enumerate() {
    println!(
        "L{}: {} bytes, {} sstables, {} keys",
        i + 1,
        size,
        stats.level_num_sstables[i],
        stats.level_key_counts[i]
    );
}

println!("config: {:?}", stats.config);
```

`CfStats` fields:

| Field | Type | Description |
|-------|------|-------------|
| `name` | `String` | The family's current name (reflects renames, unlike `ColumnFamily::name()`) |
| `num_levels` | `i32` | Deepest level that currently holds an sstable (0 for a family with nothing flushed) |
| `config` | `ColumnFamilyConfig` | A copy of the column family configuration |
| `level_sizes` | `Vec<usize>` | On-disk size of each level; index 0 is L1 |
| `level_num_sstables` | `Vec<i32>` | Sstable count of each level |
| `level_key_counts` | `Vec<u64>` | Key count of each level |
| `level_tombstone_counts` | `Vec<u64>` | Tombstone count of each level |
| `total_keys` | `u64` | Sum of each sstable's distinct key count (a key present in several sstables counts once per sstable) |
| `total_data_size` | `u64` | The family's own on-disk size (sum of its key logs); separated values are counted in `DbStats::vlog_file_size` |
| `avg_key_size` | `f64` | Average key length in bytes |
| `avg_value_size` | `f64` | Average value length in bytes |
| `read_amp` | `f64` | Point-lookup read amplification (sstables a worst-case get may probe) |
| `btree_total_nodes` | `u64` | Total btree nodes |
| `btree_max_height` | `u32` | Maximum btree height |
| `btree_avg_height` | `f64` | Average btree height |
| `total_tombstones` | `u64` | Total tombstones across every sstable |
| `tombstone_ratio` | `f64` | `total_tombstones / total_keys` |
| `max_sst_density` | `f64` | Worst per-sstable tombstone density observed |
| `max_sst_density_level` | `i32` | 1-based level of `max_sst_density`, 0 if none |
| `wal_bytes_written` | `u64` | This family's attributed share of write-ahead log bytes |
| `flush_bytes_written` | `u64` | On-disk bytes this family's flushes wrote |
| `compaction_bytes_written` | `u64` | On-disk bytes this family's compactions wrote |
| `compaction_bytes_read` | `u64` | On-disk bytes this family's compactions read |
| `user_bytes_written` | `u64` | Logical key+value bytes committed to this family |
| `compaction_count` | `u64` | Compactions this family has run |
| `unflushed_key_count` | `u64` | Distinct keys still only in the shared memtables |
| `filter_resident_bytes` | `u64` | Memory held by this family's bloom filter directories outside the block cache |

The `level_*` vectors all have `num_levels` entries (empty when nothing is flushed); a level between L1 and the deepest one that holds nothing shows as zeros. Memtable figures are database-level and live in `DbStats`.

### Cardinality and Range Statistics

```rust
let distinct = cf.estimate_cardinality()?;
println!("about {distinct} distinct keys");

let rs = cf.range_stats(b"k000", b"k100")?; // [key_a, key_b)
println!(
    "{} sorted runs, {} keys ({})",
    rs.sstables_overlapping,
    rs.estimated_keys,
    if rs.keys_exact { "exact" } else { "estimated" }
);
```

`range_stats` describes the key range `[key_a, key_b)` for a query planner. `RangeStats` fields:

| Field | Type | Description |
|-------|------|-------------|
| `sstables_overlapping` | `u64` | Sstables overlapping the range, counting each one even when several share a level |
| `estimated_keys` | `u64` | Live keys the range holds |
| `keys_exact` | `bool` | Whether `estimated_keys` was counted exactly rather than estimated |

### Database Statistics

```rust
let s = db.get_db_stats()?;
println!("column families: {}", s.num_column_families);
println!("sequence: {}", s.global_seq);
println!("commits: {}  conflicts: {}", s.txn_commits, s.txn_conflicts);
println!("memtable: {} bytes, flushing: {}", s.memtable_bytes, s.is_flushing);
println!("sstables: {}, open: {}", s.total_sstable_count, s.num_open_sstables);
println!("vlog: {} bytes in {} segments", s.vlog_file_size, s.vlog_segment_count);
```

`DbStats` fields:

| Field | Type | Description |
|-------|------|-------------|
| `num_column_families` | `i32` | Number of column families |
| `immutable_memtable_count` | `i32` | Immutable memtables awaiting flush (the L0 queue depth) |
| `compaction_pending_count` | `i32` | Compaction jobs queued |
| `total_sstable_count` | `i32` | Total sstables across every column family and level |
| `total_data_size_bytes` | `u64` | On-disk key-log bytes across every column family |
| `num_open_sstables` | `i32` | Currently open sstable file handles |
| `global_seq` | `u64` | Current global sequence number (the MVCC clock) |
| `min_snapshot_seq` | `u64` | Oldest frozen snapshot any live transaction holds |
| `active_txn_count` | `i32` | Live registered transactions (read committed and stronger) |
| `txn_memory_bytes` | `i64` | Bytes held by in-flight transactions |
| `txn_commits` | `u64` | Transactions committed since open |
| `txn_conflicts` | `u64` | Commits and prepares refused with a conflict since open |
| `memtable_bytes` | `usize` | Memory the active memtable occupies |
| `is_flushing` | `bool` | Whether an immutable memtable is queued or flushing |
| `next_cf_index` | `u32` | Next column family id to be assigned |
| `wal_generation` | `u64` | Generation of the active write-ahead log |
| `flush_count` | `u64` | Sstables flushed across every column family |
| `compaction_count` | `u64` | Compactions run across every column family |
| `flush_bytes_written` | `u64` | Flush output bytes |
| `compaction_bytes_written` | `u64` | Compaction output bytes |
| `compaction_bytes_read` | `u64` | Compaction input bytes |
| `wal_bytes_written` | `u64` | Framed write-ahead log bytes, measured at the append |
| `user_bytes_written` | `u64` | Logical committed bytes |
| `vlog_file_size` | `u64` | Total value-log file size |
| `vlog_value_count` | `u64` | Values currently indexed in the value log |
| `vlog_used_bytes` | `u64` | Uncompressed length the indexed values represent |
| `vlog_stored_bytes` | `u64` | On-disk length of the indexed values |
| `vlog_live_bytes` | `u64` | Value-log bytes the live sstables still reference |
| `vlog_segment_count` | `u64` | Open value-log segment files |
| `vlog_bytes_written` | `u64` | Value-log bytes ever appended, reclamation rewrites included |
| `vlog_dead_bytes` | `u64` | Value-log bytes beyond what live values account for |
| `vlog_reclaim_calls` | `u64` | Value-log reclaims attempted |
| `vlog_reclaim_passes` | `u64` | Value-log reclaim passes that drained a segment |
| `vlog_segments_retired` | `u64` | Value-log segment files a reclaim has unlinked |
| `vlog_segments_drainable` | `u64` | Sealed segments worth draining |
| `writes_throttled` | `u64` | Commits the L0 admission policy made dwell |
| `writes_blocked` | `u64` | Commits the L0 admission policy made wait for the flush queue |
| `write_stall_us` | `u64` | Total microseconds commits were held in L0 admission |
| `write_stall_ceiling_hits` | `u64` | Commits admitted only because the admission wait ceiling expired |

### Cache Statistics

```rust
let c = db.get_cache_stats()?;
if c.enabled {
    println!(
        "{} entries, {} bytes, hit rate {:.1}% ({} hits / {} misses), {} partitions",
        c.total_entries, c.total_bytes, c.hit_rate * 100.0, c.hits, c.misses, c.num_partitions
    );
}
```

| Field | Type | Description |
|-------|------|-------------|
| `enabled` | `bool` | Whether the cache is enabled |
| `total_entries` | `usize` | Number of cached entries |
| `total_bytes` | `usize` | Bytes used by the cache |
| `hits` | `u64` | Cache hits |
| `misses` | `u64` | Cache misses |
| `hit_rate` | `f64` | `hits / (hits + misses)`, 0 before any lookup |
| `num_partitions` | `usize` | Number of cache shards |

### Stall Statistics

`get_stall_stats` reports where writers have been made to wait since the database opened. Each reason is a `StallStat { count, total_us, max_us }` (number of waits, summed wait in microseconds, longest single wait in microseconds):

```rust
let stalls = db.get_stall_stats()?;
for (reason, stat) in stalls.iter() {
    println!("{reason:>16}: {} waits, {} us total, {} us max", stat.count, stat.total_us, stat.max_us);
}
println!("admission waits: {}", stalls.admission.count);
```

| `StallStats` field | Description |
|--------------------|-------------|
| `wal_append` | Waiting on the write-ahead log (staging space or, under a syncing mode, the write) |
| `rotate_lock` | Waiting to take the rotation lock while another committer rotated |
| `rotate_work` | Performing a memtable rotation on everyone's behalf |
| `admission` | Held by write admission because the unflushed backlog was too deep |
| `manifest_commit` | Inside a manifest commit |

`iter()` yields every reason with the library's stable short name, for logging or a stats table.

### I/O Statistics

`get_io_stats` reports what each class of file asked of the device since the database opened. Each class is an `IoStat { ops, bytes, total_us, max_us }` (writes issued, bytes written, summed time inside those writes, slowest single write; times in microseconds):

```rust
let io = db.get_io_stats()?;
for (class, stat) in io.iter() {
    println!("{class:>8}: {} writes, {} bytes, {} us max", stat.ops, stat.bytes, stat.max_us);
}
```

| `IoStats` field | Description |
|-----------------|-------------|
| `sstable` | Key logs, written by flush and compaction |
| `wal` | The write-ahead log |
| `vlog` | Value log segments |

`iter()` yields every class with the library's stable short name.

### Encoding Statistics

`get_klog_encoding_stats` reports what each encoding chain achieved on the key logs, one entry per chain the live sstables were written with. `get_vlog_encoding_stats` does the same for the separated values in the value log.

```rust
use tidesdb::CompressionAlgorithm;

for e in db.get_klog_encoding_stats()? {
    let chain: Vec<_> = e.ids.iter().map(|id| CompressionAlgorithm::from_id(*id)).collect();
    println!(
        "{:?}: {} -> {} bytes over {} items, ratio {:.2}",
        chain, e.logical_bytes, e.stored_bytes, e.item_count, e.ratio()
    );
}
```

| `EncodingStats` field | Type | Description |
|-----------------------|------|-------------|
| `ids` | `Vec<u8>` | Encoding ids in the order applied; empty when stored verbatim. Ids `1..=4` are `CompressionAlgorithm` values |
| `logical_bytes` | `u64` | Size of the data before encoding |
| `stored_bytes` | `u64` | Size of the data on disk |
| `item_count` | `u64` | Values for the value log, sstables for the key logs |

`ratio()` returns `logical_bytes / stored_bytes`, or 0 when nothing is stored.

## Configuration

### Sync Modes

The write-ahead log is shared, so durability is set once per database with `Config::memtable_sync_mode`:

```rust
use tidesdb::{Config, SyncMode};

let config = Config::new("./mydb")
    .memtable_sync_mode(SyncMode::Interval)
    .memtable_sync_interval_us(100_000); // 100 ms
```

| `SyncMode` | Behavior |
|------------|----------|
| `SyncMode::None` | Nothing at commit; fastest, a crash of the process or the machine may lose commits (default) |
| `SyncMode::Full` | fsync every commit; slowest, no commit loss on crash |
| `SyncMode::Interval` | fsync on a background interval (`memtable_sync_interval_us`); only a machine crash can lose the last interval |

Under `None` or `Interval`, call `db.sync_wal()` or `db.checkpoint()` to force durability at a point of your choosing.

### Compression

Compression is configured per column family as an **encoding pipeline**: up to eight stages applied in order to key-log nodes and separated values. The default pipeline is empty, which stores data verbatim (no compression).

```rust
use tidesdb::{ColumnFamilyConfig, CompressionAlgorithm};

// one stage
let lz4 = ColumnFamilyConfig::new().compression_algorithm(CompressionAlgorithm::Lz4);

// explicit pipeline
let zstd = ColumnFamilyConfig::new().encoding_pipeline(vec![CompressionAlgorithm::Zstd]);

// no compression (clears the pipeline)
let plain = ColumnFamilyConfig::new().compression_algorithm(CompressionAlgorithm::None);
```

| `CompressionAlgorithm` | Cargo feature |
|------------------------|---------------|
| `None` | always available |
| `Snappy` | `snappy` |
| `Lz4` | `lz4` |
| `Lz4Fast` | `lz4` |
| `Zstd` | `zstd` |

Every variant is always defined, but a codec is only usable when the C library was built with it. Check before choosing one:

```rust
use tidesdb::{ColumnFamilyConfig, CompressionAlgorithm};

let algo = if CompressionAlgorithm::Zstd.is_available() {
    CompressionAlgorithm::Zstd
} else {
    CompressionAlgorithm::None
};
db.create_column_family("logs", ColumnFamilyConfig::new().compression_algorithm(algo))?;
```

`CompressionAlgorithm::from_id(u8)` maps an encoding id (as found in `EncodingStats::ids`) back to the algorithm. A pipeline of more than eight stages is rejected with `InvalidArgs` when the column family is created or updated; `None` entries in a pipeline are skipped.

### Log Levels

| `LogLevel` | Emits |
|------------|-------|
| `LogLevel::None` | Nothing |
| `LogLevel::Trace` | Highly detailed messages for technical debugging, and everything below |
| `LogLevel::Info` | Engine status and operations, and everything below (default) |
| `LogLevel::Warn` | Non-imminent errors that require awareness, and errors |
| `LogLevel::Error` | Failed operations only |

Logs go to stderr unless `log_to_file(true)` is set, in which case they go to `<db_path>/LOG`. Logging is process-wide: the level is set by the most recently opened database, and the last database opened with `log_to_file` owns the log file until it closes. `log_truncation_at` only applies with `log_to_file`.

## Database Configuration Reference

`Config` holds the settings shared by every column family: the memtable, write-ahead log, value log, block cache and worker pool. Every field is public and has a builder method of the same name. `Config::new(path)` and `Config::default()` take their values from the C library's defaults; `Config::default()` leaves `db_path` empty.

A numeric field set to `0` is resolved to the library default at open for the thread counts, `block_cache_size`, `max_open_sstables`, `value_separation_threshold`, `vlog_segment_size`, `memtable_write_buffer_size` and the memtable skip-list settings.

| Field / builder | Type | Default | Description |
|-----------------|------|---------|-------------|
| `db_path` | `String` | set by `Config::new` | Path to the database directory |
| `num_flush_threads(n)` | `i32` | 2 | Number of flush worker threads |
| `num_compaction_threads(n)` | `i32` | 2 | Number of compaction worker threads |
| `log_level(level)` | `LogLevel` | `Info` | Minimum severity to log (process-wide; the last database opened sets it) |
| `block_cache_size(size)` | `usize` | 64 MiB | Size in bytes of the block cache for hot sstable blocks |
| `max_open_sstables(n)` | `usize` | 1024 | Maximum concurrently open sstable file handles; lowered at open to fit the process's open-file ceiling (see `raise_open_file_limit`) |
| `log_to_file(enable)` | `bool` | `false` | Write the log to `<db_path>/LOG` instead of stderr (process-wide sink) |
| `log_truncation_at(size)` | `usize` | 0 | Size in bytes past which the log file is truncated (0 = never; only with `log_to_file`) |
| `memtable_write_buffer_size(size)` | `usize` | 64 MiB | Memory the active memtable may occupy before it is rotated (0 = auto) |
| `memtable_skip_list_max_level(level)` | `i32` | 12 | Memtable skip list max level (0 = default) |
| `memtable_skip_list_probability(prob)` | `f32` | 0.25 | Memtable skip list level probability (0 = default) |
| `memtable_sync_mode(mode)` | `SyncMode` | `None` | Durability mode for the write-ahead log |
| `memtable_sync_interval_us(interval)` | `u64` | 1,000,000 (1 s) | fsync interval in microseconds for `SyncMode::Interval` |
| `value_separation_threshold(size)` | `usize` | 1024 | Values at or above this size go to the shared value log (0 = default). Keep it at or under a quarter of a column family's `btree_klog_block_size` |
| `vlog_segment_size(size)` | `usize` | 256 MiB | Size at which the value log seals a segment and opens a new one (0 = default) |
| `memtable_l0_queue_stall_threshold(n)` | `i32` | 16 | Immutable-memtable queue depth at which writes stall for backpressure (0 = never) |
| `memtable_idle_flush_seconds(secs)` | `i32` | 30 | Seconds the active memtable may sit unwritten before it is rotated (0 = never) |
| `txn_timeout_seconds(secs)` | `i64` | 0 | Seconds a transaction may stay active before its next operation expires it (0 = no timeout); override per transaction with `set_timeout` |

The defaults listed are those of TidesDB 10.2.1 and are read from the library at runtime.

## Column Family Configuration Reference

Every field is public and has a builder method of the same name, plus `compression_algorithm(algo)` as a shorthand for a one-stage `encoding_pipeline`. `ColumnFamilyConfig::new()` and `ColumnFamilyConfig::default()` take their values from the C library. Every field can be changed at runtime with `ColumnFamily::update_runtime_config`.

| Field / builder | Type | Default | Description |
|-----------------|------|---------|-------------|
| `level_size_ratio(ratio)` | `usize` | 10 | Target size ratio between successive levels |
| `min_levels(levels)` | `i32` | 1 | Floor on the level count |
| `dividing_level_offset(offset)` | `i32` | 1 | How far above the largest level the dividing level sits |
| `keep_values_inline(enable)` | `bool` | `false` | Keep every value in the key log regardless of the database's `value_separation_threshold` |
| `btree_klog_block_size(size)` | `usize` | 4096 | Target btree key-log node size in bytes (0 = let the btree choose its own, larger, default) |
| `encoding_pipeline(vec)` | `Vec<CompressionAlgorithm>` | empty | Encodings applied in order to key-log nodes and separated values, at most 8 stages; empty stores data verbatim |
| `compression_algorithm(algo)` | (sets `encoding_pipeline`) | | One-stage pipeline; `CompressionAlgorithm::None` clears it |
| `enable_bloom_filter(enable)` | `bool` | `true` | Build bloom filters for point-get pruning |
| `bloom_fpr(fpr)` | `f64` | 0.01 | Target bloom filter false-positive rate |
| `default_isolation_level(level)` | `IsolationLevel` | `ReadCommitted` | Isolation level for transactions begun with `begin_transaction_cf` |
| `l1_file_count_trigger(n)` | `i32` | 4 | L1 sstable count that triggers compaction (0 = this trigger is off) |
| `tombstone_density_trigger(ratio)` | `f64` | 0.5 | Tombstone density in `[0, 1]` at or above which an sstable escalates compaction (0 = disabled) |
| `tombstone_density_min_entries(n)` | `u64` | 4096 | Minimum entries for an sstable to be judged by the density trigger (0 = no minimum) |

`ColumnFamilyConfig` implements `PartialEq`, so a configuration read back through `cf.get_stats()?.config` can be compared with the one you created.

## Error Handling

All fallible calls return `tidesdb::Result<T>`, an alias for `Result<T, tidesdb::Error>`.

```rust
use tidesdb::{Error, ErrorCode};

match txn.get(&cf, b"key") {
    Ok(value) => println!("{value:?}"),
    Err(e) if e.is_not_found() => println!("not found"),
    Err(Error::TidesDB { code, context }) => {
        match code {
            ErrorCode::Io => eprintln!("I/O error in {context}"),
            ErrorCode::NoSpace => eprintln!("disk full; retry once space is freed"),
            _ => eprintln!("{context}: {code}"),
        }
    }
    Err(e) => eprintln!("{e}"),
}
```

### `Error`

| Variant | Description |
|---------|-------------|
| `Error::TidesDB { code: ErrorCode, context: &'static str }` | An error from the TidesDB C library. Displays as `"<context>: <library message>"` |
| `Error::NullPointer(&'static str)` | The library returned a null handle |
| `Error::InvalidString(Utf8Error)` | Invalid UTF-8 string |
| `Error::NulError(NulError)` | A name or path contained an interior NUL byte |

| Method | Description |
|--------|-------------|
| `code()` | `Option<ErrorCode>`, the library error code if the error came from the C library |
| `is_not_found()` | `code() == Some(ErrorCode::NotFound)` |
| `is_conflict()` | `code() == Some(ErrorCode::Conflict)` |
| `is_locked()` | `code() == Some(ErrorCode::Locked)`; transient contention that should be retried |
| `Error::from_code(code, context)` | Build an `Error::TidesDB` from a raw C error code |

### `ErrorCode`

`ErrorCode` is `#[non_exhaustive]`: new codes may be added in minor releases, so always include a wildcard arm when matching. Its `Display` uses the library's own message. `ErrorCode::from_code(i32)` converts a raw C code (unrecognised codes map to `Unknown`), and `code()` returns the raw `TDB_ERR_*` value.

| Variant | C code | Meaning |
|---------|--------|---------|
| `Memory` | -1 | Memory allocation failed |
| `InvalidArgs` | -2 | Invalid arguments provided |
| `NotFound` | -3 | Key or resource not found |
| `Io` | -4 | I/O error occurred |
| `Corruption` | -5 | Data corruption detected |
| `Exists` | -6 | Resource already exists |
| `Conflict` | -7 | Another transaction committed a conflicting write first; retry the transaction |
| `TooLarge` | -8 | A value does not fit the space that has to hold it |
| `MemoryLimit` | -9 | Memory limit exceeded |
| `InvalidDb` | -10 | Invalid database handle, or the database is closing |
| `Unknown` | -11 | Unknown error; also what `ErrorCode::from_code` returns for an unrecognised code |
| `Locked` | -12 | Transient contention: nothing was written, retry. Never means absence or data loss. Also returned when opening a directory another handle has open |
| `ReadOnly` | -13 | Read-only database; defined by TidesDB 10 but not returned by any operation |
| `TxnExpired` | -14 | The transaction outlived its timeout and was aborted |
| `NoSpace` | -15 | The device or filesystem is out of space; retrying succeeds once space is freed |
| `TxnAborted` | -16 | The transaction was aborted from another thread with `AbortHandle::request_abort` |
| `TooOld` | -17 | The requested point in time is no longer reconstructable |

## Complete Example

```rust
use std::time::Duration;
use tidesdb::{
    ColumnFamilyConfig, CompressionAlgorithm, Config, IsolationLevel, LogLevel, SyncMode, TidesDB,
};

fn main() -> tidesdb::Result<()> {
    let config = Config::new("./example_db")
        .log_level(LogLevel::Warn)
        .memtable_sync_mode(SyncMode::Interval)
        .memtable_sync_interval_us(100_000);
    let db = TidesDB::open(config)?;

    let algo = if CompressionAlgorithm::Lz4.is_available() {
        CompressionAlgorithm::Lz4
    } else {
        CompressionAlgorithm::None
    };
    db.create_column_family(
        "users",
        ColumnFamilyConfig::new()
            .compression_algorithm(algo)
            .bloom_fpr(0.01),
    )?;
    let cf = db.get_column_family("users")?;

    // write
    let mut txn = db.begin_transaction()?;
    txn.put(&cf, b"user:1", b"Alice")?;
    txn.put(&cf, b"user:2", b"Bob")?;
    txn.put_with_ttl(&cf, b"session:abc", b"temp", Duration::from_secs(30))?;
    txn.commit()?;

    // read
    let txn = db.begin_transaction_with_isolation(IsolationLevel::RepeatableRead)?;
    let value = txn.get(&cf, b"user:1")?;
    println!("user:1 = {}", String::from_utf8_lossy(&value));

    // scan
    let mut iter = txn.new_iterator(&cf)?;
    iter.seek(b"user:")?;
    while iter.is_valid() {
        let (key, value) = iter.key_value()?;
        if !key.starts_with(b"user:") {
            break;
        }
        println!("  {} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        iter.next()?;
    }
    drop(iter);
    drop(txn);

    // maintenance and stats
    db.flush_memtable()?;
    let stats = cf.get_stats()?;
    println!("levels: {}, keys: {}", stats.num_levels, stats.total_keys);
    let db_stats = db.get_db_stats()?;
    println!("commits: {}", db_stats.txn_commits);

    Ok(())
}
```

## Thread Safety

| Type | `Send` | `Sync` | Notes |
|------|--------|--------|-------|
| `TidesDB` | yes | yes | Share by reference (e.g. `std::thread::scope`) or in an `Arc` |
| `ColumnFamily<'db>` | yes | yes | Cheap to `Clone` |
| `Snapshot<'db>` | yes | yes | Many threads may begin transactions at one snapshot |
| `Transaction<'db>` | yes | no | Move it to another thread, but do not share it; abort it from elsewhere with an `AbortHandle` |
| `AbortHandle<'db>` | yes | yes | `Clone`; may outlive its transaction but not the database |
| `Iterator<'txn>` | no | no | Use on the thread that owns the transaction |

Because handles borrow the database, `std::thread::scope` is the simplest way to share it:

```rust
use tidesdb::{TidesDB, Config, ColumnFamilyConfig};

fn main() -> tidesdb::Result<()> {
    let db = TidesDB::open(Config::new("./mydb"))?;
    db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
    let cf = db.get_column_family("my_cf")?;

    std::thread::scope(|s| {
        for t in 0..4 {
            let (db, cf) = (&db, &cf);
            s.spawn(move || {
                for i in 0..100 {
                    let mut txn = db.begin_transaction().unwrap();
                    txn.put(cf, format!("t{t}-{i}").as_bytes(), b"v").unwrap();
                    txn.commit().unwrap();
                }
            });
        }
    });

    Ok(())
}
```

With `Arc<TidesDB>` and `std::thread::spawn`, look up the column family inside each thread (`db.get_column_family(name)`), since a `ColumnFamily` borrowed from one thread's `Arc` cannot be sent to a `'static` thread.

## Utility Functions

### `tidesdb::TIDESDB_VERSION`

The TidesDB C library release this crate binds and bundles (`"10.2.1"`). See [Versioning](#versioning).

### `tidesdb::TIDESDB_LINKED_VERSION`

The TidesDB release actually linked: equal to `TIDESDB_VERSION` when the bundled source was built, or the version of the system TidesDB found through pkg-config.

### `tidesdb::raise_open_file_limit`

`fn raise_open_file_limit(desired: i64) -> i64`. See [Open File Limit](#open-file-limit).

## Testing

```bash
git submodule update --init
cargo test

# with every compression codec
cargo test --features compression

# on an alternative allocator (at most one)
cargo test --features jemalloc

# with output
cargo test -- --nocapture

# a single test
cargo test test_open_close

# release optimizations
cargo test --release
```
