// Package tidesdb
// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # TidesDB
//!
//! Safe Rust bindings for TidesDB, a high-performance embeddable key-value storage engine.
//!
//! TidesDB is written in C and based on a log-structured merge-tree (LSM-tree). This crate
//! binds TidesDB 10 and bundles its source, so no system install is needed. Crate majors map
//! to TidesDB majors: crate N.x binds TidesDB (N+9).x (1.x ↔ 10.x, 2.x ↔ 11.x).
//!
//! ## Features
//!
//! - MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
//! - Snapshots and point-in-time reads
//! - Two-phase commit (prepare / commit prepared / recovery)
//! - Column families sharing one memtable, write-ahead log and value log
//! - Range and prefix deletes that cost one entry however many keys they cover
//! - Bidirectional iterators, including range-bounded iterators for narrow scans
//! - TTL with automatic key expiration
//! - Optional LZ4, LZ4 Fast, Zstd and Snappy compression pipelines
//! - Bloom filters, a shared block cache, savepoints, commit hooks for change data capture
//! - Detailed statistics: per family, database, cache, stalls, I/O and encoding
//!
//! Keys are always ordered byte-wise. To sort by another order, encode keys so their bytes
//! compare in that order (for example big-endian integers).
//!
//! ## Example
//!
//! ```no_run
//! use tidesdb::{TidesDB, Config, ColumnFamilyConfig};
//! use std::time::Duration;
//!
//! fn main() -> tidesdb::Result<()> {
//!     let db = TidesDB::open(Config::new("./mydb"))?;
//!     db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
//!     let cf = db.get_column_family("my_cf")?;
//!
//!     // Write data in a transaction
//!     let mut txn = db.begin_transaction()?;
//!     txn.put(&cf, b"key1", b"value1")?;
//!     txn.put_with_ttl(&cf, b"session", b"abc", Duration::from_secs(60))?;
//!     txn.commit()?;
//!
//!     // Read it back
//!     let txn = db.begin_transaction()?;
//!     let value = txn.get(&cf, b"key1")?;
//!     println!("Value: {:?}", String::from_utf8_lossy(&value));
//!
//!     // Iterate
//!     let mut iter = txn.new_iterator(&cf)?;
//!     iter.seek_to_first()?;
//!     while iter.is_valid() {
//!         let (key, value) = iter.key_value()?;
//!         println!("{:?} = {:?}", key, value);
//!         iter.next()?;
//!     }
//!
//!     Ok(())
//! }
//! ```

mod config;
mod db;
mod error;
mod ffi;
mod iterator;
mod stats;
mod transaction;

#[cfg(test)]
mod tests;

pub use config::{
    ColumnFamilyConfig, CompressionAlgorithm, Config, IsolationLevel, LogLevel, SyncMode,
};
pub use db::{
    ColumnFamily, CommitOp, PreparedTransaction, Snapshot, TidesDB, raise_open_file_limit,
};
pub use error::{Error, ErrorCode, Result};
pub use iterator::Iterator;
pub use stats::{
    CacheStats, CfStats, DbStats, EncodingStats, IoStat, IoStats, RangeStats, StallStat, StallStats,
};
pub use transaction::{AbortHandle, Transaction, TxnState};

/// The TidesDB C library release this crate binds and bundles.
///
/// The crate's major version maps to a TidesDB major: crate N.x binds TidesDB (N+9).x, so
/// crate 1.x binds TidesDB 10.x and crate 2.x will bind TidesDB 11.x.
pub const TIDESDB_VERSION: &str = ffi::TIDESDB_VERSION;

/// The TidesDB release actually linked: [`TIDESDB_VERSION`] when the bundled source was built,
/// or the version of a system TidesDB found through pkg-config (same major, at least
/// [`TIDESDB_VERSION`]).
pub const TIDESDB_LINKED_VERSION: &str = env!("TIDESDB_LINKED_VERSION");
