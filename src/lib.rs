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
//! Safe Rust bindings for TidesDB - A high-performance embedded key-value storage engine.
//!
//! TidesDB is a fast and efficient key-value storage engine library written in C.
//! The underlying data structure is based on a log-structured merge-tree (LSM-tree).
//! This Rust binding provides a safe, idiomatic Rust interface to TidesDB with full
//! support for all features.
//!
//! ## Features
//!
//! - MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
//! - Column families (isolated key-value stores with independent configuration)
//! - Bidirectional iterators with forward/backward traversal and seek support
//! - TTL (time to live) support with automatic key expiration
//! - LZ4, LZ4 Fast, ZSTD, Snappy, or no compression
//! - Bloom filters with configurable false positive rates
//! - Global block CLOCK cache for hot blocks
//! - Savepoints for partial transaction rollback
//! - Six built-in comparators plus custom registration
//!
//! ## Example
//!
//! ```no_run
//! use tidesdb::{TidesDB, Config, ColumnFamilyConfig, IsolationLevel};
//!
//! fn main() -> tidesdb::Result<()> {
//!     // Open database
//!     let config = Config::new("./mydb")
//!         .num_flush_threads(2)
//!         .num_compaction_threads(2);
//!
//!     let db = TidesDB::open(config)?;
//!
//!     // Create a column family
//!     let cf_config = ColumnFamilyConfig::default();
//!     db.create_column_family("my_cf", cf_config)?;
//!
//!     // Get the column family
//!     let cf = db.get_column_family("my_cf")?;
//!
//!     // Write data in a transaction
//!     let txn = db.begin_transaction()?;
//!     txn.put(&cf, b"key1", b"value1", -1)?;
//!     txn.put(&cf, b"key2", b"value2", -1)?;
//!     txn.commit()?;
//!
//!     // Read data
//!     let txn = db.begin_transaction()?;
//!     let value = txn.get(&cf, b"key1")?;
//!     println!("Value: {:?}", String::from_utf8_lossy(&value));
//!
//!     // Iterate over data
//!     let mut iter = txn.new_iterator(&cf)?;
//!     iter.seek_to_first()?;
//!     while iter.is_valid() {
//!         let key = iter.key()?;
//!         let value = iter.value()?;
//!         println!("Key: {:?}, Value: {:?}",
//!             String::from_utf8_lossy(&key),
//!             String::from_utf8_lossy(&value));
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

// Re-export public types
pub use config::{
    ColumnFamilyConfig, CompressionAlgorithm, Config, IsolationLevel, LogLevel, SyncMode,
};
pub use db::{ColumnFamily, TidesDB};
pub use error::{Error, ErrorCode, Result};
pub use iterator::Iterator;
pub use stats::{CacheStats, Stats};
pub use transaction::Transaction;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    fn create_test_db() -> (TidesDB, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path())
            .num_flush_threads(2)
            .num_compaction_threads(2)
            .log_level(LogLevel::Info)
            .block_cache_size(64 * 1024 * 1024)
            .max_open_sstables(256);

        let db = TidesDB::open(config).unwrap();
        (db, temp_dir)
    }

    #[test]
    fn test_open_close() {
        let (db, _temp_dir) = create_test_db();
        drop(db);
    }

    #[test]
    fn test_create_drop_column_family() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();

        let cf = db.get_column_family("test_cf").unwrap();
        assert_eq!(cf.name(), "test_cf");

        let families = db.list_column_families().unwrap();
        assert!(families.contains(&"test_cf".to_string()));

        db.drop_column_family("test_cf").unwrap();
    }

    #[test]
    fn test_transaction_put_get_delete() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Put
        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"key", b"value", -1).unwrap();
        txn.commit().unwrap();

        // Get
        let txn = db.begin_transaction().unwrap();
        let value = txn.get(&cf, b"key").unwrap();
        assert_eq!(value, b"value");
        drop(txn);

        // Delete
        let txn = db.begin_transaction().unwrap();
        txn.delete(&cf, b"key").unwrap();
        txn.commit().unwrap();

        // Verify deleted
        let txn = db.begin_transaction().unwrap();
        let result = txn.get(&cf, b"key");
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_with_ttl() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Set TTL to 2 seconds from now
        let ttl = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 2;

        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"temp_key", b"temp_value", ttl).unwrap();
        txn.commit().unwrap();

        // Verify key exists before expiration
        let txn = db.begin_transaction().unwrap();
        let value = txn.get(&cf, b"temp_key").unwrap();
        assert_eq!(value, b"temp_value");
    }

    #[test]
    fn test_multi_operation_transaction() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Multiple operations in one transaction
        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"key1", b"value1", -1).unwrap();
        txn.put(&cf, b"key2", b"value2", -1).unwrap();
        txn.put(&cf, b"key3", b"value3", -1).unwrap();
        txn.commit().unwrap();

        // Verify all keys exist
        let txn = db.begin_transaction().unwrap();
        for i in 1..=3 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let value = txn.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(value, expected_value.as_bytes());
        }
    }

    #[test]
    fn test_transaction_rollback() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"rollback_key", b"rollback_value", -1).unwrap();
        txn.rollback().unwrap();

        // Verify key does not exist
        let txn = db.begin_transaction().unwrap();
        let result = txn.get(&cf, b"rollback_key");
        assert!(result.is_err());
    }

    #[test]
    fn test_savepoints() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        let txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"key1", b"value1", -1).unwrap();

        txn.savepoint("sp1").unwrap();
        txn.put(&cf, b"key2", b"value2", -1).unwrap();

        // Rollback to savepoint -- key2 is discarded, key1 remains
        txn.rollback_to_savepoint("sp1").unwrap();

        // Add different operation after rollback
        txn.put(&cf, b"key3", b"value3", -1).unwrap();

        txn.commit().unwrap();

        // Verify results
        let txn = db.begin_transaction().unwrap();

        // key1 should exist
        assert!(txn.get(&cf, b"key1").is_ok());

        // key2 should not exist (rolled back)
        assert!(txn.get(&cf, b"key2").is_err());

        // key3 should exist
        assert!(txn.get(&cf, b"key3").is_ok());
    }

    #[test]
    fn test_iterator() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        let txn = db.begin_transaction().unwrap();
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
        }
        txn.commit().unwrap();

        // Forward iteration
        let txn = db.begin_transaction().unwrap();
        let mut iter = txn.new_iterator(&cf).unwrap();
        iter.seek_to_first().unwrap();

        let mut count = 0;
        while iter.is_valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            assert!(!key.is_empty());
            assert!(!value.is_empty());
            count += 1;
            iter.next().unwrap();
        }
        assert_eq!(count, 10);

        // Backward iteration
        iter.seek_to_last().unwrap();
        count = 0;
        while iter.is_valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            assert!(!key.is_empty());
            assert!(!value.is_empty());
            count += 1;
            iter.prev().unwrap();
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_isolation_levels() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();

        // Test different isolation levels
        for level in [
            IsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead,
            IsolationLevel::Snapshot,
            IsolationLevel::Serializable,
        ] {
            let txn = db.begin_transaction_with_isolation(level).unwrap();
            drop(txn);
        }
    }

    #[test]
    fn test_column_family_stats() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        let txn = db.begin_transaction().unwrap();
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
        }
        txn.commit().unwrap();

        let stats = cf.get_stats().unwrap();
        assert!(stats.num_levels >= 0);
    }

    #[test]
    fn test_cache_stats() {
        let (db, _temp_dir) = create_test_db();

        let stats = db.get_cache_stats().unwrap();
        // Just verify we can get stats without error
        let _ = stats.enabled;
    }

    #[test]
    fn test_custom_column_family_config() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::new()
            .write_buffer_size(128 * 1024 * 1024)
            .level_size_ratio(10)
            .min_levels(5)
            .compression_algorithm(CompressionAlgorithm::Lz4)
            .enable_bloom_filter(true)
            .bloom_fpr(0.01)
            .enable_block_indexes(true)
            .sync_mode(SyncMode::Interval)
            .sync_interval_us(128000)
            .default_isolation_level(IsolationLevel::ReadCommitted);

        db.create_column_family("custom_cf", cf_config).unwrap();

        let cf = db.get_column_family("custom_cf").unwrap();
        assert_eq!(cf.name(), "custom_cf");
    }
}
