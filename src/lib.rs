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
//!     let mut txn = db.begin_transaction()?;
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
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key", b"value", -1).unwrap();
            txn.commit().unwrap();
        }

        // Get
        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key").unwrap();
            assert_eq!(value, b"value");
        }

        // Delete
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.delete(&cf, b"key").unwrap();
            txn.commit().unwrap();
        }

        // Verify deleted
        {
            let txn = db.begin_transaction().unwrap();
            let result = txn.get(&cf, b"key");
            assert!(result.is_err());
        }
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

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"temp_key", b"temp_value", ttl).unwrap();
            txn.commit().unwrap();
        }

        // Verify key exists before expiration
        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"temp_key").unwrap();
            assert_eq!(value, b"temp_value");
        }
    }

    #[test]
    fn test_multi_operation_transaction() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Multiple operations in one transaction
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.put(&cf, b"key3", b"value3", -1).unwrap();
            txn.commit().unwrap();
        }

        // Verify all keys exist
        {
            let txn = db.begin_transaction().unwrap();
            for i in 1..=3 {
                let key = format!("key{}", i);
                let expected_value = format!("value{}", i);
                let value = txn.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(value, expected_value.as_bytes());
            }
        }
    }

    #[test]
    fn test_transaction_rollback() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"rollback_key", b"rollback_value", -1).unwrap();
            txn.rollback().unwrap();
        }

        // Verify key does not exist
        {
            let txn = db.begin_transaction().unwrap();
            let result = txn.get(&cf, b"rollback_key");
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_savepoints() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();

            txn.savepoint("sp1").unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();

            // Rollback to savepoint -- key2 is discarded, key1 remains
            txn.rollback_to_savepoint("sp1").unwrap();

            // Add different operation after rollback
            txn.put(&cf, b"key3", b"value3", -1).unwrap();

            txn.commit().unwrap();
        }

        // Verify results
        {
            let txn = db.begin_transaction().unwrap();

            // key1 should exist
            assert!(txn.get(&cf, b"key1").is_ok());

            // key2 should not exist (rolled back)
            assert!(txn.get(&cf, b"key2").is_err());

            // key3 should exist
            assert!(txn.get(&cf, b"key3").is_ok());
        }
    }

    #[test]
    fn test_iterator() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..10 {
                let key = format!("key{:02}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Forward iteration
        {
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
            // iter and txn dropped here in correct order
        }

        // Backward iteration in separate scope
        {
            let txn = db.begin_transaction().unwrap();
            let mut iter = txn.new_iterator(&cf).unwrap();
            iter.seek_to_last().unwrap();

            let mut count = 0;
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
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..100 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

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

    #[test]
    fn test_rename_column_family() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("old_name", cf_config).unwrap();

        // Insert some data
        {
            let cf = db.get_column_family("old_name").unwrap();
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key", b"value", -1).unwrap();
            txn.commit().unwrap();
        }

        // Rename the column family
        db.rename_column_family("old_name", "new_name").unwrap();

        // Verify old name no longer exists
        assert!(db.get_column_family("old_name").is_err());

        // Verify new name exists and data is preserved
        let cf = db.get_column_family("new_name").unwrap();
        let txn = db.begin_transaction().unwrap();
        let value = txn.get(&cf, b"key").unwrap();
        assert_eq!(value, b"value");
    }

    #[test]
    fn test_backup() {
        let (db, temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();

        // Insert some data
        {
            let cf = db.get_column_family("test_cf").unwrap();
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key", b"value", -1).unwrap();
            txn.commit().unwrap();
        }

        // Create backup directory
        let backup_dir = temp_dir.path().join("backup");
        db.backup(backup_dir.to_str().unwrap()).unwrap();

        // Verify backup directory exists
        assert!(backup_dir.exists());
    }

    #[test]
    fn test_checkpoint() {
        let (db, temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();

        // Insert some data
        {
            let cf = db.get_column_family("test_cf").unwrap();
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.commit().unwrap();
        }

        // Create checkpoint directory
        let checkpoint_dir = temp_dir.path().join("checkpoint");
        db.checkpoint(checkpoint_dir.to_str().unwrap()).unwrap();

        // Verify checkpoint directory exists
        assert!(checkpoint_dir.exists());

        // Open the checkpoint as a separate database and verify data
        let checkpoint_config = Config::new(checkpoint_dir.to_str().unwrap())
            .num_flush_threads(2)
            .num_compaction_threads(2)
            .log_level(LogLevel::Info)
            .block_cache_size(64 * 1024 * 1024)
            .max_open_sstables(256);

        let checkpoint_db = TidesDB::open(checkpoint_config).unwrap();
        let cf = checkpoint_db.get_column_family("test_cf").unwrap();
        let txn = checkpoint_db.begin_transaction().unwrap();
        let v1 = txn.get(&cf, b"key1").unwrap();
        assert_eq!(v1, b"value1");
        let v2 = txn.get(&cf, b"key2").unwrap();
        assert_eq!(v2, b"value2");
    }

    #[test]
    fn test_is_flushing_compacting() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // These should return false when no operations are in progress
        // (just testing that the methods work without error)
        let _ = cf.is_flushing();
        let _ = cf.is_compacting();
    }

    #[test]
    fn test_update_runtime_config() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Create new configuration
        let new_config = ColumnFamilyConfig::new()
            .write_buffer_size(256 * 1024 * 1024)
            .compression_algorithm(CompressionAlgorithm::Zstd);

        // Update runtime config
        cf.update_runtime_config(&new_config, false).unwrap();
    }

    #[test]
    fn test_extended_stats() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..100 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        let stats = cf.get_stats().unwrap();

        // Verify extended stats fields exist and are accessible
        assert!(stats.num_levels >= 0);
        let _ = stats.total_keys;
        let _ = stats.total_data_size;
        let _ = stats.avg_key_size;
        let _ = stats.avg_value_size;
        let _ = stats.read_amp;
        let _ = stats.hit_rate;
        let _ = stats.level_key_counts;
    }

    #[test]
    fn test_column_family_config_builders() {
        // Test all the new builder methods
        let config = ColumnFamilyConfig::new()
            .write_buffer_size(64 * 1024 * 1024)
            .level_size_ratio(10)
            .min_levels(4)
            .dividing_level_offset(2)
            .klog_value_threshold(1024)
            .compression_algorithm(CompressionAlgorithm::Lz4)
            .enable_bloom_filter(true)
            .bloom_fpr(0.01)
            .enable_block_indexes(true)
            .index_sample_ratio(16)
            .block_index_prefix_len(4)
            .sync_mode(SyncMode::Interval)
            .sync_interval_us(128000)
            .comparator_name("memcmp")
            .skip_list_max_level(12)
            .skip_list_probability(0.25)
            .default_isolation_level(IsolationLevel::ReadCommitted)
            .min_disk_space(1024 * 1024 * 1024)
            .l1_file_count_trigger(4)
            .l0_queue_stall_threshold(8)
            .use_btree(false);

        // Verify some values
        assert_eq!(config.write_buffer_size, 64 * 1024 * 1024);
        assert_eq!(config.min_levels, 4);
        assert_eq!(config.dividing_level_offset, 2);
        assert_eq!(config.klog_value_threshold, 1024);
        assert_eq!(config.index_sample_ratio, 16);
        assert_eq!(config.block_index_prefix_len, 4);
        assert_eq!(config.comparator_name, "memcmp");
        assert_eq!(config.skip_list_max_level, 12);
        assert_eq!(config.min_disk_space, 1024 * 1024 * 1024);
        assert_eq!(config.l1_file_count_trigger, 4);
        assert_eq!(config.l0_queue_stall_threshold, 8);
        assert_eq!(config.use_btree, false);
    }

    #[test]
    fn test_use_btree_config() {
        let (db, _temp_dir) = create_test_db();

        // Create column family with B+tree format enabled
        let cf_config = ColumnFamilyConfig::new()
            .use_btree(true)
            .compression_algorithm(CompressionAlgorithm::Lz4);

        db.create_column_family("btree_cf", cf_config).unwrap();

        let cf = db.get_column_family("btree_cf").unwrap();
        assert_eq!(cf.name(), "btree_cf");

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..50 {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Verify data can be read back
        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key0000").unwrap();
            assert_eq!(value, b"value0");
        }
    }

    #[test]
    fn test_btree_stats() {
        let (db, _temp_dir) = create_test_db();

        // Create column family with B+tree format
        let cf_config = ColumnFamilyConfig::new()
            .use_btree(true);

        db.create_column_family("btree_stats_cf", cf_config).unwrap();
        let cf = db.get_column_family("btree_stats_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..100 {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Get stats and verify B+tree fields are accessible
        let stats = cf.get_stats().unwrap();

        // Verify B+tree stats fields exist and are accessible
        let _ = stats.use_btree;
        let _ = stats.btree_total_nodes;
        let _ = stats.btree_max_height;
        let _ = stats.btree_avg_height;

        // Basic sanity check
        assert!(stats.num_levels >= 0);
    }

    #[test]
    fn test_clone_column_family() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("source_cf", cf_config).unwrap();

        // Insert data into source
        {
            let cf = db.get_column_family("source_cf").unwrap();
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.put(&cf, b"key3", b"value3", -1).unwrap();
            txn.commit().unwrap();
        }

        // Clone the column family
        db.clone_column_family("source_cf", "cloned_cf").unwrap();

        // Verify cloned column family exists
        let cloned_cf = db.get_column_family("cloned_cf").unwrap();
        assert_eq!(cloned_cf.name(), "cloned_cf");

        // Verify data is present in the clone
        {
            let txn = db.begin_transaction().unwrap();
            let v1 = txn.get(&cloned_cf, b"key1").unwrap();
            assert_eq!(v1, b"value1");
            let v2 = txn.get(&cloned_cf, b"key2").unwrap();
            assert_eq!(v2, b"value2");
            let v3 = txn.get(&cloned_cf, b"key3").unwrap();
            assert_eq!(v3, b"value3");
        }

        // Verify source still exists and is independent
        let source_cf = db.get_column_family("source_cf").unwrap();
        {
            let txn = db.begin_transaction().unwrap();
            let v1 = txn.get(&source_cf, b"key1").unwrap();
            assert_eq!(v1, b"value1");
        }

        // Verify clone is independent -- write to clone, source unaffected
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cloned_cf, b"key4", b"value4", -1).unwrap();
            txn.commit().unwrap();
        }

        {
            let txn = db.begin_transaction().unwrap();
            // key4 should exist in clone
            let v4 = txn.get(&cloned_cf, b"key4").unwrap();
            assert_eq!(v4, b"value4");
            // key4 should not exist in source
            assert!(txn.get(&source_cf, b"key4").is_err());
        }
    }

    #[test]
    fn test_clone_column_family_errors() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("source_cf", cf_config).unwrap();

        // Clone to a name that already exists should fail
        let cf_config2 = ColumnFamilyConfig::default();
        db.create_column_family("existing_cf", cf_config2).unwrap();
        assert!(db.clone_column_family("source_cf", "existing_cf").is_err());

        // Clone from non-existent source should fail
        assert!(db.clone_column_family("nonexistent_cf", "new_cf").is_err());
    }

    #[test]
    fn test_transaction_reset() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Begin transaction, do work, commit, then reset and reuse
        let mut txn = db.begin_transaction().unwrap();

        // First batch
        txn.put(&cf, b"key1", b"value1", -1).unwrap();
        txn.commit().unwrap();

        // Reset with same isolation level
        txn.reset(IsolationLevel::ReadCommitted).unwrap();

        // Second batch using the same transaction
        txn.put(&cf, b"key2", b"value2", -1).unwrap();
        txn.commit().unwrap();

        // Verify both keys exist
        {
            let txn = db.begin_transaction().unwrap();
            let v1 = txn.get(&cf, b"key1").unwrap();
            assert_eq!(v1, b"value1");
            let v2 = txn.get(&cf, b"key2").unwrap();
            assert_eq!(v2, b"value2");
        }
    }

    #[test]
    fn test_transaction_reset_with_different_isolation() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Begin with ReadCommitted
        let mut txn = db.begin_transaction_with_isolation(IsolationLevel::ReadCommitted).unwrap();
        txn.put(&cf, b"key1", b"value1", -1).unwrap();
        txn.commit().unwrap();

        // Reset with different isolation level
        txn.reset(IsolationLevel::RepeatableRead).unwrap();
        txn.put(&cf, b"key2", b"value2", -1).unwrap();
        txn.commit().unwrap();

        // Reset again with yet another level
        txn.reset(IsolationLevel::Snapshot).unwrap();
        txn.put(&cf, b"key3", b"value3", -1).unwrap();
        txn.commit().unwrap();

        // Verify all keys
        {
            let txn = db.begin_transaction().unwrap();
            assert_eq!(txn.get(&cf, b"key1").unwrap(), b"value1");
            assert_eq!(txn.get(&cf, b"key2").unwrap(), b"value2");
            assert_eq!(txn.get(&cf, b"key3").unwrap(), b"value3");
        }
    }

    #[test]
    fn test_transaction_reset_after_rollback() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        let mut txn = db.begin_transaction().unwrap();

        // Do work and rollback
        txn.put(&cf, b"key1", b"value1", -1).unwrap();
        txn.rollback().unwrap();

        // Reset after rollback should work
        txn.reset(IsolationLevel::ReadCommitted).unwrap();

        // Do new work and commit
        txn.put(&cf, b"key2", b"value2", -1).unwrap();
        txn.commit().unwrap();

        // Verify key1 does not exist (was rolled back), key2 exists
        {
            let txn = db.begin_transaction().unwrap();
            assert!(txn.get(&cf, b"key1").is_err());
            assert_eq!(txn.get(&cf, b"key2").unwrap(), b"value2");
        }
    }

    #[test]
    fn test_range_cost() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..100 {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Estimate range cost
        let cost = cf.range_cost(b"key0000", b"key0099").unwrap();
        // Cost should be non-negative
        assert!(cost >= 0.0);
    }

    #[test]
    fn test_range_cost_comparison() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..1000 {
                let key = format!("key{:06}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Compare costs of different ranges
        let cost_small = cf.range_cost(b"key000000", b"key000009").unwrap();
        let cost_large = cf.range_cost(b"key000000", b"key000999").unwrap();

        // Both should be non-negative
        assert!(cost_small >= 0.0);
        assert!(cost_large >= 0.0);
    }

    #[test]
    fn test_range_cost_key_order_invariant() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..50 {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Key order should not matter -- both orderings produce the same cost
        let cost_ab = cf.range_cost(b"key0000", b"key0049").unwrap();
        let cost_ba = cf.range_cost(b"key0049", b"key0000").unwrap();
        assert!((cost_ab - cost_ba).abs() < f64::EPSILON);
    }

    #[test]
    fn test_range_cost_empty_range() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Cost on empty column family should be 0.0
        let cost = cf.range_cost(b"key_a", b"key_b").unwrap();
        assert!(cost >= 0.0);
    }

    #[test]
    fn test_block_based_vs_btree_config() {
        let (db, _temp_dir) = create_test_db();

        // Create block-based column family (default)
        let block_config = ColumnFamilyConfig::new()
            .use_btree(false);
        db.create_column_family("block_cf", block_config).unwrap();

        // Create B+tree column family
        let btree_config = ColumnFamilyConfig::new()
            .use_btree(true);
        db.create_column_family("btree_cf", btree_config).unwrap();

        // Verify both can be used
        let block_cf = db.get_column_family("block_cf").unwrap();
        let btree_cf = db.get_column_family("btree_cf").unwrap();

        // Insert data into both
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&block_cf, b"key1", b"value1", -1).unwrap();
            txn.put(&btree_cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }

        // Read from both
        {
            let txn = db.begin_transaction().unwrap();
            let v1 = txn.get(&block_cf, b"key1").unwrap();
            let v2 = txn.get(&btree_cf, b"key1").unwrap();
            assert_eq!(v1, b"value1");
            assert_eq!(v2, b"value1");
        }
    }
}
