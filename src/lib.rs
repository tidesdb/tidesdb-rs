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
    ColumnFamilyConfig, CompressionAlgorithm, Config, IsolationLevel, LogLevel, ObjectStoreConfig,
    SyncMode,
};
pub use db::{finalize, free, init, init_with_allocator, ColumnFamily, CommitOp, TidesDB};
pub use ffi::{
    tidesdb_calloc_fn, tidesdb_free_fn, tidesdb_malloc_fn, tidesdb_realloc_fn,
};
pub use error::{Error, ErrorCode, Result};
pub use iterator::Iterator;
pub use stats::{CacheStats, DbStats, Stats};
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
    fn test_commit_hook_basic() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        // Track operations received by the hook
        let ops_log: Arc<Mutex<Vec<(Vec<u8>, Option<Vec<u8>>, bool)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let ops_log_clone = ops_log.clone();

        // Set commit hook
        cf.set_commit_hook(move |ops, _commit_seq| {
            let mut log = ops_log_clone.lock().unwrap();
            for op in ops {
                log.push((op.key.clone(), op.value.clone(), op.is_delete));
            }
            0
        })
        .unwrap();

        // Commit a transaction -- hook should fire
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }

        // Verify hook received the operation
        let log = ops_log.lock().unwrap();
        assert!(!log.is_empty());
        assert_eq!(log[0].0, b"key1");
        assert_eq!(log[0].1, Some(b"value1".to_vec()));
        assert!(!log[0].2); // not a delete
    }

    #[test]
    fn test_commit_hook_delete_operations() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        // Insert data first
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }

        // Now set hook and delete
        let ops_log: Arc<Mutex<Vec<(Vec<u8>, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let ops_log_clone = ops_log.clone();

        cf.set_commit_hook(move |ops, _commit_seq| {
            let mut log = ops_log_clone.lock().unwrap();
            for op in ops {
                log.push((op.key.clone(), op.is_delete));
            }
            0
        })
        .unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.delete(&cf, b"key1").unwrap();
            txn.commit().unwrap();
        }

        let log = ops_log.lock().unwrap();
        assert!(!log.is_empty());
        // Find the delete operation
        let delete_op = log.iter().find(|(k, _)| k == b"key1");
        assert!(delete_op.is_some());
        assert!(delete_op.unwrap().1); // is_delete should be true
    }

    #[test]
    fn test_commit_hook_sequence_numbers() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        let seqs: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let seqs_clone = seqs.clone();

        cf.set_commit_hook(move |_ops, commit_seq| {
            let mut s = seqs_clone.lock().unwrap();
            s.push(commit_seq);
            0
        })
        .unwrap();

        // Multiple commits
        for i in 0..3 {
            let mut txn = db.begin_transaction().unwrap();
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            txn.commit().unwrap();
        }

        let seqs = seqs.lock().unwrap();
        assert_eq!(seqs.len(), 3);
        // Sequence numbers should be monotonically increasing
        for i in 1..seqs.len() {
            assert!(seqs[i] > seqs[i - 1]);
        }
    }

    #[test]
    fn test_commit_hook_clear() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        let count: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let count_clone = count.clone();

        cf.set_commit_hook(move |_ops, _commit_seq| {
            let mut c = count_clone.lock().unwrap();
            *c += 1;
            0
        })
        .unwrap();

        // First commit -- hook fires
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(*count.lock().unwrap(), 1);

        // Clear the hook
        cf.clear_commit_hook().unwrap();

        // Second commit -- hook should NOT fire
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(*count.lock().unwrap(), 1); // Still 1, not 2
    }

    #[test]
    fn test_commit_hook_replace() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        let hook1_count: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
        let hook2_count: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

        // Set first hook
        let h1 = hook1_count.clone();
        cf.set_commit_hook(move |_ops, _commit_seq| {
            *h1.lock().unwrap() += 1;
            0
        })
        .unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(*hook1_count.lock().unwrap(), 1);

        // Replace with second hook
        let h2 = hook2_count.clone();
        cf.set_commit_hook(move |_ops, _commit_seq| {
            *h2.lock().unwrap() += 1;
            0
        })
        .unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.commit().unwrap();
        }

        // First hook should still be 1, second hook should be 1
        assert_eq!(*hook1_count.lock().unwrap(), 1);
        assert_eq!(*hook2_count.lock().unwrap(), 1);
    }

    #[test]
    fn test_commit_hook_multi_op_transaction() {
        use std::sync::{Arc, Mutex};

        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let mut cf = db.get_column_family("test_cf").unwrap();

        let ops_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let ops_count_clone = ops_count.clone();

        cf.set_commit_hook(move |ops, _commit_seq| {
            *ops_count_clone.lock().unwrap() += ops.len();
            0
        })
        .unwrap();

        // Commit transaction with multiple operations
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.put(&cf, b"key3", b"value3", -1).unwrap();
            txn.commit().unwrap();
        }

        // Hook should have received all 3 operations
        assert_eq!(*ops_count.lock().unwrap(), 3);
    }

    #[test]
    fn test_init_finalize() {
        // init() may fail if already auto-initialized by another test,
        // but finalize() should always be safe to call
        let _ = init();
        finalize();
    }

    #[test]
    fn test_max_memory_usage_config() {
        let config = Config::new("./test_mem")
            .max_memory_usage(512 * 1024 * 1024); // 512MB
        assert_eq!(config.max_memory_usage, 512 * 1024 * 1024);

        // Default should be 0 (auto)
        let default_config = Config::default();
        assert_eq!(default_config.max_memory_usage, 0);
    }

    #[test]
    fn test_delete_column_family() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("to_delete", cf_config).unwrap();

        // Insert some data
        {
            let cf = db.get_column_family("to_delete").unwrap();
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key", b"value", -1).unwrap();
            txn.commit().unwrap();
        }

        // Get the column family and delete by pointer
        let cf = db.get_column_family("to_delete").unwrap();
        db.delete_column_family(cf).unwrap();

        // Verify it's gone
        assert!(db.get_column_family("to_delete").is_err());
    }

    #[test]
    fn test_register_custom_comparator() {
        let (db, _temp_dir) = create_test_db();

        // Register a reverse comparator
        db.register_comparator("test_reverse", |key1, key2| {
            let min_len = key1.len().min(key2.len());
            for i in 0..min_len {
                if key1[i] != key2[i] {
                    return key2[i] as i32 - key1[i] as i32;
                }
            }
            key2.len() as i32 - key1.len() as i32
        })
        .unwrap();

        // Verify it's registered
        assert!(db.has_comparator("test_reverse"));

        // Create a column family using this comparator
        let cf_config = ColumnFamilyConfig::new()
            .comparator_name("test_reverse");
        db.create_column_family("reverse_cf", cf_config).unwrap();

        let cf = db.get_column_family("reverse_cf").unwrap();

        // Insert data
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"aaa", b"first", -1).unwrap();
            txn.put(&cf, b"zzz", b"last", -1).unwrap();
            txn.commit().unwrap();
        }

        // With reverse comparator, iterating forward should give zzz before aaa
        {
            let txn = db.begin_transaction().unwrap();
            let mut iter = txn.new_iterator(&cf).unwrap();
            iter.seek_to_first().unwrap();

            assert!(iter.is_valid());
            let first_key = iter.key().unwrap();
            assert_eq!(first_key, b"zzz");

            iter.next().unwrap();
            assert!(iter.is_valid());
            let second_key = iter.key().unwrap();
            assert_eq!(second_key, b"aaa");
        }
    }

    #[test]
    fn test_has_comparator_builtin() {
        let (db, _temp_dir) = create_test_db();

        // Built-in comparators should be registered
        assert!(db.has_comparator("memcmp"));
        assert!(db.has_comparator("reverse"));
        assert!(db.has_comparator("lexicographic"));
        assert!(db.has_comparator("uint64"));
        assert!(db.has_comparator("int64"));
        assert!(db.has_comparator("case_insensitive"));

        // Non-existent comparator
        assert!(!db.has_comparator("nonexistent_comparator"));
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

    #[test]
    fn test_purge_cf() {
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

        // Purge the column family (synchronous flush + compaction)
        cf.purge().unwrap();

        // Verify data is still accessible after purge
        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key0000").unwrap();
            assert_eq!(value, b"value0");
            let value = txn.get(&cf, b"key0049").unwrap();
            assert_eq!(value, b"value49");
        }
    }

    #[test]
    fn test_purge_db() {
        let (db, _temp_dir) = create_test_db();

        // Create multiple column families with data
        for cf_name in &["cf_a", "cf_b"] {
            let cf_config = ColumnFamilyConfig::default();
            db.create_column_family(cf_name, cf_config).unwrap();
            let cf = db.get_column_family(cf_name).unwrap();

            let mut txn = db.begin_transaction().unwrap();
            for i in 0..20 {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Purge entire database
        db.purge().unwrap();

        // Verify data is still accessible after purge
        for cf_name in &["cf_a", "cf_b"] {
            let cf = db.get_column_family(cf_name).unwrap();
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key0000").unwrap();
            assert_eq!(value, b"value0");
        }
    }

    #[test]
    fn test_sync_wal() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::new()
            .sync_mode(SyncMode::None);
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert some data
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.put(&cf, b"key2", b"value2", -1).unwrap();
            txn.commit().unwrap();
        }

        // Force WAL sync
        cf.sync_wal().unwrap();

        // Verify data is still accessible
        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key1").unwrap();
            assert_eq!(value, b"value1");
        }
    }

    #[test]
    fn test_sync_wal_interval_mode() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::new()
            .sync_mode(SyncMode::Interval)
            .sync_interval_us(1000000);
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert data and sync WAL
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }

        // Force immediate WAL durability
        cf.sync_wal().unwrap();
    }

    #[test]
    fn test_get_db_stats() {
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

        let stats = db.get_db_stats().unwrap();

        // Should have at least 1 column family
        assert!(stats.num_column_families >= 1);
        // Total memory should be non-zero
        assert!(stats.total_memory > 0);
        // Memory pressure should be in valid range (0-3)
        assert!(stats.memory_pressure_level >= 0 && stats.memory_pressure_level <= 3);
    }

    #[test]
    fn test_get_db_stats_multiple_cfs() {
        let (db, _temp_dir) = create_test_db();

        // Create multiple column families
        for cf_name in &["cf_1", "cf_2", "cf_3"] {
            let cf_config = ColumnFamilyConfig::default();
            db.create_column_family(cf_name, cf_config).unwrap();
        }

        let stats = db.get_db_stats().unwrap();
        assert!(stats.num_column_families >= 3);
    }

    #[test]
    fn test_purge_cf_empty() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Purge empty column family should succeed
        cf.purge().unwrap();
    }

    #[test]
    fn test_purge_db_empty() {
        let (db, _temp_dir) = create_test_db();

        // Purge database with no column families should succeed
        db.purge().unwrap();
    }

    #[test]
    fn test_unified_memtable_config() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path())
            .num_flush_threads(2)
            .num_compaction_threads(2)
            .log_level(LogLevel::Info)
            .block_cache_size(64 * 1024 * 1024)
            .max_open_sstables(256)
            .unified_memtable(true)
            .unified_memtable_write_buffer_size(128 * 1024 * 1024)
            .unified_memtable_skip_list_max_level(16)
            .unified_memtable_skip_list_probability(0.25)
            .unified_memtable_sync_mode(SyncMode::None)
            .unified_memtable_sync_interval_us(0);

        assert!(config.unified_memtable);
        assert_eq!(config.unified_memtable_write_buffer_size, 128 * 1024 * 1024);
        assert_eq!(config.unified_memtable_skip_list_max_level, 16);

        let db = TidesDB::open(config).unwrap();
        db.create_column_family("test_cf", ColumnFamilyConfig::default()).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert and read data to verify unified memtable works
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"key1", b"value1", -1).unwrap();
            txn.commit().unwrap();
        }

        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"key1").unwrap();
            assert_eq!(value, b"value1");
        }
    }

    #[test]
    fn test_iterator_key_value() {
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

        // Use key_value() combined method
        {
            let txn = db.begin_transaction().unwrap();
            let mut iter = txn.new_iterator(&cf).unwrap();
            iter.seek_to_first().unwrap();

            let mut count = 0;
            while iter.is_valid() {
                let (key, value) = iter.key_value().unwrap();
                assert!(!key.is_empty());
                assert!(!value.is_empty());
                count += 1;
                iter.next().unwrap();
            }
            assert_eq!(count, 10);
        }
    }

    #[test]
    fn test_db_stats_unified_fields() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();

        let stats = db.get_db_stats().unwrap();

        // Unified memtable fields should be accessible
        let _ = stats.unified_memtable_enabled;
        let _ = stats.unified_memtable_bytes;
        let _ = stats.unified_immutable_count;
        let _ = stats.unified_is_flushing;
        let _ = stats.unified_next_cf_index;
        let _ = stats.unified_wal_generation;

        // Object store fields should be accessible
        let _ = stats.object_store_enabled;
        let _ = stats.object_store_connector;
        let _ = stats.local_cache_bytes_used;
        let _ = stats.local_cache_bytes_max;
        let _ = stats.local_cache_num_files;
        let _ = stats.last_uploaded_generation;
        let _ = stats.upload_queue_depth;
        let _ = stats.total_uploads;
        let _ = stats.total_upload_failures;
        let _ = stats.replica_mode;

        // For a non-unified, non-object-store db:
        assert!(!stats.unified_memtable_enabled);
        assert!(!stats.object_store_enabled);
        assert!(!stats.replica_mode);
    }

    #[test]
    fn test_db_stats_unified_memtable_enabled() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path())
            .num_flush_threads(2)
            .num_compaction_threads(2)
            .log_level(LogLevel::Info)
            .block_cache_size(64 * 1024 * 1024)
            .max_open_sstables(256)
            .unified_memtable(true);

        let db = TidesDB::open(config).unwrap();
        db.create_column_family("test_cf", ColumnFamilyConfig::default()).unwrap();

        let stats = db.get_db_stats().unwrap();
        assert!(stats.unified_memtable_enabled);
    }

    #[test]
    fn test_cf_config_object_store_fields() {
        let config = ColumnFamilyConfig::new()
            .object_lazy_compaction(true)
            .object_prefetch_compaction(false);

        assert!(config.object_lazy_compaction);
        assert!(!config.object_prefetch_compaction);
    }

    #[test]
    fn test_error_code_readonly() {
        // Verify ReadOnly error code is recognized
        let code = ErrorCode::from_code(-13);
        assert_eq!(code, Some(ErrorCode::ReadOnly));
    }

    #[test]
    fn test_promote_to_primary_not_replica() {
        let (db, _temp_dir) = create_test_db();

        // promote_to_primary on a non-replica db should return an error
        let result = db.promote_to_primary();
        assert!(result.is_err());
    }

    #[cfg(any(feature = "v9_1_0", feature = "v9_2_0"))]
    #[test]
    fn test_transaction_single_delete() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Put once, single-delete once (matches the single_delete contract)
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"sd_key", b"sd_value", -1).unwrap();
            txn.commit().unwrap();
        }

        {
            let txn = db.begin_transaction().unwrap();
            let value = txn.get(&cf, b"sd_key").unwrap();
            assert_eq!(value, b"sd_value");
        }

        {
            let mut txn = db.begin_transaction().unwrap();
            txn.single_delete(&cf, b"sd_key").unwrap();
            txn.commit().unwrap();
        }

        // Verify the key is no longer visible
        {
            let txn = db.begin_transaction().unwrap();
            let result = txn.get(&cf, b"sd_key");
            assert!(result.is_err());
        }
    }

    #[cfg(any(feature = "v9_1_0", feature = "v9_2_0"))]
    #[test]
    fn test_transaction_single_delete_in_batch() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("test_cf", cf_config).unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        // Insert several keys
        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..5 {
                let key = format!("k{}", i);
                let val = format!("v{}", i);
                txn.put(&cf, key.as_bytes(), val.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }

        // Mix put + single_delete in one batch (different keys)
        {
            let mut txn = db.begin_transaction().unwrap();
            txn.put(&cf, b"k_new", b"v_new", -1).unwrap();
            txn.single_delete(&cf, b"k0").unwrap();
            txn.single_delete(&cf, b"k1").unwrap();
            txn.commit().unwrap();
        }

        {
            let txn = db.begin_transaction().unwrap();
            assert!(txn.get(&cf, b"k0").is_err());
            assert!(txn.get(&cf, b"k1").is_err());
            assert_eq!(txn.get(&cf, b"k2").unwrap(), b"v2");
            assert_eq!(txn.get(&cf, b"k_new").unwrap(), b"v_new");
        }
    }

    #[test]
    fn test_tombstone_cf_config_roundtrip() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::new()
            .tombstone_density_trigger(0.5)
            .tombstone_density_min_entries(256);

        db.create_column_family("ts_cf", cf_config).unwrap();
        let cf = db.get_column_family("ts_cf").unwrap();

        let stats = cf.get_stats().unwrap();
        let cfg = stats.config.expect("stats should expose CF config");
        assert_eq!(cfg.tombstone_density_trigger, 0.5);
        assert_eq!(cfg.tombstone_density_min_entries, 256);

        // Library defaults should be sensible (min_entries ~1024).
        let defaults = ColumnFamilyConfig::default();
        assert!(
            defaults.tombstone_density_min_entries > 0,
            "default min_entries should be sourced from C library"
        );
    }

    #[test]
    fn test_tombstone_stats_populated_after_deletes() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("ts_stats_cf", cf_config).unwrap();
        let cf = db.get_column_family("ts_stats_cf").unwrap();

        const N: usize = 200;

        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..N {
                let key = format!("key{:04}", i);
                let value = format!("value{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
        }
        cf.flush_memtable().unwrap();

        {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..(N / 2) {
                let key = format!("key{:04}", i);
                txn.delete(&cf, key.as_bytes()).unwrap();
            }
            txn.commit().unwrap();
        }
        cf.flush_memtable().unwrap();

        // Give the flush a moment to land on disk.
        std::thread::sleep(std::time::Duration::from_millis(200));

        let stats = cf.get_stats().unwrap();
        assert!(stats.total_tombstones > 0, "expected tombstones after deletes");
        assert!(stats.tombstone_ratio >= 0.0 && stats.tombstone_ratio <= 1.0);
        assert!(stats.max_sst_density >= 0.0 && stats.max_sst_density <= 1.0);
        assert_eq!(
            stats.level_tombstone_counts.len(),
            stats.num_levels as usize
        );
    }

    #[test]
    fn test_compact_range_basic() {
        let (db, _temp_dir) = create_test_db();

        let cf_config = ColumnFamilyConfig::default();
        db.create_column_family("range_cf", cf_config).unwrap();
        let cf = db.get_column_family("range_cf").unwrap();

        // Multiple flushed batches to create several SSTables.
        for batch in 0..3 {
            let mut txn = db.begin_transaction().unwrap();
            for i in 0..50 {
                let key = format!("key{:02}{:04}", batch, i);
                let value = format!("v{}", i);
                txn.put(&cf, key.as_bytes(), value.as_bytes(), -1).unwrap();
            }
            txn.commit().unwrap();
            cf.flush_memtable().unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Narrow range over batch 1 only.
        cf.compact_range(Some(b"key010000"), Some(b"key019999")).unwrap();

        // Both endpoints unbounded should be rejected.
        let err = cf.compact_range(None, None).unwrap_err();
        assert!(matches!(err, Error::TidesDB { code: ErrorCode::InvalidArgs, .. }));

        // Empty slices should also be treated as unbounded -> rejected.
        let err = cf.compact_range(Some(&[]), Some(&[])).unwrap_err();
        assert!(matches!(err, Error::TidesDB { code: ErrorCode::InvalidArgs, .. }));

        // A key outside the compacted range remains readable and unchanged.
        let txn = db.begin_transaction().unwrap();
        let val = txn.get(&cf, b"key020000").unwrap();
        assert_eq!(val, b"v0");
    }

    #[test]
    fn test_max_concurrent_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new(temp_dir.path()).max_concurrent_flushes(1);
        assert_eq!(config.max_concurrent_flushes, 1);

        let db = TidesDB::open(config).unwrap();
        db.create_column_family("test_cf", ColumnFamilyConfig::default())
            .unwrap();
        let cf = db.get_column_family("test_cf").unwrap();

        let mut txn = db.begin_transaction().unwrap();
        txn.put(&cf, b"k", b"v", -1).unwrap();
        txn.commit().unwrap();
        cf.flush_memtable().unwrap();

        let txn = db.begin_transaction().unwrap();
        assert_eq!(txn.get(&cf, b"k").unwrap(), b"v");
    }

    #[test]
    fn test_default_config_sources_max_concurrent_flushes() {
        let cfg = Config::default();
        assert!(
            cfg.max_concurrent_flushes != 0,
            "default_config().max_concurrent_flushes should be sourced from tidesdb_default_config()"
        );
    }
}
