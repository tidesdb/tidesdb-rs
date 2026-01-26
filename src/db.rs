// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Main database types and operations.

use crate::config::{ColumnFamilyConfig, Config, IsolationLevel};
use crate::error::{check_result, Error, Result};
use crate::ffi;
use crate::stats::CacheStats;
use crate::transaction::Transaction;
use libc::c_char;
use std::ffi::{CStr, CString};
use std::ptr;

/// A TidesDB database instance.
///
/// This is the main entry point for interacting with TidesDB.
/// The database is automatically closed when dropped.
///
/// # Example
///
/// ```no_run
/// use tidesdb::{TidesDB, Config, ColumnFamilyConfig};
///
/// let config = Config::new("./mydb")
///     .num_flush_threads(2)
///     .num_compaction_threads(2);
///
/// let db = TidesDB::open(config)?;
///
/// // Create a column family
/// db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
///
/// // Get the column family
/// let cf = db.get_column_family("my_cf")?;
///
/// // Begin a transaction
/// let txn = db.begin_transaction()?;
/// txn.put(&cf, b"key", b"value", -1)?;
/// txn.commit()?;
/// # Ok::<(), tidesdb::Error>(())
/// ```
pub struct TidesDB {
    db: *mut ffi::tidesdb_t,
}

// TidesDB uses internal locking for thread safety
unsafe impl Send for TidesDB {}
unsafe impl Sync for TidesDB {}

impl TidesDB {
    /// Opens a TidesDB instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The database configuration
    ///
    /// # Returns
    ///
    /// A new TidesDB instance or an error if the database cannot be opened.
    pub fn open(config: Config) -> Result<Self> {
        let (c_config, _c_path) = config.to_c_config()?;
        let mut db: *mut ffi::tidesdb_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_open(&c_config, &mut db) };
        check_result(result, "failed to open database")?;

        if db.is_null() {
            return Err(Error::NullPointer("database handle"));
        }

        Ok(TidesDB { db })
    }

    /// Creates a new column family with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column family
    /// * `config` - The column family configuration
    pub fn create_column_family(&self, name: &str, config: ColumnFamilyConfig) -> Result<()> {
        let c_name = CString::new(name)?;
        let c_config = config.to_c_config();

        let result = unsafe {
            ffi::tidesdb_create_column_family(self.db, c_name.as_ptr(), &c_config)
        };
        check_result(result, "failed to create column family")
    }

    /// Drops a column family and all associated data.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column family to drop
    pub fn drop_column_family(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;

        let result = unsafe { ffi::tidesdb_drop_column_family(self.db, c_name.as_ptr()) };
        check_result(result, "failed to drop column family")
    }

    /// Atomically renames a column family and its underlying directory.
    /// Waits for any in-progress flush/compaction to complete before renaming.
    ///
    /// # Arguments
    ///
    /// * `old_name` - Current name of the column family
    /// * `new_name` - New name for the column family
    pub fn rename_column_family(&self, old_name: &str, new_name: &str) -> Result<()> {
        let c_old_name = CString::new(old_name)?;
        let c_new_name = CString::new(new_name)?;

        let result = unsafe {
            ffi::tidesdb_rename_column_family(self.db, c_old_name.as_ptr(), c_new_name.as_ptr())
        };
        check_result(result, "failed to rename column family")
    }

    /// Retrieves a column family by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the column family
    ///
    /// # Returns
    ///
    /// The column family or an error if not found.
    pub fn get_column_family(&self, name: &str) -> Result<ColumnFamily> {
        let c_name = CString::new(name)?;

        let cf = unsafe { ffi::tidesdb_get_column_family(self.db, c_name.as_ptr()) };
        if cf.is_null() {
            return Err(Error::from_code(
                ffi::TDB_ERR_NOT_FOUND,
                "column family not found",
            ));
        }

        Ok(ColumnFamily {
            cf,
            name: name.to_string(),
        })
    }

    /// Lists all column families in the database.
    ///
    /// # Returns
    ///
    /// A vector of column family names.
    pub fn list_column_families(&self) -> Result<Vec<String>> {
        let mut names: *mut *mut c_char = ptr::null_mut();
        let mut count: i32 = 0;

        let result =
            unsafe { ffi::tidesdb_list_column_families(self.db, &mut names, &mut count) };
        check_result(result, "failed to list column families")?;

        if count == 0 || names.is_null() {
            return Ok(Vec::new());
        }

        let mut result_names = Vec::with_capacity(count as usize);

        unsafe {
            for i in 0..count as isize {
                let name_ptr = *names.offset(i);
                if !name_ptr.is_null() {
                    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
                    result_names.push(name);
                    libc::free(name_ptr as *mut libc::c_void);
                }
            }
            libc::free(names as *mut libc::c_void);
        }

        Ok(result_names)
    }

    /// Begins a new transaction with the default isolation level.
    ///
    /// # Returns
    ///
    /// A new transaction.
    pub fn begin_transaction(&self) -> Result<Transaction> {
        let mut txn: *mut ffi::tidesdb_txn_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_txn_begin(self.db, &mut txn) };
        check_result(result, "failed to begin transaction")?;

        if txn.is_null() {
            return Err(Error::NullPointer("transaction handle"));
        }

        Ok(Transaction::new(txn))
    }

    /// Begins a new transaction with the specified isolation level.
    ///
    /// # Arguments
    ///
    /// * `isolation` - The isolation level for the transaction
    ///
    /// # Returns
    ///
    /// A new transaction.
    pub fn begin_transaction_with_isolation(
        &self,
        isolation: IsolationLevel,
    ) -> Result<Transaction> {
        let mut txn: *mut ffi::tidesdb_txn_t = ptr::null_mut();

        let result = unsafe {
            ffi::tidesdb_txn_begin_with_isolation(self.db, isolation as i32, &mut txn)
        };
        check_result(result, "failed to begin transaction with isolation")?;

        if txn.is_null() {
            return Err(Error::NullPointer("transaction handle"));
        }

        Ok(Transaction::new(txn))
    }

    /// Retrieves statistics about the block cache.
    ///
    /// # Returns
    ///
    /// Cache statistics.
    pub fn get_cache_stats(&self) -> Result<CacheStats> {
        let mut c_stats = ffi::tidesdb_cache_stats_t {
            enabled: 0,
            total_entries: 0,
            total_bytes: 0,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
            num_partitions: 0,
        };

        let result = unsafe { ffi::tidesdb_get_cache_stats(self.db, &mut c_stats) };
        check_result(result, "failed to get cache stats")?;

        Ok(CacheStats {
            enabled: c_stats.enabled != 0,
            total_entries: c_stats.total_entries,
            total_bytes: c_stats.total_bytes,
            hits: c_stats.hits as usize,
            misses: c_stats.misses as usize,
            hit_rate: c_stats.hit_rate,
            num_partitions: c_stats.num_partitions,
        })
    }

    /// Registers a custom comparator with the database.
    ///
    /// # Arguments
    ///
    /// * `name` - The comparator name
    /// * `context` - Optional context string
    pub fn register_comparator(&self, name: &str, context: Option<&str>) -> Result<()> {
        let c_name = CString::new(name)?;
        let c_context = context.map(|s| CString::new(s)).transpose()?;

        let ctx_ptr = c_context
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null());

        let result = unsafe {
            ffi::tidesdb_register_comparator(
                self.db,
                c_name.as_ptr(),
                ptr::null(),
                ctx_ptr,
                ptr::null(),
            )
        };
        check_result(result, "failed to register comparator")
    }

    /// Creates a backup of the database to the specified directory.
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory to backup to
    pub fn backup(&self, dir: &str) -> Result<()> {
        let c_dir = CString::new(dir)?;

        let result = unsafe { ffi::tidesdb_backup(self.db, c_dir.as_ptr() as *mut c_char) };
        check_result(result, "failed to backup database")
    }
}

impl Drop for TidesDB {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                ffi::tidesdb_close(self.db);
            }
            self.db = ptr::null_mut();
        }
    }
}

/// A column family in TidesDB.
///
/// Column families are isolated key-value stores with independent configuration.
pub struct ColumnFamily {
    pub(crate) cf: *mut ffi::tidesdb_column_family_t,
    name: String,
}

// ColumnFamily uses internal locking for thread safety
unsafe impl Send for ColumnFamily {}
unsafe impl Sync for ColumnFamily {}

impl ColumnFamily {
    /// Gets the name of this column family.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Retrieves statistics about this column family.
    pub fn get_stats(&self) -> Result<crate::stats::Stats> {
        let mut c_stats: *mut ffi::tidesdb_stats_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_get_stats(self.cf, &mut c_stats) };
        check_result(result, "failed to get stats")?;

        if c_stats.is_null() {
            return Err(Error::NullPointer("stats"));
        }

        let stats = unsafe {
            let num_levels = (*c_stats).num_levels;
            let memtable_size = (*c_stats).memtable_size;

            let mut level_sizes = Vec::new();
            if num_levels > 0 && !(*c_stats).level_sizes.is_null() {
                for i in 0..num_levels as isize {
                    level_sizes.push(*(*c_stats).level_sizes.offset(i));
                }
            }

            let mut level_num_sstables = Vec::new();
            if num_levels > 0 && !(*c_stats).level_num_sstables.is_null() {
                for i in 0..num_levels as isize {
                    level_num_sstables.push(*(*c_stats).level_num_sstables.offset(i));
                }
            }

            let mut level_key_counts = Vec::new();
            if num_levels > 0 && !(*c_stats).level_key_counts.is_null() {
                for i in 0..num_levels as isize {
                    level_key_counts.push(*(*c_stats).level_key_counts.offset(i));
                }
            }

            let total_keys = (*c_stats).total_keys;
            let total_data_size = (*c_stats).total_data_size;
            let avg_key_size = (*c_stats).avg_key_size;
            let avg_value_size = (*c_stats).avg_value_size;
            let read_amp = (*c_stats).read_amp;
            let hit_rate = (*c_stats).hit_rate;

            ffi::tidesdb_free_stats(c_stats);

            crate::stats::Stats {
                num_levels,
                memtable_size,
                level_sizes,
                level_num_sstables,
                config: None,
                total_keys,
                total_data_size,
                avg_key_size,
                avg_value_size,
                level_key_counts,
                read_amp,
                hit_rate,
            }
        };

        Ok(stats)
    }

    /// Manually triggers compaction for this column family.
    pub fn compact(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_compact(self.cf) };
        check_result(result, "failed to compact column family")
    }

    /// Manually triggers memtable flush for this column family.
    pub fn flush_memtable(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_flush_memtable(self.cf) };
        check_result(result, "failed to flush memtable")
    }

    /// Checks if this column family has a flush operation in progress.
    pub fn is_flushing(&self) -> bool {
        unsafe { ffi::tidesdb_is_flushing(self.cf) != 0 }
    }

    /// Checks if this column family has a compaction operation in progress.
    pub fn is_compacting(&self) -> bool {
        unsafe { ffi::tidesdb_is_compacting(self.cf) != 0 }
    }

    /// Updates the runtime configuration for this column family.
    ///
    /// # Arguments
    ///
    /// * `config` - The new configuration
    /// * `persist_to_disk` - Whether to persist the configuration to disk
    pub fn update_runtime_config(
        &self,
        config: &ColumnFamilyConfig,
        persist_to_disk: bool,
    ) -> Result<()> {
        let c_config = config.to_c_config();
        let result = unsafe {
            ffi::tidesdb_cf_update_runtime_config(
                self.cf,
                &c_config,
                if persist_to_disk { 1 } else { 0 },
            )
        };
        check_result(result, "failed to update runtime config")
    }
}
