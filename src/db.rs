// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Main database types and operations.

use crate::config::{ColumnFamilyConfig, Config, IsolationLevel};
use crate::error::{Error, Result, check_result};
use crate::ffi;
use crate::stats::{CacheStats, DbStats};
use crate::transaction::Transaction;
use libc::{c_char, c_int, c_void, size_t};
use std::ffi::{CStr, CString};
use std::ptr;

/// A single operation from a committed transaction batch.
///
/// Passed to commit hook callbacks. Contains owned copies of the key and value data.
#[derive(Debug, Clone)]
pub struct CommitOp {
    /// The key
    pub key: Vec<u8>,
    /// The value (`None` for delete operations)
    pub value: Option<Vec<u8>>,
    /// TTL (time-to-live) as Unix timestamp, 0 means no expiry
    pub ttl: i64,
    /// Whether this is a delete operation
    pub is_delete: bool,
}

/// Type alias for the boxed commit hook callback.
type CommitHookCallback = Box<dyn Fn(&[CommitOp], u64) -> i32 + Send>;

/// Type alias for the boxed comparator callback.
type ComparatorCallback = Box<dyn Fn(&[u8], &[u8]) -> i32 + Send + Sync>;

/// Trampoline function that bridges the C comparator callback to the Rust closure.
unsafe extern "C" fn comparator_trampoline(
    key1: *const u8,
    key1_size: size_t,
    key2: *const u8,
    key2_size: size_t,
    ctx: *mut c_void,
) -> c_int {
    if ctx.is_null() {
        return 0;
    }

    let callback = unsafe { &*(ctx as *const ComparatorCallback) };

    let k1 = if key1.is_null() || key1_size == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(key1, key1_size) }
    };
    let k2 = if key2.is_null() || key2_size == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(key2, key2_size) }
    };

    callback(k1, k2)
}

/// Initializes TidesDB with the system allocator.
///
/// This must be called exactly once before any other TidesDB function
/// when using the explicit initialization path. If not called, TidesDB
/// will auto-initialize with the system allocator on the first `TidesDB::open()`.
///
/// # Returns
///
/// `Ok(())` on success, or an error if already initialized.
pub fn init() -> Result<()> {
    let result = unsafe { ffi::tidesdb_init(None, None, None, None) };
    check_result(result, "failed to initialize TidesDB")
}

/// Initializes TidesDB with custom C-level memory allocator functions.
///
/// This is an advanced function for integrating with custom memory managers
/// (e.g., jemalloc, mimalloc, Redis module allocator). Must be called exactly
/// once before any other TidesDB function.
///
/// # Safety
///
/// The caller must ensure that the provided function pointers are valid C allocator
/// functions with correct signatures and semantics (malloc/calloc/realloc/free).
///
/// # Arguments
///
/// * `malloc_fn` - Custom malloc function
/// * `calloc_fn` - Custom calloc function
/// * `realloc_fn` - Custom realloc function
/// * `free_fn` - Custom free function
pub unsafe fn init_with_allocator(
    malloc_fn: ffi::tidesdb_malloc_fn,
    calloc_fn: ffi::tidesdb_calloc_fn,
    realloc_fn: ffi::tidesdb_realloc_fn,
    free_fn: ffi::tidesdb_free_fn,
) -> Result<()> {
    let result = unsafe { ffi::tidesdb_init(malloc_fn, calloc_fn, realloc_fn, free_fn) };
    check_result(result, "failed to initialize TidesDB with custom allocator")
}

/// Finalizes TidesDB and resets the allocator.
///
/// Should be called after all TidesDB operations are complete (all databases closed).
/// After calling this, `init()` or `init_with_allocator()` can be called again.
pub fn finalize() {
    unsafe {
        ffi::tidesdb_finalize();
    }
}

/// Raises or reports the process open-file ceiling.
///
/// Call this before opening a database when a larger `max_open_sstables` budget
/// is needed. Passing `0` or a negative value reports the current ceiling
/// without requesting a change.
#[cfg(tidesdb_has_raise_open_file_limit)]
pub fn raise_open_file_limit(desired: libc::c_long) -> libc::c_long {
    unsafe { ffi::tidesdb_raise_open_file_limit(desired) }
}

/// Frees memory allocated by TidesDB.
///
/// This is primarily useful for FFI scenarios where memory allocated by TidesDB
/// needs to be freed using the same allocator. For normal Rust usage, the safe
/// wrappers handle memory management automatically.
///
/// # Safety
///
/// The pointer must have been allocated by TidesDB.
pub unsafe fn free(ptr: *mut c_void) {
    unsafe {
        ffi::tidesdb_free(ptr);
    }
}

/// Trampoline function that bridges the C callback to the Rust closure.
unsafe extern "C" fn commit_hook_trampoline(
    ops: *const ffi::tidesdb_commit_op_t,
    num_ops: c_int,
    commit_seq: u64,
    ctx: *mut c_void,
) -> c_int {
    if ctx.is_null() || ops.is_null() || num_ops <= 0 {
        return -1;
    }

    let callback = unsafe { &*(ctx as *const CommitHookCallback) };

    let mut rust_ops = Vec::with_capacity(num_ops as usize);
    for i in 0..num_ops as isize {
        let op = unsafe { &*ops.offset(i) };
        let key = unsafe { std::slice::from_raw_parts(op.key, op.key_size) }.to_vec();
        let value = if op.is_delete != 0 || op.value.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(op.value, op.value_size) }.to_vec())
        };
        rust_ops.push(CommitOp {
            key,
            value,
            ttl: op.ttl as i64,
            is_delete: op.is_delete != 0,
        });
    }

    callback(&rust_ops, commit_seq)
}

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
/// let mut txn = db.begin_transaction()?;
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
        let c_data = config.to_c_config()?;
        let mut db: *mut ffi::tidesdb_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_open(&c_data.config, &mut db) };
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

        let result =
            unsafe { ffi::tidesdb_create_column_family(self.db, c_name.as_ptr(), &c_config) };
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

    /// Deletes a column family by pointer, skipping the name lookup.
    ///
    /// This is faster than `drop_column_family` when you already hold a `ColumnFamily`.
    /// The `ColumnFamily` is consumed and should not be used after this call.
    ///
    /// # Arguments
    ///
    /// * `cf` - The column family to delete
    pub fn delete_column_family(&self, cf: ColumnFamily) -> Result<()> {
        let result = unsafe { ffi::tidesdb_delete_column_family(self.db, cf.cf) };
        // Prevent ColumnFamily's Drop from trying to clear commit hook on a deleted CF
        std::mem::forget(cf);
        check_result(result, "failed to delete column family")
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

    /// Creates a complete copy of an existing column family with a new name.
    /// The clone contains all the data from the source at the time of cloning.
    ///
    /// # Arguments
    ///
    /// * `source_name` - Name of the source column family to clone
    /// * `dest_name` - Name for the new cloned column family
    pub fn clone_column_family(&self, source_name: &str, dest_name: &str) -> Result<()> {
        let c_source_name = CString::new(source_name)?;
        let c_dest_name = CString::new(dest_name)?;

        let result = unsafe {
            ffi::tidesdb_clone_column_family(self.db, c_source_name.as_ptr(), c_dest_name.as_ptr())
        };
        check_result(result, "failed to clone column family")
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
            hook_ctx: None,
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

        let result = unsafe { ffi::tidesdb_list_column_families(self.db, &mut names, &mut count) };
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
                    ffi::tidesdb_free(name_ptr as *mut c_void);
                }
            }
            ffi::tidesdb_free(names as *mut c_void);
        }

        Ok(result_names)
    }

    /// Cancels background compaction work for this database.
    ///
    /// Flushes are unaffected, so durability is preserved. TidesDB documents
    /// this as a fast-shutdown helper intended to be called before close.
    #[cfg(tidesdb_has_cancel_background_work)]
    pub fn cancel_background_work(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_cancel_background_work(self.db) };
        check_result(result, "failed to cancel background work")
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

        let result =
            unsafe { ffi::tidesdb_txn_begin_with_isolation(self.db, isolation as i32, &mut txn) };
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
    /// The comparator function determines the sort order of keys throughout the entire
    /// syste, memtables, SSTables, block indexes, and iterators. Once a comparator
    /// is set for a column family, it **cannot be changed** without corrupting data.
    ///
    /// # Arguments
    ///
    /// * `name` - The comparator name (used in `ColumnFamilyConfig::comparator_name`)
    /// * `compare_fn` - A comparison function that returns <0 if key1 < key2,
    ///   0 if equal, >0 if key1 > key2
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tidesdb::{TidesDB, Config};
    ///
    /// let db = TidesDB::open(Config::new("./mydb"))?;
    ///
    /// db.register_comparator("reverse", |key1, key2| {
    ///     // Reverse byte comparison
    ///     let min_len = key1.len().min(key2.len());
    ///     for i in 0..min_len {
    ///         if key1[i] != key2[i] {
    ///             return key2[i] as i32 - key1[i] as i32;
    ///         }
    ///     }
    ///     key2.len() as i32 - key1.len() as i32
    /// })?;
    /// # Ok::<(), tidesdb::Error>(())
    /// ```
    pub fn register_comparator<F>(&self, name: &str, compare_fn: F) -> Result<()>
    where
        F: Fn(&[u8], &[u8]) -> i32 + Send + Sync + 'static,
    {
        let c_name = CString::new(name)?;

        let boxed: Box<ComparatorCallback> = Box::new(Box::new(compare_fn));
        let raw = Box::into_raw(boxed);

        let result = unsafe {
            ffi::tidesdb_register_comparator(
                self.db,
                c_name.as_ptr(),
                Some(comparator_trampoline),
                std::ptr::null(), // ctx_str
                raw as *mut c_void,
            )
        };

        if result != ffi::TDB_SUCCESS {
            // Reclaim the box if the C call failed
            unsafe {
                drop(Box::from_raw(raw));
            }
            return Err(Error::from_code(result, "failed to register comparator"));
        }

        // The context is now owned by the C library for the lifetime of the database.
        // It will be leaked intentionally - the C API has no destroy callback for comparators.
        // The memory is freed when the process exits.

        Ok(())
    }

    /// Checks if a comparator is registered with the database.
    ///
    /// # Arguments
    ///
    /// * `name` - The comparator name to look up
    ///
    /// # Returns
    ///
    /// `true` if the comparator is registered, `false` otherwise.
    pub fn has_comparator(&self, name: &str) -> bool {
        let c_name = match CString::new(name) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let mut fn_out: ffi::tidesdb_comparator_fn = None;
        let mut ctx_out: *mut c_void = ptr::null_mut();

        let result = unsafe {
            ffi::tidesdb_get_comparator(self.db, c_name.as_ptr(), &mut fn_out, &mut ctx_out)
        };

        result == ffi::TDB_SUCCESS
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

    /// Creates a lightweight, near-instant snapshot of the database using hard links
    /// instead of copying SSTable data.
    ///
    /// # Arguments
    ///
    /// * `checkpoint_dir` - The directory to create the checkpoint in.
    ///   Must be a non-existent or empty directory.
    pub fn checkpoint(&self, checkpoint_dir: &str) -> Result<()> {
        let c_dir = CString::new(checkpoint_dir)?;

        let result = unsafe { ffi::tidesdb_checkpoint(self.db, c_dir.as_ptr()) };
        check_result(result, "failed to checkpoint database")
    }

    /// Forces a synchronous flush and aggressive compaction for **all** column families,
    /// then drains both the global flush and compaction queues.
    ///
    /// This is a blocking operation - it will not return until all flush and compaction
    /// work is complete.
    pub fn purge(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_purge(self.db) };
        check_result(result, "failed to purge database")
    }

    /// Retrieves aggregate statistics across the entire database instance.
    ///
    /// Unlike `ColumnFamily::get_stats` (which heap-allocates), this fills a
    /// caller-provided struct on the stack. No free is needed.
    pub fn get_db_stats(&self) -> Result<DbStats> {
        let mut c_stats = std::mem::MaybeUninit::<ffi::tidesdb_db_stats_t>::zeroed();

        let result = unsafe { ffi::tidesdb_get_db_stats(self.db, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get database stats")?;

        let c_stats = unsafe { c_stats.assume_init() };

        // Write-amplification counters (tidesdb >= 9.3.4; 0 on older libraries).
        #[cfg(tidesdb_has_write_amp_stats)]
        let (
            uwal_bytes_written,
            wal_bytes_written,
            flush_bytes_written,
            compaction_bytes_written,
            compaction_bytes_read,
            user_bytes_written,
            flush_count,
            compaction_count,
        ) = (
            c_stats.uwal_bytes_written,
            c_stats.wal_bytes_written,
            c_stats.flush_bytes_written,
            c_stats.compaction_bytes_written,
            c_stats.compaction_bytes_read,
            c_stats.user_bytes_written,
            c_stats.flush_count,
            c_stats.compaction_count,
        );
        #[cfg(not(tidesdb_has_write_amp_stats))]
        let (
            uwal_bytes_written,
            wal_bytes_written,
            flush_bytes_written,
            compaction_bytes_written,
            compaction_bytes_read,
            user_bytes_written,
            flush_count,
            compaction_count,
        ) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);

        let object_store_connector = if c_stats.object_store_connector.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(c_stats.object_store_connector) }
                .to_string_lossy()
                .into_owned()
        };

        Ok(DbStats {
            num_column_families: c_stats.num_column_families,
            total_memory: c_stats.total_memory,
            available_memory: c_stats.available_memory,
            resolved_memory_limit: c_stats.resolved_memory_limit,
            memory_pressure_level: c_stats.memory_pressure_level,
            flush_pending_count: c_stats.flush_pending_count,
            total_memtable_bytes: c_stats.total_memtable_bytes,
            total_immutable_count: c_stats.total_immutable_count,
            total_sstable_count: c_stats.total_sstable_count,
            total_data_size_bytes: c_stats.total_data_size_bytes,
            num_open_sstables: c_stats.num_open_sstables,
            global_seq: c_stats.global_seq,
            txn_memory_bytes: c_stats.txn_memory_bytes,
            compaction_queue_size: c_stats.compaction_queue_size,
            flush_queue_size: c_stats.flush_queue_size,
            unified_memtable_enabled: c_stats.unified_memtable_enabled != 0,
            unified_memtable_bytes: c_stats.unified_memtable_bytes,
            unified_immutable_count: c_stats.unified_immutable_count,
            unified_is_flushing: c_stats.unified_is_flushing != 0,
            unified_next_cf_index: c_stats.unified_next_cf_index,
            unified_wal_generation: c_stats.unified_wal_generation,
            object_store_enabled: c_stats.object_store_enabled != 0,
            object_store_connector,
            local_cache_bytes_used: c_stats.local_cache_bytes_used,
            local_cache_bytes_max: c_stats.local_cache_bytes_max,
            local_cache_num_files: c_stats.local_cache_num_files,
            last_uploaded_generation: c_stats.last_uploaded_generation,
            upload_queue_depth: c_stats.upload_queue_depth,
            total_uploads: c_stats.total_uploads,
            total_upload_failures: c_stats.total_upload_failures,
            replica_mode: c_stats.replica_mode != 0,
            uwal_bytes_written,
            wal_bytes_written,
            flush_bytes_written,
            compaction_bytes_written,
            compaction_bytes_read,
            user_bytes_written,
            flush_count,
            compaction_count,
        })
    }

    /// Switches a read-only replica to primary mode.
    ///
    /// This is only valid when the database was opened in replica mode
    /// (via object store configuration with `replica_mode` enabled).
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the database is not in replica mode.
    pub fn promote_to_primary(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_promote_to_primary(self.db) };
        check_result(result, "failed to promote to primary")
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
    /// Stored commit hook context for cleanup on drop/clear.
    hook_ctx: Option<*mut CommitHookCallback>,
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

            let level_tombstone_counts = {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    let mut counts = Vec::new();
                    if num_levels > 0 && !(*c_stats).level_tombstone_counts.is_null() {
                        for i in 0..num_levels as isize {
                            counts.push(*(*c_stats).level_tombstone_counts.offset(i));
                        }
                    }
                    counts
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    Vec::new()
                }
            };

            let total_keys = (*c_stats).total_keys;
            let total_data_size = (*c_stats).total_data_size;
            let avg_key_size = (*c_stats).avg_key_size;
            let avg_value_size = (*c_stats).avg_value_size;
            let read_amp = (*c_stats).read_amp;
            let hit_rate = (*c_stats).hit_rate;
            let use_btree = (*c_stats).use_btree != 0;
            let btree_total_nodes = (*c_stats).btree_total_nodes;
            let btree_max_height = (*c_stats).btree_max_height;
            let btree_avg_height = (*c_stats).btree_avg_height;
            let total_tombstones = {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    (*c_stats).total_tombstones
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0
                }
            };
            let tombstone_ratio = {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    (*c_stats).tombstone_ratio
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0.0
                }
            };
            let max_sst_density = {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    (*c_stats).max_sst_density
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0.0
                }
            };
            let max_sst_density_level = {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    (*c_stats).max_sst_density_level
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0
                }
            };

            // Write-amplification counters (tidesdb >= 9.3.4; 0 on older libraries).
            #[cfg(tidesdb_has_write_amp_stats)]
            let (
                wal_bytes_written,
                flush_bytes_written,
                compaction_bytes_written,
                compaction_bytes_read,
                user_bytes_written,
                flush_count,
                compaction_count,
            ) = (
                (*c_stats).wal_bytes_written,
                (*c_stats).flush_bytes_written,
                (*c_stats).compaction_bytes_written,
                (*c_stats).compaction_bytes_read,
                (*c_stats).user_bytes_written,
                (*c_stats).flush_count,
                (*c_stats).compaction_count,
            );
            #[cfg(not(tidesdb_has_write_amp_stats))]
            let (
                wal_bytes_written,
                flush_bytes_written,
                compaction_bytes_written,
                compaction_bytes_read,
                user_bytes_written,
                flush_count,
                compaction_count,
            ) = (0u64, 0u64, 0u64, 0u64, 0u64, 0u64, 0u64);

            let config = if (*c_stats).config.is_null() {
                None
            } else {
                Some(ColumnFamilyConfig::from_c_config_ptr((*c_stats).config))
            };

            ffi::tidesdb_free_stats(c_stats);

            crate::stats::Stats {
                num_levels,
                memtable_size,
                level_sizes,
                level_num_sstables,
                config,
                total_keys,
                total_data_size,
                avg_key_size,
                avg_value_size,
                level_key_counts,
                read_amp,
                hit_rate,
                use_btree,
                btree_total_nodes,
                btree_max_height,
                btree_avg_height,
                total_tombstones,
                tombstone_ratio,
                level_tombstone_counts,
                max_sst_density,
                max_sst_density_level,
                wal_bytes_written,
                flush_bytes_written,
                compaction_bytes_written,
                compaction_bytes_read,
                user_bytes_written,
                flush_count,
                compaction_count,
            }
        };

        Ok(stats)
    }

    /// Manually triggers compaction for this column family.
    pub fn compact(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_compact(self.cf) };
        check_result(result, "failed to compact column family")
    }

    /// Synchronously compacts every SSTable whose key range overlaps `[start_key, end_key)`.
    ///
    /// Blocks the calling thread until the merge commits or fails (it does not enqueue
    /// onto the compaction thread pool). `None` means unbounded on that side; passing
    /// `None` for both endpoints is rejected -- use [`compact`](Self::compact) for full
    /// column-family compaction.
    ///
    /// # Arguments
    ///
    /// * `start_key` - Inclusive start of the range, or `None` for unbounded
    /// * `end_key` - Exclusive end of the range, or `None` for unbounded
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error: `InvalidArgs` if both endpoints are `None`,
    /// `Locked` if another compaction is already running, or standard I/O / memory errors.
    #[cfg(tidesdb_has_compact_range)]
    pub fn compact_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> Result<()> {
        // Empty slices are treated as unbounded (NULL) to avoid passing a
        // dangling pointer-of-element-zero deref to C.
        let (start_ptr, start_len) = match start_key {
            Some(s) if !s.is_empty() => (s.as_ptr(), s.len()),
            _ => (std::ptr::null(), 0),
        };
        let (end_ptr, end_len) = match end_key {
            Some(s) if !s.is_empty() => (s.as_ptr(), s.len()),
            _ => (std::ptr::null(), 0),
        };

        let result =
            unsafe { ffi::tidesdb_compact_range(self.cf, start_ptr, start_len, end_ptr, end_len) };
        check_result(result, "failed to compact range")
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

    /// Forces a synchronous flush and aggressive compaction for this column family.
    ///
    /// Unlike `flush_memtable` and `compact` (which are non-blocking), purge blocks
    /// until all flush and compaction I/O is complete.
    ///
    /// **Behavior:**
    /// 1. Waits for any in-progress flush to complete
    /// 2. Force-flushes the active memtable (even if below threshold)
    /// 3. Waits for flush I/O to fully complete
    /// 4. Waits for any in-progress compaction to complete
    /// 5. Triggers synchronous compaction inline (bypasses the compaction queue)
    /// 6. Waits for any queued compaction to drain
    pub fn purge(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_purge_cf(self.cf) };
        check_result(result, "failed to purge column family")
    }

    /// Forces an immediate fsync of the active write-ahead log for this column family.
    ///
    /// This is useful for explicit durability control when using `SyncMode::None` or
    /// `SyncMode::Interval`.
    ///
    /// **When to use:**
    /// - Application-controlled durability after a batch of related writes
    /// - Pre-checkpoint to ensure all buffered WAL data is on disk
    /// - Graceful shutdown to flush WAL buffers before closing
    /// - Critical writes that need durability without `SyncMode::Full` for all writes
    pub fn sync_wal(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_sync_wal(self.cf) };
        check_result(result, "failed to sync WAL")
    }

    /// Estimates the computational cost of iterating between two keys in this column family.
    /// The returned value is an opaque double - meaningful only for comparison with other
    /// values from the same function. Uses only in-memory metadata and performs no disk I/O.
    ///
    /// Key order does not matter - the function normalizes the range so `key_a > key_b`
    /// produces the same result as `key_b > key_a`.
    ///
    /// # Arguments
    ///
    /// * `key_a` - First key (bound of range)
    /// * `key_b` - Second key (bound of range)
    ///
    /// # Returns
    ///
    /// Estimated traversal cost (higher = more expensive). A cost of 0.0 means no
    /// overlapping SSTables or memtable entries were found for the range.
    pub fn range_cost(&self, key_a: &[u8], key_b: &[u8]) -> Result<f64> {
        let mut cost: f64 = 0.0;

        let result = unsafe {
            ffi::tidesdb_range_cost(
                self.cf,
                key_a.as_ptr(),
                key_a.len(),
                key_b.as_ptr(),
                key_b.len(),
                &mut cost,
            )
        };
        check_result(result, "failed to estimate range cost")?;

        Ok(cost)
    }

    /// Sets a commit hook callback for this column family.
    ///
    /// The hook fires synchronously after every transaction commit on this column family.
    /// It receives the full batch of committed operations atomically, enabling real-time
    /// change data capture without WAL parsing.
    ///
    /// The hook fires after WAL write, memtable apply, and commit status marking are
    /// complete - the data is fully durable before the callback runs. Hook failure
    /// (non-zero return) is logged but does not affect the commit result.
    ///
    /// # Arguments
    ///
    /// * `callback` - A closure receiving a slice of `CommitOp` and the monotonic
    ///   commit sequence number. Return 0 on success, non-zero on failure.
    pub fn set_commit_hook<F>(&mut self, callback: F) -> Result<()>
    where
        F: Fn(&[CommitOp], u64) -> i32 + Send + 'static,
    {
        // Clear any existing hook first
        self.clear_commit_hook()?;

        let boxed: Box<CommitHookCallback> = Box::new(Box::new(callback));
        let raw = Box::into_raw(boxed);

        let result = unsafe {
            ffi::tidesdb_cf_set_commit_hook(
                self.cf,
                Some(commit_hook_trampoline),
                raw as *mut c_void,
            )
        };

        if result != ffi::TDB_SUCCESS {
            // Reclaim the box if the C call failed
            unsafe {
                drop(Box::from_raw(raw));
            }
            return Err(Error::from_code(result, "failed to set commit hook"));
        }

        self.hook_ctx = Some(raw);
        Ok(())
    }

    /// Clears the commit hook for this column family.
    ///
    /// After clearing, no callback will fire on subsequent commits.
    pub fn clear_commit_hook(&mut self) -> Result<()> {
        if let Some(raw) = self.hook_ctx.take() {
            let result = unsafe { ffi::tidesdb_cf_set_commit_hook(self.cf, None, ptr::null_mut()) };
            // Free the boxed callback regardless of C call result
            unsafe {
                drop(Box::from_raw(raw));
            }
            check_result(result, "failed to clear commit hook")?;
        }
        Ok(())
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

impl Drop for ColumnFamily {
    fn drop(&mut self) {
        // Clear the commit hook to free the boxed callback.
        // We ignore the result since we're in Drop.
        if let Some(raw) = self.hook_ctx.take() {
            unsafe {
                let _ = ffi::tidesdb_cf_set_commit_hook(self.cf, None, ptr::null_mut());
                drop(Box::from_raw(raw));
            }
        }
    }
}
