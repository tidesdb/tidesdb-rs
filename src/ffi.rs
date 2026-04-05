// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Raw FFI bindings to the TidesDB C library.
//! These are unsafe and should not be used directly.

use libc::{c_char, c_double, c_float, c_int, c_void, size_t, time_t};

/// Maximum comparator name length
pub const TDB_MAX_COMPARATOR_NAME: usize = 64;
/// Maximum comparator context string length
pub const TDB_MAX_COMPARATOR_CTX: usize = 256;
/// Maximum column family name length
pub const TDB_MAX_CF_NAME_LEN: usize = 128;

/// Error codes (negative values as per C API)
pub const TDB_SUCCESS: c_int = 0;
pub const TDB_ERR_MEMORY: c_int = -1;
pub const TDB_ERR_INVALID_ARGS: c_int = -2;
pub const TDB_ERR_NOT_FOUND: c_int = -3;
pub const TDB_ERR_IO: c_int = -4;
pub const TDB_ERR_CORRUPTION: c_int = -5;
pub const TDB_ERR_EXISTS: c_int = -6;
pub const TDB_ERR_CONFLICT: c_int = -7;
pub const TDB_ERR_TOO_LARGE: c_int = -8;
pub const TDB_ERR_MEMORY_LIMIT: c_int = -9;
pub const TDB_ERR_INVALID_DB: c_int = -10;
pub const TDB_ERR_UNKNOWN: c_int = -11;
pub const TDB_ERR_LOCKED: c_int = -12;
pub const TDB_ERR_READONLY: c_int = -13;

/// Compression algorithms (values from C API documentation)
pub const NO_COMPRESSION: c_int = 0;
pub const SNAPPY_COMPRESSION: c_int = 1;
pub const LZ4_COMPRESSION: c_int = 2;
pub const ZSTD_COMPRESSION: c_int = 3;
pub const LZ4_FAST_COMPRESSION: c_int = 4;

/// Sync modes
pub const TDB_SYNC_NONE: c_int = 0;
pub const TDB_SYNC_FULL: c_int = 1;
pub const TDB_SYNC_INTERVAL: c_int = 2;

/// Log levels
pub const TDB_LOG_DEBUG: c_int = 0;
pub const TDB_LOG_INFO: c_int = 1;
pub const TDB_LOG_WARN: c_int = 2;
pub const TDB_LOG_ERROR: c_int = 3;
pub const TDB_LOG_FATAL: c_int = 4;
pub const TDB_LOG_NONE: c_int = 99;

/// Isolation levels
pub const TDB_ISOLATION_READ_UNCOMMITTED: c_int = 0;
pub const TDB_ISOLATION_READ_COMMITTED: c_int = 1;
pub const TDB_ISOLATION_REPEATABLE_READ: c_int = 2;
pub const TDB_ISOLATION_SNAPSHOT: c_int = 3;
pub const TDB_ISOLATION_SERIALIZABLE: c_int = 4;

/// Opaque types
#[repr(C)]
pub struct tidesdb_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_txn_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_column_family_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_iter_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_objstore_t {
    _private: [u8; 0],
}

/// Object store behavior configuration
#[repr(C)]
pub struct tidesdb_objstore_config_t {
    pub local_cache_path: *const c_char,
    pub local_cache_max_bytes: size_t,
    pub cache_on_read: c_int,
    pub cache_on_write: c_int,
    pub max_concurrent_uploads: c_int,
    pub max_concurrent_downloads: c_int,
    pub multipart_threshold: size_t,
    pub multipart_part_size: size_t,
    pub sync_manifest_to_object: c_int,
    pub replicate_wal: c_int,
    pub wal_upload_sync: c_int,
    pub wal_sync_threshold_bytes: size_t,
    pub wal_sync_on_commit: c_int,
    pub replica_mode: c_int,
    pub replica_sync_interval_us: u64,
    pub replica_replay_wal: c_int,
}

/// Custom allocator function types
#[allow(non_camel_case_types)]
pub type tidesdb_malloc_fn = Option<unsafe extern "C" fn(size: size_t) -> *mut c_void>;
#[allow(non_camel_case_types)]
pub type tidesdb_calloc_fn = Option<unsafe extern "C" fn(nmemb: size_t, size: size_t) -> *mut c_void>;
#[allow(non_camel_case_types)]
pub type tidesdb_realloc_fn = Option<unsafe extern "C" fn(ptr: *mut c_void, size: size_t) -> *mut c_void>;
#[allow(non_camel_case_types)]
pub type tidesdb_free_fn = Option<unsafe extern "C" fn(ptr: *mut c_void)>;

/// Comparator function type
#[allow(non_camel_case_types)]
pub type tidesdb_comparator_fn = Option<
    unsafe extern "C" fn(
        key1: *const u8,
        key1_size: size_t,
        key2: *const u8,
        key2_size: size_t,
        ctx: *mut c_void,
    ) -> c_int,
>;


/// Database configuration
#[repr(C)]
pub struct tidesdb_config_t {
    pub db_path: *const c_char,
    pub num_flush_threads: c_int,
    pub num_compaction_threads: c_int,
    pub log_level: c_int,
    pub block_cache_size: size_t,
    pub max_open_sstables: size_t,
    pub log_to_file: c_int,
    pub log_truncation_at: size_t,
    pub max_memory_usage: size_t,
    pub unified_memtable: c_int,
    pub unified_memtable_write_buffer_size: size_t,
    pub unified_memtable_skip_list_max_level: c_int,
    pub unified_memtable_skip_list_probability: c_float,
    pub unified_memtable_sync_mode: c_int,
    pub unified_memtable_sync_interval_us: u64,
    pub object_store: *mut tidesdb_objstore_t,
    pub object_store_config: *mut tidesdb_objstore_config_t,
}

/// Commit operation passed to commit hook callback
#[repr(C)]
pub struct tidesdb_commit_op_t {
    pub key: *const u8,
    pub key_size: size_t,
    pub value: *const u8,
    pub value_size: size_t,
    pub ttl: time_t,
    pub is_delete: c_int,
}

/// Commit hook callback function type
#[allow(non_camel_case_types)]
pub type tidesdb_commit_hook_fn = Option<
    unsafe extern "C" fn(
        ops: *const tidesdb_commit_op_t,
        num_ops: c_int,
        commit_seq: u64,
        ctx: *mut c_void,
    ) -> c_int,
>;

/// Column family configuration
#[repr(C)]
pub struct tidesdb_column_family_config_t {
    pub name: [c_char; TDB_MAX_CF_NAME_LEN],
    pub write_buffer_size: size_t,
    pub level_size_ratio: size_t,
    pub min_levels: c_int,
    pub dividing_level_offset: c_int,
    pub klog_value_threshold: size_t,
    pub compression_algo: c_int,
    pub enable_bloom_filter: c_int,
    pub bloom_fpr: c_double,
    pub enable_block_indexes: c_int,
    pub index_sample_ratio: c_int,
    pub block_index_prefix_len: c_int,
    pub sync_mode: c_int,
    pub sync_interval_us: u64,
    pub comparator_name: [c_char; TDB_MAX_COMPARATOR_NAME],
    pub comparator_ctx_str: [c_char; TDB_MAX_COMPARATOR_CTX],
    pub comparator_fn_cached: *mut c_void,
    pub comparator_ctx_cached: *mut c_void,
    pub skip_list_max_level: c_int,
    pub skip_list_probability: c_float,
    pub default_isolation_level: c_int,
    pub min_disk_space: u64,
    pub l1_file_count_trigger: c_int,
    pub l0_queue_stall_threshold: c_int,
    pub use_btree: c_int,
    pub commit_hook_fn: tidesdb_commit_hook_fn,
    pub commit_hook_ctx: *mut c_void,
    pub object_target_file_size: size_t,
    pub object_lazy_compaction: c_int,
    pub object_prefetch_compaction: c_int,
}

/// Statistics for a column family
#[repr(C)]
pub struct tidesdb_stats_t {
    pub num_levels: c_int,
    pub memtable_size: size_t,
    pub level_sizes: *mut size_t,
    pub level_num_sstables: *mut c_int,
    pub config: *mut tidesdb_column_family_config_t,
    pub total_keys: u64,
    pub total_data_size: u64,
    pub avg_key_size: c_double,
    pub avg_value_size: c_double,
    pub level_key_counts: *mut u64,
    pub read_amp: c_double,
    pub hit_rate: c_double,
    pub use_btree: c_int,
    pub btree_total_nodes: u64,
    pub btree_max_height: u32,
    pub btree_avg_height: c_double,
}

/// Database-level aggregate statistics
#[repr(C)]
pub struct tidesdb_db_stats_t {
    pub num_column_families: c_int,
    pub total_memory: u64,
    pub available_memory: u64,
    pub resolved_memory_limit: size_t,
    pub memory_pressure_level: c_int,
    pub flush_pending_count: c_int,
    pub total_memtable_bytes: i64,
    pub total_immutable_count: c_int,
    pub total_sstable_count: c_int,
    pub total_data_size_bytes: u64,
    pub num_open_sstables: c_int,
    pub global_seq: u64,
    pub txn_memory_bytes: i64,
    pub compaction_queue_size: size_t,
    pub flush_queue_size: size_t,
    pub unified_memtable_enabled: c_int,
    pub unified_memtable_bytes: i64,
    pub unified_immutable_count: c_int,
    pub unified_is_flushing: c_int,
    pub unified_next_cf_index: u32,
    pub unified_wal_generation: u64,
    pub object_store_enabled: c_int,
    pub object_store_connector: *const c_char,
    pub local_cache_bytes_used: size_t,
    pub local_cache_bytes_max: size_t,
    pub local_cache_num_files: c_int,
    pub last_uploaded_generation: u64,
    pub upload_queue_depth: size_t,
    pub total_uploads: u64,
    pub total_upload_failures: u64,
    pub replica_mode: c_int,
}

/// Cache statistics
#[repr(C)]
pub struct tidesdb_cache_stats_t {
    pub enabled: c_int,
    pub total_entries: size_t,
    pub total_bytes: size_t,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: c_double,
    pub num_partitions: size_t,
}

#[link(name = "tidesdb")]
unsafe extern "C" {
    // Initialization / custom allocator
    pub fn tidesdb_init(
        malloc_fn: tidesdb_malloc_fn,
        calloc_fn: tidesdb_calloc_fn,
        realloc_fn: tidesdb_realloc_fn,
        free_fn: tidesdb_free_fn,
    ) -> c_int;
    pub fn tidesdb_finalize();

    // Database operations
    pub fn tidesdb_open(config: *const tidesdb_config_t, db: *mut *mut tidesdb_t) -> c_int;
    pub fn tidesdb_close(db: *mut tidesdb_t) -> c_int;

    // Column family operations
    pub fn tidesdb_create_column_family(
        db: *mut tidesdb_t,
        name: *const c_char,
        config: *const tidesdb_column_family_config_t,
    ) -> c_int;
    pub fn tidesdb_drop_column_family(db: *mut tidesdb_t, name: *const c_char) -> c_int;
    pub fn tidesdb_delete_column_family(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
    ) -> c_int;
    pub fn tidesdb_get_column_family(
        db: *mut tidesdb_t,
        name: *const c_char,
    ) -> *mut tidesdb_column_family_t;
    pub fn tidesdb_list_column_families(
        db: *mut tidesdb_t,
        names: *mut *mut *mut c_char,
        count: *mut c_int,
    ) -> c_int;

    // Default configuration
    pub fn tidesdb_default_column_family_config() -> tidesdb_column_family_config_t;

    // Statistics
    pub fn tidesdb_get_stats(
        cf: *mut tidesdb_column_family_t,
        stats: *mut *mut tidesdb_stats_t,
    ) -> c_int;
    pub fn tidesdb_free_stats(stats: *mut tidesdb_stats_t);
    pub fn tidesdb_get_cache_stats(
        db: *mut tidesdb_t,
        stats: *mut tidesdb_cache_stats_t,
    ) -> c_int;

    // Maintenance
    pub fn tidesdb_compact(cf: *mut tidesdb_column_family_t) -> c_int;
    pub fn tidesdb_flush_memtable(cf: *mut tidesdb_column_family_t) -> c_int;

    // Comparator
    pub fn tidesdb_register_comparator(
        db: *mut tidesdb_t,
        name: *const c_char,
        compare_fn: tidesdb_comparator_fn,
        ctx_str: *const c_char,
        ctx: *mut c_void,
    ) -> c_int;

    // Transaction operations
    pub fn tidesdb_txn_begin(db: *mut tidesdb_t, txn: *mut *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_begin_with_isolation(
        db: *mut tidesdb_t,
        isolation: c_int,
        txn: *mut *mut tidesdb_txn_t,
    ) -> c_int;
    pub fn tidesdb_txn_put(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_len: size_t,
        value: *const u8,
        value_len: size_t,
        ttl: time_t,
    ) -> c_int;
    pub fn tidesdb_txn_get(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_len: size_t,
        value: *mut *mut u8,
        value_len: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_txn_delete(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_len: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_commit(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_rollback(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_free(txn: *mut tidesdb_txn_t);
    pub fn tidesdb_txn_reset(txn: *mut tidesdb_txn_t, isolation: c_int) -> c_int;

    // Savepoints
    pub fn tidesdb_txn_savepoint(txn: *mut tidesdb_txn_t, name: *const c_char) -> c_int;
    pub fn tidesdb_txn_rollback_to_savepoint(
        txn: *mut tidesdb_txn_t,
        name: *const c_char,
    ) -> c_int;
    pub fn tidesdb_txn_release_savepoint(txn: *mut tidesdb_txn_t, name: *const c_char) -> c_int;

    // Iterator operations
    pub fn tidesdb_iter_new(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        iter: *mut *mut tidesdb_iter_t,
    ) -> c_int;
    pub fn tidesdb_iter_seek_to_first(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_seek_to_last(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_seek(iter: *mut tidesdb_iter_t, key: *const u8, key_len: size_t) -> c_int;
    pub fn tidesdb_iter_seek_for_prev(
        iter: *mut tidesdb_iter_t,
        key: *const u8,
        key_len: size_t,
    ) -> c_int;
    pub fn tidesdb_iter_valid(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_next(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_prev(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_key(
        iter: *mut tidesdb_iter_t,
        key: *mut *mut u8,
        key_len: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_iter_value(
        iter: *mut tidesdb_iter_t,
        value: *mut *mut u8,
        value_len: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_iter_free(iter: *mut tidesdb_iter_t);

    // Column family rename
    pub fn tidesdb_rename_column_family(
        db: *mut tidesdb_t,
        old_name: *const c_char,
        new_name: *const c_char,
    ) -> c_int;

    // Column family clone
    pub fn tidesdb_clone_column_family(
        db: *mut tidesdb_t,
        source_name: *const c_char,
        dest_name: *const c_char,
    ) -> c_int;

    // Default config
    pub fn tidesdb_default_config() -> tidesdb_config_t;

    // Backup
    pub fn tidesdb_backup(db: *mut tidesdb_t, dir: *mut c_char) -> c_int;

    // Checkpoint
    pub fn tidesdb_checkpoint(db: *mut tidesdb_t, checkpoint_dir: *const c_char) -> c_int;

    // Flushing/compacting status
    pub fn tidesdb_is_flushing(cf: *mut tidesdb_column_family_t) -> c_int;
    pub fn tidesdb_is_compacting(cf: *mut tidesdb_column_family_t) -> c_int;

    // Config INI operations
    pub fn tidesdb_cf_config_load_from_ini(
        ini_file: *const c_char,
        section_name: *const c_char,
        config: *mut tidesdb_column_family_config_t,
    ) -> c_int;
    pub fn tidesdb_cf_config_save_to_ini(
        ini_file: *const c_char,
        section_name: *const c_char,
        config: *const tidesdb_column_family_config_t,
    ) -> c_int;

    // Runtime config update
    pub fn tidesdb_cf_update_runtime_config(
        cf: *mut tidesdb_column_family_t,
        new_config: *const tidesdb_column_family_config_t,
        persist_to_disk: c_int,
    ) -> c_int;

    // Get comparator
    pub fn tidesdb_get_comparator(
        db: *mut tidesdb_t,
        name: *const c_char,
        fn_out: *mut tidesdb_comparator_fn,
        ctx_out: *mut *mut c_void,
    ) -> c_int;

    // Commit hook
    pub fn tidesdb_cf_set_commit_hook(
        cf: *mut tidesdb_column_family_t,
        hook_fn: tidesdb_commit_hook_fn,
        ctx: *mut c_void,
    ) -> c_int;

    // Range cost estimation
    pub fn tidesdb_range_cost(
        cf: *mut tidesdb_column_family_t,
        key_a: *const u8,
        key_a_size: size_t,
        key_b: *const u8,
        key_b_size: size_t,
        cost: *mut c_double,
    ) -> c_int;

    // Purge operations
    pub fn tidesdb_purge_cf(cf: *mut tidesdb_column_family_t) -> c_int;
    pub fn tidesdb_purge(db: *mut tidesdb_t) -> c_int;

    // WAL sync
    pub fn tidesdb_sync_wal(cf: *mut tidesdb_column_family_t) -> c_int;

    // Database-level statistics
    pub fn tidesdb_get_db_stats(
        db: *mut tidesdb_t,
        stats: *mut tidesdb_db_stats_t,
    ) -> c_int;

    // Iterator key+value combined
    pub fn tidesdb_iter_key_value(
        iter: *mut tidesdb_iter_t,
        key: *mut *mut u8,
        key_size: *mut size_t,
        value: *mut *mut u8,
        value_size: *mut size_t,
    ) -> c_int;

    // Promote replica to primary
    pub fn tidesdb_promote_to_primary(db: *mut tidesdb_t) -> c_int;

    // Object store connector factories
    pub fn tidesdb_objstore_fs_create(root_dir: *const c_char) -> *mut tidesdb_objstore_t;

    // Object store default config
    pub fn tidesdb_objstore_default_config() -> tidesdb_objstore_config_t;

    // Generic free
    pub fn tidesdb_free(ptr: *mut c_void);
}
