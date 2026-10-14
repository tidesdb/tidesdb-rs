// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Raw FFI bindings to the TidesDB C library.
//!
//! Mirrors `include/db.h` of the bundled TidesDB release (see `TIDESDB_VERSION`).
//! Every `#[repr(C)]` struct here must match the C layout exactly, since several
//! are filled in place by the library. The layout tests at the bottom of this file
//! pin the sizes so a header change cannot slip through silently.
//!
//! These are unsafe and should not be used directly.

#![allow(dead_code)]
#![allow(non_camel_case_types)]

use libc::{c_char, c_double, c_float, c_int, c_long, c_void, size_t};

/// The C `time_t` of the compiler that built TidesDB. `libc::time_t` is not used because it
/// is 32-bit on 32-bit glibc targets even where the C toolchain defaults to a 64-bit
/// `time_t`; build.rs asks the target compiler and sets `tidesdb_time_t_32` when it is 32-bit.
#[cfg(not(tidesdb_time_t_32))]
pub type time_t = i64;
#[cfg(tidesdb_time_t_32)]
pub type time_t = i32;

/// The TidesDB release these bindings were written against.
pub const TIDESDB_VERSION: &str = "10.2.1";

// ===== enums =====

/// Log levels (`tidesdb_log_level_t`)
pub const TDB_LOG_NONE: c_int = 0;
pub const TDB_LOG_TRACE: c_int = 1;
pub const TDB_LOG_INFO: c_int = 2;
pub const TDB_LOG_WARN: c_int = 3;
pub const TDB_LOG_ERROR: c_int = 4;

/// Isolation levels (`tidesdb_isolation_level_t`)
pub const TDB_ISOLATION_READ_UNCOMMITTED: c_int = 0;
pub const TDB_ISOLATION_READ_COMMITTED: c_int = 1;
pub const TDB_ISOLATION_REPEATABLE_READ: c_int = 2;
pub const TDB_ISOLATION_SNAPSHOT: c_int = 3;
pub const TDB_ISOLATION_SERIALIZABLE: c_int = 4;

/// Transaction lifecycle states (`tidesdb_txn_state_t`)
pub const TDB_TXN_STATE_ACTIVE: c_int = 0;
pub const TDB_TXN_STATE_PREPARED: c_int = 1;
pub const TDB_TXN_STATE_COMMITTED: c_int = 2;
pub const TDB_TXN_STATE_ABORTED: c_int = 3;

/// Compression algorithms (`tidesdb_compression_algorithm_t`), also the encoding ids
/// accepted in a column family's encoding pipeline.
pub const TDB_COMPRESS_NONE: c_int = 0;
pub const TDB_COMPRESS_SNAPPY: c_int = 1;
pub const TDB_COMPRESS_LZ4: c_int = 2;
pub const TDB_COMPRESS_ZSTD: c_int = 3;
pub const TDB_COMPRESS_LZ4_FAST: c_int = 4;

/// Sync modes (`tidesdb_sync_mode_t`)
pub const TDB_SYNC_NONE: c_int = 0;
pub const TDB_SYNC_FULL: c_int = 1;
pub const TDB_SYNC_INTERVAL: c_int = 2;

/// Stall reasons (`tidesdb_stall_reason_t`)
pub const TDB_STALL_WAL_APPEND: c_int = 0;
pub const TDB_STALL_ROTATE_LOCK: c_int = 1;
pub const TDB_STALL_ROTATE_WORK: c_int = 2;
pub const TDB_STALL_ADMISSION: c_int = 3;
pub const TDB_STALL_MANIFEST_COMMIT: c_int = 4;
pub const TDB_STALL_COUNT: usize = 5;

/// I/O classes (`tidesdb_io_class_t`)
pub const TDB_IO_SSTABLE: c_int = 0;
pub const TDB_IO_WAL: c_int = 1;
pub const TDB_IO_VLOG: c_int = 2;
pub const TDB_IO_COUNT: usize = 3;

// ===== error codes =====

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
pub const TDB_ERR_TXN_EXPIRED: c_int = -14;
pub const TDB_ERR_NO_SPACE: c_int = -15;
pub const TDB_ERR_TXN_ABORTED: c_int = -16;
pub const TDB_ERR_TOO_OLD: c_int = -17;

// ===== configuration limits =====

pub const TDB_MAX_CF_NAME_LEN: usize = 128;
pub const TDB_ENCODING_PIPELINE_MAX: usize = 8;
pub const TDB_MAX_RANGE_BOUND_SIZE: usize = 256;
pub const TDB_MAX_LEVELS: usize = 8;
pub const TDB_MAX_ENCODING_CHAINS: usize = 16;

// ===== opaque handles =====

#[repr(C)]
pub struct tidesdb_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_column_family_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_txn_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_snapshot_t {
    _private: [u8; 0],
}

#[repr(C)]
pub struct tidesdb_iter_t {
    _private: [u8; 0],
}

// ===== callback types =====

/// One operation in a committed transaction batch, passed to the commit hook
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
pub type tidesdb_commit_hook_fn = Option<
    unsafe extern "C" fn(
        ops: *const tidesdb_commit_op_t,
        num_ops: c_int,
        commit_seq: u64,
        ctx: *mut c_void,
    ) -> c_int,
>;

// ===== configuration =====

/// Per-column-family configuration
#[repr(C)]
#[derive(Clone, Copy)]
pub struct tidesdb_column_family_config_t {
    pub name: [c_char; TDB_MAX_CF_NAME_LEN],
    pub level_size_ratio: size_t,
    pub min_levels: c_int,
    pub dividing_level_offset: c_int,
    pub keep_values_inline: c_int,
    pub btree_klog_block_size: size_t,
    pub encoding_pipeline: [u8; TDB_ENCODING_PIPELINE_MAX],
    pub encoding_count: u8,
    pub enable_bloom_filter: c_int,
    pub bloom_fpr: c_double,
    pub default_isolation_level: c_int,
    pub l1_file_count_trigger: c_int,
    pub tombstone_density_trigger: c_double,
    pub tombstone_density_min_entries: u64,
    pub commit_hook_fn: tidesdb_commit_hook_fn,
    pub commit_hook_ctx: *mut c_void,
}

/// Database-level configuration
#[repr(C)]
pub struct tidesdb_config_t {
    pub db_path: *mut c_char,
    pub num_flush_threads: c_int,
    pub num_compaction_threads: c_int,
    pub log_level: c_int,
    pub block_cache_size: size_t,
    pub max_open_sstables: size_t,
    pub log_to_file: c_int,
    pub log_truncation_at: size_t,
    pub memtable_write_buffer_size: size_t,
    pub memtable_skip_list_max_level: c_int,
    pub memtable_skip_list_probability: c_float,
    pub memtable_sync_mode: c_int,
    pub memtable_sync_interval_us: u64,
    pub value_separation_threshold: size_t,
    pub vlog_segment_size: size_t,
    pub memtable_l0_queue_stall_threshold: c_int,
    pub memtable_idle_flush_seconds: c_int,
    pub txn_timeout_seconds: i64,
}

// ===== statistics =====

/// Per-column-family statistics, a flat value with nothing to free
#[repr(C)]
pub struct tidesdb_cf_stats_t {
    pub num_levels: c_int,
    pub config: tidesdb_column_family_config_t,
    pub level_sizes: [size_t; TDB_MAX_LEVELS],
    pub level_num_sstables: [c_int; TDB_MAX_LEVELS],
    pub level_key_counts: [u64; TDB_MAX_LEVELS],
    pub level_tombstone_counts: [u64; TDB_MAX_LEVELS],
    pub total_keys: u64,
    pub total_data_size: u64,
    pub avg_key_size: c_double,
    pub avg_value_size: c_double,
    pub read_amp: c_double,
    pub btree_total_nodes: u64,
    pub btree_max_height: u32,
    pub btree_avg_height: c_double,
    pub total_tombstones: u64,
    pub tombstone_ratio: c_double,
    pub max_sst_density: c_double,
    pub max_sst_density_level: c_int,
    pub wal_bytes_written: u64,
    pub flush_bytes_written: u64,
    pub compaction_bytes_written: u64,
    pub compaction_bytes_read: u64,
    pub user_bytes_written: u64,
    pub compaction_count: u64,
    pub unflushed_key_count: u64,
    pub filter_resident_bytes: u64,
}

/// Block cache statistics
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

/// What one encoding chain achieved on the data it wrote
#[repr(C)]
#[derive(Clone, Copy)]
pub struct tidesdb_encoding_stats_t {
    pub ids: [u8; TDB_ENCODING_PIPELINE_MAX],
    pub id_count: c_int,
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub item_count: u64,
}

/// Database-level statistics
#[repr(C)]
pub struct tidesdb_db_stats_t {
    pub num_column_families: c_int,
    pub immutable_memtable_count: c_int,
    pub compaction_pending_count: c_int,
    pub total_sstable_count: c_int,
    pub total_data_size_bytes: u64,
    pub num_open_sstables: c_int,
    pub global_seq: u64,
    pub min_snapshot_seq: u64,
    pub active_txn_count: c_int,
    pub txn_memory_bytes: i64,
    pub txn_commits: u64,
    pub txn_conflicts: u64,
    pub memtable_bytes: size_t,
    pub is_flushing: c_int,
    pub next_cf_index: u32,
    pub wal_generation: u64,
    pub flush_count: u64,
    pub compaction_count: u64,
    pub flush_bytes_written: u64,
    pub compaction_bytes_written: u64,
    pub compaction_bytes_read: u64,
    pub wal_bytes_written: u64,
    pub user_bytes_written: u64,
    pub vlog_file_size: u64,
    pub vlog_value_count: u64,
    pub vlog_used_bytes: u64,
    pub vlog_stored_bytes: u64,
    pub vlog_live_bytes: u64,
    pub vlog_segment_count: u64,
    pub vlog_bytes_written: u64,
    pub vlog_dead_bytes: u64,
    pub vlog_reclaim_calls: u64,
    pub vlog_reclaim_passes: u64,
    pub vlog_segments_retired: u64,
    pub vlog_segments_drainable: u64,
    pub writes_throttled: u64,
    pub writes_blocked: u64,
    pub write_stall_us: u64,
    pub write_stall_ceiling_hits: u64,
}

/// How much waiting one stall reason accounted for
#[repr(C)]
#[derive(Clone, Copy)]
pub struct tidesdb_stall_stat_t {
    pub count: u64,
    pub total_us: u64,
    pub max_us: u64,
}

/// Every stall reason, indexed by `tidesdb_stall_reason_t`
#[repr(C)]
pub struct tidesdb_stall_stats_t {
    pub reasons: [tidesdb_stall_stat_t; TDB_STALL_COUNT],
}

/// What one class of file asked of the device
#[repr(C)]
#[derive(Clone, Copy)]
pub struct tidesdb_io_stat_t {
    pub ops: u64,
    pub bytes: u64,
    pub total_us: u64,
    pub max_us: u64,
}

/// Every I/O class, indexed by `tidesdb_io_class_t`
#[repr(C)]
pub struct tidesdb_io_stats_t {
    pub classes: [tidesdb_io_stat_t; TDB_IO_COUNT],
}

/// Query-planner statistics for a key range
#[repr(C)]
pub struct tidesdb_range_stats_t {
    pub sstables_overlapping: u64,
    pub estimated_keys: u64,
    pub keys_exact: c_int,
}

/// A transaction durably prepared before a restart and still awaiting a decision
#[repr(C)]
pub struct tidesdb_prepared_txn_t {
    pub txn: *mut tidesdb_txn_t,
    pub xid: *const u8,
    pub xid_size: size_t,
}

#[link(name = "tidesdb")]
unsafe extern "C" {
    // Utilities
    pub fn tidesdb_compression_available(algo: c_int) -> c_int;
    pub fn tidesdb_strerror(code: c_int) -> *const c_char;
    pub fn tidesdb_free(ptr: *mut c_void);

    // Defaults
    pub fn tidesdb_default_column_family_config() -> tidesdb_column_family_config_t;
    pub fn tidesdb_default_config() -> tidesdb_config_t;

    // Process limits
    pub fn tidesdb_raise_open_file_limit(desired: c_long) -> c_long;

    // Database lifecycle
    pub fn tidesdb_open(config: *const tidesdb_config_t, db: *mut *mut tidesdb_t) -> c_int;
    pub fn tidesdb_close(db: *mut tidesdb_t) -> c_int;

    // Column families
    pub fn tidesdb_create_column_family(
        db: *mut tidesdb_t,
        name: *const c_char,
        config: *const tidesdb_column_family_config_t,
    ) -> c_int;
    pub fn tidesdb_drop_column_family(db: *mut tidesdb_t, name: *const c_char) -> c_int;
    pub fn tidesdb_rename_column_family(
        db: *mut tidesdb_t,
        old_name: *const c_char,
        new_name: *const c_char,
    ) -> c_int;
    pub fn tidesdb_clone_column_family(
        db: *mut tidesdb_t,
        src_name: *const c_char,
        dst_name: *const c_char,
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
    pub fn tidesdb_cf_update_runtime_config(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
        new_config: *const tidesdb_column_family_config_t,
        persist_to_disk: c_int,
    ) -> c_int;
    pub fn tidesdb_cf_set_commit_hook(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
        hook_fn: tidesdb_commit_hook_fn,
        ctx: *mut c_void,
    ) -> c_int;

    // Transactions
    pub fn tidesdb_txn_begin(db: *mut tidesdb_t, txn: *mut *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_begin_with_isolation(
        db: *mut tidesdb_t,
        isolation: c_int,
        txn: *mut *mut tidesdb_txn_t,
    ) -> c_int;
    pub fn tidesdb_txn_begin_cf(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
        txn: *mut *mut tidesdb_txn_t,
    ) -> c_int;

    // Snapshots and point-in-time reads
    pub fn tidesdb_snapshot_create(
        db: *mut tidesdb_t,
        snapshot: *mut *mut tidesdb_snapshot_t,
    ) -> c_int;
    pub fn tidesdb_snapshot_release(snapshot: *mut tidesdb_snapshot_t);
    pub fn tidesdb_snapshot_seq(snapshot: *const tidesdb_snapshot_t) -> u64;
    pub fn tidesdb_txn_begin_at_snapshot(
        db: *mut tidesdb_t,
        snapshot: *mut tidesdb_snapshot_t,
        txn: *mut *mut tidesdb_txn_t,
    ) -> c_int;
    pub fn tidesdb_txn_begin_at_seq(
        db: *mut tidesdb_t,
        seq: u64,
        txn: *mut *mut tidesdb_txn_t,
    ) -> c_int;
    pub fn tidesdb_oldest_readable_seq(db: *const tidesdb_t) -> u64;

    // Transaction operations
    pub fn tidesdb_txn_put(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
        value: *const u8,
        value_size: size_t,
        ttl: time_t,
    ) -> c_int;
    pub fn tidesdb_txn_get(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
        value: *mut *mut u8,
        value_size: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_txn_get_ttl(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
        ttl: *mut time_t,
    ) -> c_int;
    pub fn tidesdb_txn_get_notrack(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
        value: *mut *mut u8,
        value_size: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_txn_read_snapshot(txn: *const tidesdb_txn_t) -> u64;
    pub fn tidesdb_txn_contains(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_delete_range(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        lo: *const u8,
        lo_size: size_t,
        hi: *const u8,
        hi_size: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_delete_prefix(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        prefix: *const u8,
        prefix_size: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_delete(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_single_delete(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        key: *const u8,
        key_size: size_t,
    ) -> c_int;
    pub fn tidesdb_txn_commit(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_rollback(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_set_timeout(txn: *mut tidesdb_txn_t, seconds: i64) -> c_int;
    pub fn tidesdb_txn_request_abort(txn: *mut tidesdb_txn_t);
    pub fn tidesdb_txn_reset(txn: *mut tidesdb_txn_t, isolation: c_int) -> c_int;
    pub fn tidesdb_txn_free(txn: *mut tidesdb_txn_t);

    // Two-phase commit
    pub fn tidesdb_txn_prepare(txn: *mut tidesdb_txn_t, xid: *const u8, xid_size: size_t) -> c_int;
    pub fn tidesdb_txn_commit_prepared(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_txn_rollback_prepared(txn: *mut tidesdb_txn_t) -> c_int;
    pub fn tidesdb_recover_prepared(
        db: *mut tidesdb_t,
        out: *mut tidesdb_prepared_txn_t,
        max: c_int,
        out_count: *mut c_int,
    ) -> c_int;
    pub fn tidesdb_txn_state(txn: *const tidesdb_txn_t, out_state: *mut c_int) -> c_int;

    // Savepoints
    pub fn tidesdb_txn_savepoint(txn: *mut tidesdb_txn_t, name: *const c_char) -> c_int;
    pub fn tidesdb_txn_rollback_to_savepoint(txn: *mut tidesdb_txn_t, name: *const c_char)
    -> c_int;
    pub fn tidesdb_txn_release_savepoint(txn: *mut tidesdb_txn_t, name: *const c_char) -> c_int;

    // Iterators
    pub fn tidesdb_iter_new(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        iter: *mut *mut tidesdb_iter_t,
    ) -> c_int;
    pub fn tidesdb_iter_new_range(
        txn: *mut tidesdb_txn_t,
        cf: *mut tidesdb_column_family_t,
        lower: *const u8,
        lower_size: size_t,
        upper: *const u8,
        upper_size: size_t,
        iter: *mut *mut tidesdb_iter_t,
    ) -> c_int;
    pub fn tidesdb_iter_seek(iter: *mut tidesdb_iter_t, key: *const u8, key_size: size_t) -> c_int;
    pub fn tidesdb_iter_seek_for_prev(
        iter: *mut tidesdb_iter_t,
        key: *const u8,
        key_size: size_t,
    ) -> c_int;
    pub fn tidesdb_iter_seek_to_first(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_seek_to_last(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_next(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_prev(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_valid(iter: *mut tidesdb_iter_t) -> c_int;
    pub fn tidesdb_iter_key(
        iter: *mut tidesdb_iter_t,
        key: *mut *mut u8,
        key_size: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_iter_value(
        iter: *mut tidesdb_iter_t,
        value: *mut *mut u8,
        value_size: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_iter_ttl(iter: *mut tidesdb_iter_t, ttl: *mut time_t) -> c_int;
    pub fn tidesdb_iter_key_value(
        iter: *mut tidesdb_iter_t,
        key: *mut *mut u8,
        key_size: *mut size_t,
        value: *mut *mut u8,
        value_size: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_iter_free(iter: *mut tidesdb_iter_t);

    // Maintenance
    pub fn tidesdb_compact(db: *mut tidesdb_t, cf: *mut tidesdb_column_family_t) -> c_int;
    pub fn tidesdb_compact_range(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
        start_key: *const u8,
        start_key_size: size_t,
        end_key: *const u8,
        end_key_size: size_t,
    ) -> c_int;
    pub fn tidesdb_flush_memtable(db: *mut tidesdb_t) -> c_int;
    pub fn tidesdb_is_flushing(db: *mut tidesdb_t) -> c_int;
    pub fn tidesdb_is_compacting(cf: *mut tidesdb_column_family_t) -> c_int;
    pub fn tidesdb_sync_wal(db: *mut tidesdb_t) -> c_int;
    pub fn tidesdb_backup(db: *mut tidesdb_t, dir: *const c_char) -> c_int;
    pub fn tidesdb_checkpoint(db: *mut tidesdb_t) -> c_int;

    // Statistics
    pub fn tidesdb_get_cf_stats(
        cf: *mut tidesdb_column_family_t,
        stats: *mut tidesdb_cf_stats_t,
    ) -> c_int;
    pub fn tidesdb_cf_estimate_cardinality(
        cf: *mut tidesdb_column_family_t,
        out_estimate: *mut u64,
    ) -> c_int;
    pub fn tidesdb_get_db_stats(db: *mut tidesdb_t, stats: *mut tidesdb_db_stats_t) -> c_int;
    pub fn tidesdb_get_klog_encoding_stats(
        db: *mut tidesdb_t,
        out: *mut tidesdb_encoding_stats_t,
        max: size_t,
        out_count: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_get_vlog_encoding_stats(
        db: *mut tidesdb_t,
        out: *mut tidesdb_encoding_stats_t,
        max: size_t,
        out_count: *mut size_t,
    ) -> c_int;
    pub fn tidesdb_get_cache_stats(db: *mut tidesdb_t, stats: *mut tidesdb_cache_stats_t) -> c_int;
    pub fn tidesdb_stall_reason_name(reason: c_int) -> *const c_char;
    pub fn tidesdb_get_stall_stats(db: *mut tidesdb_t, stats: *mut tidesdb_stall_stats_t) -> c_int;
    pub fn tidesdb_io_class_name(cls: c_int) -> *const c_char;
    pub fn tidesdb_get_io_stats(db: *mut tidesdb_t, stats: *mut tidesdb_io_stats_t) -> c_int;
    pub fn tidesdb_range_stats(
        db: *mut tidesdb_t,
        cf: *mut tidesdb_column_family_t,
        key_a: *const u8,
        key_a_size: size_t,
        key_b: *const u8,
        key_b_size: size_t,
        out: *mut tidesdb_range_stats_t,
    ) -> c_int;
}

#[cfg(test)]
mod layout_tests {
    use super::*;
    use std::mem::{offset_of, size_of};

    // Sizes and tail offsets for LP64 targets (Linux/macOS x86_64 and aarch64), taken from a C
    // program compiled against db.h. A mismatch means the C header changed and the struct above
    // must be brought back in step.
    #[cfg(all(target_pointer_width = "64", not(windows)))]
    #[test]
    fn struct_layouts_match_c() {
        assert_eq!(size_of::<tidesdb_column_family_config_t>(), 224);
        assert_eq!(
            offset_of!(tidesdb_column_family_config_t, commit_hook_ctx),
            216
        );
        assert_eq!(size_of::<tidesdb_config_t>(), 120);
        assert_eq!(offset_of!(tidesdb_config_t, txn_timeout_seconds), 112);
        assert_eq!(size_of::<tidesdb_commit_op_t>(), 48);
        assert_eq!(size_of::<tidesdb_cache_stats_t>(), 56);
        assert_eq!(size_of::<tidesdb_encoding_stats_t>(), 40);
        assert_eq!(size_of::<tidesdb_stall_stats_t>(), 120);
        assert_eq!(size_of::<tidesdb_io_stats_t>(), 96);
        assert_eq!(size_of::<tidesdb_range_stats_t>(), 24);
        assert_eq!(size_of::<tidesdb_prepared_txn_t>(), 24);
        assert_eq!(size_of::<tidesdb_cf_stats_t>(), 616);
        assert_eq!(offset_of!(tidesdb_cf_stats_t, level_sizes), 232);
        assert_eq!(offset_of!(tidesdb_cf_stats_t, filter_resident_bytes), 608);
        assert_eq!(size_of::<tidesdb_db_stats_t>(), 288);
        assert_eq!(
            offset_of!(tidesdb_db_stats_t, write_stall_ceiling_hits),
            280
        );
    }
}
