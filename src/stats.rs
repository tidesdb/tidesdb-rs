// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Statistics types for TidesDB.

use crate::config::ColumnFamilyConfig;

/// Statistics for a column family.
#[derive(Debug, Clone)]
pub struct Stats {
    /// Number of levels
    pub num_levels: i32,
    /// Memtable size in bytes
    pub memtable_size: usize,
    /// Size of each level in bytes
    pub level_sizes: Vec<usize>,
    /// Number of SSTables in each level
    pub level_num_sstables: Vec<i32>,
    /// Column family configuration (if available)
    pub config: Option<ColumnFamilyConfig>,
    /// Total number of keys across memtable and all sstables
    pub total_keys: u64,
    /// Total data size (klog + vlog) across all sstables
    pub total_data_size: u64,
    /// Average key size in bytes
    pub avg_key_size: f64,
    /// Average value size in bytes
    pub avg_value_size: f64,
    /// Number of keys per level
    pub level_key_counts: Vec<u64>,
    /// Read amplification (point lookup cost multiplier)
    pub read_amp: f64,
    /// Cache hit rate (0.0 if cache disabled)
    pub hit_rate: f64,
    /// Whether column family uses B+tree format
    pub use_btree: bool,
    /// Total B+tree nodes across all SSTables (only populated if use_btree=true)
    pub btree_total_nodes: u64,
    /// Maximum tree height across all SSTables (only populated if use_btree=true)
    pub btree_max_height: u32,
    /// Average tree height across all SSTables (only populated if use_btree=true)
    pub btree_avg_height: f64,
    /// Sum of `tombstone_count` across every SSTable in the column family
    pub total_tombstones: u64,
    /// `total_tombstones / total_keys` (0.0 if `total_keys == 0`); range `[0.0, 1.0]`
    pub tombstone_ratio: f64,
    /// Per-level tombstone counts (parallels `level_key_counts`)
    pub level_tombstone_counts: Vec<u64>,
    /// Worst per-SSTable tombstone density observed in the column family; range `[0.0, 1.0]`
    pub max_sst_density: f64,
    /// 1-based level index where `max_sst_density` was observed (0 if no SSTables)
    pub max_sst_density_level: i32,
    /// Framed bytes appended to this CF's WAL (0 in unified mode).
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub wal_bytes_written: u64,
    /// On-disk bytes this CF's flushes wrote to L0 SSTables.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub flush_bytes_written: u64,
    /// On-disk bytes this CF's compactions wrote.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_bytes_written: u64,
    /// On-disk bytes this CF's compactions read as input.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_bytes_read: u64,
    /// Logical key+value bytes committed to this CF (write-amplification denominator).
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub user_bytes_written: u64,
    /// Flushed SSTables produced by this CF.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub flush_count: u64,
    /// Compaction output SSTables produced by this CF.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_count: u64,
}

/// Database-level aggregate statistics.
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Number of column families
    pub num_column_families: i32,
    /// System total memory
    pub total_memory: u64,
    /// System available memory at open time
    pub available_memory: u64,
    /// Resolved memory limit (auto or configured)
    pub resolved_memory_limit: usize,
    /// Current memory pressure (0=normal, 1=elevated, 2=high, 3=critical)
    pub memory_pressure_level: i32,
    /// Number of pending flush operations (queued + in-flight)
    pub flush_pending_count: i32,
    /// Total bytes in active memtables across all CFs
    pub total_memtable_bytes: i64,
    /// Total immutable memtables across all CFs
    pub total_immutable_count: i32,
    /// Total SSTables across all CFs and levels
    pub total_sstable_count: i32,
    /// Total data size (klog + vlog) across all CFs
    pub total_data_size_bytes: u64,
    /// Number of currently open SSTable file handles
    pub num_open_sstables: i32,
    /// Current global sequence number
    pub global_seq: u64,
    /// Bytes held by in-flight transactions
    pub txn_memory_bytes: i64,
    /// Number of pending compaction tasks
    pub compaction_queue_size: usize,
    /// Number of pending flush tasks in queue
    pub flush_queue_size: usize,
    /// Whether unified memtable mode is active
    pub unified_memtable_enabled: bool,
    /// Bytes in unified active memtable
    pub unified_memtable_bytes: i64,
    /// Number of unified immutable memtables
    pub unified_immutable_count: i32,
    /// Whether unified memtable is currently flushing/rotating
    pub unified_is_flushing: bool,
    /// Next CF index to be assigned in unified mode
    pub unified_next_cf_index: u32,
    /// Current unified WAL generation counter
    pub unified_wal_generation: u64,
    /// Whether object store mode is active
    pub object_store_enabled: bool,
    /// Connector name ("s3", "gcs", "fs", etc.)
    pub object_store_connector: String,
    /// Current local file cache usage in bytes
    pub local_cache_bytes_used: usize,
    /// Configured maximum local cache size in bytes
    pub local_cache_bytes_max: usize,
    /// Number of files tracked in local cache
    pub local_cache_num_files: i32,
    /// Highest WAL generation confirmed uploaded
    pub last_uploaded_generation: u64,
    /// Number of pending upload jobs in the queue
    pub upload_queue_depth: usize,
    /// Lifetime count of objects uploaded to object store
    pub total_uploads: u64,
    /// Lifetime count of permanently failed uploads (after all retries)
    pub total_upload_failures: u64,
    /// Whether running in read-only replica mode
    pub replica_mode: bool,
    /// Single-writer fencing: lease epoch this primary currently holds (0 when not a
    /// primary / no lease). Requires tidesdb >= 9.3.8; reported as 0 on older libraries.
    pub primary_epoch: u64,
    /// Single-writer fencing: highest lease epoch a replica has observed.
    /// Requires tidesdb >= 9.3.8; reported as 0 on older libraries.
    pub seen_epoch: u64,
    /// Framed bytes appended to the shared unified WAL (0 if unified mode off).
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub uwal_bytes_written: u64,
    /// Per-CF WAL bytes summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub wal_bytes_written: u64,
    /// Flush output bytes summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub flush_bytes_written: u64,
    /// Compaction output bytes summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_bytes_written: u64,
    /// Compaction input bytes summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_bytes_read: u64,
    /// Logical committed bytes summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub user_bytes_written: u64,
    /// Flushed SSTables summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub flush_count: u64,
    /// Compaction output SSTables summed across all column families.
    /// Requires tidesdb >= 9.3.4; reported as 0 on older libraries.
    pub compaction_count: u64,
}

/// Statistics for the block cache.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Whether the cache is enabled
    pub enabled: bool,
    /// Total number of entries in the cache
    pub total_entries: usize,
    /// Total bytes used by the cache
    pub total_bytes: usize,
    /// Number of cache hits
    pub hits: usize,
    /// Number of cache misses
    pub misses: usize,
    /// Cache hit rate (0.0 to 1.0)
    pub hit_rate: f64,
    /// Number of cache partitions
    pub num_partitions: usize,
}
