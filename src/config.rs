// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Configuration types for TidesDB.

use crate::error::{Error, Result};
use crate::ffi;
use std::ffi::CString;
use std::path::Path;

/// Compression algorithm, used as a stage in a column family's encoding pipeline.
///
/// Every algorithm is always defined, but a backend is only usable when the C library
/// was built with it (the `snappy`, `lz4` and `zstd` Cargo features). Check with
/// [`CompressionAlgorithm::is_available`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum CompressionAlgorithm {
    /// No compression
    #[default]
    None = ffi::TDB_COMPRESS_NONE,
    /// Snappy compression
    Snappy = ffi::TDB_COMPRESS_SNAPPY,
    /// LZ4 compression
    Lz4 = ffi::TDB_COMPRESS_LZ4,
    /// Zstandard compression
    Zstd = ffi::TDB_COMPRESS_ZSTD,
    /// LZ4 fast compression
    Lz4Fast = ffi::TDB_COMPRESS_LZ4_FAST,
}

impl CompressionAlgorithm {
    /// Whether this build of the C library can compress and decompress with this algorithm.
    /// [`CompressionAlgorithm::None`] is always available.
    pub fn is_available(self) -> bool {
        unsafe { ffi::tidesdb_compression_available(self as i32) != 0 }
    }

    /// The algorithm with the given encoding id, if it names one.
    pub fn from_id(id: u8) -> Option<Self> {
        match id as i32 {
            ffi::TDB_COMPRESS_NONE => Some(CompressionAlgorithm::None),
            ffi::TDB_COMPRESS_SNAPPY => Some(CompressionAlgorithm::Snappy),
            ffi::TDB_COMPRESS_LZ4 => Some(CompressionAlgorithm::Lz4),
            ffi::TDB_COMPRESS_ZSTD => Some(CompressionAlgorithm::Zstd),
            ffi::TDB_COMPRESS_LZ4_FAST => Some(CompressionAlgorithm::Lz4Fast),
            _ => None,
        }
    }
}

/// Durability mode for the write-ahead log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SyncMode {
    /// Nothing at commit; fastest, a crash of the process or the machine may lose commits
    #[default]
    None = ffi::TDB_SYNC_NONE,
    /// fsync every commit; slowest, no commit loss on crash
    Full = ffi::TDB_SYNC_FULL,
    /// fsync on a background interval; only a machine crash can lose the last interval
    Interval = ffi::TDB_SYNC_INTERVAL,
}

impl SyncMode {
    fn from_c(mode: i32) -> Self {
        match mode {
            ffi::TDB_SYNC_FULL => SyncMode::Full,
            ffi::TDB_SYNC_INTERVAL => SyncMode::Interval,
            _ => SyncMode::None,
        }
    }
}

/// Logging threshold. A message is emitted when its severity meets or exceeds the level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum LogLevel {
    /// No logging
    None = ffi::TDB_LOG_NONE,
    /// Highly detailed messages for technical debugging
    Trace = ffi::TDB_LOG_TRACE,
    /// Engine status and operations
    #[default]
    Info = ffi::TDB_LOG_INFO,
    /// Non-imminent errors that require awareness
    Warn = ffi::TDB_LOG_WARN,
    /// An operation failed
    Error = ffi::TDB_LOG_ERROR,
}

impl LogLevel {
    fn from_c(level: i32) -> Self {
        match level {
            ffi::TDB_LOG_NONE => LogLevel::None,
            ffi::TDB_LOG_TRACE => LogLevel::Trace,
            ffi::TDB_LOG_WARN => LogLevel::Warn,
            ffi::TDB_LOG_ERROR => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

/// Transaction isolation level, weakest to strongest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum IsolationLevel {
    /// Every version is visible, including sequences still in progress
    ReadUncommitted = ffi::TDB_ISOLATION_READ_UNCOMMITTED,
    /// The newest committed version, re-read on every operation (default)
    #[default]
    ReadCommitted = ffi::TDB_ISOLATION_READ_COMMITTED,
    /// Frozen snapshot; commit validates that every key read is unchanged and that no key
    /// appeared inside a scanned range
    RepeatableRead = ffi::TDB_ISOLATION_REPEATABLE_READ,
    /// Frozen snapshot; commit claims each written key first-committer-wins
    Snapshot = ffi::TDB_ISOLATION_SNAPSHOT,
    /// Both of the checks above
    Serializable = ffi::TDB_ISOLATION_SERIALIZABLE,
}

impl IsolationLevel {
    fn from_c(level: i32) -> Self {
        match level {
            ffi::TDB_ISOLATION_READ_UNCOMMITTED => IsolationLevel::ReadUncommitted,
            ffi::TDB_ISOLATION_REPEATABLE_READ => IsolationLevel::RepeatableRead,
            ffi::TDB_ISOLATION_SNAPSHOT => IsolationLevel::Snapshot,
            ffi::TDB_ISOLATION_SERIALIZABLE => IsolationLevel::Serializable,
            _ => IsolationLevel::ReadCommitted,
        }
    }
}

/// C configuration plus the owned data its pointers refer to; must outlive `tidesdb_open`.
pub(crate) struct CConfigData {
    pub config: ffi::tidesdb_config_t,
    _db_path: CString,
}

/// Database configuration.
///
/// The memtable, write-ahead log, value log, block cache and worker pool are shared by
/// every column family, so their settings live here. A numeric field left at `0` is
/// resolved to the library default for the thread counts, `block_cache_size`,
/// `max_open_sstables`, `vlog_segment_size`, `memtable_write_buffer_size` and the
/// memtable skip-list settings.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the database directory
    pub db_path: String,
    /// Number of flush worker threads
    pub num_flush_threads: i32,
    /// Number of compaction worker threads
    pub num_compaction_threads: i32,
    /// Minimum severity to log
    pub log_level: LogLevel,
    /// Size in bytes of the block cache for hot sstable blocks
    pub block_cache_size: usize,
    /// Maximum number of concurrently open sstable file handles. Lowered at open to what
    /// the process's open-file ceiling allows; see [`raise_open_file_limit`](crate::raise_open_file_limit).
    pub max_open_sstables: usize,
    /// Write the log to a file named `LOG` inside `db_path` instead of stderr. The sink is
    /// process-wide: the last database opened with this set owns it.
    pub log_to_file: bool,
    /// Size in bytes past which the log file is truncated (0 = never)
    pub log_truncation_at: usize,
    /// Memory the active memtable may occupy before it is rotated (0 = auto)
    pub memtable_write_buffer_size: usize,
    /// Memtable skip list max level (0 = default 12)
    pub memtable_skip_list_max_level: i32,
    /// Memtable skip list level probability (0 = default 0.25)
    pub memtable_skip_list_probability: f32,
    /// Durability mode for the write-ahead log
    pub memtable_sync_mode: SyncMode,
    /// fsync interval in microseconds for [`SyncMode::Interval`] (0 = one second)
    pub memtable_sync_interval_us: u64,
    /// Values at or above this size go to the shared value log (0 = default). Keep it at or
    /// under a quarter of a column family's `btree_klog_block_size`.
    pub value_separation_threshold: usize,
    /// Size at which the value log seals a segment and opens a new one (0 = default)
    pub vlog_segment_size: usize,
    /// Immutable-memtable queue depth at which writes stall for backpressure (0 = never)
    pub memtable_l0_queue_stall_threshold: i32,
    /// Seconds the active memtable may sit unwritten before it is rotated (0 = never)
    pub memtable_idle_flush_seconds: i32,
    /// Seconds a transaction may stay active before its next operation expires it (0 = no timeout)
    pub txn_timeout_seconds: i64,
}

impl Config {
    /// Create a new configuration with the given database path.
    ///
    /// All other fields are initialized from `tidesdb_default_config()` so the
    /// binding tracks the library defaults automatically.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Self {
        Config {
            db_path: db_path.as_ref().to_string_lossy().into_owned(),
            ..Self::default()
        }
    }

    /// Set the number of flush worker threads.
    pub fn num_flush_threads(mut self, n: i32) -> Self {
        self.num_flush_threads = n;
        self
    }

    /// Set the number of compaction worker threads.
    pub fn num_compaction_threads(mut self, n: i32) -> Self {
        self.num_compaction_threads = n;
        self
    }

    /// Set the logging threshold.
    pub fn log_level(mut self, level: LogLevel) -> Self {
        self.log_level = level;
        self
    }

    /// Set the block cache size in bytes.
    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Set the maximum number of open sstable file handles.
    pub fn max_open_sstables(mut self, n: usize) -> Self {
        self.max_open_sstables = n;
        self
    }

    /// Write logs to `<db_path>/LOG` instead of stderr.
    pub fn log_to_file(mut self, enable: bool) -> Self {
        self.log_to_file = enable;
        self
    }

    /// Set the log file truncation threshold in bytes (0 = never).
    pub fn log_truncation_at(mut self, size: usize) -> Self {
        self.log_truncation_at = size;
        self
    }

    /// Set the memtable write buffer size in bytes (0 = auto).
    pub fn memtable_write_buffer_size(mut self, size: usize) -> Self {
        self.memtable_write_buffer_size = size;
        self
    }

    /// Set the memtable skip list max level (0 = default 12).
    pub fn memtable_skip_list_max_level(mut self, level: i32) -> Self {
        self.memtable_skip_list_max_level = level;
        self
    }

    /// Set the memtable skip list level probability (0 = default 0.25).
    pub fn memtable_skip_list_probability(mut self, prob: f32) -> Self {
        self.memtable_skip_list_probability = prob;
        self
    }

    /// Set the write-ahead log durability mode.
    pub fn memtable_sync_mode(mut self, mode: SyncMode) -> Self {
        self.memtable_sync_mode = mode;
        self
    }

    /// Set the fsync interval for [`SyncMode::Interval`], in microseconds.
    pub fn memtable_sync_interval_us(mut self, interval: u64) -> Self {
        self.memtable_sync_interval_us = interval;
        self
    }

    /// Set the size at or above which values are stored in the value log (0 = default).
    pub fn value_separation_threshold(mut self, size: usize) -> Self {
        self.value_separation_threshold = size;
        self
    }

    /// Set the value log segment size in bytes (0 = default).
    pub fn vlog_segment_size(mut self, size: usize) -> Self {
        self.vlog_segment_size = size;
        self
    }

    /// Set the immutable-queue depth at which writes stall (0 = never stall).
    pub fn memtable_l0_queue_stall_threshold(mut self, threshold: i32) -> Self {
        self.memtable_l0_queue_stall_threshold = threshold;
        self
    }

    /// Set how long the memtable may sit idle before it is rotated (0 = never).
    pub fn memtable_idle_flush_seconds(mut self, seconds: i32) -> Self {
        self.memtable_idle_flush_seconds = seconds;
        self
    }

    /// Set the default transaction timeout in seconds (0 = no timeout).
    pub fn txn_timeout_seconds(mut self, seconds: i64) -> Self {
        self.txn_timeout_seconds = seconds;
        self
    }

    /// Convert to the C configuration struct.
    pub(crate) fn to_c_config(&self) -> Result<CConfigData> {
        let c_path = CString::new(self.db_path.as_str())?;

        let config = ffi::tidesdb_config_t {
            // borrowed by tidesdb_open, which copies it
            db_path: c_path.as_ptr() as *mut _,
            num_flush_threads: self.num_flush_threads,
            num_compaction_threads: self.num_compaction_threads,
            log_level: self.log_level as i32,
            block_cache_size: self.block_cache_size,
            max_open_sstables: self.max_open_sstables,
            log_to_file: self.log_to_file as i32,
            log_truncation_at: self.log_truncation_at,
            memtable_write_buffer_size: self.memtable_write_buffer_size,
            memtable_skip_list_max_level: self.memtable_skip_list_max_level,
            memtable_skip_list_probability: self.memtable_skip_list_probability,
            memtable_sync_mode: self.memtable_sync_mode as i32,
            memtable_sync_interval_us: self.memtable_sync_interval_us,
            value_separation_threshold: self.value_separation_threshold,
            vlog_segment_size: self.vlog_segment_size,
            memtable_l0_queue_stall_threshold: self.memtable_l0_queue_stall_threshold,
            memtable_idle_flush_seconds: self.memtable_idle_flush_seconds,
            txn_timeout_seconds: self.txn_timeout_seconds,
        };

        Ok(CConfigData {
            config,
            _db_path: c_path,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        // Pull defaults from the C library so the binding tracks engine defaults
        // automatically. `db_path` is left empty; `Config::new` sets it.
        let c = unsafe { ffi::tidesdb_default_config() };
        Config {
            db_path: String::new(),
            num_flush_threads: c.num_flush_threads,
            num_compaction_threads: c.num_compaction_threads,
            log_level: LogLevel::from_c(c.log_level),
            block_cache_size: c.block_cache_size,
            max_open_sstables: c.max_open_sstables,
            log_to_file: c.log_to_file != 0,
            log_truncation_at: c.log_truncation_at,
            memtable_write_buffer_size: c.memtable_write_buffer_size,
            memtable_skip_list_max_level: c.memtable_skip_list_max_level,
            memtable_skip_list_probability: c.memtable_skip_list_probability,
            memtable_sync_mode: SyncMode::from_c(c.memtable_sync_mode),
            memtable_sync_interval_us: c.memtable_sync_interval_us,
            value_separation_threshold: c.value_separation_threshold,
            vlog_segment_size: c.vlog_segment_size,
            memtable_l0_queue_stall_threshold: c.memtable_l0_queue_stall_threshold,
            memtable_idle_flush_seconds: c.memtable_idle_flush_seconds,
            txn_timeout_seconds: c.txn_timeout_seconds,
        }
    }
}

/// Column family configuration.
///
/// Every field can be changed at runtime with
/// [`ColumnFamily::update_runtime_config`](crate::ColumnFamily::update_runtime_config),
/// since keys are always ordered byte-wise and sstables stay mergeable.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnFamilyConfig {
    /// Target size ratio between successive levels
    pub level_size_ratio: usize,
    /// Floor on the level count
    pub min_levels: i32,
    /// How far above the largest level the dividing level sits
    pub dividing_level_offset: i32,
    /// Keep every value in the key log regardless of the database's
    /// `value_separation_threshold`
    pub keep_values_inline: bool,
    /// Target btree key-log node size in bytes (0 = let the btree choose its own, larger, default)
    pub btree_klog_block_size: usize,
    /// Encodings applied in order to key-log nodes and separated values, at most
    /// 8 stages. Empty means data is stored verbatim.
    pub encoding_pipeline: Vec<CompressionAlgorithm>,
    /// Build bloom filters for point-get pruning
    pub enable_bloom_filter: bool,
    /// Target bloom filter false-positive rate
    pub bloom_fpr: f64,
    /// Isolation level for transactions begun with
    /// [`TidesDB::begin_transaction_cf`](crate::TidesDB::begin_transaction_cf)
    pub default_isolation_level: IsolationLevel,
    /// L1 sstable count that triggers compaction
    pub l1_file_count_trigger: i32,
    /// Tombstone density in `[0, 1]` above which an sstable escalates compaction (0 = disabled)
    pub tombstone_density_trigger: f64,
    /// Minimum entries for an sstable to be judged by the density trigger (0 = no minimum)
    pub tombstone_density_min_entries: u64,
}

impl ColumnFamilyConfig {
    /// Create a new column family configuration with library defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the size ratio between levels.
    pub fn level_size_ratio(mut self, ratio: usize) -> Self {
        self.level_size_ratio = ratio;
        self
    }

    /// Set the minimum number of levels.
    pub fn min_levels(mut self, levels: i32) -> Self {
        self.min_levels = levels;
        self
    }

    /// Set the dividing level offset.
    pub fn dividing_level_offset(mut self, offset: i32) -> Self {
        self.dividing_level_offset = offset;
        self
    }

    /// Keep every value inline in the key log.
    pub fn keep_values_inline(mut self, enable: bool) -> Self {
        self.keep_values_inline = enable;
        self
    }

    /// Set the btree key-log node size in bytes.
    pub fn btree_klog_block_size(mut self, size: usize) -> Self {
        self.btree_klog_block_size = size;
        self
    }

    /// Compress with a single algorithm; [`CompressionAlgorithm::None`] clears the pipeline.
    pub fn compression_algorithm(mut self, algo: CompressionAlgorithm) -> Self {
        self.encoding_pipeline = match algo {
            CompressionAlgorithm::None => Vec::new(),
            other => vec![other],
        };
        self
    }

    /// Set the full encoding pipeline, applied in order (at most 8 stages).
    pub fn encoding_pipeline(mut self, pipeline: Vec<CompressionAlgorithm>) -> Self {
        self.encoding_pipeline = pipeline;
        self
    }

    /// Enable or disable bloom filters.
    pub fn enable_bloom_filter(mut self, enable: bool) -> Self {
        self.enable_bloom_filter = enable;
        self
    }

    /// Set the bloom filter false-positive rate.
    pub fn bloom_fpr(mut self, fpr: f64) -> Self {
        self.bloom_fpr = fpr;
        self
    }

    /// Set the default isolation level.
    pub fn default_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.default_isolation_level = level;
        self
    }

    /// Set the L1 file count that triggers compaction.
    pub fn l1_file_count_trigger(mut self, trigger: i32) -> Self {
        self.l1_file_count_trigger = trigger;
        self
    }

    /// Set the tombstone density compaction trigger (0 disables).
    pub fn tombstone_density_trigger(mut self, ratio: f64) -> Self {
        self.tombstone_density_trigger = ratio;
        self
    }

    /// Set the minimum entry count for the tombstone density trigger.
    pub fn tombstone_density_min_entries(mut self, n: u64) -> Self {
        self.tombstone_density_min_entries = n;
        self
    }

    /// Convert to the C column family configuration struct.
    ///
    /// The name and commit hook are left empty: the name is passed separately and the
    /// hook is set with [`ColumnFamily::set_commit_hook`](crate::ColumnFamily::set_commit_hook).
    pub(crate) fn to_c_config(&self) -> Result<ffi::tidesdb_column_family_config_t> {
        let stages: Vec<u8> = self
            .encoding_pipeline
            .iter()
            .filter(|a| **a != CompressionAlgorithm::None)
            .map(|a| *a as i32 as u8)
            .collect();
        if stages.len() > ffi::TDB_ENCODING_PIPELINE_MAX {
            return Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "encoding pipeline has more than 8 stages",
            ));
        }

        let mut c = unsafe { ffi::tidesdb_default_column_family_config() };
        c.level_size_ratio = self.level_size_ratio;
        c.min_levels = self.min_levels;
        c.dividing_level_offset = self.dividing_level_offset;
        c.keep_values_inline = self.keep_values_inline as i32;
        c.btree_klog_block_size = self.btree_klog_block_size;
        c.encoding_pipeline = [0; ffi::TDB_ENCODING_PIPELINE_MAX];
        c.encoding_pipeline[..stages.len()].copy_from_slice(&stages);
        c.encoding_count = stages.len() as u8;
        c.enable_bloom_filter = self.enable_bloom_filter as i32;
        c.bloom_fpr = self.bloom_fpr;
        c.default_isolation_level = self.default_isolation_level as i32;
        c.l1_file_count_trigger = self.l1_file_count_trigger;
        c.tombstone_density_trigger = self.tombstone_density_trigger;
        c.tombstone_density_min_entries = self.tombstone_density_min_entries;
        Ok(c)
    }

    /// Build from a C column family configuration.
    pub(crate) fn from_c_config(c: &ffi::tidesdb_column_family_config_t) -> Self {
        let count = (c.encoding_count as usize).min(ffi::TDB_ENCODING_PIPELINE_MAX);
        ColumnFamilyConfig {
            level_size_ratio: c.level_size_ratio,
            min_levels: c.min_levels,
            dividing_level_offset: c.dividing_level_offset,
            keep_values_inline: c.keep_values_inline != 0,
            btree_klog_block_size: c.btree_klog_block_size,
            encoding_pipeline: c.encoding_pipeline[..count]
                .iter()
                .filter_map(|id| CompressionAlgorithm::from_id(*id))
                .collect(),
            enable_bloom_filter: c.enable_bloom_filter != 0,
            bloom_fpr: c.bloom_fpr,
            default_isolation_level: IsolationLevel::from_c(c.default_isolation_level),
            l1_file_count_trigger: c.l1_file_count_trigger,
            tombstone_density_trigger: c.tombstone_density_trigger,
            tombstone_density_min_entries: c.tombstone_density_min_entries,
        }
    }
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        let c = unsafe { ffi::tidesdb_default_column_family_config() };
        Self::from_c_config(&c)
    }
}
