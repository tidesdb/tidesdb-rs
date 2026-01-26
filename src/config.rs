// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Configuration types for TidesDB.

use crate::error::{check_result, Result};
use crate::ffi;
use std::ffi::CString;
use std::path::Path;

/// Compression algorithm for column families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum CompressionAlgorithm {
    /// No compression
    #[default]
    None = ffi::NO_COMPRESSION,
    /// Snappy compression
    Snappy = ffi::SNAPPY_COMPRESSION,
    /// LZ4 compression (default in C API)
    Lz4 = ffi::LZ4_COMPRESSION,
    /// Zstandard compression
    Zstd = ffi::ZSTD_COMPRESSION,
    /// LZ4 fast compression
    Lz4Fast = ffi::LZ4_FAST_COMPRESSION,
}

/// Sync mode for durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SyncMode {
    /// No sync (fastest, least durable)
    #[default]
    None = ffi::TDB_SYNC_NONE,
    /// Full sync on every write (slowest, most durable)
    Full = ffi::TDB_SYNC_FULL,
    /// Sync at intervals
    Interval = ffi::TDB_SYNC_INTERVAL,
}

/// Logging level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum LogLevel {
    /// Debug level
    Debug = ffi::TDB_LOG_DEBUG,
    /// Info level
    #[default]
    Info = ffi::TDB_LOG_INFO,
    /// Warning level
    Warn = ffi::TDB_LOG_WARN,
    /// Error level
    Error = ffi::TDB_LOG_ERROR,
    /// Fatal level
    Fatal = ffi::TDB_LOG_FATAL,
    /// No logging
    None = ffi::TDB_LOG_NONE,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum IsolationLevel {
    /// Read uncommitted - sees all data including uncommitted changes
    ReadUncommitted = ffi::TDB_ISOLATION_READ_UNCOMMITTED,
    /// Read committed - sees only committed data (default)
    #[default]
    ReadCommitted = ffi::TDB_ISOLATION_READ_COMMITTED,
    /// Repeatable read - consistent snapshot, phantom reads possible
    RepeatableRead = ffi::TDB_ISOLATION_REPEATABLE_READ,
    /// Snapshot - write-write conflict detection
    Snapshot = ffi::TDB_ISOLATION_SNAPSHOT,
    /// Serializable - full read-write conflict detection (SSI)
    Serializable = ffi::TDB_ISOLATION_SERIALIZABLE,
}

/// Database configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the database directory
    pub db_path: String,
    /// Number of flush threads
    pub num_flush_threads: i32,
    /// Number of compaction threads
    pub num_compaction_threads: i32,
    /// Logging level
    pub log_level: LogLevel,
    /// Block cache size in bytes
    pub block_cache_size: usize,
    /// Maximum number of open SSTable files
    pub max_open_sstables: usize,
}

impl Config {
    /// Create a new configuration with the given database path.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Self {
        Config {
            db_path: db_path.as_ref().to_string_lossy().into_owned(),
            num_flush_threads: 2,
            num_compaction_threads: 2,
            log_level: LogLevel::Info,
            block_cache_size: 64 * 1024 * 1024, // 64MB
            max_open_sstables: 256,
        }
    }

    /// Set the number of flush threads.
    pub fn num_flush_threads(mut self, n: i32) -> Self {
        self.num_flush_threads = n;
        self
    }

    /// Set the number of compaction threads.
    pub fn num_compaction_threads(mut self, n: i32) -> Self {
        self.num_compaction_threads = n;
        self
    }

    /// Set the logging level.
    pub fn log_level(mut self, level: LogLevel) -> Self {
        self.log_level = level;
        self
    }

    /// Set the block cache size in bytes.
    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.block_cache_size = size;
        self
    }

    /// Set the maximum number of open SSTable files.
    pub fn max_open_sstables(mut self, n: usize) -> Self {
        self.max_open_sstables = n;
        self
    }

    /// Convert to C configuration struct.
    pub(crate) fn to_c_config(&self) -> crate::error::Result<(ffi::tidesdb_config_t, CString)> {
        let c_path = CString::new(self.db_path.as_str())?;
        let config = ffi::tidesdb_config_t {
            db_path: c_path.as_ptr(),
            num_flush_threads: self.num_flush_threads,
            num_compaction_threads: self.num_compaction_threads,
            log_level: self.log_level as i32,
            block_cache_size: self.block_cache_size,
            max_open_sstables: self.max_open_sstables,
        };
        Ok((config, c_path))
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            db_path: String::new(),
            num_flush_threads: 2,
            num_compaction_threads: 2,
            log_level: LogLevel::Info,
            block_cache_size: 64 * 1024 * 1024,
            max_open_sstables: 256,
        }
    }
}

/// Column family configuration.
#[derive(Debug, Clone)]
pub struct ColumnFamilyConfig {
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Level size ratio
    pub level_size_ratio: usize,
    /// Minimum number of levels
    pub min_levels: i32,
    /// Dividing level offset
    pub dividing_level_offset: i32,
    /// Key-log value threshold
    pub klog_value_threshold: usize,
    /// Compression algorithm
    pub compression_algorithm: CompressionAlgorithm,
    /// Enable bloom filter
    pub enable_bloom_filter: bool,
    /// Bloom filter false positive rate
    pub bloom_fpr: f64,
    /// Enable block indexes
    pub enable_block_indexes: bool,
    /// Index sample ratio
    pub index_sample_ratio: i32,
    /// Block index prefix length
    pub block_index_prefix_len: i32,
    /// Sync mode
    pub sync_mode: SyncMode,
    /// Sync interval in microseconds
    pub sync_interval_us: u64,
    /// Comparator name
    pub comparator_name: String,
    /// Skip list maximum level
    pub skip_list_max_level: i32,
    /// Skip list probability
    pub skip_list_probability: f32,
    /// Default isolation level
    pub default_isolation_level: IsolationLevel,
    /// Minimum disk space
    pub min_disk_space: u64,
    /// L1 file count trigger
    pub l1_file_count_trigger: i32,
    /// L0 queue stall threshold
    pub l0_queue_stall_threshold: i32,
}

impl ColumnFamilyConfig {
    /// Create a new column family configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the write buffer size.
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Set the level size ratio.
    pub fn level_size_ratio(mut self, ratio: usize) -> Self {
        self.level_size_ratio = ratio;
        self
    }

    /// Set the minimum number of levels.
    pub fn min_levels(mut self, levels: i32) -> Self {
        self.min_levels = levels;
        self
    }

    /// Set the compression algorithm.
    pub fn compression_algorithm(mut self, algo: CompressionAlgorithm) -> Self {
        self.compression_algorithm = algo;
        self
    }

    /// Enable or disable bloom filter.
    pub fn enable_bloom_filter(mut self, enable: bool) -> Self {
        self.enable_bloom_filter = enable;
        self
    }

    /// Set the bloom filter false positive rate.
    pub fn bloom_fpr(mut self, fpr: f64) -> Self {
        self.bloom_fpr = fpr;
        self
    }

    /// Enable or disable block indexes.
    pub fn enable_block_indexes(mut self, enable: bool) -> Self {
        self.enable_block_indexes = enable;
        self
    }

    /// Set the sync mode.
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Set the sync interval in microseconds.
    pub fn sync_interval_us(mut self, interval: u64) -> Self {
        self.sync_interval_us = interval;
        self
    }

    /// Set the default isolation level.
    pub fn default_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.default_isolation_level = level;
        self
    }

    /// Set the dividing level offset.
    pub fn dividing_level_offset(mut self, offset: i32) -> Self {
        self.dividing_level_offset = offset;
        self
    }

    /// Set the klog value threshold.
    pub fn klog_value_threshold(mut self, threshold: usize) -> Self {
        self.klog_value_threshold = threshold;
        self
    }

    /// Set the index sample ratio.
    pub fn index_sample_ratio(mut self, ratio: i32) -> Self {
        self.index_sample_ratio = ratio;
        self
    }

    /// Set the block index prefix length.
    pub fn block_index_prefix_len(mut self, len: i32) -> Self {
        self.block_index_prefix_len = len;
        self
    }

    /// Set the comparator name.
    pub fn comparator_name(mut self, name: &str) -> Self {
        self.comparator_name = name.to_string();
        self
    }

    /// Set the skip list max level.
    pub fn skip_list_max_level(mut self, level: i32) -> Self {
        self.skip_list_max_level = level;
        self
    }

    /// Set the skip list probability.
    pub fn skip_list_probability(mut self, prob: f32) -> Self {
        self.skip_list_probability = prob;
        self
    }

    /// Set the minimum disk space required.
    pub fn min_disk_space(mut self, space: u64) -> Self {
        self.min_disk_space = space;
        self
    }

    /// Set the L1 file count trigger for compaction.
    pub fn l1_file_count_trigger(mut self, trigger: i32) -> Self {
        self.l1_file_count_trigger = trigger;
        self
    }

    /// Set the L0 queue stall threshold for backpressure.
    pub fn l0_queue_stall_threshold(mut self, threshold: i32) -> Self {
        self.l0_queue_stall_threshold = threshold;
        self
    }

    /// Load configuration from an INI file.
    ///
    /// # Arguments
    ///
    /// * `ini_file` - Path to the INI file
    /// * `section_name` - Section name in the INI file
    pub fn load_from_ini(ini_file: &str, section_name: &str) -> Result<Self> {
        let c_ini_file = CString::new(ini_file)?;
        let c_section_name = CString::new(section_name)?;
        let mut c_config = unsafe { ffi::tidesdb_default_column_family_config() };

        let result = unsafe {
            ffi::tidesdb_cf_config_load_from_ini(
                c_ini_file.as_ptr(),
                c_section_name.as_ptr(),
                &mut c_config,
            )
        };
        check_result(result, "failed to load config from INI")?;

        Ok(Self::from_c_config(&c_config))
    }

    /// Save configuration to an INI file.
    ///
    /// # Arguments
    ///
    /// * `ini_file` - Path to the INI file
    /// * `section_name` - Section name in the INI file
    pub fn save_to_ini(&self, ini_file: &str, section_name: &str) -> Result<()> {
        let c_ini_file = CString::new(ini_file)?;
        let c_section_name = CString::new(section_name)?;
        let c_config = self.to_c_config();

        let result = unsafe {
            ffi::tidesdb_cf_config_save_to_ini(
                c_ini_file.as_ptr(),
                c_section_name.as_ptr(),
                &c_config,
            )
        };
        check_result(result, "failed to save config to INI")
    }

    /// Create a ColumnFamilyConfig from a C config struct.
    fn from_c_config(c_config: &ffi::tidesdb_column_family_config_t) -> Self {
        let mut comparator_name = String::new();
        let name_bytes: Vec<u8> = c_config
            .comparator_name
            .iter()
            .take_while(|&&c| c != 0)
            .map(|&c| c as u8)
            .collect();
        if let Ok(s) = std::str::from_utf8(&name_bytes) {
            comparator_name = s.to_string();
        }

        ColumnFamilyConfig {
            write_buffer_size: c_config.write_buffer_size,
            level_size_ratio: c_config.level_size_ratio,
            min_levels: c_config.min_levels,
            dividing_level_offset: c_config.dividing_level_offset,
            klog_value_threshold: c_config.klog_value_threshold,
            compression_algorithm: match c_config.compression_algo {
                ffi::SNAPPY_COMPRESSION => CompressionAlgorithm::Snappy,
                ffi::LZ4_COMPRESSION => CompressionAlgorithm::Lz4,
                ffi::ZSTD_COMPRESSION => CompressionAlgorithm::Zstd,
                ffi::LZ4_FAST_COMPRESSION => CompressionAlgorithm::Lz4Fast,
                _ => CompressionAlgorithm::None,
            },
            enable_bloom_filter: c_config.enable_bloom_filter != 0,
            bloom_fpr: c_config.bloom_fpr,
            enable_block_indexes: c_config.enable_block_indexes != 0,
            index_sample_ratio: c_config.index_sample_ratio,
            block_index_prefix_len: c_config.block_index_prefix_len,
            sync_mode: match c_config.sync_mode {
                ffi::TDB_SYNC_FULL => SyncMode::Full,
                ffi::TDB_SYNC_INTERVAL => SyncMode::Interval,
                _ => SyncMode::None,
            },
            sync_interval_us: c_config.sync_interval_us,
            comparator_name,
            skip_list_max_level: c_config.skip_list_max_level,
            skip_list_probability: c_config.skip_list_probability,
            default_isolation_level: match c_config.default_isolation_level {
                ffi::TDB_ISOLATION_READ_UNCOMMITTED => IsolationLevel::ReadUncommitted,
                ffi::TDB_ISOLATION_REPEATABLE_READ => IsolationLevel::RepeatableRead,
                ffi::TDB_ISOLATION_SNAPSHOT => IsolationLevel::Snapshot,
                ffi::TDB_ISOLATION_SERIALIZABLE => IsolationLevel::Serializable,
                _ => IsolationLevel::ReadCommitted,
            },
            min_disk_space: c_config.min_disk_space,
            l1_file_count_trigger: c_config.l1_file_count_trigger,
            l0_queue_stall_threshold: c_config.l0_queue_stall_threshold,
        }
    }

    /// Convert to C configuration struct.
    pub(crate) fn to_c_config(&self) -> ffi::tidesdb_column_family_config_t {
        let mut config = ffi::tidesdb_column_family_config_t {
            write_buffer_size: self.write_buffer_size,
            level_size_ratio: self.level_size_ratio,
            min_levels: self.min_levels,
            dividing_level_offset: self.dividing_level_offset,
            klog_value_threshold: self.klog_value_threshold,
            compression_algo: self.compression_algorithm as i32,
            enable_bloom_filter: if self.enable_bloom_filter { 1 } else { 0 },
            bloom_fpr: self.bloom_fpr,
            enable_block_indexes: if self.enable_block_indexes { 1 } else { 0 },
            index_sample_ratio: self.index_sample_ratio,
            block_index_prefix_len: self.block_index_prefix_len,
            sync_mode: self.sync_mode as i32,
            sync_interval_us: self.sync_interval_us,
            comparator_name: [0; ffi::TDB_MAX_COMPARATOR_NAME],
            comparator_ctx_str: [0; ffi::TDB_MAX_COMPARATOR_CTX],
            comparator_fn_cached: std::ptr::null_mut(),
            comparator_ctx_cached: std::ptr::null_mut(),
            skip_list_max_level: self.skip_list_max_level,
            skip_list_probability: self.skip_list_probability,
            default_isolation_level: self.default_isolation_level as i32,
            min_disk_space: self.min_disk_space,
            l1_file_count_trigger: self.l1_file_count_trigger,
            l0_queue_stall_threshold: self.l0_queue_stall_threshold,
        };

        // Copy comparator name
        if !self.comparator_name.is_empty() {
            let bytes = self.comparator_name.as_bytes();
            let len = bytes.len().min(ffi::TDB_MAX_COMPARATOR_NAME - 1);
            for (i, &b) in bytes[..len].iter().enumerate() {
                config.comparator_name[i] = b as i8;
            }
        }

        config
    }
}

impl Default for ColumnFamilyConfig {
    fn default() -> Self {
        // Get defaults from C library
        let c_config = unsafe { ffi::tidesdb_default_column_family_config() };

        let mut comparator_name = String::new();
        let name_bytes: Vec<u8> = c_config
            .comparator_name
            .iter()
            .take_while(|&&c| c != 0)
            .map(|&c| c as u8)
            .collect();
        if let Ok(s) = std::str::from_utf8(&name_bytes) {
            comparator_name = s.to_string();
        }

        ColumnFamilyConfig {
            write_buffer_size: c_config.write_buffer_size,
            level_size_ratio: c_config.level_size_ratio,
            min_levels: c_config.min_levels,
            dividing_level_offset: c_config.dividing_level_offset,
            klog_value_threshold: c_config.klog_value_threshold,
            compression_algorithm: match c_config.compression_algo {
                ffi::SNAPPY_COMPRESSION => CompressionAlgorithm::Snappy,
                ffi::LZ4_COMPRESSION => CompressionAlgorithm::Lz4,
                ffi::ZSTD_COMPRESSION => CompressionAlgorithm::Zstd,
                ffi::LZ4_FAST_COMPRESSION => CompressionAlgorithm::Lz4Fast,
                _ => CompressionAlgorithm::None,
            },
            enable_bloom_filter: c_config.enable_bloom_filter != 0,
            bloom_fpr: c_config.bloom_fpr,
            enable_block_indexes: c_config.enable_block_indexes != 0,
            index_sample_ratio: c_config.index_sample_ratio,
            block_index_prefix_len: c_config.block_index_prefix_len,
            sync_mode: match c_config.sync_mode {
                ffi::TDB_SYNC_FULL => SyncMode::Full,
                ffi::TDB_SYNC_INTERVAL => SyncMode::Interval,
                _ => SyncMode::None,
            },
            sync_interval_us: c_config.sync_interval_us,
            comparator_name,
            skip_list_max_level: c_config.skip_list_max_level,
            skip_list_probability: c_config.skip_list_probability,
            default_isolation_level: match c_config.default_isolation_level {
                ffi::TDB_ISOLATION_READ_UNCOMMITTED => IsolationLevel::ReadUncommitted,
                ffi::TDB_ISOLATION_REPEATABLE_READ => IsolationLevel::RepeatableRead,
                ffi::TDB_ISOLATION_SNAPSHOT => IsolationLevel::Snapshot,
                ffi::TDB_ISOLATION_SERIALIZABLE => IsolationLevel::Serializable,
                _ => IsolationLevel::ReadCommitted,
            },
            min_disk_space: c_config.min_disk_space,
            l1_file_count_trigger: c_config.l1_file_count_trigger,
            l0_queue_stall_threshold: c_config.l0_queue_stall_threshold,
        }
    }
}
