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
