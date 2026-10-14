// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Statistics types for TidesDB.

use crate::config::ColumnFamilyConfig;
use crate::ffi;
use std::ffi::CStr;

/// Per-column-family statistics, returned by
/// [`ColumnFamily::get_stats`](crate::ColumnFamily::get_stats).
///
/// Memtable figures are database-level and live in [`DbStats`].
#[derive(Debug, Clone)]
pub struct CfStats {
    /// The family's current name. Unlike [`ColumnFamily::name`](crate::ColumnFamily::name),
    /// this reflects a rename made after the handle was looked up.
    pub name: String,
    /// Deepest level that currently holds an sstable (0 for a family with nothing flushed)
    pub num_levels: i32,
    /// A copy of the column family configuration
    pub config: ColumnFamilyConfig,
    /// On-disk size of each level, index 0 is L1
    pub level_sizes: Vec<usize>,
    /// SSTable count of each level
    pub level_num_sstables: Vec<i32>,
    /// Key count of each level
    pub level_key_counts: Vec<u64>,
    /// Tombstone count of each level
    pub level_tombstone_counts: Vec<u64>,
    /// Sum of each sstable's distinct key count (a key present in several sstables counts
    /// once per sstable)
    pub total_keys: u64,
    /// The family's own on-disk size (sum of its key logs); separated values are counted
    /// in [`DbStats::vlog_file_size`]
    pub total_data_size: u64,
    /// Average key length in bytes
    pub avg_key_size: f64,
    /// Average value length in bytes
    pub avg_value_size: f64,
    /// Point-lookup read amplification (sstables a worst-case get may probe)
    pub read_amp: f64,
    /// Total btree nodes
    pub btree_total_nodes: u64,
    /// Maximum btree height
    pub btree_max_height: u32,
    /// Average btree height
    pub btree_avg_height: f64,
    /// Total tombstones across every sstable
    pub total_tombstones: u64,
    /// `total_tombstones / total_keys`
    pub tombstone_ratio: f64,
    /// Worst per-sstable tombstone density observed
    pub max_sst_density: f64,
    /// 1-based level of `max_sst_density`, 0 if none
    pub max_sst_density_level: i32,
    /// This family's attributed share of write-ahead log bytes
    pub wal_bytes_written: u64,
    /// On-disk bytes this family's flushes wrote
    pub flush_bytes_written: u64,
    /// On-disk bytes this family's compactions wrote
    pub compaction_bytes_written: u64,
    /// On-disk bytes this family's compactions read
    pub compaction_bytes_read: u64,
    /// Logical key+value bytes committed to this family
    pub user_bytes_written: u64,
    /// Compactions this family has run
    pub compaction_count: u64,
    /// Distinct keys still only in the shared memtables
    pub unflushed_key_count: u64,
    /// Memory held by this family's bloom filter directories outside the block cache
    pub filter_resident_bytes: u64,
}

impl CfStats {
    pub(crate) fn from_c(c: &ffi::tidesdb_cf_stats_t) -> Self {
        let n = (c.num_levels.max(0) as usize).min(ffi::TDB_MAX_LEVELS);
        // NUL-terminated within the fixed buffer; bounded in case it is not
        let name_bytes: Vec<u8> = c
            .config
            .name
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as u8)
            .collect();
        CfStats {
            name: String::from_utf8_lossy(&name_bytes).into_owned(),
            num_levels: c.num_levels,
            config: ColumnFamilyConfig::from_c_config(&c.config),
            level_sizes: c.level_sizes[..n].to_vec(),
            level_num_sstables: c.level_num_sstables[..n].to_vec(),
            level_key_counts: c.level_key_counts[..n].to_vec(),
            level_tombstone_counts: c.level_tombstone_counts[..n].to_vec(),
            total_keys: c.total_keys,
            total_data_size: c.total_data_size,
            avg_key_size: c.avg_key_size,
            avg_value_size: c.avg_value_size,
            read_amp: c.read_amp,
            btree_total_nodes: c.btree_total_nodes,
            btree_max_height: c.btree_max_height,
            btree_avg_height: c.btree_avg_height,
            total_tombstones: c.total_tombstones,
            tombstone_ratio: c.tombstone_ratio,
            max_sst_density: c.max_sst_density,
            max_sst_density_level: c.max_sst_density_level,
            wal_bytes_written: c.wal_bytes_written,
            flush_bytes_written: c.flush_bytes_written,
            compaction_bytes_written: c.compaction_bytes_written,
            compaction_bytes_read: c.compaction_bytes_read,
            user_bytes_written: c.user_bytes_written,
            compaction_count: c.compaction_count,
            unflushed_key_count: c.unflushed_key_count,
            filter_resident_bytes: c.filter_resident_bytes,
        }
    }
}

/// Database-level statistics, returned by [`TidesDB::get_db_stats`](crate::TidesDB::get_db_stats).
#[derive(Debug, Clone)]
pub struct DbStats {
    /// Number of column families
    pub num_column_families: i32,
    /// Immutable memtables awaiting flush (the L0 queue depth)
    pub immutable_memtable_count: i32,
    /// Compaction jobs queued
    pub compaction_pending_count: i32,
    /// Total sstables across every column family and level
    pub total_sstable_count: i32,
    /// On-disk key-log bytes across every column family
    pub total_data_size_bytes: u64,
    /// Currently open sstable file handles
    pub num_open_sstables: i32,
    /// Current global sequence number (the MVCC clock)
    pub global_seq: u64,
    /// Oldest frozen snapshot any live transaction holds
    pub min_snapshot_seq: u64,
    /// Live registered transactions (read committed and stronger)
    pub active_txn_count: i32,
    /// Bytes held by in-flight transactions
    pub txn_memory_bytes: i64,
    /// Transactions committed since open
    pub txn_commits: u64,
    /// Commits and prepares refused with a conflict since open
    pub txn_conflicts: u64,
    /// Memory the active memtable occupies
    pub memtable_bytes: usize,
    /// Whether an immutable memtable is queued or flushing
    pub is_flushing: bool,
    /// Next column family id to be assigned
    pub next_cf_index: u32,
    /// Generation of the active write-ahead log
    pub wal_generation: u64,
    /// SSTables flushed across every column family
    pub flush_count: u64,
    /// Compactions run across every column family
    pub compaction_count: u64,
    /// Flush output bytes
    pub flush_bytes_written: u64,
    /// Compaction output bytes
    pub compaction_bytes_written: u64,
    /// Compaction input bytes
    pub compaction_bytes_read: u64,
    /// Framed write-ahead log bytes, measured at the append
    pub wal_bytes_written: u64,
    /// Logical committed bytes
    pub user_bytes_written: u64,
    /// Total value-log file size
    pub vlog_file_size: u64,
    /// Values currently indexed in the value log
    pub vlog_value_count: u64,
    /// Uncompressed length the indexed values represent
    pub vlog_used_bytes: u64,
    /// On-disk length of the indexed values
    pub vlog_stored_bytes: u64,
    /// Value-log bytes the live sstables still reference
    pub vlog_live_bytes: u64,
    /// Open value-log segment files
    pub vlog_segment_count: u64,
    /// Value-log bytes ever appended, reclamation rewrites included
    pub vlog_bytes_written: u64,
    /// Value-log bytes beyond what live values account for
    pub vlog_dead_bytes: u64,
    /// Value-log reclaims attempted
    pub vlog_reclaim_calls: u64,
    /// Value-log reclaim passes that drained a segment
    pub vlog_reclaim_passes: u64,
    /// Value-log segment files a reclaim has unlinked
    pub vlog_segments_retired: u64,
    /// Sealed segments worth draining
    pub vlog_segments_drainable: u64,
    /// Commits the L0 admission policy made dwell
    pub writes_throttled: u64,
    /// Commits the L0 admission policy made wait for the flush queue
    pub writes_blocked: u64,
    /// Total microseconds commits were held in L0 admission
    pub write_stall_us: u64,
    /// Commits admitted only because the admission wait ceiling expired
    pub write_stall_ceiling_hits: u64,
}

impl DbStats {
    pub(crate) fn from_c(c: &ffi::tidesdb_db_stats_t) -> Self {
        DbStats {
            num_column_families: c.num_column_families,
            immutable_memtable_count: c.immutable_memtable_count,
            compaction_pending_count: c.compaction_pending_count,
            total_sstable_count: c.total_sstable_count,
            total_data_size_bytes: c.total_data_size_bytes,
            num_open_sstables: c.num_open_sstables,
            global_seq: c.global_seq,
            min_snapshot_seq: c.min_snapshot_seq,
            active_txn_count: c.active_txn_count,
            txn_memory_bytes: c.txn_memory_bytes,
            txn_commits: c.txn_commits,
            txn_conflicts: c.txn_conflicts,
            memtable_bytes: c.memtable_bytes,
            is_flushing: c.is_flushing != 0,
            next_cf_index: c.next_cf_index,
            wal_generation: c.wal_generation,
            flush_count: c.flush_count,
            compaction_count: c.compaction_count,
            flush_bytes_written: c.flush_bytes_written,
            compaction_bytes_written: c.compaction_bytes_written,
            compaction_bytes_read: c.compaction_bytes_read,
            wal_bytes_written: c.wal_bytes_written,
            user_bytes_written: c.user_bytes_written,
            vlog_file_size: c.vlog_file_size,
            vlog_value_count: c.vlog_value_count,
            vlog_used_bytes: c.vlog_used_bytes,
            vlog_stored_bytes: c.vlog_stored_bytes,
            vlog_live_bytes: c.vlog_live_bytes,
            vlog_segment_count: c.vlog_segment_count,
            vlog_bytes_written: c.vlog_bytes_written,
            vlog_dead_bytes: c.vlog_dead_bytes,
            vlog_reclaim_calls: c.vlog_reclaim_calls,
            vlog_reclaim_passes: c.vlog_reclaim_passes,
            vlog_segments_retired: c.vlog_segments_retired,
            vlog_segments_drainable: c.vlog_segments_drainable,
            writes_throttled: c.writes_throttled,
            writes_blocked: c.writes_blocked,
            write_stall_us: c.write_stall_us,
            write_stall_ceiling_hits: c.write_stall_ceiling_hits,
        }
    }
}

/// Block cache statistics, returned by [`TidesDB::get_cache_stats`](crate::TidesDB::get_cache_stats).
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Whether the cache is enabled
    pub enabled: bool,
    /// Number of cached entries
    pub total_entries: usize,
    /// Bytes used by the cache
    pub total_bytes: usize,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// `hits / (hits + misses)`
    pub hit_rate: f64,
    /// Number of cache shards
    pub num_partitions: usize,
}

/// What one encoding chain achieved on the data it wrote, returned by
/// [`TidesDB::get_klog_encoding_stats`](crate::TidesDB::get_klog_encoding_stats) and
/// [`TidesDB::get_vlog_encoding_stats`](crate::TidesDB::get_vlog_encoding_stats).
#[derive(Debug, Clone)]
pub struct EncodingStats {
    /// Encoding ids in the order applied; empty when stored verbatim. Ids `1..=4` are
    /// [`CompressionAlgorithm`](crate::CompressionAlgorithm) values.
    pub ids: Vec<u8>,
    /// Size of the data before encoding
    pub logical_bytes: u64,
    /// Size of the data on disk
    pub stored_bytes: u64,
    /// Values for the value log, sstables for the key logs
    pub item_count: u64,
}

impl EncodingStats {
    pub(crate) fn from_c(c: &ffi::tidesdb_encoding_stats_t) -> Self {
        let n = (c.id_count.max(0) as usize).min(ffi::TDB_ENCODING_PIPELINE_MAX);
        EncodingStats {
            ids: c.ids[..n].to_vec(),
            logical_bytes: c.logical_bytes,
            stored_bytes: c.stored_bytes,
            item_count: c.item_count,
        }
    }

    /// The realised ratio, `logical_bytes / stored_bytes` (0 when nothing is stored).
    pub fn ratio(&self) -> f64 {
        if self.stored_bytes == 0 {
            0.0
        } else {
            self.logical_bytes as f64 / self.stored_bytes as f64
        }
    }
}

/// How much waiting one stall reason accounted for since the database opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StallStat {
    /// How many times a thread waited here
    pub count: u64,
    /// The summed wait in microseconds
    pub total_us: u64,
    /// The longest single wait in microseconds
    pub max_us: u64,
}

impl From<ffi::tidesdb_stall_stat_t> for StallStat {
    fn from(c: ffi::tidesdb_stall_stat_t) -> Self {
        StallStat {
            count: c.count,
            total_us: c.total_us,
            max_us: c.max_us,
        }
    }
}

/// Where writers have been made to wait, returned by
/// [`TidesDB::get_stall_stats`](crate::TidesDB::get_stall_stats).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StallStats {
    /// Waiting on the write-ahead log (staging space or, under a syncing mode, the write)
    pub wal_append: StallStat,
    /// Waiting to take the rotation lock while another committer rotated
    pub rotate_lock: StallStat,
    /// Performing a memtable rotation on everyone's behalf
    pub rotate_work: StallStat,
    /// Held by write admission because the unflushed backlog was too deep
    pub admission: StallStat,
    /// Inside a manifest commit
    pub manifest_commit: StallStat,
}

impl StallStats {
    pub(crate) fn from_c(c: &ffi::tidesdb_stall_stats_t) -> Self {
        StallStats {
            wal_append: c.reasons[ffi::TDB_STALL_WAL_APPEND as usize].into(),
            rotate_lock: c.reasons[ffi::TDB_STALL_ROTATE_LOCK as usize].into(),
            rotate_work: c.reasons[ffi::TDB_STALL_ROTATE_WORK as usize].into(),
            admission: c.reasons[ffi::TDB_STALL_ADMISSION as usize].into(),
            manifest_commit: c.reasons[ffi::TDB_STALL_MANIFEST_COMMIT as usize].into(),
        }
    }

    /// Every reason with the library's stable short name, for logging or a stats table.
    pub fn iter(&self) -> impl std::iter::Iterator<Item = (&'static str, StallStat)> {
        [
            (ffi::TDB_STALL_WAL_APPEND, self.wal_append),
            (ffi::TDB_STALL_ROTATE_LOCK, self.rotate_lock),
            (ffi::TDB_STALL_ROTATE_WORK, self.rotate_work),
            (ffi::TDB_STALL_ADMISSION, self.admission),
            (ffi::TDB_STALL_MANIFEST_COMMIT, self.manifest_commit),
        ]
        .into_iter()
        .map(|(reason, stat)| {
            (
                static_name(unsafe { ffi::tidesdb_stall_reason_name(reason) }),
                stat,
            )
        })
    }
}

/// What one class of file asked of the device.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStat {
    /// Writes issued
    pub ops: u64,
    /// Bytes written
    pub bytes: u64,
    /// Summed time inside those writes, in microseconds
    pub total_us: u64,
    /// The slowest single write, in microseconds
    pub max_us: u64,
}

impl From<ffi::tidesdb_io_stat_t> for IoStat {
    fn from(c: ffi::tidesdb_io_stat_t) -> Self {
        IoStat {
            ops: c.ops,
            bytes: c.bytes,
            total_us: c.total_us,
            max_us: c.max_us,
        }
    }
}

/// Device writes per file class, returned by [`TidesDB::get_io_stats`](crate::TidesDB::get_io_stats).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStats {
    /// Key logs, written by flush and compaction
    pub sstable: IoStat,
    /// The write-ahead log
    pub wal: IoStat,
    /// Value log segments
    pub vlog: IoStat,
}

impl IoStats {
    pub(crate) fn from_c(c: &ffi::tidesdb_io_stats_t) -> Self {
        IoStats {
            sstable: c.classes[ffi::TDB_IO_SSTABLE as usize].into(),
            wal: c.classes[ffi::TDB_IO_WAL as usize].into(),
            vlog: c.classes[ffi::TDB_IO_VLOG as usize].into(),
        }
    }

    /// Every class with the library's stable short name, for logging or a stats table.
    pub fn iter(&self) -> impl std::iter::Iterator<Item = (&'static str, IoStat)> {
        [
            (ffi::TDB_IO_SSTABLE, self.sstable),
            (ffi::TDB_IO_WAL, self.wal),
            (ffi::TDB_IO_VLOG, self.vlog),
        ]
        .into_iter()
        .map(|(cls, stat)| {
            (
                static_name(unsafe { ffi::tidesdb_io_class_name(cls) }),
                stat,
            )
        })
    }
}

/// Query-planner statistics for a key range, returned by
/// [`ColumnFamily::range_stats`](crate::ColumnFamily::range_stats).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeStats {
    /// Sorted runs a scan of the range would merge
    pub sstables_overlapping: u64,
    /// Live keys the range holds
    pub estimated_keys: u64,
    /// Whether `estimated_keys` was counted exactly rather than estimated
    pub keys_exact: bool,
}

/// The C name functions return static literals that are never NULL and never freed.
fn static_name(ptr: *const libc::c_char) -> &'static str {
    if ptr.is_null() {
        return "unknown";
    }
    unsafe { CStr::from_ptr(ptr) }.to_str().unwrap_or("unknown")
}
