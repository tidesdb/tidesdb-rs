// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Configuration types for TidesDB.

use crate::error::{Result, check_result};
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

/// Object store behavior configuration.
///
/// Controls caching, upload/download parallelism, multipart thresholds,
/// WAL replication, and replica mode for object store deployments.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    /// Local directory for cached SSTable files (None = use db_path)
    pub local_cache_path: Option<String>,
    /// Maximum local cache size in bytes (0 = unlimited)
    pub local_cache_max_bytes: usize,
    /// Cache downloaded files locally (default: true)
    pub cache_on_read: bool,
    /// Keep local copy after upload (default: true)
    pub cache_on_write: bool,
    /// Number of parallel upload threads (default: 4)
    pub max_concurrent_uploads: i32,
    /// Number of parallel download threads (default: 8)
    pub max_concurrent_downloads: i32,
    /// Use multipart upload above this size in bytes (default: 64MB)
    pub multipart_threshold: usize,
    /// Chunk size for multipart uploads in bytes (default: 8MB)
    pub multipart_part_size: usize,
    /// Upload MANIFEST after each compaction (default: true)
    pub sync_manifest_to_object: bool,
    /// Upload closed WAL segments for replication (default: true)
    pub replicate_wal: bool,
    /// false = background WAL upload (default), true = block flush until uploaded
    pub wal_upload_sync: bool,
    /// Sync active WAL when it grows by this many bytes (default: 1MB, 0 = off)
    pub wal_sync_threshold_bytes: usize,
    /// Upload WAL after every txn commit for RPO=0 replication (default: false)
    pub wal_sync_on_commit: bool,
    /// Enable read-only replica mode (default: false)
    pub replica_mode: bool,
    /// MANIFEST poll interval for replica sync in microseconds (default: 5s)
    pub replica_sync_interval_us: u64,
    /// Replay WAL from object store for near-real-time reads on replicas (default: true)
    pub replica_replay_wal: bool,
}

impl ObjectStoreConfig {
    /// Create a new object store configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the local cache path (None = use db_path).
    pub fn local_cache_path(mut self, path: &str) -> Self {
        self.local_cache_path = Some(path.to_string());
        self
    }

    /// Set the maximum local cache size in bytes (0 = unlimited).
    pub fn local_cache_max_bytes(mut self, size: usize) -> Self {
        self.local_cache_max_bytes = size;
        self
    }

    /// Enable or disable caching downloaded files locally.
    pub fn cache_on_read(mut self, enable: bool) -> Self {
        self.cache_on_read = enable;
        self
    }

    /// Enable or disable keeping local copy after upload.
    pub fn cache_on_write(mut self, enable: bool) -> Self {
        self.cache_on_write = enable;
        self
    }

    /// Set the number of parallel upload threads.
    pub fn max_concurrent_uploads(mut self, n: i32) -> Self {
        self.max_concurrent_uploads = n;
        self
    }

    /// Set the number of parallel download threads.
    pub fn max_concurrent_downloads(mut self, n: i32) -> Self {
        self.max_concurrent_downloads = n;
        self
    }

    /// Set the multipart upload threshold in bytes.
    pub fn multipart_threshold(mut self, size: usize) -> Self {
        self.multipart_threshold = size;
        self
    }

    /// Set the multipart chunk size in bytes.
    pub fn multipart_part_size(mut self, size: usize) -> Self {
        self.multipart_part_size = size;
        self
    }

    /// Enable or disable uploading MANIFEST after each compaction.
    pub fn sync_manifest_to_object(mut self, enable: bool) -> Self {
        self.sync_manifest_to_object = enable;
        self
    }

    /// Enable or disable uploading closed WAL segments for replication.
    pub fn replicate_wal(mut self, enable: bool) -> Self {
        self.replicate_wal = enable;
        self
    }

    /// Set WAL upload sync mode (false = background, true = block flush until uploaded).
    pub fn wal_upload_sync(mut self, enable: bool) -> Self {
        self.wal_upload_sync = enable;
        self
    }

    /// Set the WAL sync threshold in bytes (0 = off).
    pub fn wal_sync_threshold_bytes(mut self, size: usize) -> Self {
        self.wal_sync_threshold_bytes = size;
        self
    }

    /// Enable or disable uploading WAL after every txn commit (RPO=0).
    pub fn wal_sync_on_commit(mut self, enable: bool) -> Self {
        self.wal_sync_on_commit = enable;
        self
    }

    /// Enable or disable read-only replica mode.
    pub fn replica_mode(mut self, enable: bool) -> Self {
        self.replica_mode = enable;
        self
    }

    /// Set the MANIFEST poll interval for replica sync in microseconds.
    pub fn replica_sync_interval_us(mut self, interval: u64) -> Self {
        self.replica_sync_interval_us = interval;
        self
    }

    /// Enable or disable WAL replay on replicas for near-real-time reads.
    pub fn replica_replay_wal(mut self, enable: bool) -> Self {
        self.replica_replay_wal = enable;
        self
    }

    /// Convert to C configuration struct.
    /// Returns the C struct and an optional CString for the cache path that must stay alive.
    pub(crate) fn to_c_config(&self) -> (ffi::tidesdb_objstore_config_t, Option<CString>) {
        let (cache_path_ptr, cache_path_cstr) = match &self.local_cache_path {
            Some(p) => {
                let cs = CString::new(p.as_str()).unwrap_or_default();
                let ptr = cs.as_ptr();
                (ptr, Some(cs))
            }
            None => (std::ptr::null(), None),
        };

        let config = ffi::tidesdb_objstore_config_t {
            local_cache_path: cache_path_ptr,
            local_cache_max_bytes: self.local_cache_max_bytes,
            cache_on_read: if self.cache_on_read { 1 } else { 0 },
            cache_on_write: if self.cache_on_write { 1 } else { 0 },
            max_concurrent_uploads: self.max_concurrent_uploads,
            max_concurrent_downloads: self.max_concurrent_downloads,
            multipart_threshold: self.multipart_threshold,
            multipart_part_size: self.multipart_part_size,
            sync_manifest_to_object: if self.sync_manifest_to_object { 1 } else { 0 },
            replicate_wal: if self.replicate_wal { 1 } else { 0 },
            wal_upload_sync: if self.wal_upload_sync { 1 } else { 0 },
            wal_sync_threshold_bytes: self.wal_sync_threshold_bytes,
            wal_sync_on_commit: if self.wal_sync_on_commit { 1 } else { 0 },
            replica_mode: if self.replica_mode { 1 } else { 0 },
            replica_sync_interval_us: self.replica_sync_interval_us,
            replica_replay_wal: if self.replica_replay_wal { 1 } else { 0 },
        };

        (config, cache_path_cstr)
    }
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        let c = unsafe { ffi::tidesdb_objstore_default_config() };
        ObjectStoreConfig {
            local_cache_path: None,
            local_cache_max_bytes: c.local_cache_max_bytes,
            cache_on_read: c.cache_on_read != 0,
            cache_on_write: c.cache_on_write != 0,
            max_concurrent_uploads: c.max_concurrent_uploads,
            max_concurrent_downloads: c.max_concurrent_downloads,
            multipart_threshold: c.multipart_threshold,
            multipart_part_size: c.multipart_part_size,
            sync_manifest_to_object: c.sync_manifest_to_object != 0,
            replicate_wal: c.replicate_wal != 0,
            wal_upload_sync: c.wal_upload_sync != 0,
            wal_sync_threshold_bytes: c.wal_sync_threshold_bytes,
            wal_sync_on_commit: c.wal_sync_on_commit != 0,
            replica_mode: c.replica_mode != 0,
            replica_sync_interval_us: c.replica_sync_interval_us,
            replica_replay_wal: c.replica_replay_wal != 0,
        }
    }
}

/// S3-compatible object store connector configuration.
///
/// Used with [`Config::object_store_s3`] to back a database with an S3 bucket
/// (AWS S3, MinIO, etc.). Requires the `objectstore` Cargo feature and a TidesDB
/// C library built with `TIDESDB_WITH_S3=ON`.
///
/// The all-default values are secure: TLS verification is on, no custom CA is
/// used, and the library's built-in multipart sizes apply (`0` = library default).
#[cfg(feature = "objectstore")]
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    /// S3 endpoint (e.g. `"s3.amazonaws.com"` or `"minio.local:9000"`).
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Key prefix (e.g. `"production/db1/"`), or `None`.
    pub prefix: Option<String>,
    /// AWS access key ID.
    pub access_key: String,
    /// AWS secret access key.
    pub secret_key: String,
    /// AWS region (e.g. `"us-east-1"`), or `None` for MinIO / the default.
    pub region: Option<String>,
    /// `true` for HTTPS, `false` for HTTP.
    pub use_ssl: bool,
    /// `true` for path-style URLs (MinIO), `false` for virtual-hosted (AWS).
    pub use_path_style: bool,
    /// Custom CA bundle file path, or `None` for the system bundle.
    pub tls_ca_path: Option<String>,
    /// `true` disables TLS peer+host verification (test only, insecure).
    pub tls_insecure_skip_verify: bool,
    /// Object size at/above which multipart upload is used; `0` = library default.
    pub multipart_threshold: usize,
    /// Multipart chunk size in bytes; `0` = library default.
    pub multipart_part_size: usize,
}

#[cfg(feature = "objectstore")]
impl S3Config {
    /// Create a new S3 configuration with the required endpoint, bucket, and credentials.
    pub fn new(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str) -> Self {
        S3Config {
            endpoint: endpoint.to_string(),
            bucket: bucket.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            ..Default::default()
        }
    }

    /// Set the key prefix.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.prefix = Some(prefix.to_string());
        self
    }

    /// Set the AWS region.
    pub fn region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }

    /// Enable or disable HTTPS (TLS).
    pub fn use_ssl(mut self, enable: bool) -> Self {
        self.use_ssl = enable;
        self
    }

    /// Enable or disable path-style URLs (required for MinIO).
    pub fn use_path_style(mut self, enable: bool) -> Self {
        self.use_path_style = enable;
        self
    }

    /// Set a custom CA bundle file path.
    pub fn tls_ca_path(mut self, path: &str) -> Self {
        self.tls_ca_path = Some(path.to_string());
        self
    }

    /// Disable TLS peer+host verification (test only, insecure).
    pub fn tls_insecure_skip_verify(mut self, enable: bool) -> Self {
        self.tls_insecure_skip_verify = enable;
        self
    }

    /// Set the multipart upload threshold in bytes (`0` = library default).
    pub fn multipart_threshold(mut self, size: usize) -> Self {
        self.multipart_threshold = size;
        self
    }

    /// Set the multipart chunk size in bytes (`0` = library default).
    pub fn multipart_part_size(mut self, size: usize) -> Self {
        self.multipart_part_size = size;
        self
    }
}

/// Holds all C-side allocations that must outlive the `tidesdb_open()` call.
pub(crate) struct CConfigData {
    pub config: ffi::tidesdb_config_t,
    _db_path: CString,
    _objstore_config: Option<Box<ffi::tidesdb_objstore_config_t>>,
    _cache_path: Option<CString>,
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
    /// Global memory limit in bytes (0 = auto, 50% of system RAM; minimum: 5% of system RAM)
    pub max_memory_usage: usize,
    /// Write logs to file instead of stderr
    pub log_to_file: bool,
    /// Log file truncation threshold in bytes (0 = no truncation)
    pub log_truncation_at: usize,
    /// Enable unified memtable mode (default: false = per-CF memtables)
    pub unified_memtable: bool,
    /// Unified memtable write buffer size (0 = auto)
    pub unified_memtable_write_buffer_size: usize,
    /// Skip list max level for unified memtable (0 = default 12)
    pub unified_memtable_skip_list_max_level: i32,
    /// Skip list probability for unified memtable (0 = default 0.25)
    pub unified_memtable_skip_list_probability: f32,
    /// Sync mode for unified WAL (default: SyncMode::None)
    pub unified_memtable_sync_mode: SyncMode,
    /// Sync interval for unified WAL in microseconds (0 = default)
    pub unified_memtable_sync_interval_us: u64,
    /// Filesystem root directory for the object store connector (None = no object store)
    pub object_store_fs_path: Option<String>,
    /// S3 object store connector configuration (None = no S3 object store).
    /// Takes precedence over `object_store_fs_path` when both are set.
    #[cfg(feature = "objectstore")]
    pub object_store_s3: Option<S3Config>,
    /// Object store behavior configuration (None = use defaults when object store is set)
    pub object_store_config: Option<ObjectStoreConfig>,
    /// Global semaphore on the number of in-flight memtable flushes across all column
    /// families. Bounds peak transient memory and work-queue depth where supported.
    /// `0` uses the C library's version-specific default behavior.
    pub max_concurrent_flushes: i32,
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

    /// Set the global memory limit in bytes.
    /// 0 = auto (50% of system RAM). Minimum: 5% of system RAM.
    pub fn max_memory_usage(mut self, size: usize) -> Self {
        self.max_memory_usage = size;
        self
    }

    /// Enable writing logs to a file instead of stderr.
    pub fn log_to_file(mut self, enable: bool) -> Self {
        self.log_to_file = enable;
        self
    }

    /// Set the log file truncation threshold in bytes (0 = no truncation).
    pub fn log_truncation_at(mut self, size: usize) -> Self {
        self.log_truncation_at = size;
        self
    }

    /// Enable or disable unified memtable mode.
    /// When enabled, all column families share a single memtable and WAL.
    pub fn unified_memtable(mut self, enable: bool) -> Self {
        self.unified_memtable = enable;
        self
    }

    /// Set the unified memtable write buffer size (0 = auto).
    pub fn unified_memtable_write_buffer_size(mut self, size: usize) -> Self {
        self.unified_memtable_write_buffer_size = size;
        self
    }

    /// Set the skip list max level for unified memtable (0 = default 12).
    pub fn unified_memtable_skip_list_max_level(mut self, level: i32) -> Self {
        self.unified_memtable_skip_list_max_level = level;
        self
    }

    /// Set the skip list probability for unified memtable (0 = default 0.25).
    pub fn unified_memtable_skip_list_probability(mut self, prob: f32) -> Self {
        self.unified_memtable_skip_list_probability = prob;
        self
    }

    /// Set the sync mode for unified WAL.
    pub fn unified_memtable_sync_mode(mut self, mode: SyncMode) -> Self {
        self.unified_memtable_sync_mode = mode;
        self
    }

    /// Set the sync interval for unified WAL in microseconds.
    pub fn unified_memtable_sync_interval_us(mut self, interval: u64) -> Self {
        self.unified_memtable_sync_interval_us = interval;
        self
    }

    /// Enable object store mode with a filesystem connector.
    ///
    /// Stores objects as files under `root_dir` mirroring the key path structure.
    /// Useful for testing and local replication (e.g., NFS mount).
    ///
    /// Object store mode automatically enables unified memtable mode.
    pub fn object_store_fs(mut self, root_dir: &str) -> Self {
        self.object_store_fs_path = Some(root_dir.to_string());
        self
    }

    /// Enable object store mode with an S3-compatible connector (AWS S3, MinIO, etc.).
    ///
    /// Requires the `objectstore` Cargo feature and a TidesDB C library built with
    /// `TIDESDB_WITH_S3=ON`. Takes precedence over [`object_store_fs`](Self::object_store_fs)
    /// when both are set. Object store mode automatically enables unified memtable mode.
    #[cfg(feature = "objectstore")]
    pub fn object_store_s3(mut self, config: S3Config) -> Self {
        self.object_store_s3 = Some(config);
        self
    }

    /// Set the object store behavior configuration.
    ///
    /// Controls caching, upload/download parallelism, WAL replication,
    /// replica mode, and other object store behavior. If not set when an
    /// object store connector is configured, defaults are used.
    pub fn object_store_config(mut self, config: ObjectStoreConfig) -> Self {
        self.object_store_config = Some(config);
        self
    }

    /// Set the global cap on in-flight memtable flushes across all column families.
    ///
    /// Bounds peak transient memory and work-queue depth when many column families
    /// flush at once. `0` falls back to the library default.
    pub fn max_concurrent_flushes(mut self, n: i32) -> Self {
        self.max_concurrent_flushes = n;
        self
    }

    /// Convert to C configuration struct.
    /// Returns a `CConfigData` that owns all heap allocations needed during `tidesdb_open`.
    pub(crate) fn to_c_config(&self) -> crate::error::Result<CConfigData> {
        let c_path = CString::new(self.db_path.as_str())?;

        // Build object store connector if configured
        let objstore_ptr = match &self.object_store_fs_path {
            Some(root_dir) => {
                let c_root = CString::new(root_dir.as_str())?;
                let ptr = unsafe { ffi::tidesdb_objstore_fs_create(c_root.as_ptr()) };
                if ptr.is_null() {
                    return Err(crate::error::Error::NullPointer("object store connector"));
                }
                ptr
            }
            None => std::ptr::null_mut(),
        };

        // An S3 connector takes precedence over the filesystem connector.
        // The C factory copies all fields, so the temporary CStrings need only
        // outlive the call itself.
        #[cfg(feature = "objectstore")]
        let objstore_ptr = if let Some(s3) = &self.object_store_s3 {
            let c_endpoint = CString::new(s3.endpoint.as_str())?;
            let c_bucket = CString::new(s3.bucket.as_str())?;
            let c_access = CString::new(s3.access_key.as_str())?;
            let c_secret = CString::new(s3.secret_key.as_str())?;
            let c_prefix = s3.prefix.as_deref().map(CString::new).transpose()?;
            let c_region = s3.region.as_deref().map(CString::new).transpose()?;
            let c_ca = s3.tls_ca_path.as_deref().map(CString::new).transpose()?;

            let s3_c_config = ffi::tidesdb_objstore_s3_config_t {
                endpoint: c_endpoint.as_ptr(),
                bucket: c_bucket.as_ptr(),
                prefix: c_prefix.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                access_key: c_access.as_ptr(),
                secret_key: c_secret.as_ptr(),
                region: c_region.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                use_ssl: if s3.use_ssl { 1 } else { 0 },
                use_path_style: if s3.use_path_style { 1 } else { 0 },
                tls_ca_path: c_ca.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                tls_insecure_skip_verify: if s3.tls_insecure_skip_verify { 1 } else { 0 },
                multipart_threshold: s3.multipart_threshold,
                multipart_part_size: s3.multipart_part_size,
            };

            let ptr = unsafe { ffi::tidesdb_objstore_s3_create_config(&s3_c_config) };
            if ptr.is_null() {
                return Err(crate::error::Error::NullPointer("S3 object store connector"));
            }
            ptr
        } else {
            objstore_ptr
        };

        // Build object store config if configured (or if connector is set, use defaults)
        let (boxed_os_config, cache_path_cstr) = if !objstore_ptr.is_null() {
            let os_cfg = self
                .object_store_config
                .as_ref()
                .cloned()
                .unwrap_or_default();
            let (c_os_config, cache_cstr) = os_cfg.to_c_config();
            (Some(Box::new(c_os_config)), cache_cstr)
        } else {
            (None, None)
        };

        let os_config_ptr = match &boxed_os_config {
            Some(b) => &**b as *const ffi::tidesdb_objstore_config_t as *mut _,
            None => std::ptr::null_mut(),
        };

        let config = ffi::tidesdb_config_t {
            db_path: c_path.as_ptr(),
            num_flush_threads: self.num_flush_threads,
            num_compaction_threads: self.num_compaction_threads,
            log_level: self.log_level as i32,
            block_cache_size: self.block_cache_size,
            max_open_sstables: self.max_open_sstables,
            log_to_file: if self.log_to_file { 1 } else { 0 },
            log_truncation_at: self.log_truncation_at,
            max_memory_usage: self.max_memory_usage,
            unified_memtable: if self.unified_memtable { 1 } else { 0 },
            unified_memtable_write_buffer_size: self.unified_memtable_write_buffer_size,
            unified_memtable_skip_list_max_level: self.unified_memtable_skip_list_max_level,
            unified_memtable_skip_list_probability: self.unified_memtable_skip_list_probability,
            unified_memtable_sync_mode: self.unified_memtable_sync_mode as i32,
            unified_memtable_sync_interval_us: self.unified_memtable_sync_interval_us,
            object_store: objstore_ptr,
            object_store_config: os_config_ptr,
            #[cfg(tidesdb_has_max_concurrent_flushes)]
            max_concurrent_flushes: self.max_concurrent_flushes,
        };

        Ok(CConfigData {
            config,
            _db_path: c_path,
            _objstore_config: boxed_os_config,
            _cache_path: cache_path_cstr,
        })
    }
}

impl Default for Config {
    fn default() -> Self {
        // Pull defaults from the C library so the binding tracks engine
        // defaults automatically. `db_path` is left empty; callers must set it
        // before opening (`Config::new` and the `db_path` builder do this).
        let c = unsafe { ffi::tidesdb_default_config() };
        Config {
            db_path: String::new(),
            num_flush_threads: c.num_flush_threads,
            num_compaction_threads: c.num_compaction_threads,
            log_level: match c.log_level {
                ffi::TDB_LOG_DEBUG => LogLevel::Debug,
                ffi::TDB_LOG_WARN => LogLevel::Warn,
                ffi::TDB_LOG_ERROR => LogLevel::Error,
                ffi::TDB_LOG_FATAL => LogLevel::Fatal,
                ffi::TDB_LOG_NONE => LogLevel::None,
                _ => LogLevel::Info,
            },
            block_cache_size: c.block_cache_size,
            max_open_sstables: c.max_open_sstables,
            max_memory_usage: c.max_memory_usage,
            log_to_file: c.log_to_file != 0,
            log_truncation_at: c.log_truncation_at,
            unified_memtable: c.unified_memtable != 0,
            unified_memtable_write_buffer_size: c.unified_memtable_write_buffer_size,
            unified_memtable_skip_list_max_level: c.unified_memtable_skip_list_max_level,
            unified_memtable_skip_list_probability: c.unified_memtable_skip_list_probability,
            unified_memtable_sync_mode: match c.unified_memtable_sync_mode {
                ffi::TDB_SYNC_FULL => SyncMode::Full,
                ffi::TDB_SYNC_INTERVAL => SyncMode::Interval,
                _ => SyncMode::None,
            },
            unified_memtable_sync_interval_us: c.unified_memtable_sync_interval_us,
            object_store_fs_path: None,
            #[cfg(feature = "objectstore")]
            object_store_s3: None,
            object_store_config: None,
            max_concurrent_flushes: {
                #[cfg(tidesdb_has_max_concurrent_flushes)]
                {
                    c.max_concurrent_flushes
                }
                #[cfg(not(tidesdb_has_max_concurrent_flushes))]
                {
                    0
                }
            },
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
    /// Per-SSTable tombstone density (`tombstone_count / num_entries`) above which compaction
    /// priority escalates. Range `[0.0, 1.0]`; `0.0` disables the check.
    pub tombstone_density_trigger: f64,
    /// SSTables with fewer entries than this are ignored by the density trigger
    /// (prevents tiny-sstable noise). `0` falls back to the library default.
    pub tombstone_density_min_entries: u64,
    /// Use B+tree format for klog (default: false = block-based)
    pub use_btree: bool,
    /// Compact less aggressively in object store mode (default: false)
    pub object_lazy_compaction: bool,
    /// Download all inputs before merge in object store mode (default: true)
    pub object_prefetch_compaction: bool,
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

    /// Set the per-SSTable tombstone density above which compaction priority escalates.
    /// Range `[0.0, 1.0]`; `0.0` disables the check.
    pub fn tombstone_density_trigger(mut self, ratio: f64) -> Self {
        self.tombstone_density_trigger = ratio;
        self
    }

    /// Set the minimum entry count for an SSTable to be considered by the tombstone
    /// density trigger. SSTables with fewer entries are skipped.
    pub fn tombstone_density_min_entries(mut self, n: u64) -> Self {
        self.tombstone_density_min_entries = n;
        self
    }

    /// Enable or disable B+tree format for klog.
    /// When enabled, uses B+tree structure instead of block-based format.
    pub fn use_btree(mut self, enable: bool) -> Self {
        self.use_btree = enable;
        self
    }

    /// Enable or disable lazy compaction in object store mode.
    pub fn object_lazy_compaction(mut self, enable: bool) -> Self {
        self.object_lazy_compaction = enable;
        self
    }

    /// Enable or disable prefetch compaction in object store mode.
    pub fn object_prefetch_compaction(mut self, enable: bool) -> Self {
        self.object_prefetch_compaction = enable;
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

    /// Create a ColumnFamilyConfig by reading from a C config struct pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a valid `tidesdb_column_family_config_t`. This is
    /// crate-internal; callers (e.g., the stats readback) are expected to
    /// validate the pointer before invoking.
    pub(crate) fn from_c_config_ptr(ptr: *const ffi::tidesdb_column_family_config_t) -> Self {
        let c_config = unsafe { &*ptr };
        Self::from_c_config(c_config)
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
            tombstone_density_trigger: {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    c_config.tombstone_density_trigger
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0.0
                }
            },
            tombstone_density_min_entries: {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    c_config.tombstone_density_min_entries
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0
                }
            },
            use_btree: c_config.use_btree != 0,
            object_lazy_compaction: c_config.object_lazy_compaction != 0,
            object_prefetch_compaction: c_config.object_prefetch_compaction != 0,
        }
    }

    /// Convert to C configuration struct.
    pub(crate) fn to_c_config(&self) -> ffi::tidesdb_column_family_config_t {
        let mut config = ffi::tidesdb_column_family_config_t {
            name: [0; ffi::TDB_MAX_CF_NAME_LEN],
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
            #[cfg(tidesdb_has_tombstone_stats)]
            tombstone_density_trigger: self.tombstone_density_trigger,
            #[cfg(tidesdb_has_tombstone_stats)]
            tombstone_density_min_entries: self.tombstone_density_min_entries,
            use_btree: if self.use_btree { 1 } else { 0 },
            commit_hook_fn: None,
            commit_hook_ctx: std::ptr::null_mut(),
            object_target_file_size: 0,
            object_lazy_compaction: if self.object_lazy_compaction { 1 } else { 0 },
            object_prefetch_compaction: if self.object_prefetch_compaction {
                1
            } else {
                0
            },
        };

        // Copy comparator name
        if !self.comparator_name.is_empty() {
            let bytes = self.comparator_name.as_bytes();
            let len = bytes.len().min(ffi::TDB_MAX_COMPARATOR_NAME - 1);
            for (i, &b) in bytes[..len].iter().enumerate() {
                config.comparator_name[i] = b as libc::c_char;
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
            tombstone_density_trigger: {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    c_config.tombstone_density_trigger
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0.0
                }
            },
            tombstone_density_min_entries: {
                #[cfg(tidesdb_has_tombstone_stats)]
                {
                    c_config.tombstone_density_min_entries
                }
                #[cfg(not(tidesdb_has_tombstone_stats))]
                {
                    0
                }
            },
            use_btree: c_config.use_btree != 0,
            object_lazy_compaction: c_config.object_lazy_compaction != 0,
            object_prefetch_compaction: c_config.object_prefetch_compaction != 0,
        }
    }
}
