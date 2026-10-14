// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Main database types and operations.

use crate::config::{ColumnFamilyConfig, Config, IsolationLevel};
use crate::error::{Error, Result, check_result};
use crate::ffi;
use crate::stats::{CacheStats, CfStats, DbStats, EncodingStats, IoStats, RangeStats, StallStats};
use crate::transaction::Transaction;
use libc::{c_char, c_int, c_void};
use std::ffi::{CStr, CString};
use std::mem::MaybeUninit;
use std::path::Path;
use std::ptr;
use std::sync::Mutex;

/// A single operation from a committed transaction batch.
///
/// Passed to commit hook callbacks. Contains owned copies of the key and value data.
///
/// | Operation | `key` | `value` | `range_end` | `is_delete` |
/// |-----------|-------|---------|-------------|-------------|
/// | `put` / `put_with_ttl` | the key | `Some(value)` | `None` | `false` |
/// | `delete` / `single_delete` | the key | `None` | `None` | `true` |
/// | `delete_range(lo, Some(hi))` | `lo` | `None` | `Some(hi)` | `true` |
/// | `delete_prefix(p)` | `p` | `None` | `Some(first key past p)` | `true` |
/// | `delete_range(lo, None)` | `lo` | `None` | `None` | `true` |
///
/// A range delete with no upper bound (and a prefix delete of a prefix made only of `0xFF`
/// bytes, which has no key past it) arrives exactly like a point delete of its lower bound:
/// the C library passes nothing that tells them apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOp {
    /// The key, or the inclusive lower bound of a range delete
    pub key: Vec<u8>,
    /// The value (`None` for delete operations)
    pub value: Option<Vec<u8>>,
    /// The exclusive upper bound of a range or prefix delete: every key in
    /// `[key, range_end)` was deleted. `None` for every other operation.
    pub range_end: Option<Vec<u8>>,
    /// Absolute expiry as a Unix timestamp in seconds, `None` if the entry never expires.
    /// This is the deadline the engine stored, not the lifetime passed to `put_with_ttl`.
    pub expires_at: Option<i64>,
    /// Whether this is a delete operation (point, range or prefix)
    pub is_delete: bool,
}

/// The boxed commit hook callback. Commits on different threads may invoke the same
/// hook concurrently, hence `Sync`.
type CommitHookCallback = Box<dyn Fn(&[CommitOp], u64) -> i32 + Send + Sync>;

/// Trampoline that bridges the C commit hook to the Rust closure.
unsafe extern "C" fn commit_hook_trampoline(
    ops: *const ffi::tidesdb_commit_op_t,
    num_ops: c_int,
    commit_seq: u64,
    ctx: *mut c_void,
) -> c_int {
    if ctx.is_null() {
        return -1;
    }
    let callback = unsafe { &*(ctx as *const CommitHookCallback) };

    let count = if ops.is_null() {
        0
    } else {
        num_ops.max(0) as usize
    };
    let mut rust_ops = Vec::with_capacity(count);
    for i in 0..count {
        let op = unsafe { &*ops.add(i) };
        let key = unsafe { bytes_from_raw(op.key, op.key_size) };
        let is_delete = op.is_delete != 0;
        let bytes = unsafe { bytes_from_raw(op.value, op.value_size) };
        // a range delete carries its upper bound in the value; a point delete has none
        let (value, range_end) = match (is_delete, bytes.is_empty()) {
            (false, _) => (Some(bytes), None),
            (true, true) => (None, None),
            (true, false) => (None, Some(bytes)),
        };
        rust_ops.push(CommitOp {
            key,
            value,
            range_end,
            expires_at: ttl_from_c(op.ttl),
            is_delete,
        });
    }

    // A panic must not unwind into C; report it as a hook failure instead.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        callback(&rust_ops, commit_seq)
    }))
    .unwrap_or(-1)
}

/// Copy `len` bytes from a C pointer, treating NULL or zero length as empty.
pub(crate) unsafe fn bytes_from_raw(ptr: *const u8, len: usize) -> Vec<u8> {
    if ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()
    }
}

/// Copy a buffer the library allocated for the caller, then free it.
pub(crate) unsafe fn take_c_buffer(ptr: *mut u8, len: usize) -> Vec<u8> {
    let out = unsafe { bytes_from_raw(ptr, len) };
    if !ptr.is_null() {
        unsafe { ffi::tidesdb_free(ptr as *mut c_void) };
    }
    out
}

/// Convert a C absolute expiry (`-1` = never) to an `Option`.
// time_t is not i64 on every target, so the conversion is not always a no-op
#[allow(clippy::useless_conversion)]
pub(crate) fn ttl_from_c(ttl: libc::time_t) -> Option<i64> {
    if ttl < 0 { None } else { Some(i64::from(ttl)) }
}

/// Raises the process open-file ceiling toward `desired` descriptors.
///
/// TidesDB reads the ceiling at open and lowers `max_open_sstables` to fit it, so call
/// this before [`TidesDB::open`] when a larger budget is wanted. Passing `0` or a negative
/// value only reports the current ceiling. A failed or partial raise is not an error.
///
/// Returns the open-file ceiling in effect after the attempt.
pub fn raise_open_file_limit(desired: i64) -> i64 {
    unsafe { ffi::tidesdb_raise_open_file_limit(desired as libc::c_long) as i64 }
}

/// A TidesDB database instance.
///
/// The database is closed when dropped. Every [`ColumnFamily`], [`Transaction`],
/// [`Snapshot`] and [`Iterator`](crate::Iterator) borrows the database, so the borrow
/// checker guarantees they are all gone before it closes.
///
/// # Example
///
/// ```no_run
/// use tidesdb::{TidesDB, Config, ColumnFamilyConfig};
///
/// let db = TidesDB::open(Config::new("./mydb"))?;
/// db.create_column_family("my_cf", ColumnFamilyConfig::default())?;
/// let cf = db.get_column_family("my_cf")?;
///
/// let mut txn = db.begin_transaction()?;
/// txn.put(&cf, b"key", b"value")?;
/// txn.commit()?;
/// # Ok::<(), tidesdb::Error>(())
/// ```
pub struct TidesDB {
    pub(crate) db: *mut ffi::tidesdb_t,
    /// Every commit hook ever installed. The C library swaps a hook's function and context
    /// as two separate atomic stores, so a committer may still be running a hook just after
    /// it was replaced; each callback is therefore kept alive until the database closes.
    // boxed so each callback keeps its address when the Vec grows; C holds that address
    #[allow(clippy::vec_box)]
    hooks: Mutex<Vec<Box<CommitHookCallback>>>,
}

// The C library synchronises every database-level call internally.
unsafe impl Send for TidesDB {}
unsafe impl Sync for TidesDB {}

impl TidesDB {
    /// Opens or creates a database with the given configuration.
    ///
    /// Returns [`ErrorCode::Locked`](crate::ErrorCode::Locked) if another handle, in this
    /// process or another, already has the directory open.
    pub fn open(config: Config) -> Result<Self> {
        let c_data = config.to_c_config()?;
        let mut db: *mut ffi::tidesdb_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_open(&c_data.config, &mut db) };
        check_result(result, "failed to open database")?;

        if db.is_null() {
            return Err(Error::NullPointer("database handle"));
        }

        Ok(TidesDB {
            db,
            hooks: Mutex::new(Vec::new()),
        })
    }

    // ===== column families =====

    /// Creates a new column family with the given configuration.
    pub fn create_column_family(&self, name: &str, config: ColumnFamilyConfig) -> Result<()> {
        let c_name = CString::new(name)?;
        let c_config = config.to_c_config()?;

        let result =
            unsafe { ffi::tidesdb_create_column_family(self.db, c_name.as_ptr(), &c_config) };
        check_result(result, "failed to create column family")
    }

    /// Drops a column family and deletes all of its data. This cannot be undone.
    ///
    /// Takes `&mut self` because dropping invalidates every handle to the family; the
    /// borrow checker proves none are alive. To drop a family on a database shared across
    /// threads, see [`drop_column_family_unchecked`](Self::drop_column_family_unchecked).
    pub fn drop_column_family(&mut self, name: &str) -> Result<()> {
        unsafe { self.drop_column_family_unchecked(name) }
    }

    /// Drops a column family through a shared reference.
    ///
    /// # Safety
    ///
    /// No [`ColumnFamily`] handle for `name`, and no transaction or iterator that has
    /// used one, may be used after this call returns.
    pub unsafe fn drop_column_family_unchecked(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_drop_column_family(self.db, c_name.as_ptr()) };
        check_result(result, "failed to drop column family")
    }

    /// Renames a column family in place.
    ///
    /// Existing handles stay valid and keep referring to the family, but
    /// [`ColumnFamily::name`] keeps reporting the name the handle was looked up by.
    pub fn rename_column_family(&self, old_name: &str, new_name: &str) -> Result<()> {
        let c_old_name = CString::new(old_name)?;
        let c_new_name = CString::new(new_name)?;

        let result = unsafe {
            ffi::tidesdb_rename_column_family(self.db, c_old_name.as_ptr(), c_new_name.as_ptr())
        };
        check_result(result, "failed to rename column family")
    }

    /// Creates a point-in-time copy of a column family under a new name.
    ///
    /// Everything written before the call is included; later writes to the source are not.
    pub fn clone_column_family(&self, source_name: &str, dest_name: &str) -> Result<()> {
        let c_source_name = CString::new(source_name)?;
        let c_dest_name = CString::new(dest_name)?;

        let result = unsafe {
            ffi::tidesdb_clone_column_family(self.db, c_source_name.as_ptr(), c_dest_name.as_ptr())
        };
        check_result(result, "failed to clone column family")
    }

    /// Retrieves a column family by name.
    pub fn get_column_family(&self, name: &str) -> Result<ColumnFamily<'_>> {
        let c_name = CString::new(name)?;

        let cf = unsafe { ffi::tidesdb_get_column_family(self.db, c_name.as_ptr()) };
        if cf.is_null() {
            return Err(Error::from_code(
                ffi::TDB_ERR_NOT_FOUND,
                "column family not found",
            ));
        }

        Ok(ColumnFamily {
            db: self,
            cf,
            name: name.to_string(),
        })
    }

    /// Lists the names of all column families.
    pub fn list_column_families(&self) -> Result<Vec<String>> {
        let mut names: *mut *mut c_char = ptr::null_mut();
        let mut count: c_int = 0;

        let result = unsafe { ffi::tidesdb_list_column_families(self.db, &mut names, &mut count) };
        check_result(result, "failed to list column families")?;

        if count <= 0 || names.is_null() {
            return Ok(Vec::new());
        }

        let mut result_names = Vec::with_capacity(count as usize);
        unsafe {
            for i in 0..count as usize {
                let name_ptr = *names.add(i);
                if !name_ptr.is_null() {
                    result_names.push(CStr::from_ptr(name_ptr).to_string_lossy().into_owned());
                    ffi::tidesdb_free(name_ptr as *mut c_void);
                }
            }
            ffi::tidesdb_free(names as *mut c_void);
        }

        Ok(result_names)
    }

    // ===== transactions =====

    fn wrap_txn(&self, result: c_int, txn: *mut ffi::tidesdb_txn_t) -> Result<Transaction<'_>> {
        check_result(result, "failed to begin transaction")?;
        if txn.is_null() {
            return Err(Error::NullPointer("transaction handle"));
        }
        Ok(unsafe { Transaction::from_raw(self.db, txn) })
    }

    /// Begins a transaction at the database default isolation level (read committed).
    pub fn begin_transaction(&self) -> Result<Transaction<'_>> {
        let mut txn = ptr::null_mut();
        let result = unsafe { ffi::tidesdb_txn_begin(self.db, &mut txn) };
        self.wrap_txn(result, txn)
    }

    /// Begins a transaction at an explicit isolation level.
    pub fn begin_transaction_with_isolation(
        &self,
        isolation: IsolationLevel,
    ) -> Result<Transaction<'_>> {
        let mut txn = ptr::null_mut();
        let result =
            unsafe { ffi::tidesdb_txn_begin_with_isolation(self.db, isolation as i32, &mut txn) };
        self.wrap_txn(result, txn)
    }

    /// Begins a transaction at the column family's configured default isolation level.
    pub fn begin_transaction_cf(&self, cf: &ColumnFamily<'_>) -> Result<Transaction<'_>> {
        self.check_cf(cf)?;
        let mut txn = ptr::null_mut();
        let result = unsafe { ffi::tidesdb_txn_begin_cf(self.db, cf.cf, &mut txn) };
        self.wrap_txn(result, txn)
    }

    /// Takes a snapshot of the database as it stands now.
    ///
    /// The snapshot holds back compaction of the versions it can see for as long as it
    /// lives, which has the same cost as a long-running transaction; drop it as soon as
    /// the point in time is no longer needed.
    pub fn snapshot(&self) -> Result<Snapshot<'_>> {
        let mut snap = ptr::null_mut();
        let result = unsafe { ffi::tidesdb_snapshot_create(self.db, &mut snap) };
        check_result(result, "failed to create snapshot")?;
        if snap.is_null() {
            return Err(Error::NullPointer("snapshot handle"));
        }
        Ok(Snapshot { db: self, snap })
    }

    /// Begins a repeatable-read transaction that sees the database as of `snapshot`.
    ///
    /// The transaction is not read-only: writes are accepted, and its commit validates its
    /// reads against that older point in time. It borrows the snapshot, which must outlive it.
    pub fn begin_transaction_at_snapshot<'s>(
        &'s self,
        snapshot: &'s Snapshot<'s>,
    ) -> Result<Transaction<'s>> {
        if !ptr::eq(snapshot.db, self) {
            return Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "snapshot belongs to a different database",
            ));
        }
        let mut txn = ptr::null_mut();
        let result =
            unsafe { ffi::tidesdb_txn_begin_at_snapshot(self.db, snapshot.snap, &mut txn) };
        self.wrap_txn(result, txn)
    }

    /// Begins a repeatable-read transaction that reads as of an explicit sequence number,
    /// such as one read back from [`Transaction::read_snapshot`]. Like
    /// [`begin_transaction_at_snapshot`](Self::begin_transaction_at_snapshot), it accepts writes.
    ///
    /// Returns [`ErrorCode::TooOld`](crate::ErrorCode::TooOld) when compaction has already
    /// discarded the versions that sequence resolves to. A sequence stays readable only
    /// while an open transaction or a [`Snapshot`] holds it. A sequence above the current one
    /// is treated as the current one.
    pub fn begin_transaction_at_seq(&self, seq: u64) -> Result<Transaction<'_>> {
        let mut txn = ptr::null_mut();
        let result = unsafe { ffi::tidesdb_txn_begin_at_seq(self.db, seq, &mut txn) };
        self.wrap_txn(result, txn)
    }

    /// The oldest sequence [`begin_transaction_at_seq`](Self::begin_transaction_at_seq)
    /// still accepts. It only rises while the database is open; it is not persisted and
    /// starts again from 0 after a reopen.
    pub fn oldest_readable_seq(&self) -> u64 {
        unsafe { ffi::tidesdb_oldest_readable_seq(self.db) }
    }

    /// Lists transactions that were prepared (two-phase commit) before the last shutdown
    /// and never committed or rolled back.
    ///
    /// Each returned transaction is in the prepared state; resolve it with
    /// [`Transaction::commit_prepared`] or [`Transaction::rollback_prepared`].
    ///
    /// The set is fixed when the database opens: calling this again before reopening lists
    /// the same transactions, including ones already resolved. Resolve each entry once —
    /// committing a stale entry applies its batch again, over any newer writes.
    pub fn recover_prepared(&self) -> Result<Vec<PreparedTransaction<'_>>> {
        let mut count: c_int = 0;
        let result =
            unsafe { ffi::tidesdb_recover_prepared(self.db, ptr::null_mut(), 0, &mut count) };
        check_result(result, "failed to count prepared transactions")?;
        if count <= 0 {
            return Ok(Vec::new());
        }

        // The set is fixed when the database opens, so the count cannot change between calls.
        let mut out: Vec<ffi::tidesdb_prepared_txn_t> = Vec::with_capacity(count as usize);
        let mut filled: c_int = 0;
        let result =
            unsafe { ffi::tidesdb_recover_prepared(self.db, out.as_mut_ptr(), count, &mut filled) };
        check_result(result, "failed to recover prepared transactions")?;
        unsafe { out.set_len(filled.clamp(0, count) as usize) };

        Ok(out
            .into_iter()
            .filter(|p| !p.txn.is_null())
            .map(|p| PreparedTransaction {
                // the xid is borrowed from the database; copy it out
                xid: unsafe { bytes_from_raw(p.xid, p.xid_size) },
                txn: unsafe { Transaction::from_raw(self.db, p.txn) },
            })
            .collect())
    }

    // ===== maintenance =====

    /// Rotates the memtable and flushes it to sstables, waiting up to 60 seconds for the
    /// immutable queue to drain ([`ErrorCode::Locked`](crate::ErrorCode::Locked) if it does
    /// not). The memtable is shared, so this flushes every column family.
    pub fn flush_memtable(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_flush_memtable(self.db) };
        check_result(result, "failed to flush memtable")
    }

    /// Whether the memtable is currently flushing or rotating.
    pub fn is_flushing(&self) -> bool {
        unsafe { ffi::tidesdb_is_flushing(self.db) != 0 }
    }

    /// Forces an fsync of the write-ahead log.
    ///
    /// Useful for explicit durability control under [`SyncMode::None`](crate::SyncMode::None)
    /// or [`SyncMode::Interval`](crate::SyncMode::Interval).
    pub fn sync_wal(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_sync_wal(self.db) };
        check_result(result, "failed to sync WAL")
    }

    /// Writes a consistent, directly openable copy of the database into `dir`, which must
    /// be absent or empty (a non-empty directory fails with
    /// [`ErrorCode::Io`](crate::ErrorCode::Io)).
    pub fn backup<P: AsRef<Path>>(&self, dir: P) -> Result<()> {
        let c_dir = CString::new(dir.as_ref().to_string_lossy().as_bytes())?;
        let result = unsafe { ffi::tidesdb_backup(self.db, c_dir.as_ptr()) };
        check_result(result, "failed to backup database")
    }

    /// Establishes a durability barrier: flushes the memtable to L1, then forces the value
    /// log, write-ahead log and manifest to disk regardless of the configured sync mode.
    pub fn checkpoint(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_checkpoint(self.db) };
        check_result(result, "failed to checkpoint database")
    }

    // ===== statistics =====

    /// Database-level statistics.
    pub fn get_db_stats(&self) -> Result<DbStats> {
        let mut c_stats = MaybeUninit::<ffi::tidesdb_db_stats_t>::zeroed();
        let result = unsafe { ffi::tidesdb_get_db_stats(self.db, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get database stats")?;
        Ok(DbStats::from_c(unsafe { c_stats.assume_init_ref() }))
    }

    /// Block cache statistics.
    pub fn get_cache_stats(&self) -> Result<CacheStats> {
        let mut c_stats = MaybeUninit::<ffi::tidesdb_cache_stats_t>::zeroed();
        let result = unsafe { ffi::tidesdb_get_cache_stats(self.db, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get cache stats")?;
        let c = unsafe { c_stats.assume_init() };
        Ok(CacheStats {
            enabled: c.enabled != 0,
            total_entries: c.total_entries,
            total_bytes: c.total_bytes,
            hits: c.hits,
            misses: c.misses,
            hit_rate: c.hit_rate,
            num_partitions: c.num_partitions,
        })
    }

    /// Where writers have been made to wait since the database opened.
    pub fn get_stall_stats(&self) -> Result<StallStats> {
        let mut c_stats = MaybeUninit::<ffi::tidesdb_stall_stats_t>::zeroed();
        let result = unsafe { ffi::tidesdb_get_stall_stats(self.db, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get stall stats")?;
        Ok(StallStats::from_c(unsafe { c_stats.assume_init_ref() }))
    }

    /// What each class of file asked of the device since the database opened.
    pub fn get_io_stats(&self) -> Result<IoStats> {
        let mut c_stats = MaybeUninit::<ffi::tidesdb_io_stats_t>::zeroed();
        let result = unsafe { ffi::tidesdb_get_io_stats(self.db, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get I/O stats")?;
        Ok(IoStats::from_c(unsafe { c_stats.assume_init_ref() }))
    }

    /// What each encoding chain achieved on the key logs, one entry per chain the live
    /// sstables were written with.
    pub fn get_klog_encoding_stats(&self) -> Result<Vec<EncodingStats>> {
        self.encoding_stats(ffi::tidesdb_get_klog_encoding_stats)
    }

    /// What each encoding chain achieved on the separated values in the value log.
    pub fn get_vlog_encoding_stats(&self) -> Result<Vec<EncodingStats>> {
        self.encoding_stats(ffi::tidesdb_get_vlog_encoding_stats)
    }

    fn encoding_stats(
        &self,
        f: unsafe extern "C" fn(
            *mut ffi::tidesdb_t,
            *mut ffi::tidesdb_encoding_stats_t,
            usize,
            *mut usize,
        ) -> c_int,
    ) -> Result<Vec<EncodingStats>> {
        let mut out =
            [MaybeUninit::<ffi::tidesdb_encoding_stats_t>::zeroed(); ffi::TDB_MAX_ENCODING_CHAINS];
        let mut count: usize = 0;
        let result = unsafe {
            f(
                self.db,
                out.as_mut_ptr() as *mut ffi::tidesdb_encoding_stats_t,
                out.len(),
                &mut count,
            )
        };
        check_result(result, "failed to get encoding stats")?;
        Ok(out[..count.min(out.len())]
            .iter()
            .map(|e| EncodingStats::from_c(unsafe { e.assume_init_ref() }))
            .collect())
    }

    fn check_cf(&self, cf: &ColumnFamily<'_>) -> Result<()> {
        if ptr::eq(cf.db, self) {
            Ok(())
        } else {
            Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "column family belongs to a different database",
            ))
        }
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
        // The hooks are freed by the Mutex's own drop, after close has joined every
        // engine thread, so no commit can still be running one.
    }
}

/// A column family: an isolated key space with its own configuration.
///
/// Handles are cheap to clone and borrow the [`TidesDB`] they came from.
#[derive(Clone)]
pub struct ColumnFamily<'db> {
    pub(crate) db: &'db TidesDB,
    pub(crate) cf: *mut ffi::tidesdb_column_family_t,
    name: String,
}

// The C library synchronises column family operations internally.
unsafe impl Send for ColumnFamily<'_> {}
unsafe impl Sync for ColumnFamily<'_> {}

impl std::fmt::Debug for ColumnFamily<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamily")
            .field("name", &self.name)
            .finish()
    }
}

impl<'db> ColumnFamily<'db> {
    /// The name this handle was looked up by. After a rename it still reports the old name;
    /// [`CfStats::name`](crate::CfStats::name) from [`get_stats`](Self::get_stats) is the
    /// current one.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Statistics for this column family.
    pub fn get_stats(&self) -> Result<CfStats> {
        // boxed: the struct is over 600 bytes and filled in place
        let mut c_stats = Box::new(MaybeUninit::<ffi::tidesdb_cf_stats_t>::zeroed());
        let result = unsafe { ffi::tidesdb_get_cf_stats(self.cf, c_stats.as_mut_ptr()) };
        check_result(result, "failed to get column family stats")?;
        Ok(CfStats::from_c(unsafe { c_stats.assume_init_ref() }))
    }

    /// Estimates the number of distinct keys in this column family.
    pub fn estimate_cardinality(&self) -> Result<u64> {
        let mut estimate: u64 = 0;
        let result = unsafe { ffi::tidesdb_cf_estimate_cardinality(self.cf, &mut estimate) };
        check_result(result, "failed to estimate cardinality")?;
        Ok(estimate)
    }

    /// Describes the key range `[key_a, key_b)` for a query planner: how many sorted runs a
    /// scan would merge and how many live keys it holds.
    pub fn range_stats(&self, key_a: &[u8], key_b: &[u8]) -> Result<RangeStats> {
        let mut out = MaybeUninit::<ffi::tidesdb_range_stats_t>::zeroed();
        let result = unsafe {
            ffi::tidesdb_range_stats(
                self.db.db,
                self.cf,
                key_a.as_ptr(),
                key_a.len(),
                key_b.as_ptr(),
                key_b.len(),
                out.as_mut_ptr(),
            )
        };
        check_result(result, "failed to get range stats")?;
        let c = unsafe { out.assume_init() };
        Ok(RangeStats {
            sstables_overlapping: c.sstables_overlapping,
            estimated_keys: c.estimated_keys,
            keys_exact: c.keys_exact != 0,
        })
    }

    /// Synchronously runs one forced compaction pass, merging even when no trigger is due.
    ///
    /// Returns [`ErrorCode::Locked`](crate::ErrorCode::Locked) without waiting if the family
    /// is already compacting, being reconfigured or being renamed.
    pub fn compact(&self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_compact(self.db.db, self.cf) };
        check_result(result, "failed to compact column family")
    }

    /// Synchronously compacts every sstable overlapping `[start_key, end_key)`.
    ///
    /// `None` is unbounded on that side; passing `None` for both is rejected, use
    /// [`compact`](Self::compact) instead. Returns [`ErrorCode::Locked`](crate::ErrorCode::Locked)
    /// like `compact`, and does nothing when fewer than two sstables overlap the range.
    pub fn compact_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) -> Result<()> {
        let (start_ptr, start_len) = start_key.map_or((ptr::null(), 0), |s| (s.as_ptr(), s.len()));
        let (end_ptr, end_len) = end_key.map_or((ptr::null(), 0), |s| (s.as_ptr(), s.len()));

        let result = unsafe {
            ffi::tidesdb_compact_range(self.db.db, self.cf, start_ptr, start_len, end_ptr, end_len)
        };
        check_result(result, "failed to compact range")
    }

    /// Whether this column family has a compaction in progress.
    pub fn is_compacting(&self) -> bool {
        unsafe { ffi::tidesdb_is_compacting(self.cf) != 0 }
    }

    /// Applies a new configuration at runtime. The name is preserved.
    ///
    /// With `persist_to_disk` the new configuration is written to the manifest; otherwise
    /// it lasts until the database is reopened (a later rename persists it too). Returns
    /// [`ErrorCode::Locked`](crate::ErrorCode::Locked) without waiting if the family is
    /// compacting or under another reconfiguration.
    pub fn update_runtime_config(
        &self,
        config: &ColumnFamilyConfig,
        persist_to_disk: bool,
    ) -> Result<()> {
        let c_config = config.to_c_config()?;
        let result = unsafe {
            ffi::tidesdb_cf_update_runtime_config(
                self.db.db,
                self.cf,
                &c_config,
                persist_to_disk as c_int,
            )
        };
        check_result(result, "failed to update runtime config")
    }

    /// Installs a commit hook for this column family, replacing any existing one.
    ///
    /// The hook fires synchronously on the committing thread after every commit that
    /// touches this family, with the family's whole batch, once the commit is logged,
    /// applied and visible (not necessarily synced to disk, depending on the sync mode).
    /// Commits on different threads may run it concurrently, so calls can arrive out of
    /// `commit_seq` order. A range or prefix delete arrives as one delete op keyed by its
    /// lower bound. A non-zero return is logged by the engine but does not undo the commit;
    /// a panic is caught and counts as a failure.
    ///
    /// The hook belongs to the column family, not to this handle: it stays installed until
    /// cleared or the database closes. Replaced hooks are kept alive until close.
    pub fn set_commit_hook<F>(&self, callback: F) -> Result<()>
    where
        F: Fn(&[CommitOp], u64) -> i32 + Send + Sync + 'static,
    {
        let boxed: Box<CommitHookCallback> = Box::new(Box::new(callback));
        let ctx = &*boxed as *const CommitHookCallback as *mut c_void;

        // Hold the lock across the swap so the retained list never misses a live hook.
        let mut hooks = self.db.hooks.lock().unwrap_or_else(|e| e.into_inner());
        let result = unsafe {
            ffi::tidesdb_cf_set_commit_hook(self.db.db, self.cf, Some(commit_hook_trampoline), ctx)
        };
        check_result(result, "failed to set commit hook")?;
        hooks.push(boxed);
        Ok(())
    }

    /// Removes this column family's commit hook.
    pub fn clear_commit_hook(&self) -> Result<()> {
        let result =
            unsafe { ffi::tidesdb_cf_set_commit_hook(self.db.db, self.cf, None, ptr::null_mut()) };
        check_result(result, "failed to clear commit hook")
    }
}

/// A named point in time that transactions can read at.
///
/// Created by [`TidesDB::snapshot`] and released when dropped. While it lives, the versions
/// it can see are protected from compaction.
pub struct Snapshot<'db> {
    db: &'db TidesDB,
    snap: *mut ffi::tidesdb_snapshot_t,
}

// A snapshot is only read after creation; beginning transactions at it from several
// threads is safe.
unsafe impl Send for Snapshot<'_> {}
unsafe impl Sync for Snapshot<'_> {}

impl Snapshot<'_> {
    /// The sequence number this snapshot reads at.
    pub fn seq(&self) -> u64 {
        unsafe { ffi::tidesdb_snapshot_seq(self.snap) }
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        unsafe { ffi::tidesdb_snapshot_release(self.snap) };
    }
}

/// A transaction recovered by [`TidesDB::recover_prepared`].
pub struct PreparedTransaction<'db> {
    /// The transaction id it was prepared under
    pub xid: Vec<u8>,
    /// The transaction, in the prepared state
    pub txn: Transaction<'db>,
}
