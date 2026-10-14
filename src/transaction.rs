// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Transaction operations for TidesDB.

use crate::config::IsolationLevel;
use crate::db::{ColumnFamily, TidesDB, take_c_buffer, ttl_from_c};
use crate::error::{Error, Result, check_result};
use crate::ffi;
use crate::iterator::Iterator;
use std::cell::Cell;
use std::ffi::CString;
use std::marker::PhantomData;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

/// Lifecycle state of a transaction, see [`Transaction::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Buffering writes, not yet resolved
    Active,
    /// Durably prepared under an xid, awaiting commit or rollback
    Prepared,
    /// Committed and applied
    Committed,
    /// Rolled back, aborted or expired
    Aborted,
}

/// Owns the C transaction. Shared with [`AbortHandle`]s so that `tidesdb_txn_request_abort`
/// can never run against a freed transaction.
struct TxnHandle {
    txn: *mut ffi::tidesdb_txn_t,
}

// Only `tidesdb_txn_request_abort` is reached through a shared handle, and that is the one
// call the C library allows from a thread other than the owner. Every other call goes
// through `Transaction`, which is not `Sync`.
unsafe impl Send for TxnHandle {}
unsafe impl Sync for TxnHandle {}

impl Drop for TxnHandle {
    fn drop(&mut self) {
        // rolls back if still open
        unsafe { ffi::tidesdb_txn_free(self.txn) };
    }
}

/// A transaction.
///
/// All reads and writes go through a transaction. Writes are buffered until
/// [`commit`](Self::commit); dropping an uncommitted transaction rolls it back.
///
/// A transaction may be moved to another thread but not shared between threads. To abort
/// it from another thread, use an [`AbortHandle`].
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
pub struct Transaction<'db> {
    handle: Arc<TxnHandle>,
    db: *mut ffi::tidesdb_t,
    // borrows the database; Cell makes it !Sync while staying Send
    _marker: PhantomData<(&'db TidesDB, Cell<()>)>,
}

unsafe impl Send for Transaction<'_> {}

fn key_ptr(key: &[u8]) -> *const u8 {
    // a dangling-but-non-null pointer for an empty slice; sizes of zero are never read
    key.as_ptr()
}

impl<'db> Transaction<'db> {
    /// Wrap a C transaction.
    ///
    /// # Safety
    ///
    /// `txn` must be a live transaction of `db`, owned by the caller.
    pub(crate) unsafe fn from_raw(db: *mut ffi::tidesdb_t, txn: *mut ffi::tidesdb_txn_t) -> Self {
        Transaction {
            handle: Arc::new(TxnHandle { txn }),
            db,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn txn(&self) -> *mut ffi::tidesdb_txn_t {
        self.handle.txn
    }

    #[inline]
    fn cf_ptr(&self, cf: &ColumnFamily<'_>) -> Result<*mut ffi::tidesdb_column_family_t> {
        if cf.db.db == self.db {
            Ok(cf.cf)
        } else {
            Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "column family belongs to a different database",
            ))
        }
    }

    /// Writes a key-value pair that never expires.
    ///
    /// An empty value is allowed and is distinct from an absent key: reads return it as an
    /// empty `Vec`.
    pub fn put(&self, cf: &ColumnFamily<'_>, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_raw(cf, key, value, 0)
    }

    /// Writes a key-value pair that expires after `ttl`.
    ///
    /// The lifetime is rounded up to whole seconds and converted to an absolute deadline
    /// when the write is buffered. A zero `ttl` is rejected.
    pub fn put_with_ttl(
        &self,
        cf: &ColumnFamily<'_>,
        key: &[u8],
        value: &[u8],
        ttl: Duration,
    ) -> Result<()> {
        if ttl.is_zero() {
            return Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "ttl must be greater than zero",
            ));
        }
        let secs = ttl
            .as_secs()
            .saturating_add(u64::from(ttl.subsec_nanos() > 0));
        let secs = ffi::time_t::try_from(secs).unwrap_or(ffi::time_t::MAX);
        self.put_raw(cf, key, value, secs)
    }

    fn put_raw(
        &self,
        cf: &ColumnFamily<'_>,
        key: &[u8],
        value: &[u8],
        ttl: ffi::time_t,
    ) -> Result<()> {
        let result = unsafe {
            ffi::tidesdb_txn_put(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(key),
                key.len(),
                value.as_ptr(),
                value.len(),
                ttl,
            )
        };
        check_result(result, "failed to put key-value pair")
    }

    /// Reads a key, recording the read for conflict detection.
    ///
    /// Returns an error with [`ErrorCode::NotFound`](crate::ErrorCode::NotFound) when the
    /// key is absent, deleted or expired.
    pub fn get(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<Vec<u8>> {
        self.get_with(ffi::tidesdb_txn_get, cf, key, "failed to get value")
    }

    /// Reads a key without recording it for conflict detection, for existence probes such
    /// as uniqueness checks that should not add to the transaction's read set.
    pub fn get_notrack(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<Vec<u8>> {
        self.get_with(ffi::tidesdb_txn_get_notrack, cf, key, "failed to get value")
    }

    fn get_with(
        &self,
        f: unsafe extern "C" fn(
            *mut ffi::tidesdb_txn_t,
            *mut ffi::tidesdb_column_family_t,
            *const u8,
            usize,
            *mut *mut u8,
            *mut usize,
        ) -> libc::c_int,
        cf: &ColumnFamily<'_>,
        key: &[u8],
        context: &'static str,
    ) -> Result<Vec<u8>> {
        let mut value_ptr: *mut u8 = ptr::null_mut();
        let mut value_len: usize = 0;
        let result = unsafe {
            f(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(key),
                key.len(),
                &mut value_ptr,
                &mut value_len,
            )
        };
        check_result(result, context)?;
        Ok(unsafe { take_c_buffer(value_ptr, value_len) })
    }

    /// Reads a key's expiry, as an absolute Unix timestamp in seconds. `Ok(None)` means the
    /// key exists and never expires.
    pub fn get_expiry(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<Option<i64>> {
        let mut ttl: ffi::time_t = -1;
        let result = unsafe {
            ffi::tidesdb_txn_get_ttl(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(key),
                key.len(),
                &mut ttl,
            )
        };
        check_result(result, "failed to get ttl")?;
        Ok(ttl_from_c(ttl))
    }

    /// Whether a key exists, without recording the read for conflict detection.
    pub fn contains(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<bool> {
        let result = unsafe {
            ffi::tidesdb_txn_contains(self.txn(), self.cf_ptr(cf)?, key_ptr(key), key.len())
        };
        match result {
            ffi::TDB_SUCCESS => Ok(true),
            ffi::TDB_ERR_NOT_FOUND => Ok(false),
            code => Err(Error::from_code(code, "failed to check key")),
        }
    }

    /// Deletes a key.
    pub fn delete(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<()> {
        let result = unsafe {
            ffi::tidesdb_txn_delete(self.txn(), self.cf_ptr(cf)?, key_ptr(key), key.len())
        };
        check_result(result, "failed to delete key")
    }

    /// Writes a single-delete tombstone for the key.
    ///
    /// Reads behave as for [`delete`](Self::delete), but the caller promises the key was
    /// put **at most once** since its previous single-delete (or since it was created), so
    /// compaction can drop the put and the tombstone together. The engine does not verify
    /// this; breaking the contract can leave older puts visible. When in doubt, use
    /// [`delete`](Self::delete).
    pub fn single_delete(&self, cf: &ColumnFamily<'_>, key: &[u8]) -> Result<()> {
        let result = unsafe {
            ffi::tidesdb_txn_single_delete(self.txn(), self.cf_ptr(cf)?, key_ptr(key), key.len())
        };
        check_result(result, "failed to single-delete key")
    }

    /// Deletes every key in `[lo, hi)`, or from `lo` to the end of the family when `hi` is
    /// `None`.
    ///
    /// It costs one entry however many keys it covers, and it covers keys written before
    /// it, not merely the keys present when it is called; a newer write inside the range
    /// survives it. `lo` must be non-empty, `hi` (when given) greater than `lo`, and each
    /// bound at most 256 bytes; otherwise [`ErrorCode::InvalidArgs`](crate::ErrorCode::InvalidArgs).
    pub fn delete_range(&self, cf: &ColumnFamily<'_>, lo: &[u8], hi: Option<&[u8]>) -> Result<()> {
        // the C library reads an empty upper bound as "no upper bound", which would turn an
        // empty interval into a delete of everything from `lo` on
        if hi.is_some_and(|h| h.is_empty()) {
            return Err(Error::from_code(
                ffi::TDB_ERR_INVALID_ARGS,
                "range delete upper bound must be non-empty; use None for no upper bound",
            ));
        }
        let (hi_ptr, hi_len) = hi.map_or((ptr::null(), 0), |h| (h.as_ptr(), h.len()));
        let result = unsafe {
            ffi::tidesdb_txn_delete_range(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(lo),
                lo.len(),
                hi_ptr,
                hi_len,
            )
        };
        check_result(result, "failed to delete range")
    }

    /// Deletes every key that begins with `prefix` (non-empty, at most 256 bytes).
    ///
    /// Same semantics as [`delete_range`](Self::delete_range).
    pub fn delete_prefix(&self, cf: &ColumnFamily<'_>, prefix: &[u8]) -> Result<()> {
        let result = unsafe {
            ffi::tidesdb_txn_delete_prefix(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(prefix),
                prefix.len(),
            )
        };
        check_result(result, "failed to delete prefix")
    }

    /// Commits the transaction.
    ///
    /// Returns [`ErrorCode::Conflict`](crate::ErrorCode::Conflict) when another transaction
    /// committed a conflicting write first.
    pub fn commit(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_commit(self.txn()) };
        check_result(result, "failed to commit transaction")
    }

    /// Discards the transaction's buffered writes.
    pub fn rollback(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_rollback(self.txn()) };
        check_result(result, "failed to rollback transaction")
    }

    /// Discards the transaction's state, committed or not, and begins it again at a new
    /// isolation level and a fresh snapshot. The database's default timeout is restored and
    /// any pending abort request is cleared.
    pub fn reset(&mut self, isolation: IsolationLevel) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_reset(self.txn(), isolation as i32) };
        check_result(result, "failed to reset transaction")
    }

    /// Bounds how long this transaction may stay active, overriding the database's
    /// `txn_timeout_seconds`. Measured from this call; `None` clears the timeout.
    ///
    /// The transaction is not aborted in the background: its next operation notices and
    /// fails with [`ErrorCode::TxnExpired`](crate::ErrorCode::TxnExpired). Deadlines are
    /// checked against the engine's one-second clock, so expiry can land up to about two
    /// seconds after the nominal deadline.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        let seconds = timeout.map_or(0, |t| {
            let secs = t.as_secs().saturating_add(u64::from(t.subsec_nanos() > 0));
            i64::try_from(secs).unwrap_or(i64::MAX).max(1)
        });
        let result = unsafe { ffi::tidesdb_txn_set_timeout(self.txn(), seconds) };
        check_result(result, "failed to set transaction timeout")
    }

    /// The sequence ceiling this transaction's reads see: `u64::MAX` for read uncommitted,
    /// the current sequence for read committed, and the sequence frozen at begin for
    /// repeatable read and stronger.
    pub fn read_snapshot(&self) -> u64 {
        unsafe { ffi::tidesdb_txn_read_snapshot(self.txn()) }
    }

    /// The transaction's lifecycle state.
    pub fn state(&self) -> Result<TxnState> {
        let mut state: libc::c_int = 0;
        let result = unsafe { ffi::tidesdb_txn_state(self.txn(), &mut state) };
        check_result(result, "failed to get transaction state")?;
        Ok(match state {
            ffi::TDB_TXN_STATE_ACTIVE => TxnState::Active,
            ffi::TDB_TXN_STATE_PREPARED => TxnState::Prepared,
            ffi::TDB_TXN_STATE_COMMITTED => TxnState::Committed,
            _ => TxnState::Aborted,
        })
    }

    /// Returns a handle that can abort this transaction from another thread.
    pub fn abort_handle(&self) -> AbortHandle<'db> {
        AbortHandle {
            handle: Arc::clone(&self.handle),
            _marker: PhantomData,
        }
    }

    // ===== two-phase commit =====

    /// Two-phase commit, phase one: runs the commit's conflict checks and durably logs the
    /// write batch under `xid`, leaving the writes invisible until
    /// [`commit_prepared`](Self::commit_prepared) or [`rollback_prepared`](Self::rollback_prepared).
    ///
    /// Dropping a prepared transaction without resolving it leaves it in doubt on disk; after
    /// the next open it is listed by [`TidesDB::recover_prepared`].
    pub fn prepare(&mut self, xid: &[u8]) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_prepare(self.txn(), xid.as_ptr(), xid.len()) };
        check_result(result, "failed to prepare transaction")
    }

    /// Two-phase commit, phase two: commits a prepared transaction. A transient I/O failure
    /// leaves it prepared so the call can be retried.
    pub fn commit_prepared(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_commit_prepared(self.txn()) };
        check_result(result, "failed to commit prepared transaction")
    }

    /// Two-phase commit, phase two: rolls back a prepared transaction.
    pub fn rollback_prepared(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_rollback_prepared(self.txn()) };
        check_result(result, "failed to rollback prepared transaction")
    }

    // ===== savepoints =====

    /// Marks a named savepoint.
    pub fn savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_txn_savepoint(self.txn(), c_name.as_ptr()) };
        check_result(result, "failed to create savepoint")
    }

    /// Discards writes buffered since the named savepoint.
    pub fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_txn_rollback_to_savepoint(self.txn(), c_name.as_ptr()) };
        check_result(result, "failed to rollback to savepoint")
    }

    /// Releases a savepoint without rolling back.
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_txn_release_savepoint(self.txn(), c_name.as_ptr()) };
        check_result(result, "failed to release savepoint")
    }

    // ===== iterators =====

    /// Creates an iterator over a whole column family at this transaction's snapshot.
    ///
    /// The iterator borrows the transaction, so it must be dropped before commit. At
    /// repeatable read and serializable that is also what records the scanned range for
    /// conflict detection.
    pub fn new_iterator(&self, cf: &ColumnFamily<'_>) -> Result<Iterator<'_>> {
        let mut iter: *mut ffi::tidesdb_iter_t = ptr::null_mut();
        let result = unsafe { ffi::tidesdb_iter_new(self.txn(), self.cf_ptr(cf)?, &mut iter) };
        check_result(result, "failed to create iterator")?;
        if iter.is_null() {
            return Err(Error::NullPointer("iterator handle"));
        }
        Ok(unsafe { Iterator::from_raw(iter) })
    }

    /// Creates an iterator that only opens the sstables overlapping `[lower, upper]`.
    ///
    /// Much cheaper than [`new_iterator`](Self::new_iterator) for a narrow scan of a large
    /// family. The range is a promise, not a fence: results are only defined inside it, and
    /// seeking or stepping outside it may report a key that exists as absent.
    pub fn new_range_iterator(
        &self,
        cf: &ColumnFamily<'_>,
        lower: &[u8],
        upper: &[u8],
    ) -> Result<Iterator<'_>> {
        let mut iter: *mut ffi::tidesdb_iter_t = ptr::null_mut();
        let result = unsafe {
            ffi::tidesdb_iter_new_range(
                self.txn(),
                self.cf_ptr(cf)?,
                key_ptr(lower),
                lower.len(),
                key_ptr(upper),
                upper.len(),
                &mut iter,
            )
        };
        check_result(result, "failed to create range iterator")?;
        if iter.is_null() {
            return Err(Error::NullPointer("iterator handle"));
        }
        Ok(unsafe { Iterator::from_raw(iter) })
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // If an AbortHandle still holds the C transaction, it is freed when that handle
        // drops; roll back now so it stops holding its snapshot in the meantime.
        if Arc::strong_count(&self.handle) > 1 {
            unsafe { ffi::tidesdb_txn_rollback(self.txn()) };
        }
    }
}

/// Aborts a [`Transaction`] from another thread.
///
/// Obtained from [`Transaction::abort_handle`]. The handle is `Send + Sync` and may outlive
/// the transaction (but not the database); aborting a finished transaction does nothing.
#[derive(Clone)]
pub struct AbortHandle<'db> {
    handle: Arc<TxnHandle>,
    _marker: PhantomData<&'db TidesDB>,
}

impl AbortHandle<'_> {
    /// Asks the transaction to abort.
    ///
    /// This only sets a flag: the owning thread's next operation on the transaction aborts
    /// it and returns [`ErrorCode::TxnAborted`](crate::ErrorCode::TxnAborted), which is
    /// distinct from the engine's own [`ErrorCode::Conflict`](crate::ErrorCode::Conflict).
    /// A transaction that has already prepared is not affected.
    pub fn request_abort(&self) {
        unsafe { ffi::tidesdb_txn_request_abort(self.handle.txn) };
    }
}
