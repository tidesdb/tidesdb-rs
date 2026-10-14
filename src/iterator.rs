// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Iterator operations for TidesDB.

use crate::db::{take_c_buffer, ttl_from_c};
use crate::error::{Error, Result, check_result};
use crate::ffi;
use crate::transaction::Transaction;
use std::marker::PhantomData;
use std::ptr;

/// A bidirectional cursor over a column family at a transaction's snapshot.
///
/// Created by [`Transaction::new_iterator`] or [`Transaction::new_range_iterator`] and
/// freed when dropped. It borrows the transaction, so it must be dropped before the
/// transaction commits.
///
/// Positioning calls (`seek*`, `next`, `prev`) succeed when they run off either end; check
/// [`is_valid`](Self::is_valid) to see whether the iterator is on an entry.
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
/// let txn = db.begin_transaction()?;
/// let mut iter = txn.new_iterator(&cf)?;
/// iter.seek_to_first()?;
/// while iter.is_valid() {
///     let (key, value) = iter.key_value()?;
///     println!("{:?} = {:?}", key, value);
///     iter.next()?;
/// }
/// # Ok::<(), tidesdb::Error>(())
/// ```
pub struct Iterator<'txn> {
    iter: *mut ffi::tidesdb_iter_t,
    _marker: PhantomData<&'txn Transaction<'txn>>,
}

/// A positioning result of NOT_FOUND only means the iterator ran off the end.
fn check_position(result: i32, context: &'static str) -> Result<()> {
    if result == ffi::TDB_SUCCESS || result == ffi::TDB_ERR_NOT_FOUND {
        Ok(())
    } else {
        Err(Error::from_code(result, context))
    }
}

impl Iterator<'_> {
    /// Wrap a C iterator.
    ///
    /// # Safety
    ///
    /// `iter` must be a live iterator owned by the caller.
    pub(crate) unsafe fn from_raw(iter: *mut ffi::tidesdb_iter_t) -> Self {
        Iterator {
            iter,
            _marker: PhantomData,
        }
    }

    /// Positions the iterator at the first key.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek_to_first(self.iter) };
        check_position(result, "failed to seek to first")
    }

    /// Positions the iterator at the last key.
    pub fn seek_to_last(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek_to_last(self.iter) };
        check_position(result, "failed to seek to last")
    }

    /// Positions the iterator at the first key >= `key`.
    pub fn seek(&mut self, key: &[u8]) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek(self.iter, key.as_ptr(), key.len()) };
        check_position(result, "failed to seek")
    }

    /// Positions the iterator at the last key <= `key`.
    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek_for_prev(self.iter, key.as_ptr(), key.len()) };
        check_position(result, "failed to seek for prev")
    }

    /// Whether the iterator is positioned on an entry.
    pub fn is_valid(&self) -> bool {
        unsafe { ffi::tidesdb_iter_valid(self.iter) != 0 }
    }

    /// Moves to the next entry.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_next(self.iter) };
        check_position(result, "failed to move to next")
    }

    /// Moves to the previous entry.
    pub fn prev(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_prev(self.iter) };
        check_position(result, "failed to move to prev")
    }

    /// The key at the current position.
    pub fn key(&self) -> Result<Vec<u8>> {
        let mut key_ptr: *mut u8 = ptr::null_mut();
        let mut key_len: usize = 0;
        let result = unsafe { ffi::tidesdb_iter_key(self.iter, &mut key_ptr, &mut key_len) };
        check_result(result, "failed to get key")?;
        Ok(unsafe { take_c_buffer(key_ptr, key_len) })
    }

    /// The value at the current position.
    pub fn value(&self) -> Result<Vec<u8>> {
        let mut value_ptr: *mut u8 = ptr::null_mut();
        let mut value_len: usize = 0;
        let result = unsafe { ffi::tidesdb_iter_value(self.iter, &mut value_ptr, &mut value_len) };
        check_result(result, "failed to get value")?;
        Ok(unsafe { take_c_buffer(value_ptr, value_len) })
    }

    /// The key and value at the current position, in one call.
    pub fn key_value(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut key_ptr: *mut u8 = ptr::null_mut();
        let mut key_len: usize = 0;
        let mut value_ptr: *mut u8 = ptr::null_mut();
        let mut value_len: usize = 0;
        let result = unsafe {
            ffi::tidesdb_iter_key_value(
                self.iter,
                &mut key_ptr,
                &mut key_len,
                &mut value_ptr,
                &mut value_len,
            )
        };
        check_result(result, "failed to get key-value")?;
        let key = unsafe { take_c_buffer(key_ptr, key_len) };
        let value = unsafe { take_c_buffer(value_ptr, value_len) };
        Ok((key, value))
    }

    /// The current entry's expiry as an absolute Unix timestamp in seconds, `None` if it
    /// never expires.
    pub fn expires_at(&self) -> Result<Option<i64>> {
        let mut ttl: ffi::time_t = -1;
        let result = unsafe { ffi::tidesdb_iter_ttl(self.iter, &mut ttl) };
        check_result(result, "failed to get ttl")?;
        Ok(ttl_from_c(ttl))
    }
}

impl Drop for Iterator<'_> {
    fn drop(&mut self) {
        unsafe { ffi::tidesdb_iter_free(self.iter) };
    }
}
