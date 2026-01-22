// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Transaction operations for TidesDB.

use crate::db::ColumnFamily;
use crate::error::{check_result, Error, Result};
use crate::ffi;
use crate::iterator::Iterator;
use std::ffi::CString;
use std::ptr;

/// A transaction in TidesDB.
///
/// Transactions provide atomic operations on the database.
/// The transaction is automatically freed when dropped.
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
/// txn.put(&cf, b"key", b"value", -1)?;
/// txn.commit()?;
/// # Ok::<(), tidesdb::Error>(())
/// ```
pub struct Transaction {
    txn: *mut ffi::tidesdb_txn_t,
    committed: bool,
}

// Transaction uses internal locking for thread safety
unsafe impl Send for Transaction {}

impl Transaction {
    /// Create a new transaction wrapper.
    pub(crate) fn new(txn: *mut ffi::tidesdb_txn_t) -> Self {
        Transaction {
            txn,
            committed: false,
        }
    }

    /// Adds a key-value pair to the transaction.
    ///
    /// # Arguments
    ///
    /// * `cf` - The column family
    /// * `key` - The key
    /// * `value` - The value
    /// * `ttl` - Unix timestamp (seconds since epoch) for expiration, or -1 for no expiration
    pub fn put(&self, cf: &ColumnFamily, key: &[u8], value: &[u8], ttl: i64) -> Result<()> {
        let key_ptr = if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        };
        let value_ptr = if value.is_empty() {
            ptr::null()
        } else {
            value.as_ptr()
        };

        let result = unsafe {
            ffi::tidesdb_txn_put(
                self.txn,
                cf.cf,
                key_ptr,
                key.len(),
                value_ptr,
                value.len(),
                ttl as libc::time_t,
            )
        };
        check_result(result, "failed to put key-value pair")
    }

    /// Retrieves a value from the transaction.
    ///
    /// # Arguments
    ///
    /// * `cf` - The column family
    /// * `key` - The key
    ///
    /// # Returns
    ///
    /// The value or an error if not found.
    pub fn get(&self, cf: &ColumnFamily, key: &[u8]) -> Result<Vec<u8>> {
        let key_ptr = if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        };

        let mut value_ptr: *mut u8 = ptr::null_mut();
        let mut value_len: usize = 0;

        let result = unsafe {
            ffi::tidesdb_txn_get(
                self.txn,
                cf.cf,
                key_ptr,
                key.len(),
                &mut value_ptr,
                &mut value_len,
            )
        };
        check_result(result, "failed to get value")?;

        if value_ptr.is_null() {
            return Err(Error::NullPointer("value"));
        }

        let value = unsafe {
            let slice = std::slice::from_raw_parts(value_ptr, value_len);
            let vec = slice.to_vec();
            libc::free(value_ptr as *mut libc::c_void);
            vec
        };

        Ok(value)
    }

    /// Removes a key-value pair from the transaction.
    ///
    /// # Arguments
    ///
    /// * `cf` - The column family
    /// * `key` - The key
    pub fn delete(&self, cf: &ColumnFamily, key: &[u8]) -> Result<()> {
        let key_ptr = if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        };

        let result = unsafe { ffi::tidesdb_txn_delete(self.txn, cf.cf, key_ptr, key.len()) };
        check_result(result, "failed to delete key")
    }

    /// Commits the transaction.
    ///
    /// After committing, the transaction cannot be used for further operations.
    pub fn commit(mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_commit(self.txn) };
        self.committed = true;
        check_result(result, "failed to commit transaction")
    }

    /// Rolls back the transaction.
    ///
    /// After rolling back, the transaction cannot be used for further operations.
    pub fn rollback(mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_txn_rollback(self.txn) };
        self.committed = true; // Mark as done to prevent double-free
        check_result(result, "failed to rollback transaction")
    }

    /// Creates a savepoint within the transaction.
    ///
    /// # Arguments
    ///
    /// * `name` - The savepoint name
    pub fn savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_txn_savepoint(self.txn, c_name.as_ptr()) };
        check_result(result, "failed to create savepoint")
    }

    /// Rolls back the transaction to a savepoint.
    ///
    /// # Arguments
    ///
    /// * `name` - The savepoint name
    pub fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result =
            unsafe { ffi::tidesdb_txn_rollback_to_savepoint(self.txn, c_name.as_ptr()) };
        check_result(result, "failed to rollback to savepoint")
    }

    /// Releases a savepoint without rolling back.
    ///
    /// # Arguments
    ///
    /// * `name` - The savepoint name
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let c_name = CString::new(name)?;
        let result = unsafe { ffi::tidesdb_txn_release_savepoint(self.txn, c_name.as_ptr()) };
        check_result(result, "failed to release savepoint")
    }

    /// Creates a new iterator for a column family within this transaction.
    ///
    /// # Arguments
    ///
    /// * `cf` - The column family
    ///
    /// # Returns
    ///
    /// A new iterator.
    pub fn new_iterator(&self, cf: &ColumnFamily) -> Result<Iterator> {
        let mut iter: *mut ffi::tidesdb_iter_t = ptr::null_mut();

        let result = unsafe { ffi::tidesdb_iter_new(self.txn, cf.cf, &mut iter) };
        check_result(result, "failed to create iterator")?;

        if iter.is_null() {
            return Err(Error::NullPointer("iterator handle"));
        }

        Ok(Iterator::new(iter))
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.txn.is_null() {
            unsafe {
                ffi::tidesdb_txn_free(self.txn);
            }
            self.txn = ptr::null_mut();
        }
    }
}
