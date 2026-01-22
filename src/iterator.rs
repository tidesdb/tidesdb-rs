// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Iterator operations for TidesDB.

use crate::error::{check_result, Error, Result};
use crate::ffi;
use std::ptr;

/// An iterator for traversing key-value pairs in a column family.
///
/// Provides efficient bidirectional traversal.
/// The iterator is automatically freed when dropped.
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
///
/// // Insert some data
/// txn.put(&cf, b"key1", b"value1", -1)?;
/// txn.put(&cf, b"key2", b"value2", -1)?;
/// txn.commit()?;
///
/// // Iterate
/// let txn = db.begin_transaction()?;
/// let mut iter = txn.new_iterator(&cf)?;
/// iter.seek_to_first()?;
///
/// while iter.is_valid() {
///     let key = iter.key()?;
///     let value = iter.value()?;
///     println!("Key: {:?}, Value: {:?}", key, value);
///     iter.next()?;
/// }
/// # Ok::<(), tidesdb::Error>(())
/// ```
pub struct Iterator {
    iter: *mut ffi::tidesdb_iter_t,
}

// Safety: Iterator uses internal locking for thread safety
unsafe impl Send for Iterator {}

impl Iterator {
    /// Create a new iterator wrapper.
    pub(crate) fn new(iter: *mut ffi::tidesdb_iter_t) -> Self {
        Iterator { iter }
    }

    /// Positions the iterator at the first key.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek_to_first(self.iter) };
        check_result(result, "failed to seek to first")
    }

    /// Positions the iterator at the last key.
    pub fn seek_to_last(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_seek_to_last(self.iter) };
        check_result(result, "failed to seek to last")
    }

    /// Positions the iterator at the first key >= target key.
    ///
    /// # Arguments
    ///
    /// * `key` - The target key
    pub fn seek(&mut self, key: &[u8]) -> Result<()> {
        let key_ptr = if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        };

        let result = unsafe { ffi::tidesdb_iter_seek(self.iter, key_ptr, key.len()) };
        check_result(result, "failed to seek")
    }

    /// Positions the iterator at the last key <= target key.
    ///
    /// # Arguments
    ///
    /// * `key` - The target key
    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<()> {
        let key_ptr = if key.is_empty() {
            ptr::null()
        } else {
            key.as_ptr()
        };

        let result = unsafe { ffi::tidesdb_iter_seek_for_prev(self.iter, key_ptr, key.len()) };
        check_result(result, "failed to seek for prev")
    }

    /// Returns true if the iterator is positioned at a valid entry.
    pub fn is_valid(&self) -> bool {
        unsafe { ffi::tidesdb_iter_valid(self.iter) != 0 }
    }

    /// Moves the iterator to the next entry.
    ///
    /// Returns Ok(()) even when reaching the end of iteration.
    /// Use `is_valid()` to check if the iterator is still positioned at a valid entry.
    pub fn next(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_next(self.iter) };
        // NOT_FOUND is expected when reaching end of iteration
        if result == ffi::TDB_ERR_NOT_FOUND || result == ffi::TDB_SUCCESS {
            Ok(())
        } else {
            Err(crate::error::Error::from_code(result, "failed to move to next"))
        }
    }

    /// Moves the iterator to the previous entry.
    ///
    /// Returns Ok(()) even when reaching the beginning of iteration.
    /// Use `is_valid()` to check if the iterator is still positioned at a valid entry.
    pub fn prev(&mut self) -> Result<()> {
        let result = unsafe { ffi::tidesdb_iter_prev(self.iter) };
        // NOT_FOUND is expected when reaching beginning of iteration
        if result == ffi::TDB_ERR_NOT_FOUND || result == ffi::TDB_SUCCESS {
            Ok(())
        } else {
            Err(crate::error::Error::from_code(result, "failed to move to prev"))
        }
    }

    /// Retrieves the current key from the iterator.
    ///
    /// # Returns
    ///
    /// The current key.
    pub fn key(&self) -> Result<Vec<u8>> {
        let mut key_ptr: *mut u8 = ptr::null_mut();
        let mut key_len: usize = 0;

        let result = unsafe { ffi::tidesdb_iter_key(self.iter, &mut key_ptr, &mut key_len) };
        check_result(result, "failed to get key")?;

        if key_ptr.is_null() {
            return Err(Error::NullPointer("key"));
        }

        let key = unsafe {
            let slice = std::slice::from_raw_parts(key_ptr, key_len);
            slice.to_vec()
        };

        Ok(key)
    }

    /// Retrieves the current value from the iterator.
    ///
    /// # Returns
    ///
    /// The current value.
    pub fn value(&self) -> Result<Vec<u8>> {
        let mut value_ptr: *mut u8 = ptr::null_mut();
        let mut value_len: usize = 0;

        let result = unsafe { ffi::tidesdb_iter_value(self.iter, &mut value_ptr, &mut value_len) };
        check_result(result, "failed to get value")?;

        if value_ptr.is_null() {
            return Err(Error::NullPointer("value"));
        }

        let value = unsafe {
            let slice = std::slice::from_raw_parts(value_ptr, value_len);
            slice.to_vec()
        };

        Ok(value)
    }
}

impl Drop for Iterator {
    fn drop(&mut self) {
        if !self.iter.is_null() {
            unsafe {
                ffi::tidesdb_iter_free(self.iter);
            }
            self.iter = ptr::null_mut();
        }
    }
}
