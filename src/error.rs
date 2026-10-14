// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Error types for TidesDB operations.

use crate::ffi;
use std::ffi::CStr;
use std::fmt;
use thiserror::Error;

/// Error codes returned by TidesDB operations.
///
/// New codes may be added in minor releases, so match with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Memory allocation failed
    Memory,
    /// Invalid arguments provided
    InvalidArgs,
    /// Key or resource not found
    NotFound,
    /// I/O error occurred
    Io,
    /// Data corruption detected
    Corruption,
    /// Resource already exists
    Exists,
    /// Transaction conflict: another transaction committed a conflicting write first
    Conflict,
    /// A value does not fit the space that has to hold it
    TooLarge,
    /// Memory limit exceeded
    MemoryLimit,
    /// Invalid database handle, or the database is closing
    InvalidDb,
    /// Unknown error
    Unknown,
    /// Transient contention. Nothing was written and the operation should be retried;
    /// it never means absence or data loss.
    Locked,
    /// Database is in read-only mode
    ReadOnly,
    /// The transaction outlived its timeout and was aborted
    TxnExpired,
    /// The device or filesystem is out of space; the operation succeeds once space is freed
    NoSpace,
    /// The transaction was aborted from another thread with
    /// [`AbortHandle::request_abort`](crate::AbortHandle::request_abort)
    TxnAborted,
    /// The requested point in time is no longer reconstructable
    TooOld,
}

impl ErrorCode {
    /// Convert from a C error code. Unrecognised codes map to [`ErrorCode::Unknown`].
    pub fn from_code(code: i32) -> Self {
        match code {
            ffi::TDB_ERR_MEMORY => ErrorCode::Memory,
            ffi::TDB_ERR_INVALID_ARGS => ErrorCode::InvalidArgs,
            ffi::TDB_ERR_NOT_FOUND => ErrorCode::NotFound,
            ffi::TDB_ERR_IO => ErrorCode::Io,
            ffi::TDB_ERR_CORRUPTION => ErrorCode::Corruption,
            ffi::TDB_ERR_EXISTS => ErrorCode::Exists,
            ffi::TDB_ERR_CONFLICT => ErrorCode::Conflict,
            ffi::TDB_ERR_TOO_LARGE => ErrorCode::TooLarge,
            ffi::TDB_ERR_MEMORY_LIMIT => ErrorCode::MemoryLimit,
            ffi::TDB_ERR_INVALID_DB => ErrorCode::InvalidDb,
            ffi::TDB_ERR_LOCKED => ErrorCode::Locked,
            ffi::TDB_ERR_READONLY => ErrorCode::ReadOnly,
            ffi::TDB_ERR_TXN_EXPIRED => ErrorCode::TxnExpired,
            ffi::TDB_ERR_NO_SPACE => ErrorCode::NoSpace,
            ffi::TDB_ERR_TXN_ABORTED => ErrorCode::TxnAborted,
            ffi::TDB_ERR_TOO_OLD => ErrorCode::TooOld,
            _ => ErrorCode::Unknown,
        }
    }

    /// The C error code (`TDB_ERR_*`) this variant corresponds to.
    pub fn code(self) -> i32 {
        match self {
            ErrorCode::Memory => ffi::TDB_ERR_MEMORY,
            ErrorCode::InvalidArgs => ffi::TDB_ERR_INVALID_ARGS,
            ErrorCode::NotFound => ffi::TDB_ERR_NOT_FOUND,
            ErrorCode::Io => ffi::TDB_ERR_IO,
            ErrorCode::Corruption => ffi::TDB_ERR_CORRUPTION,
            ErrorCode::Exists => ffi::TDB_ERR_EXISTS,
            ErrorCode::Conflict => ffi::TDB_ERR_CONFLICT,
            ErrorCode::TooLarge => ffi::TDB_ERR_TOO_LARGE,
            ErrorCode::MemoryLimit => ffi::TDB_ERR_MEMORY_LIMIT,
            ErrorCode::InvalidDb => ffi::TDB_ERR_INVALID_DB,
            ErrorCode::Unknown => ffi::TDB_ERR_UNKNOWN,
            ErrorCode::Locked => ffi::TDB_ERR_LOCKED,
            ErrorCode::ReadOnly => ffi::TDB_ERR_READONLY,
            ErrorCode::TxnExpired => ffi::TDB_ERR_TXN_EXPIRED,
            ErrorCode::NoSpace => ffi::TDB_ERR_NO_SPACE,
            ErrorCode::TxnAborted => ffi::TDB_ERR_TXN_ABORTED,
            ErrorCode::TooOld => ffi::TDB_ERR_TOO_OLD,
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // tidesdb_strerror returns a static literal, never NULL
        let msg = unsafe { CStr::from_ptr(ffi::tidesdb_strerror(self.code())) };
        f.write_str(&msg.to_string_lossy())
    }
}

/// The main error type for TidesDB operations.
#[derive(Error, Debug)]
pub enum Error {
    /// An error from the TidesDB C library
    #[error("{context}: {code}")]
    TidesDB {
        code: ErrorCode,
        context: &'static str,
    },

    /// A null pointer was encountered
    #[error("null pointer: {0}")]
    NullPointer(&'static str),

    /// Invalid UTF-8 string
    #[error("invalid UTF-8 string: {0}")]
    InvalidString(#[from] std::str::Utf8Error),

    /// String contains interior null byte
    #[error("string contains null byte: {0}")]
    NulError(#[from] std::ffi::NulError),
}

impl Error {
    /// Create a new TidesDB error from a C error code
    pub fn from_code(code: i32, context: &'static str) -> Self {
        Error::TidesDB {
            code: ErrorCode::from_code(code),
            context,
        }
    }

    /// The TidesDB error code, if this error came from the C library
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Error::TidesDB { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Check if this is a "not found" error
    pub fn is_not_found(&self) -> bool {
        self.code() == Some(ErrorCode::NotFound)
    }

    /// Check if this is a transaction conflict error
    pub fn is_conflict(&self) -> bool {
        self.code() == Some(ErrorCode::Conflict)
    }

    /// Check if this is transient contention ([`ErrorCode::Locked`]) that should be retried
    pub fn is_locked(&self) -> bool {
        self.code() == Some(ErrorCode::Locked)
    }
}

/// Result type for TidesDB operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Check a C result code and convert to Result
#[inline]
pub(crate) fn check_result(code: i32, context: &'static str) -> Result<()> {
    if code == ffi::TDB_SUCCESS {
        Ok(())
    } else {
        Err(Error::from_code(code, context))
    }
}
