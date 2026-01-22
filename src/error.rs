// Package tidesdb
// Copyright (C) TidesDB
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");

//! Error types for TidesDB operations.

use crate::ffi;
use std::fmt;
use thiserror::Error;

/// Error codes returned by TidesDB operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// Transaction conflict
    Conflict,
    /// Key or value too large
    TooLarge,
    /// Memory limit exceeded
    MemoryLimit,
    /// Invalid database handle
    InvalidDb,
    /// Unknown error
    Unknown,
    /// Database is locked
    Locked,
}

impl ErrorCode {
    /// Convert from a C error code
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            ffi::TDB_ERR_MEMORY => Some(ErrorCode::Memory),
            ffi::TDB_ERR_INVALID_ARGS => Some(ErrorCode::InvalidArgs),
            ffi::TDB_ERR_NOT_FOUND => Some(ErrorCode::NotFound),
            ffi::TDB_ERR_IO => Some(ErrorCode::Io),
            ffi::TDB_ERR_CORRUPTION => Some(ErrorCode::Corruption),
            ffi::TDB_ERR_EXISTS => Some(ErrorCode::Exists),
            ffi::TDB_ERR_CONFLICT => Some(ErrorCode::Conflict),
            ffi::TDB_ERR_TOO_LARGE => Some(ErrorCode::TooLarge),
            ffi::TDB_ERR_MEMORY_LIMIT => Some(ErrorCode::MemoryLimit),
            ffi::TDB_ERR_INVALID_DB => Some(ErrorCode::InvalidDb),
            ffi::TDB_ERR_LOCKED => Some(ErrorCode::Locked),
            _ => Some(ErrorCode::Unknown),
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::Memory => write!(f, "memory allocation failed"),
            ErrorCode::InvalidArgs => write!(f, "invalid arguments"),
            ErrorCode::NotFound => write!(f, "not found"),
            ErrorCode::Io => write!(f, "I/O error"),
            ErrorCode::Corruption => write!(f, "data corruption"),
            ErrorCode::Exists => write!(f, "already exists"),
            ErrorCode::Conflict => write!(f, "transaction conflict"),
            ErrorCode::TooLarge => write!(f, "key or value too large"),
            ErrorCode::MemoryLimit => write!(f, "memory limit exceeded"),
            ErrorCode::InvalidDb => write!(f, "invalid database handle"),
            ErrorCode::Unknown => write!(f, "unknown error"),
            ErrorCode::Locked => write!(f, "database is locked"),
        }
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
            code: ErrorCode::from_code(code).unwrap_or(ErrorCode::Unknown),
            context,
        }
    }

    /// Check if this is a "not found" error
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Error::TidesDB {
                code: ErrorCode::NotFound,
                ..
            }
        )
    }

    /// Check if this is a conflict error
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            Error::TidesDB {
                code: ErrorCode::Conflict,
                ..
            }
        )
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
