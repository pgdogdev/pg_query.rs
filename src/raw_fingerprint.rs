//! Direct fingerprinting that bypasses protobuf serialization/deserialization.
//!
//! This module provides a faster alternative to the standard fingerprint function by
//! parsing directly into PostgreSQL's internal structures and fingerprinting them
//! without going through protobuf serialization.

use crate::bindings_raw;
use crate::query::Fingerprint;
use crate::{Error, Result};
use std::ffi::{CStr, CString};

/// Fingerprints a SQL statement without going through protobuf serialization.
///
/// This function is faster than `fingerprint` because it skips the protobuf encode/decode step.
/// The SQL is parsed directly into PostgreSQL's internal structures and fingerprinted there.
///
/// # Example
///
/// ```rust
/// let result = pg_query::fingerprint_raw("SELECT * FROM contacts WHERE name='Paul'").unwrap();
/// assert_eq!(result.hex, "0e2581a461ece536");
/// ```
pub fn fingerprint_raw(statement: &str) -> Result<Fingerprint> {
    let input = CString::new(statement)?;

    // Parse the SQL into raw C structures
    let parse_result = unsafe { bindings_raw::pg_query_parse_raw(input.as_ptr()) };

    // Fingerprint the raw parse tree
    let fingerprint_result = unsafe { bindings_raw::pg_query_fingerprint_raw(parse_result) };

    // Free the parse result (the fingerprint result has its own copies of any needed data)
    unsafe { bindings_raw::pg_query_free_raw_parse_result(parse_result) };

    // Convert the fingerprint result to Rust types
    let result = if !fingerprint_result.error.is_null() {
        let message = unsafe { CStr::from_ptr((*fingerprint_result.error).message) }.to_string_lossy().to_string();
        Err(Error::Parse(message))
    } else {
        let hex = unsafe { CStr::from_ptr(fingerprint_result.fingerprint_str) };
        Ok(Fingerprint { value: fingerprint_result.fingerprint, hex: hex.to_string_lossy().to_string() })
    };

    unsafe { bindings_raw::pg_query_free_fingerprint_result(fingerprint_result) };
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_raw_basic() {
        let result = fingerprint_raw("SELECT * FROM users").unwrap();
        assert!(!result.hex.is_empty());
        assert_eq!(result.hex.len(), 16);
    }

    #[test]
    fn test_fingerprint_raw_matches_fingerprint() {
        let sql = "SELECT * FROM contacts WHERE name='Paul'";
        let raw_result = fingerprint_raw(sql).unwrap();
        let std_result = crate::fingerprint(sql).unwrap();

        assert_eq!(raw_result.value, std_result.value);
        assert_eq!(raw_result.hex, std_result.hex);
    }

    #[test]
    fn test_fingerprint_raw_normalizes_values() {
        // These should have the same fingerprint since values are normalized
        let fp1 = fingerprint_raw("SELECT * FROM users WHERE id = 1").unwrap();
        let fp2 = fingerprint_raw("SELECT * FROM users WHERE id = 999").unwrap();
        assert_eq!(fp1.value, fp2.value);
        assert_eq!(fp1.hex, fp2.hex);
    }

    #[test]
    fn test_fingerprint_raw_error() {
        let result = fingerprint_raw("NOT VALID SQL @#$");
        assert!(result.is_err());
    }

    #[test]
    fn test_fingerprint_raw_comment_only() {
        // Comment-only queries should produce the same fingerprint as the regular function
        let raw_result = fingerprint_raw("-- ping").unwrap();
        let std_result = crate::fingerprint("-- ping").unwrap();
        assert_eq!(raw_result.value, std_result.value);
        assert_eq!(raw_result.hex, std_result.hex);
    }

    #[test]
    fn test_fingerprint_raw_empty() {
        // Empty queries should produce the same fingerprint as the regular function
        let raw_result = fingerprint_raw("").unwrap();
        let std_result = crate::fingerprint("").unwrap();
        assert_eq!(raw_result.value, std_result.value);
        assert_eq!(raw_result.hex, std_result.hex);
    }
}
