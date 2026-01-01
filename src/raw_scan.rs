//! Direct scanning that bypasses protobuf serialization/deserialization.
//!
//! This module provides a faster alternative to the protobuf-based scanning by
//! directly reading the scanner's token output and converting it to Rust protobuf types.

use crate::bindings;
use crate::bindings_raw;
use crate::protobuf;
use crate::{Error, Result};
use std::ffi::{CStr, CString};

/// Scans a SQL statement directly into protobuf types without going through protobuf serialization.
///
/// This function is faster than `scan` because it skips the protobuf encode/decode step.
/// The tokens are read directly from the C scanner output.
///
/// # Example
///
/// ```rust
/// let result = pg_query::scan_raw("SELECT * FROM users").unwrap();
/// assert!(!result.tokens.is_empty());
/// ```
pub fn scan_raw(sql: &str) -> Result<protobuf::ScanResult> {
    let input = CString::new(sql)?;
    let result = unsafe { bindings_raw::pg_query_scan_raw(input.as_ptr()) };

    let scan_result = if !result.error.is_null() {
        let message = unsafe { CStr::from_ptr((*result.error).message) }.to_string_lossy().to_string();
        Err(Error::Scan(message))
    } else {
        // Convert the C tokens to protobuf types
        let tokens = unsafe { convert_tokens(result.tokens, result.n_tokens) };
        Ok(protobuf::ScanResult { version: bindings::PG_VERSION_NUM as i32, tokens })
    };

    unsafe { bindings_raw::pg_query_free_raw_scan_result(result) };
    scan_result
}

/// Converts C scan tokens to protobuf ScanToken vector.
unsafe fn convert_tokens(tokens: *mut bindings_raw::PgQueryRawScanToken, n_tokens: usize) -> Vec<protobuf::ScanToken> {
    if tokens.is_null() || n_tokens == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(n_tokens);

    for i in 0..n_tokens {
        let token = &*tokens.add(i);
        result.push(protobuf::ScanToken { start: token.start, end: token.end, token: token.token, keyword_kind: token.keyword_kind });
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_raw_basic() {
        let result = scan_raw("SELECT * FROM users").unwrap();
        assert!(!result.tokens.is_empty());
        // First token should be SELECT
        assert_eq!(result.tokens[0].start, 0);
        assert_eq!(result.tokens[0].end, 6);
    }

    #[test]
    fn test_scan_raw_matches_scan() {
        let sql = "SELECT id, name FROM users WHERE active = true";
        let raw_result = scan_raw(sql).unwrap();
        let prost_result = crate::scan(sql).unwrap();

        assert_eq!(raw_result.version, prost_result.version);
        assert_eq!(raw_result.tokens.len(), prost_result.tokens.len());

        for (raw_token, prost_token) in raw_result.tokens.iter().zip(prost_result.tokens.iter()) {
            assert_eq!(raw_token.start, prost_token.start);
            assert_eq!(raw_token.end, prost_token.end);
            assert_eq!(raw_token.token, prost_token.token);
            assert_eq!(raw_token.keyword_kind, prost_token.keyword_kind);
        }
    }

    #[test]
    fn test_scan_raw_empty() {
        let result = scan_raw("").unwrap();
        assert!(result.tokens.is_empty());
    }

    #[test]
    fn test_scan_raw_complex() {
        let sql = r#"SELECT "column" AS left /* comment */ FROM between"#;
        let result = scan_raw(sql).unwrap();
        assert!(!result.tokens.is_empty());
    }
}
