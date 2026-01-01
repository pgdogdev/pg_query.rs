#![allow(unused_macros, dead_code)]

use std::fmt;

#[derive(PartialEq, Eq)]
pub struct MultiLineString<'a>(pub &'a str);

impl<'a> fmt::Debug for MultiLineString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.0)
    }
}

// Modified from https://github.com/colin-kiegel/rust-pretty-assertions/issues/24#issuecomment-520613247
// to optionally turn off the pretty printing so you can copy the actual string.
macro_rules! assert_debug_eq {
    ($left:expr, $right:expr) => {
        if let Ok(_diff) = std::env::var("DIFF") {
            pretty_assertions::assert_eq!(MultiLineString(&format!("{:#?}", $left)), MultiLineString($right));
        } else {
            std::assert_eq!(MultiLineString(&format!("{:#?}", $left)), MultiLineString($right));
        }
    };
}

macro_rules! assert_eq {
    ($left:expr, $right:expr) => {
        if let Ok(_diff) = std::env::var("DIFF") {
            pretty_assertions::assert_eq!($left, $right);
        } else {
            std::assert_eq!($left, $right);
        }
    };
}

pub fn assert_vec_matches<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) {
    let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
    assert!(matching == a.len() && matching == b.len())
}

/// Verifies that parse and parse_raw produce identical protobuf results
pub fn assert_parse_raw_equals_parse(query: &str) {
    let parse_result = pg_query::parse(query).expect("parse failed");
    let parse_raw_result = pg_query::parse_raw(query).expect("parse_raw failed");
    assert!(parse_result.protobuf == parse_raw_result.protobuf, "parse and parse_raw produced different protobufs for query: {query}");
}

/// Verifies that deparse_raw produces valid SQL that can be reparsed.
/// We compare fingerprints rather than full protobuf equality because:
/// 1. Location fields will differ (character offsets change with reformatting)
/// 2. Fingerprints capture the semantic content of the query
pub fn assert_deparse_raw_roundtrip(query: &str) {
    let parse_result = pg_query::parse(query).expect("parse failed");
    let deparsed = pg_query::deparse_raw(&parse_result.protobuf).expect("deparse_raw failed");
    let reparsed = pg_query::parse(&deparsed).expect(&format!("reparsing deparsed SQL failed: {}", deparsed));

    // Compare fingerprints for semantic equality
    let original_fp = pg_query::fingerprint(query).expect("fingerprint failed").hex;
    let reparsed_fp = pg_query::fingerprint(&deparsed).expect("reparsed fingerprint failed").hex;
    assert!(
        original_fp == reparsed_fp,
        "deparse_raw roundtrip produced different fingerprint for query: {query}\ndeparsed as: {deparsed}\noriginal fp: {}\nreparsed fp: {}",
        original_fp,
        reparsed_fp
    );

    // Also verify statement types match
    std::assert_eq!(
        parse_result.statement_types(),
        reparsed.statement_types(),
        "deparse_raw roundtrip produced different statement types for query: {query}\ndeparsed as: {deparsed}"
    );
}

macro_rules! cast {
    ($target: expr, $pat: path) => {{
        if let $pat(a) = $target {
            // #1
            a
        } else {
            panic!("mismatch variant when cast to {}", stringify!($pat)); // #2
        }
    }};
}
