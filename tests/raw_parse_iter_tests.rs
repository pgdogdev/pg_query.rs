//! Tests for parse_raw_iter functionality.
//!
//! These tests verify that parse_raw_iter produces equivalent results to parse.
//! Tests are split into modules for maintainability.
//!
//! Run tests one at a time from simple to complex:
//!   cargo test --test raw_parse_iter_tests raw_parse_iter::basic::it_parses_simple_select

#![allow(non_snake_case)]
#![cfg(test)]

#[macro_use]
mod support;

mod raw_parse_iter;
