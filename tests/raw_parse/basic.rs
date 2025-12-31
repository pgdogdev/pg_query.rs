//! Basic parsing tests for parse_raw.
//!
//! These tests verify fundamental parsing behavior including:
//! - Simple SELECT queries
//! - Error handling
//! - Multiple statements
//! - Empty queries

use super::*;

/// Test that parse_raw results can be deparsed back to SQL
#[test]
fn it_deparses_parse_raw_result() {
    let query = "SELECT * FROM users";
    let result = parse_raw(query).unwrap();

    let deparsed = result.deparse().unwrap();
    assert_eq!(deparsed, query);
}

/// Test that parse_raw successfully parses a simple SELECT query
#[test]
fn it_parses_simple_select() {
    let query = "SELECT 1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf.stmts.len(), 1);
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that parse_raw handles syntax errors
#[test]
fn it_handles_parse_errors() {
    let query = "SELECT * FORM users";
    let raw_error = parse_raw(query).err().unwrap();
    let proto_error = parse(query).err().unwrap();

    assert!(matches!(raw_error, Error::Parse(_)));
    assert!(matches!(proto_error, Error::Parse(_)));
}

/// Test that parse_raw and parse produce equivalent results for simple SELECT
#[test]
fn it_matches_parse_for_simple_select() {
    let query = "SELECT 1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that parse_raw and parse produce equivalent results for SELECT with table
#[test]
fn it_matches_parse_for_select_from_table() {
    let query = "SELECT * FROM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test that parse_raw handles empty queries (comments only)
#[test]
fn it_handles_empty_queries() {
    let query = "-- just a comment";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf.stmts.len(), 0);
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that parse_raw parses multiple statements
#[test]
fn it_parses_multiple_statements() {
    let query = "SELECT 1; SELECT 2; SELECT 3";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf.stmts.len(), 3);
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that tables() returns the same results for both parsers
#[test]
fn it_returns_tables_like_parse() {
    let query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["orders", "users"]);
}

/// Test that functions() returns the same results for both parsers
#[test]
fn it_returns_functions_like_parse() {
    let query = "SELECT count(*), sum(amount) FROM orders";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_funcs = raw_result.functions();
    let mut proto_funcs = proto_result.functions();
    raw_funcs.sort();
    proto_funcs.sort();
    assert_eq!(raw_funcs, proto_funcs);
    assert_eq!(raw_funcs, vec!["count", "sum"]);
}

/// Test that statement_types() returns the same results for both parsers
#[test]
fn it_returns_statement_types_like_parse() {
    let query = "SELECT 1; INSERT INTO t VALUES (1); UPDATE t SET x = 1; DELETE FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    assert_eq!(raw_result.statement_types(), proto_result.statement_types());
    assert_eq!(raw_result.statement_types(), vec!["SelectStmt", "InsertStmt", "UpdateStmt", "DeleteStmt"]);
}
