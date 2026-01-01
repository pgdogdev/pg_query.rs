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
    // Verify deparse produces original query
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);
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
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);
}

/// Test that parse_raw and parse produce equivalent results for SELECT with table
#[test]
fn it_matches_parse_for_select_from_table() {
    let query = "SELECT * FROM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);

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
    // Empty queries deparse to empty string (comments are stripped)
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), "");
}

/// Test that parse_raw parses multiple statements
#[test]
fn it_parses_multiple_statements() {
    let query = "SELECT 1; SELECT 2; SELECT 3";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf.stmts.len(), 3);
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);
}

/// Test that tables() returns the same results for both parsers
#[test]
fn it_returns_tables_like_parse() {
    let query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);

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
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);

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
    assert_eq!(deparse_raw(&raw_result.protobuf).unwrap(), query);

    assert_eq!(raw_result.statement_types(), proto_result.statement_types());
    assert_eq!(raw_result.statement_types(), vec!["SelectStmt", "InsertStmt", "UpdateStmt", "DeleteStmt"]);
}

// ============================================================================
// deparse_raw tests
// ============================================================================

/// Test that deparse_raw successfully roundtrips a simple SELECT
#[test]
fn it_deparse_raw_simple_select() {
    let query = "SELECT 1";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw successfully roundtrips SELECT FROM table
#[test]
fn it_deparse_raw_select_from_table() {
    let query = "SELECT * FROM users";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw handles complex queries
#[test]
fn it_deparse_raw_complex_select() {
    let query = "SELECT u.id, u.name FROM users u WHERE u.active = true ORDER BY u.name";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw handles INSERT statements
#[test]
fn it_deparse_raw_insert() {
    let query = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw handles UPDATE statements
#[test]
fn it_deparse_raw_update() {
    let query = "UPDATE users SET name = 'Jane' WHERE id = 1";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw handles DELETE statements
#[test]
fn it_deparse_raw_delete() {
    let query = "DELETE FROM users WHERE id = 1";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

/// Test that deparse_raw handles multiple statements
#[test]
fn it_deparse_raw_multiple_statements() {
    let query = "SELECT 1; SELECT 2; SELECT 3";
    let result = pg_query::parse(query).unwrap();
    let deparsed = pg_query::deparse_raw(&result.protobuf).unwrap();
    assert_eq!(deparsed, query);
}

// ============================================================================
// deparse_raw method tests (on structs)
// ============================================================================

/// Test that ParseResult.deparse_raw() method works
#[test]
fn it_deparse_raw_method_on_parse_result() {
    let query = "SELECT * FROM users WHERE id = 1";
    let result = pg_query::parse(query).unwrap();
    // Test the new method on ParseResult
    let deparsed = result.deparse_raw().unwrap();
    assert_eq!(deparsed, query);
}

/// Test that protobuf::ParseResult.deparse_raw() method works
#[test]
fn it_deparse_raw_method_on_protobuf_parse_result() {
    let query = "SELECT a, b, c FROM table1 JOIN table2 ON table1.id = table2.id";
    let result = pg_query::parse(query).unwrap();
    // Test the new method on protobuf::ParseResult
    let deparsed = result.protobuf.deparse_raw().unwrap();
    assert_eq!(deparsed, query);
}

/// Test that NodeRef.deparse_raw() method works
#[test]
fn it_deparse_raw_method_on_node_ref() {
    let query = "SELECT * FROM users";
    let result = pg_query::parse(query).unwrap();
    // Get the first node (SelectStmt)
    let nodes = result.protobuf.nodes();
    assert!(!nodes.is_empty());
    // Find the SelectStmt node
    for (node, _depth, _context, _has_filter) in nodes {
        if let pg_query::NodeRef::SelectStmt(_) = node {
            let deparsed = node.deparse_raw().unwrap();
            assert_eq!(deparsed, query);
            return;
        }
    }
    panic!("SelectStmt node not found");
}

/// Test that deparse_raw method produces same result as deparse method
#[test]
fn it_deparse_raw_matches_deparse() {
    let queries = vec![
        "SELECT 1",
        "SELECT * FROM users",
        "INSERT INTO t (a) VALUES (1)",
        "UPDATE t SET a = 1 WHERE b = 2",
        "DELETE FROM t WHERE id = 1",
        "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.x > 5 ORDER BY a",
    ];

    for query in queries {
        let result = pg_query::parse(query).unwrap();
        let deparse_result = result.deparse().unwrap();
        let deparse_raw_result = result.deparse_raw().unwrap();
        assert_eq!(deparse_result, deparse_raw_result);
    }
}
