//! Utility statement tests: transactions, VACUUM, SET/SHOW, LOCK, DO, LISTEN, etc.
//!
//! These tests verify parse_raw_iter_iter correctly handles utility statements.

use super::*;

// ============================================================================
// Transaction and utility statements
// ============================================================================

/// Test EXPLAIN
#[test]
fn it_parses_explain() {
    let query = "EXPLAIN SELECT * FROM users WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test EXPLAIN ANALYZE
#[test]
fn it_parses_explain_analyze() {
    let query = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test COPY
#[test]
fn it_parses_copy() {
    let query = "COPY users (id, name, email) FROM STDIN WITH (FORMAT csv, HEADER true)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test PREPARE
#[test]
fn it_parses_prepare() {
    let query = "PREPARE user_by_id (int) AS SELECT * FROM users WHERE id = $1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test EXECUTE
#[test]
fn it_parses_execute() {
    let query = "EXECUTE user_by_id(42)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DEALLOCATE
#[test]
fn it_parses_deallocate() {
    let query = "DEALLOCATE user_by_id";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Transaction statement tests
// ============================================================================

/// Test BEGIN transaction
#[test]
fn it_parses_begin() {
    let query = "BEGIN";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test BEGIN with options
#[test]
fn it_parses_begin_with_options() {
    let query = "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test COMMIT transaction
#[test]
fn it_parses_commit() {
    let query = "COMMIT";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ROLLBACK transaction
#[test]
fn it_parses_rollback() {
    let query = "ROLLBACK";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test START TRANSACTION
#[test]
fn it_parses_start_transaction() {
    let query = "START TRANSACTION";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SAVEPOINT
#[test]
fn it_parses_savepoint() {
    let query = "SAVEPOINT my_savepoint";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ROLLBACK TO SAVEPOINT
#[test]
fn it_parses_rollback_to_savepoint() {
    let query = "ROLLBACK TO SAVEPOINT my_savepoint";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RELEASE SAVEPOINT
#[test]
fn it_parses_release_savepoint() {
    let query = "RELEASE SAVEPOINT my_savepoint";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// VACUUM and ANALYZE statement tests
// ============================================================================

/// Test VACUUM
#[test]
fn it_parses_vacuum() {
    let query = "VACUUM";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM with table
#[test]
fn it_parses_vacuum_table() {
    let query = "VACUUM users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM ANALYZE
#[test]
fn it_parses_vacuum_analyze() {
    let query = "VACUUM ANALYZE users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM FULL
#[test]
fn it_parses_vacuum_full() {
    let query = "VACUUM FULL users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE
#[test]
fn it_parses_analyze() {
    let query = "ANALYZE";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE with table
#[test]
fn it_parses_analyze_table() {
    let query = "ANALYZE users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE with column list
#[test]
fn it_parses_analyze_columns() {
    let query = "ANALYZE users (id, name)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// SET and SHOW statement tests
// ============================================================================

/// Test SET statement
#[test]
fn it_parses_set() {
    let query = "SET search_path TO public";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET with equals
#[test]
fn it_parses_set_equals() {
    let query = "SET statement_timeout = 5000";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET LOCAL
#[test]
fn it_parses_set_local() {
    let query = "SET LOCAL search_path TO myschema";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET SESSION
#[test]
fn it_parses_set_session() {
    let query = "SET SESSION timezone = 'UTC'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RESET
#[test]
fn it_parses_reset() {
    let query = "RESET search_path";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RESET ALL
#[test]
fn it_parses_reset_all() {
    let query = "RESET ALL";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SHOW statement
#[test]
fn it_parses_show() {
    let query = "SHOW search_path";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SHOW ALL
#[test]
fn it_parses_show_all() {
    let query = "SHOW ALL";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// LISTEN, NOTIFY, UNLISTEN statement tests
// ============================================================================

/// Test LISTEN statement
#[test]
fn it_parses_listen() {
    let query = "LISTEN my_channel";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NOTIFY statement
#[test]
fn it_parses_notify() {
    let query = "NOTIFY my_channel";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NOTIFY with payload
#[test]
fn it_parses_notify_with_payload() {
    let query = "NOTIFY my_channel, 'hello world'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UNLISTEN statement
#[test]
fn it_parses_unlisten() {
    let query = "UNLISTEN my_channel";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UNLISTEN *
#[test]
fn it_parses_unlisten_all() {
    let query = "UNLISTEN *";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// DISCARD statement tests
// ============================================================================

/// Test DISCARD ALL
#[test]
fn it_parses_discard_all() {
    let query = "DISCARD ALL";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD PLANS
#[test]
fn it_parses_discard_plans() {
    let query = "DISCARD PLANS";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD SEQUENCES
#[test]
fn it_parses_discard_sequences() {
    let query = "DISCARD SEQUENCES";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD TEMP
#[test]
fn it_parses_discard_temp() {
    let query = "DISCARD TEMP";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// LOCK statement tests
// ============================================================================

/// Test LOCK TABLE
#[test]
fn it_parses_lock_table() {
    let query = "LOCK TABLE users IN ACCESS EXCLUSIVE MODE";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LOCK multiple tables
#[test]
fn it_parses_lock_multiple_tables() {
    let query = "LOCK TABLE users, orders IN SHARE MODE";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// DO statement tests
// ============================================================================

/// Test DO statement
#[test]
fn it_parses_do_statement() {
    let query = "DO $$ BEGIN RAISE NOTICE 'Hello'; END $$";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DO statement with language
#[test]
fn it_parses_do_with_language() {
    let query = "DO LANGUAGE plpgsql $$ BEGIN NULL; END $$";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
