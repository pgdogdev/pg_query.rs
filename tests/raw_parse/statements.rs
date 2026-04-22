//! Utility statement tests: transactions, VACUUM, SET/SHOW, LOCK, DO, LISTEN, etc.
//!
//! These tests verify parse_raw correctly handles utility statements.

use super::*;

// ============================================================================
// Transaction and utility statements
// ============================================================================

/// Test EXPLAIN
#[test]
fn it_parses_explain() {
    let query = "EXPLAIN SELECT * FROM users WHERE id = 1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test EXPLAIN ANALYZE
#[test]
fn it_parses_explain_analyze() {
    let query = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test COPY
#[test]
fn it_parses_copy() {
    let query = "COPY users (id, name, email) FROM STDIN WITH (FORMAT csv, HEADER true)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test PREPARE
#[test]
fn it_parses_prepare() {
    let query = "PREPARE user_by_id (int) AS SELECT * FROM users WHERE id = $1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test EXECUTE
#[test]
fn it_parses_execute() {
    let query = "EXECUTE user_by_id(42)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DEALLOCATE
#[test]
fn it_parses_deallocate() {
    let query = "DEALLOCATE user_by_id";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test BEGIN with options
#[test]
fn it_parses_begin_with_options() {
    let query = "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test COMMIT transaction
#[test]
fn it_parses_commit() {
    let query = "COMMIT";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ROLLBACK transaction
#[test]
fn it_parses_rollback() {
    let query = "ROLLBACK";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test START TRANSACTION
#[test]
fn it_parses_start_transaction() {
    let query = "START TRANSACTION";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SAVEPOINT
#[test]
fn it_parses_savepoint() {
    let query = "SAVEPOINT my_savepoint";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ROLLBACK TO SAVEPOINT
#[test]
fn it_parses_rollback_to_savepoint() {
    let query = "ROLLBACK TO SAVEPOINT my_savepoint";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RELEASE SAVEPOINT
#[test]
fn it_parses_release_savepoint() {
    let query = "RELEASE SAVEPOINT my_savepoint";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM with table
#[test]
fn it_parses_vacuum_table() {
    let query = "VACUUM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM ANALYZE
#[test]
fn it_parses_vacuum_analyze() {
    let query = "VACUUM ANALYZE users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test VACUUM FULL
#[test]
fn it_parses_vacuum_full() {
    let query = "VACUUM FULL users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE
#[test]
fn it_parses_analyze() {
    let query = "ANALYZE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE with table
#[test]
fn it_parses_analyze_table() {
    let query = "ANALYZE users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANALYZE with column list
#[test]
fn it_parses_analyze_columns() {
    let query = "ANALYZE users (id, name)";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET with equals
#[test]
fn it_parses_set_equals() {
    let query = "SET statement_timeout = 5000";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET LOCAL
#[test]
fn it_parses_set_local() {
    let query = "SET LOCAL search_path TO myschema";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SET SESSION
#[test]
fn it_parses_set_session() {
    let query = "SET SESSION timezone = 'UTC'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RESET
#[test]
fn it_parses_reset() {
    let query = "RESET search_path";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test RESET ALL
#[test]
fn it_parses_reset_all() {
    let query = "RESET ALL";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SHOW statement
#[test]
fn it_parses_show() {
    let query = "SHOW search_path";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SHOW ALL
#[test]
fn it_parses_show_all() {
    let query = "SHOW ALL";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NOTIFY statement
#[test]
fn it_parses_notify() {
    let query = "NOTIFY my_channel";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NOTIFY with payload
#[test]
fn it_parses_notify_with_payload() {
    let query = "NOTIFY my_channel, 'hello world'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UNLISTEN statement
#[test]
fn it_parses_unlisten() {
    let query = "UNLISTEN my_channel";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UNLISTEN *
#[test]
fn it_parses_unlisten_all() {
    let query = "UNLISTEN *";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD PLANS
#[test]
fn it_parses_discard_plans() {
    let query = "DISCARD PLANS";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD SEQUENCES
#[test]
fn it_parses_discard_sequences() {
    let query = "DISCARD SEQUENCES";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DISCARD TEMP
#[test]
fn it_parses_discard_temp() {
    let query = "DISCARD TEMP";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LOCK multiple tables
#[test]
fn it_parses_lock_multiple_tables() {
    let query = "LOCK TABLE users, orders IN SHARE MODE";
    let raw_result = parse_raw(query).unwrap();
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
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DO statement with language
#[test]
fn it_parses_do_with_language() {
    let query = "DO LANGUAGE plpgsql $$ BEGIN NULL; END $$";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// CALL statement tests (stored procedures)
// ============================================================================

/// Test CALL
#[test]
fn it_parses_call() {
    let query = "CALL transfer(1, 2, 100.00)";
    parse_test!(query);
}

/// Test CALL with no args
#[test]
fn it_parses_call_no_args() {
    let query = "CALL cleanup()";
    parse_test!(query);
}

/// Test CALL with named args
#[test]
fn it_parses_call_named_args() {
    let query = "CALL transfer(sender => 1, receiver => 2, amount => 100.00)";
    parse_test!(query);
}

// ============================================================================
// Cursor statement tests
// ============================================================================

/// Test DECLARE CURSOR
#[test]
fn it_parses_declare_cursor() {
    let query = "DECLARE mycursor CURSOR FOR SELECT * FROM users";
    parse_test!(query);
}

/// Test DECLARE CURSOR with options
#[test]
fn it_parses_declare_cursor_options() {
    let query = "DECLARE mycursor NO SCROLL CURSOR WITH HOLD FOR SELECT * FROM users";
    parse_test!(query);
}

/// Test FETCH
#[test]
fn it_parses_fetch() {
    let query = "FETCH NEXT FROM mycursor";
    parse_test!(query);
}

/// Test FETCH with count
#[test]
fn it_parses_fetch_count() {
    let query = "FETCH 5 FROM mycursor";
    parse_test!(query);
}

/// Test FETCH ALL
#[test]
fn it_parses_fetch_all() {
    let query = "FETCH ALL FROM mycursor";
    parse_test!(query);
}

/// Test MOVE
#[test]
fn it_parses_move() {
    let query = "MOVE 10 IN mycursor";
    parse_test!(query);
}

/// Test CLOSE cursor
#[test]
fn it_parses_close_cursor() {
    let query = "CLOSE mycursor";
    parse_test!(query);
}

/// Test CLOSE ALL
#[test]
fn it_parses_close_all() {
    let query = "CLOSE ALL";
    parse_test!(query);
}

// ============================================================================
// More transaction tests
// ============================================================================

/// Test BEGIN TRANSACTION READ ONLY DEFERRABLE
#[test]
fn it_parses_begin_read_only_deferrable() {
    let query = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE";
    parse_test!(query);
}

/// Test PREPARE TRANSACTION
#[test]
fn it_parses_prepare_transaction() {
    let query = "PREPARE TRANSACTION 'my_txn'";
    parse_test!(query);
}

/// Test COMMIT PREPARED
#[test]
fn it_parses_commit_prepared() {
    let query = "COMMIT PREPARED 'my_txn'";
    parse_test!(query);
}

/// Test ROLLBACK PREPARED
#[test]
fn it_parses_rollback_prepared() {
    let query = "ROLLBACK PREPARED 'my_txn'";
    parse_test!(query);
}

/// Test COMMIT AND CHAIN
#[test]
fn it_parses_commit_and_chain() {
    let query = "COMMIT AND CHAIN";
    parse_test!(query);
}

// ============================================================================
// Additional VACUUM tests
// ============================================================================

/// Test VACUUM multiple tables
#[test]
fn it_parses_vacuum_multiple() {
    let query = "VACUUM users, orders";
    parse_test!(query);
}

/// Test VACUUM with options
#[test]
fn it_parses_vacuum_with_options() {
    let query = "VACUUM (VERBOSE, ANALYZE, PARALLEL 4) users";
    parse_test!(query);
}

// ============================================================================
// SET with variable list
// ============================================================================

/// Test SET timezone with literal
#[test]
fn it_parses_set_timezone() {
    let query = "SET TIME ZONE 'UTC'";
    parse_test!(query);
}

/// Test SET variable multiple values
#[test]
fn it_parses_set_variable_multi() {
    let query = "SET search_path TO public, extensions";
    parse_test!(query);
}

/// Test SET ROLE
#[test]
fn it_parses_set_role() {
    let query = "SET ROLE admin";
    parse_test!(query);
}

/// Test SET SESSION AUTHORIZATION
#[test]
fn it_parses_set_session_authorization() {
    let query = "SET SESSION AUTHORIZATION bob";
    parse_test!(query);
}

/// Test SET TRANSACTION
#[test]
fn it_parses_set_transaction() {
    let query = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    parse_test!(query);
}

/// Test SHOW TIMEZONE
#[test]
fn it_parses_show_timezone() {
    let query = "SHOW TIME ZONE";
    parse_test!(query);
}

// ============================================================================
// SCHEMA tests
// ============================================================================

/// Test CREATE SCHEMA
#[test]
fn it_parses_create_schema() {
    let query = "CREATE SCHEMA IF NOT EXISTS myapp AUTHORIZATION bob";
    parse_test!(query);
}

/// Test CREATE SCHEMA with statements
#[test]
fn it_parses_create_schema_with_statements() {
    let query = "CREATE SCHEMA hollywood CREATE TABLE films (title text, release date, awards text[]) CREATE VIEW winners AS SELECT title, release FROM films WHERE awards IS NOT NULL";
    parse_test!(query);
}

/// Test DROP SCHEMA
#[test]
fn it_parses_drop_schema() {
    let query = "DROP SCHEMA IF EXISTS myapp CASCADE";
    parse_test!(query);
}

// ============================================================================
// EXPLAIN variations
// ============================================================================

/// Test EXPLAIN VERBOSE
#[test]
fn it_parses_explain_verbose() {
    let query = "EXPLAIN VERBOSE SELECT * FROM users";
    parse_test!(query);
}

/// Test EXPLAIN with INSERT
#[test]
fn it_parses_explain_insert() {
    let query = "EXPLAIN INSERT INTO users (name) VALUES ('bob')";
    parse_test!(query);
}

/// Test EXPLAIN with UPDATE
#[test]
fn it_parses_explain_update() {
    let query = "EXPLAIN UPDATE users SET name = 'bob' WHERE id = 1";
    parse_test!(query);
}

// ============================================================================
// COPY variations
// ============================================================================

/// Test COPY to stdout
#[test]
fn it_parses_copy_to_stdout() {
    let query = "COPY users TO STDOUT WITH (FORMAT csv, HEADER true)";
    parse_test!(query);
}

/// Test COPY from file
#[test]
fn it_parses_copy_from_file() {
    let query = "COPY users FROM '/tmp/users.csv' WITH (FORMAT csv, HEADER)";
    parse_test!(query);
}

/// Test COPY query
#[test]
fn it_parses_copy_query() {
    let query = "COPY (SELECT id, name FROM users WHERE active) TO STDOUT";
    parse_test!(query);
}
