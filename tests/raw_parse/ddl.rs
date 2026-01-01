//! DDL statement tests (CREATE, ALTER, DROP, etc.).
//!
//! These tests verify parse_raw correctly handles data definition language statements.

use super::*;

// ============================================================================
// Basic DDL tests
// ============================================================================

/// Test parsing CREATE TABLE
#[test]
fn it_parses_create_table() {
    let query = "CREATE TABLE test (id int, name text)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(raw_result.statement_types(), proto_result.statement_types());
    assert_eq!(raw_result.statement_types(), vec!["CreateStmt"]);
}

/// Test parsing DROP TABLE
#[test]
fn it_parses_drop_table() {
    let query = "DROP TABLE users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.ddl_tables();
    let mut proto_tables = proto_result.ddl_tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test parsing CREATE INDEX
#[test]
fn it_parses_create_index() {
    let query = "CREATE INDEX idx_users_name ON users (name)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(raw_result.statement_types(), proto_result.statement_types());
    assert_eq!(raw_result.statement_types(), vec!["IndexStmt"]);
}

/// Test CREATE TABLE with constraints
#[test]
fn it_parses_create_table_with_constraints() {
    let query = "CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id),
        amount DECIMAL(10, 2) CHECK (amount > 0),
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE (user_id, created_at)
    )";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE TABLE AS
#[test]
fn it_parses_create_table_as() {
    let query = "CREATE TABLE active_users AS SELECT * FROM users WHERE active = true";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE VIEW
#[test]
fn it_parses_create_view() {
    let query = "CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE MATERIALIZED VIEW
#[test]
fn it_parses_create_materialized_view() {
    let query = "CREATE MATERIALIZED VIEW monthly_sales AS SELECT date_trunc('month', created_at) AS month, SUM(amount) FROM orders GROUP BY 1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// ALTER TABLE tests
// ============================================================================

/// Test ALTER TABLE ADD COLUMN
#[test]
fn it_parses_alter_table_add_column() {
    let query = "ALTER TABLE users ADD COLUMN email TEXT NOT NULL";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER TABLE DROP COLUMN
#[test]
fn it_parses_alter_table_drop_column() {
    let query = "ALTER TABLE users DROP COLUMN deprecated_field";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER TABLE ADD CONSTRAINT
#[test]
fn it_parses_alter_table_add_constraint() {
    let query = "ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER TABLE RENAME
#[test]
fn it_parses_alter_table_rename() {
    let query = "ALTER TABLE users RENAME TO customers";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER TABLE RENAME COLUMN
#[test]
fn it_parses_alter_table_rename_column() {
    let query = "ALTER TABLE users RENAME COLUMN name TO full_name";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER TABLE OWNER
#[test]
fn it_parses_alter_owner() {
    let query = "ALTER TABLE users OWNER TO postgres";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// INDEX tests
// ============================================================================

/// Test CREATE INDEX with expression
#[test]
fn it_parses_create_index_expression() {
    let query = "CREATE INDEX idx_lower_email ON users (lower(email))";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE UNIQUE INDEX with WHERE
#[test]
fn it_parses_partial_unique_index() {
    let query = "CREATE UNIQUE INDEX idx_active_email ON users (email) WHERE active = true";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE INDEX CONCURRENTLY
#[test]
fn it_parses_create_index_concurrently() {
    let query = "CREATE INDEX CONCURRENTLY idx_name ON users (name)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// TRUNCATE test
// ============================================================================

/// Test TRUNCATE
#[test]
fn it_parses_truncate() {
    let query = "TRUNCATE TABLE logs, audit_logs RESTART IDENTITY CASCADE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Sequence tests
// ============================================================================

/// Test CREATE SEQUENCE
#[test]
fn it_parses_create_sequence() {
    let query = "CREATE SEQUENCE my_seq";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE SEQUENCE with options
#[test]
fn it_parses_create_sequence_with_options() {
    let query = "CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 10 MINVALUE 1 MAXVALUE 1000 CYCLE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE SEQUENCE IF NOT EXISTS
#[test]
fn it_parses_create_sequence_if_not_exists() {
    let query = "CREATE SEQUENCE IF NOT EXISTS my_seq";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER SEQUENCE
#[test]
fn it_parses_alter_sequence() {
    let query = "ALTER SEQUENCE my_seq RESTART WITH 1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Domain tests
// ============================================================================

/// Test CREATE DOMAIN
#[test]
fn it_parses_create_domain() {
    let query = "CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE DOMAIN with NOT NULL
#[test]
fn it_parses_create_domain_not_null() {
    let query = "CREATE DOMAIN non_empty_text AS TEXT NOT NULL CHECK (VALUE <> '')";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE DOMAIN with DEFAULT
#[test]
fn it_parses_create_domain_default() {
    let query = "CREATE DOMAIN my_text AS TEXT DEFAULT 'unknown'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Type tests
// ============================================================================

/// Test CREATE TYPE AS composite
#[test]
fn it_parses_create_composite_type() {
    let query = "CREATE TYPE address AS (street TEXT, city TEXT, zip TEXT)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE TYPE AS ENUM
#[test]
fn it_parses_create_enum_type() {
    let query = "CREATE TYPE status AS ENUM ('pending', 'approved', 'rejected')";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Extension tests
// ============================================================================

/// Test CREATE EXTENSION
#[test]
fn it_parses_create_extension() {
    let query = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE EXTENSION with schema
#[test]
fn it_parses_create_extension_with_schema() {
    let query = "CREATE EXTENSION hstore WITH SCHEMA public";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Publication and Subscription tests
// ============================================================================

/// Test CREATE PUBLICATION
#[test]
fn it_parses_create_publication() {
    let query = "CREATE PUBLICATION my_pub FOR ALL TABLES";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE PUBLICATION for specific tables
#[test]
fn it_parses_create_publication_for_tables() {
    let query = "CREATE PUBLICATION my_pub FOR TABLE users, orders";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER PUBLICATION
#[test]
fn it_parses_alter_publication() {
    let query = "ALTER PUBLICATION my_pub ADD TABLE products";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE SUBSCRIPTION
#[test]
fn it_parses_create_subscription() {
    let query = "CREATE SUBSCRIPTION my_sub CONNECTION 'host=localhost dbname=mydb' PUBLICATION my_pub";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALTER SUBSCRIPTION
#[test]
fn it_parses_alter_subscription() {
    let query = "ALTER SUBSCRIPTION my_sub DISABLE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Trigger tests
// ============================================================================

/// Test CREATE TRIGGER
#[test]
fn it_parses_create_trigger() {
    let query = "CREATE TRIGGER my_trigger BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION my_func()";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE TRIGGER AFTER UPDATE
#[test]
fn it_parses_create_trigger_after_update() {
    let query = "CREATE TRIGGER audit_trigger AFTER UPDATE ON users FOR EACH ROW WHEN (OLD.* IS DISTINCT FROM NEW.*) EXECUTE FUNCTION audit_log()";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CREATE CONSTRAINT TRIGGER
#[test]
fn it_parses_create_constraint_trigger() {
    let query = "CREATE CONSTRAINT TRIGGER check_balance AFTER INSERT OR UPDATE ON accounts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_balance()";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
