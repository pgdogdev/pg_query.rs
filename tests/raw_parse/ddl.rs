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

// ============================================================================
// Role tests (CREATE/ALTER/DROP ROLE/USER/GROUP)
// ============================================================================

/// Test CREATE ROLE
#[test]
fn it_parses_create_role() {
    let query = "CREATE ROLE admin LOGIN PASSWORD 'secret'";
    parse_test!(query);
}

/// Test CREATE USER (alias)
#[test]
fn it_parses_create_user() {
    let query = "CREATE USER bob WITH PASSWORD 'pw' CREATEDB";
    parse_test!(query);
}

/// Test CREATE GROUP
#[test]
fn it_parses_create_group() {
    let query = "CREATE GROUP reporters";
    parse_test!(query);
}

/// Test CREATE ROLE with options
#[test]
fn it_parses_create_role_with_options() {
    let query = "CREATE ROLE app_user WITH LOGIN CREATEROLE CREATEDB NOINHERIT CONNECTION LIMIT 10 VALID UNTIL '2099-12-31'";
    parse_test!(query);
}

/// Test ALTER ROLE
#[test]
fn it_parses_alter_role() {
    let query = "ALTER ROLE bob WITH PASSWORD 'new_secret'";
    parse_test!(query);
}

/// Test ALTER ROLE SET
#[test]
fn it_parses_alter_role_set() {
    let query = "ALTER ROLE bob SET search_path TO public";
    parse_test!(query);
}

/// Test ALTER ROLE RENAME
#[test]
fn it_parses_alter_role_rename() {
    let query = "ALTER ROLE bob RENAME TO robert";
    parse_test!(query);
}

/// Test DROP ROLE
#[test]
fn it_parses_drop_role() {
    let query = "DROP ROLE IF EXISTS bob";
    parse_test!(query);
}

/// Test DROP USER
#[test]
fn it_parses_drop_user() {
    let query = "DROP USER bob";
    parse_test!(query);
}

// ============================================================================
// GRANT / REVOKE tests
// ============================================================================

/// Test GRANT on table
#[test]
fn it_parses_grant_on_table() {
    let query = "GRANT SELECT, INSERT ON users TO bob";
    parse_test!(query);
}

/// Test GRANT ALL
#[test]
fn it_parses_grant_all() {
    let query = "GRANT ALL PRIVILEGES ON users TO bob WITH GRANT OPTION";
    parse_test!(query);
}

/// Test GRANT on schema
#[test]
fn it_parses_grant_on_schema() {
    let query = "GRANT USAGE ON SCHEMA public TO bob";
    parse_test!(query);
}

/// Test GRANT on sequence
#[test]
fn it_parses_grant_on_sequence() {
    let query = "GRANT USAGE, SELECT ON SEQUENCE my_seq TO bob";
    parse_test!(query);
}

/// Test GRANT on function
#[test]
fn it_parses_grant_on_function() {
    let query = "GRANT EXECUTE ON FUNCTION my_func(int) TO bob";
    parse_test!(query);
}

/// Test GRANT on all tables in schema
#[test]
fn it_parses_grant_all_tables_in_schema() {
    let query = "GRANT SELECT ON ALL TABLES IN SCHEMA public TO bob";
    parse_test!(query);
}

/// Test REVOKE
#[test]
fn it_parses_revoke() {
    let query = "REVOKE INSERT ON users FROM bob";
    parse_test!(query);
}

/// Test REVOKE CASCADE
#[test]
fn it_parses_revoke_cascade() {
    let query = "REVOKE ALL ON users FROM bob CASCADE";
    parse_test!(query);
}

/// Test GRANT role
#[test]
fn it_parses_grant_role() {
    let query = "GRANT admin TO bob";
    parse_test!(query);
}

/// Test REVOKE role
#[test]
fn it_parses_revoke_role() {
    let query = "REVOKE admin FROM bob";
    parse_test!(query);
}

// ============================================================================
// Policy tests
// ============================================================================

/// Test CREATE POLICY
#[test]
fn it_parses_create_policy() {
    let query = "CREATE POLICY user_isolation ON users FOR SELECT USING (user_id = current_user_id())";
    parse_test!(query);
}

/// Test CREATE POLICY with check
#[test]
fn it_parses_create_policy_with_check() {
    let query =
        "CREATE POLICY user_isolation ON users FOR ALL TO app_user USING (user_id = current_user_id()) WITH CHECK (user_id = current_user_id())";
    parse_test!(query);
}

/// Test ALTER POLICY
#[test]
fn it_parses_alter_policy() {
    let query = "ALTER POLICY user_isolation ON users RENAME TO row_security";
    parse_test!(query);
}

/// Test ALTER POLICY with USING
#[test]
fn it_parses_alter_policy_using() {
    let query = "ALTER POLICY user_isolation ON users USING (user_id = current_user_id())";
    parse_test!(query);
}

// ============================================================================
// Event Trigger tests
// ============================================================================

/// Test CREATE EVENT TRIGGER
#[test]
fn it_parses_create_event_trigger() {
    let query = "CREATE EVENT TRIGGER audit_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl()";
    parse_test!(query);
}

/// Test ALTER EVENT TRIGGER
#[test]
fn it_parses_alter_event_trigger() {
    let query = "ALTER EVENT TRIGGER audit_ddl DISABLE";
    parse_test!(query);
}

// ============================================================================
// Comment and security label
// ============================================================================

/// Test COMMENT ON TABLE
#[test]
fn it_parses_comment_on_table() {
    let query = "COMMENT ON TABLE users IS 'Stores users'";
    parse_test!(query);
}

/// Test COMMENT ON COLUMN
#[test]
fn it_parses_comment_on_column() {
    let query = "COMMENT ON COLUMN users.email IS 'Unique user email'";
    parse_test!(query);
}

/// Test COMMENT NULL to remove
#[test]
fn it_parses_comment_null() {
    let query = "COMMENT ON TABLE users IS NULL";
    parse_test!(query);
}

/// Test SECURITY LABEL
#[test]
fn it_parses_security_label() {
    let query = "SECURITY LABEL FOR selinux ON TABLE users IS 'system_u:object_r:sepgsql_table_t:s0'";
    parse_test!(query);
}

// ============================================================================
// CREATE RULE tests
// ============================================================================

/// Test CREATE RULE with DO INSTEAD
#[test]
fn it_parses_create_rule() {
    let query = "CREATE RULE notify_me AS ON UPDATE TO users DO ALSO NOTIFY user_updated";
    parse_test!(query);
}

/// Test CREATE RULE with DO INSTEAD NOTHING
#[test]
fn it_parses_create_rule_nothing() {
    let query = "CREATE RULE protect AS ON DELETE TO users DO INSTEAD NOTHING";
    parse_test!(query);
}

// ============================================================================
// Extension management
// ============================================================================

/// Test ALTER EXTENSION UPDATE
#[test]
fn it_parses_alter_extension_update() {
    let query = "ALTER EXTENSION postgis UPDATE TO '3.4.0'";
    parse_test!(query);
}

/// Test ALTER EXTENSION ADD
#[test]
fn it_parses_alter_extension_add() {
    let query = "ALTER EXTENSION postgis ADD TABLE my_table";
    parse_test!(query);
}

/// Test DROP EXTENSION
#[test]
fn it_parses_drop_extension() {
    let query = "DROP EXTENSION IF EXISTS pg_stat_statements CASCADE";
    parse_test!(query);
}

// ============================================================================
// ALTER (various)
// ============================================================================

/// Test ALTER DOMAIN
#[test]
fn it_parses_alter_domain() {
    let query = "ALTER DOMAIN my_domain SET DEFAULT 'default_value'";
    parse_test!(query);
}

/// Test ALTER DOMAIN ADD CHECK
#[test]
fn it_parses_alter_domain_add_check() {
    let query = "ALTER DOMAIN my_domain ADD CONSTRAINT my_check CHECK (VALUE > 0)";
    parse_test!(query);
}

/// Test ALTER FUNCTION
#[test]
fn it_parses_alter_function() {
    let query = "ALTER FUNCTION my_func(int) IMMUTABLE";
    parse_test!(query);
}

/// Test ALTER TYPE ADD VALUE
#[test]
fn it_parses_alter_type_add_value() {
    let query = "ALTER TYPE mood ADD VALUE 'ecstatic'";
    parse_test!(query);
}

/// Test ALTER TYPE ADD VALUE BEFORE
#[test]
fn it_parses_alter_type_add_value_before() {
    let query = "ALTER TYPE mood ADD VALUE 'sad' BEFORE 'happy'";
    parse_test!(query);
}

/// Test ALTER TYPE RENAME ATTRIBUTE
#[test]
fn it_parses_alter_type_rename() {
    let query = "ALTER TYPE address RENAME ATTRIBUTE street TO street_address";
    parse_test!(query);
}

/// Test ALTER SCHEMA RENAME
#[test]
fn it_parses_alter_schema_rename() {
    let query = "ALTER SCHEMA old_schema RENAME TO new_schema";
    parse_test!(query);
}

/// Test ALTER INDEX RENAME
#[test]
fn it_parses_alter_index_rename() {
    let query = "ALTER INDEX idx_users_name RENAME TO idx_users_fullname";
    parse_test!(query);
}

/// Test ALTER TABLE SET SCHEMA
#[test]
fn it_parses_alter_table_set_schema() {
    let query = "ALTER TABLE users SET SCHEMA app";
    parse_test!(query);
}

/// Test ALTER DEFAULT PRIVILEGES
#[test]
fn it_parses_alter_default_privileges() {
    let query = "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bob";
    parse_test!(query);
}

// ============================================================================
// CREATE LANGUAGE, ACCESS METHOD, CAST, CONVERSION, etc.
// ============================================================================

/// Test CREATE LANGUAGE
#[test]
fn it_parses_create_language() {
    let query = "CREATE LANGUAGE plpythonu";
    parse_test!(query);
}

/// Test CREATE ACCESS METHOD
#[test]
fn it_parses_create_access_method() {
    let query = "CREATE ACCESS METHOD my_am TYPE INDEX HANDLER my_handler";
    parse_test!(query);
}

/// Test CREATE OPERATOR CLASS
#[test]
fn it_parses_create_op_class() {
    let query = "CREATE OPERATOR CLASS my_class DEFAULT FOR TYPE int USING btree AS OPERATOR 1 <, FUNCTION 1 btint4cmp(int4, int4)";
    parse_test!(query);
}

/// Test CREATE OPERATOR FAMILY
#[test]
fn it_parses_create_op_family() {
    let query = "CREATE OPERATOR FAMILY my_family USING btree";
    parse_test!(query);
}

/// Test ALTER OPERATOR FAMILY
#[test]
fn it_parses_alter_op_family() {
    let query = "ALTER OPERATOR FAMILY my_family USING btree ADD OPERATOR 1 <(int, int)";
    parse_test!(query);
}

/// Test CREATE CAST
#[test]
fn it_parses_create_cast() {
    let query = "CREATE CAST (int AS text) WITH FUNCTION int_to_text(int) AS IMPLICIT";
    parse_test!(query);
}

/// Test CREATE TRANSFORM
#[test]
fn it_parses_create_transform() {
    let query = "CREATE TRANSFORM FOR hstore LANGUAGE plpython3u (FROM SQL WITH FUNCTION hstore_to_plpython(internal), TO SQL WITH FUNCTION plpython_to_hstore(internal))";
    parse_test!(query);
}

/// Test CREATE CONVERSION
#[test]
fn it_parses_create_conversion() {
    let query = "CREATE CONVERSION my_conv FOR 'LATIN1' TO 'UTF8' FROM latin1_to_utf8";
    parse_test!(query);
}

// ============================================================================
// Aggregate and Operator definition (DefineStmt)
// ============================================================================

/// Test CREATE AGGREGATE
#[test]
fn it_parses_create_aggregate() {
    let query = "CREATE AGGREGATE my_sum (int) (SFUNC = int4_sum, STYPE = bigint, INITCOND = '0')";
    parse_test!(query);
}

/// Test CREATE OPERATOR
#[test]
fn it_parses_create_operator() {
    let query = "CREATE OPERATOR === (LEFTARG = int, RIGHTARG = int, FUNCTION = my_eq)";
    parse_test!(query);
}

// ============================================================================
// Foreign data tests
// ============================================================================

/// Test CREATE FOREIGN DATA WRAPPER
#[test]
fn it_parses_create_fdw() {
    let query = "CREATE FOREIGN DATA WRAPPER my_fdw HANDLER my_handler VALIDATOR my_validator";
    parse_test!(query);
}

/// Test ALTER FOREIGN DATA WRAPPER
#[test]
fn it_parses_alter_fdw() {
    let query = "ALTER FOREIGN DATA WRAPPER my_fdw OPTIONS (ADD debug 'true')";
    parse_test!(query);
}

/// Test CREATE SERVER
#[test]
fn it_parses_create_server() {
    let query = "CREATE SERVER my_server FOREIGN DATA WRAPPER my_fdw OPTIONS (host 'localhost', port '5432')";
    parse_test!(query);
}

/// Test ALTER SERVER
#[test]
fn it_parses_alter_server() {
    let query = "ALTER SERVER my_server OPTIONS (SET host 'remote_host')";
    parse_test!(query);
}

/// Test CREATE FOREIGN TABLE
#[test]
fn it_parses_create_foreign_table() {
    let query = "CREATE FOREIGN TABLE my_table (id int, name text) SERVER my_server OPTIONS (schema_name 'public', table_name 'remote_users')";
    parse_test!(query);
}

/// Test CREATE USER MAPPING
#[test]
fn it_parses_create_user_mapping() {
    let query = "CREATE USER MAPPING FOR current_user SERVER my_server OPTIONS (user 'bob', password 'secret')";
    parse_test!(query);
}

/// Test ALTER USER MAPPING
#[test]
fn it_parses_alter_user_mapping() {
    let query = "ALTER USER MAPPING FOR bob SERVER my_server OPTIONS (SET password 'new_secret')";
    parse_test!(query);
}

/// Test DROP USER MAPPING
#[test]
fn it_parses_drop_user_mapping() {
    let query = "DROP USER MAPPING FOR bob SERVER my_server";
    parse_test!(query);
}

/// Test IMPORT FOREIGN SCHEMA
#[test]
fn it_parses_import_foreign_schema() {
    let query = "IMPORT FOREIGN SCHEMA public FROM SERVER my_server INTO my_schema";
    parse_test!(query);
}

/// Test IMPORT FOREIGN SCHEMA with LIMIT TO
#[test]
fn it_parses_import_foreign_schema_limit() {
    let query = "IMPORT FOREIGN SCHEMA public LIMIT TO (users, orders) FROM SERVER my_server INTO my_schema";
    parse_test!(query);
}

// ============================================================================
// Tablespace tests
// ============================================================================

/// Test CREATE TABLESPACE
#[test]
fn it_parses_create_tablespace() {
    let query = "CREATE TABLESPACE fastspace LOCATION '/ssd/tablespaces/fastspace'";
    parse_test!(query);
}

/// Test DROP TABLESPACE
#[test]
fn it_parses_drop_tablespace() {
    let query = "DROP TABLESPACE IF EXISTS fastspace";
    parse_test!(query);
}

/// Test ALTER TABLESPACE
#[test]
fn it_parses_alter_tablespace() {
    let query = "ALTER TABLESPACE fastspace SET (random_page_cost = 1.0)";
    parse_test!(query);
}

// ============================================================================
// Database tests
// ============================================================================

/// Test CREATE DATABASE
#[test]
fn it_parses_create_database() {
    let query = "CREATE DATABASE mydb OWNER bob ENCODING 'UTF8'";
    parse_test!(query);
}

/// Test DROP DATABASE
#[test]
fn it_parses_drop_database() {
    let query = "DROP DATABASE IF EXISTS mydb";
    parse_test!(query);
}

/// Test ALTER DATABASE
#[test]
fn it_parses_alter_database() {
    let query = "ALTER DATABASE mydb SET search_path TO public, extensions";
    parse_test!(query);
}

/// Test ALTER DATABASE RENAME
#[test]
fn it_parses_alter_database_rename() {
    let query = "ALTER DATABASE mydb RENAME TO newdb";
    parse_test!(query);
}

// ============================================================================
// System administration
// ============================================================================

/// Test ALTER SYSTEM
#[test]
fn it_parses_alter_system() {
    let query = "ALTER SYSTEM SET max_connections = 200";
    parse_test!(query);
}

/// Test ALTER SYSTEM RESET
#[test]
fn it_parses_alter_system_reset() {
    let query = "ALTER SYSTEM RESET max_connections";
    parse_test!(query);
}

/// Test CHECKPOINT
#[test]
fn it_parses_checkpoint() {
    let query = "CHECKPOINT";
    parse_test!(query);
}

/// Test CLUSTER
#[test]
fn it_parses_cluster() {
    let query = "CLUSTER users USING idx_users_name";
    parse_test!(query);
}

/// Test CLUSTER without table
#[test]
fn it_parses_cluster_all() {
    let query = "CLUSTER";
    parse_test!(query);
}

/// Test REINDEX
#[test]
fn it_parses_reindex() {
    let query = "REINDEX INDEX idx_users_name";
    parse_test!(query);
}

/// Test REINDEX TABLE
#[test]
fn it_parses_reindex_table() {
    let query = "REINDEX TABLE users";
    parse_test!(query);
}

/// Test REINDEX CONCURRENTLY
#[test]
fn it_parses_reindex_concurrently() {
    let query = "REINDEX TABLE CONCURRENTLY users";
    parse_test!(query);
}

/// Test LOAD
#[test]
fn it_parses_load() {
    let query = "LOAD 'auto_explain'";
    parse_test!(query);
}

/// Test SET CONSTRAINTS
#[test]
fn it_parses_set_constraints() {
    let query = "SET CONSTRAINTS ALL DEFERRED";
    parse_test!(query);
}

/// Test DROP OWNED
#[test]
fn it_parses_drop_owned() {
    let query = "DROP OWNED BY bob CASCADE";
    parse_test!(query);
}

/// Test REASSIGN OWNED
#[test]
fn it_parses_reassign_owned() {
    let query = "REASSIGN OWNED BY bob TO alice";
    parse_test!(query);
}

// ============================================================================
// Statistics tests
// ============================================================================

/// Test CREATE STATISTICS
#[test]
fn it_parses_create_statistics() {
    let query = "CREATE STATISTICS my_stats (dependencies, ndistinct) ON col1, col2 FROM my_table";
    parse_test!(query);
}

/// Test ALTER STATISTICS
#[test]
fn it_parses_alter_statistics() {
    let query = "ALTER STATISTICS my_stats SET STATISTICS 100";
    parse_test!(query);
}

// ============================================================================
// Refresh materialized view
// ============================================================================

/// Test REFRESH MATERIALIZED VIEW
#[test]
fn it_parses_refresh_mat_view() {
    let query = "REFRESH MATERIALIZED VIEW monthly_sales";
    parse_test!(query);
}

/// Test REFRESH MATERIALIZED VIEW CONCURRENTLY
#[test]
fn it_parses_refresh_mat_view_concurrently() {
    let query = "REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_sales WITH DATA";
    parse_test!(query);
}

// ============================================================================
// Range type
// ============================================================================

/// Test CREATE TYPE AS RANGE
#[test]
fn it_parses_create_range_type() {
    let query = "CREATE TYPE float_range AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi)";
    parse_test!(query);
}

// ============================================================================
// Functions, procedures
// ============================================================================

/// Test CREATE FUNCTION
#[test]
fn it_parses_create_function() {
    let query = "CREATE FUNCTION add(int, int) RETURNS int AS $$ SELECT $1 + $2 $$ LANGUAGE sql IMMUTABLE";
    parse_test!(query);
}

/// Test CREATE OR REPLACE FUNCTION
#[test]
fn it_parses_create_or_replace_function() {
    let query = "CREATE OR REPLACE FUNCTION greet(name text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN 'Hello, ' || name; END; $$";
    parse_test!(query);
}

/// Test CREATE FUNCTION with OUT parameters
#[test]
fn it_parses_create_function_out_params() {
    let query = "CREATE FUNCTION split_name(full_name text, OUT first text, OUT last text) AS $$ SELECT 'a', 'b' $$ LANGUAGE sql";
    parse_test!(query);
}

/// Test CREATE PROCEDURE
#[test]
fn it_parses_create_procedure() {
    let query = "CREATE PROCEDURE transfer(sender int, receiver int, amount numeric) LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$";
    parse_test!(query);
}

/// Test DROP FUNCTION
#[test]
fn it_parses_drop_function() {
    let query = "DROP FUNCTION IF EXISTS my_func(int, text)";
    parse_test!(query);
}

// ============================================================================
// ALTER TABLE REPLICA IDENTITY (regressions)
// ============================================================================

/// Regression: ALTER TABLE ... REPLICA IDENTITY FULL used to SIGSEGV because
/// ReplicaIdentityStmt was in the null-returning arm of write_node_inner, and
/// the deparser then dereferenced a NULL pointer in AlterTableCmd.def.
#[test]
fn it_parses_alter_replica_identity_full() {
    parse_test!("ALTER TABLE t REPLICA IDENTITY FULL");
}

/// Regression: ALTER TABLE ... REPLICA IDENTITY DEFAULT.
#[test]
fn it_parses_alter_replica_identity_default() {
    parse_test!("ALTER TABLE t REPLICA IDENTITY DEFAULT");
}

/// Regression: ALTER TABLE ... REPLICA IDENTITY NOTHING.
#[test]
fn it_parses_alter_replica_identity_nothing() {
    parse_test!("ALTER TABLE t REPLICA IDENTITY NOTHING");
}

/// Regression: ALTER TABLE ... REPLICA IDENTITY USING INDEX.
#[test]
fn it_parses_alter_replica_identity_using_index() {
    parse_test!("ALTER TABLE t REPLICA IDENTITY USING INDEX my_idx");
}
