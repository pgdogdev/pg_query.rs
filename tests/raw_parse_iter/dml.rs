//! DML statement tests (INSERT, UPDATE, DELETE).
//!
//! These tests verify parse_raw_iter_iter correctly handles data manipulation language statements.

use super::*;

// ============================================================================
// Basic DML tests
// ============================================================================

/// Test parsing INSERT statement
#[test]
fn it_parses_insert() {
    let query = "INSERT INTO users (name) VALUES ('test')";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap(), query);

    let mut raw_tables = raw_result.dml_tables();
    let mut proto_tables = proto_result.dml_tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test parsing UPDATE statement
#[test]
fn it_parses_update() {
    let query = "UPDATE users SET name = 'bob' WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap(), query);

    let mut raw_tables = raw_result.dml_tables();
    let mut proto_tables = proto_result.dml_tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test parsing DELETE statement
#[test]
fn it_parses_delete() {
    let query = "DELETE FROM users WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap(), query);

    let mut raw_tables = raw_result.dml_tables();
    let mut proto_tables = proto_result.dml_tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

// ============================================================================
// INSERT variations
// ============================================================================

/// Test parsing INSERT with ON CONFLICT
#[test]
fn it_parses_insert_on_conflict() {
    let query = "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    // PostgreSQL's deparser normalizes EXCLUDED to lowercase, so compare case-insensitively
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap().to_lowercase(), query.to_lowercase());

    let raw_tables = raw_result.dml_tables();
    let proto_tables = proto_result.dml_tables();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test parsing INSERT with RETURNING
#[test]
fn it_parses_insert_returning() {
    let query = "INSERT INTO users (name) VALUES ('test') RETURNING id";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap(), query);
}

/// Test INSERT with multiple tuples
#[test]
fn it_parses_insert_multiple_rows() {
    let query = "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 25), ('Bob', 'bob@example.com', 30), ('Charlie', 'charlie@example.com', 35)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert_eq!(deparse_raw_iter(&raw_result.protobuf).unwrap(), query);
}

/// Test INSERT ... SELECT
#[test]
fn it_parses_insert_select() {
    let query = "INSERT INTO archived_users (id, name, email) SELECT id, name, email FROM users WHERE deleted_at IS NOT NULL";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT ... SELECT with complex query
#[test]
fn it_parses_insert_select_complex() {
    let query = "INSERT INTO monthly_stats (month, user_count, order_count, total_revenue)
        SELECT date_trunc('month', created_at) AS month,
               COUNT(DISTINCT user_id),
               COUNT(*),
               SUM(amount)
        FROM orders
        WHERE created_at >= '2023-01-01'
        GROUP BY date_trunc('month', created_at)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with CTE
#[test]
fn it_parses_insert_with_cte() {
    let query = "WITH new_data AS (
        SELECT name, email FROM temp_imports WHERE valid = true
    )
    INSERT INTO users (name, email) SELECT name, email FROM new_data";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with DEFAULT values
#[test]
fn it_parses_insert_default_values() {
    let query = "INSERT INTO users (name, created_at) VALUES ('test', DEFAULT)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with ON CONFLICT DO NOTHING
#[test]
fn it_parses_insert_on_conflict_do_nothing() {
    let query = "INSERT INTO users (id, name) VALUES (1, 'test') ON CONFLICT (id) DO NOTHING";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with ON CONFLICT with WHERE clause
#[test]
fn it_parses_insert_on_conflict_with_where() {
    let query = "INSERT INTO users (id, name, updated_at) VALUES (1, 'test', NOW())
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = EXCLUDED.updated_at
        WHERE users.updated_at < EXCLUDED.updated_at";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with multiple columns in ON CONFLICT
#[test]
fn it_parses_insert_on_conflict_multiple_columns() {
    let query = "INSERT INTO user_settings (user_id, key, value) VALUES (1, 'theme', 'dark')
        ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with RETURNING multiple columns
#[test]
fn it_parses_insert_returning_multiple() {
    let query = "INSERT INTO users (name, email) VALUES ('test', 'test@example.com') RETURNING id, created_at, name";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with subquery in VALUES
#[test]
fn it_parses_insert_with_subquery_value() {
    let query = "INSERT INTO orders (user_id, total) VALUES ((SELECT id FROM users WHERE email = 'test@example.com'), 100.00)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test INSERT with OVERRIDING
#[test]
fn it_parses_insert_overriding() {
    let query = "INSERT INTO users (id, name) OVERRIDING SYSTEM VALUE VALUES (1, 'test')";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Complex UPDATE tests
// ============================================================================

/// Test UPDATE with multiple columns
#[test]
fn it_parses_update_multiple_columns() {
    let query = "UPDATE users SET name = 'new_name', email = 'new@example.com', updated_at = NOW() WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with subquery in SET
#[test]
fn it_parses_update_with_subquery_set() {
    let query = "UPDATE orders SET total = (SELECT SUM(price * quantity) FROM order_items WHERE order_id = orders.id) WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with FROM clause (PostgreSQL-specific JOIN update)
#[test]
fn it_parses_update_from() {
    let query = "UPDATE orders o SET status = 'shipped', shipped_at = NOW()
        FROM shipments s
        WHERE o.id = s.order_id AND s.status = 'delivered'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with FROM and multiple tables
#[test]
fn it_parses_update_from_multiple_tables() {
    let query = "UPDATE products p SET price = p.price * (1 + d.percentage / 100)
        FROM discounts d
        JOIN categories c ON d.category_id = c.id
        WHERE p.category_id = c.id AND d.active = true";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with CTE
#[test]
fn it_parses_update_with_cte() {
    let query = "WITH inactive_users AS (
        SELECT id FROM users WHERE last_login < NOW() - INTERVAL '1 year'
    )
    UPDATE users SET status = 'inactive' WHERE id IN (SELECT id FROM inactive_users)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with RETURNING
#[test]
fn it_parses_update_returning() {
    let query = "UPDATE users SET name = 'updated' WHERE id = 1 RETURNING id, name, updated_at";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with complex WHERE clause
#[test]
fn it_parses_update_complex_where() {
    let query = "UPDATE orders SET status = 'cancelled'
        WHERE created_at < NOW() - INTERVAL '30 days'
        AND status = 'pending'
        AND NOT EXISTS (SELECT 1 FROM payments WHERE payments.order_id = orders.id)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with row value comparison
#[test]
fn it_parses_update_row_comparison() {
    let query = "UPDATE users SET (name, email) = ('new_name', 'new@example.com') WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with CASE expression
#[test]
fn it_parses_update_with_case() {
    let query = "UPDATE products SET price = CASE
        WHEN category = 'electronics' THEN price * 0.9
        WHEN category = 'clothing' THEN price * 0.8
        ELSE price * 0.95
        END
        WHERE sale_active = true";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE with array operations
#[test]
fn it_parses_update_array() {
    let query = "UPDATE users SET tags = array_append(tags, 'verified') WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Complex DELETE tests
// ============================================================================

/// Test DELETE with subquery in WHERE
#[test]
fn it_parses_delete_with_subquery() {
    let query = "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE status = 'deleted')";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with USING clause (PostgreSQL-specific JOIN delete)
#[test]
fn it_parses_delete_using() {
    let query = "DELETE FROM order_items oi USING orders o
        WHERE oi.order_id = o.id AND o.status = 'cancelled'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with USING and multiple tables
#[test]
fn it_parses_delete_using_multiple_tables() {
    let query = "DELETE FROM notifications n
        USING users u, user_settings s
        WHERE n.user_id = u.id
        AND u.id = s.user_id
        AND s.key = 'email_notifications'
        AND s.value = 'false'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with CTE
#[test]
fn it_parses_delete_with_cte() {
    let query = "WITH old_orders AS (
        SELECT id FROM orders WHERE created_at < NOW() - INTERVAL '5 years'
    )
    DELETE FROM order_items WHERE order_id IN (SELECT id FROM old_orders)";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with RETURNING
#[test]
fn it_parses_delete_returning() {
    let query = "DELETE FROM users WHERE id = 1 RETURNING id, name, email";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with EXISTS
#[test]
fn it_parses_delete_with_exists() {
    let query = "DELETE FROM products p
        WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.product_id = p.id)
        AND p.created_at < NOW() - INTERVAL '1 year'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with complex boolean conditions
#[test]
fn it_parses_delete_complex_conditions() {
    let query = "DELETE FROM logs
        WHERE (level = 'debug' AND created_at < NOW() - INTERVAL '7 days')
        OR (level = 'info' AND created_at < NOW() - INTERVAL '30 days')
        OR (level IN ('warning', 'error') AND created_at < NOW() - INTERVAL '90 days')";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE with ONLY
#[test]
fn it_parses_delete_only() {
    let query = "DELETE FROM ONLY parent_table WHERE id = 1";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// Combined DML with CTEs
// ============================================================================

/// Test data modification CTE (INSERT in CTE)
#[test]
fn it_parses_insert_cte_returning() {
    let query = "WITH inserted AS (
        INSERT INTO users (name, email) VALUES ('test', 'test@example.com') RETURNING id, name
    )
    SELECT * FROM inserted";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UPDATE in CTE with final SELECT
#[test]
fn it_parses_update_cte_returning() {
    let query = "WITH updated AS (
        UPDATE users SET last_login = NOW() WHERE id = 1 RETURNING id, name, last_login
    )
    SELECT * FROM updated";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test DELETE in CTE with final SELECT
#[test]
fn it_parses_delete_cte_returning() {
    let query = "WITH deleted AS (
        DELETE FROM expired_sessions WHERE expires_at < NOW() RETURNING user_id
    )
    SELECT COUNT(*) FROM deleted";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test chained CTEs with multiple DML operations
#[test]
fn it_parses_chained_dml_ctes() {
    let query = "WITH
        to_archive AS (
            SELECT id FROM users WHERE last_login < NOW() - INTERVAL '2 years'
        ),
        archived AS (
            INSERT INTO archived_users SELECT * FROM users WHERE id IN (SELECT id FROM to_archive) RETURNING id
        ),
        deleted AS (
            DELETE FROM users WHERE id IN (SELECT id FROM archived) RETURNING id
        )
        SELECT COUNT(*) as archived_count FROM deleted";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
