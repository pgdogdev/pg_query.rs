//! Complex SELECT tests: JOINs, subqueries, CTEs, window functions, set operations.
//!
//! These tests verify parse_raw correctly handles complex SELECT statements.

use super::*;

// ============================================================================
// JOIN and complex SELECT tests
// ============================================================================

/// Test parsing SELECT with JOIN
#[test]
fn it_parses_join() {
    let query = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify tables are extracted correctly
    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["orders", "users"]);
}

/// Test parsing UNION query
#[test]
fn it_parses_union() {
    let query = "SELECT id FROM users UNION SELECT id FROM admins";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify tables from both sides of UNION
    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["admins", "users"]);
}

/// Test parsing WITH clause (CTE)
#[test]
fn it_parses_cte() {
    let query = "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify CTE names match
    assert_eq!(raw_result.cte_names, proto_result.cte_names);
    assert!(raw_result.cte_names.contains(&"active_users".to_string()));

    // Verify tables (should only include actual tables, not CTEs)
    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["users"]);
}

/// Test parsing subquery in SELECT
#[test]
fn it_parses_subquery() {
    let query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify all tables are found
    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["orders", "users"]);
}

/// Test parsing aggregate functions
#[test]
fn it_parses_aggregates() {
    let query = "SELECT count(*), sum(amount), avg(price) FROM orders";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify functions are extracted correctly
    let mut raw_funcs = raw_result.functions();
    let mut proto_funcs = proto_result.functions();
    raw_funcs.sort();
    proto_funcs.sort();
    assert_eq!(raw_funcs, proto_funcs);
    assert!(raw_funcs.contains(&"count".to_string()));
    assert!(raw_funcs.contains(&"sum".to_string()));
    assert!(raw_funcs.contains(&"avg".to_string()));
}

/// Test parsing CASE expression
#[test]
fn it_parses_case_expression() {
    let query = "SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify table is found
    let raw_tables = raw_result.tables();
    let proto_tables = proto_result.tables();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["t"]);
}

/// Test parsing complex SELECT with multiple clauses
#[test]
fn it_parses_complex_select() {
    let query = "SELECT u.id, u.name, count(*) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name HAVING count(*) > 0 ORDER BY order_count DESC LIMIT 10";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify tables
    let mut raw_tables = raw_result.tables();
    let mut proto_tables = proto_result.tables();
    raw_tables.sort();
    proto_tables.sort();
    assert_eq!(raw_tables, proto_tables);
    assert_eq!(raw_tables, vec!["orders", "users"]);

    // Verify functions
    let mut raw_funcs = raw_result.functions();
    let mut proto_funcs = proto_result.functions();
    raw_funcs.sort();
    proto_funcs.sort();
    assert_eq!(raw_funcs, proto_funcs);
    assert!(raw_funcs.contains(&"count".to_string()));
}

// ============================================================================
// Advanced JOIN tests
// ============================================================================

/// Test LEFT JOIN
#[test]
fn it_parses_left_join() {
    let query = "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    raw_tables.sort();
    assert_eq!(raw_tables, vec!["orders", "users"]);
}

/// Test RIGHT JOIN
#[test]
fn it_parses_right_join() {
    let query = "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test FULL OUTER JOIN
#[test]
fn it_parses_full_outer_join() {
    let query = "SELECT * FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CROSS JOIN
#[test]
fn it_parses_cross_join() {
    let query = "SELECT * FROM users CROSS JOIN products";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    raw_tables.sort();
    assert_eq!(raw_tables, vec!["products", "users"]);
}

/// Test NATURAL JOIN
#[test]
fn it_parses_natural_join() {
    let query = "SELECT * FROM users NATURAL JOIN user_profiles";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test multiple JOINs
#[test]
fn it_parses_multiple_joins() {
    let query = "SELECT u.name, o.id, p.name FROM users u
                 JOIN orders o ON u.id = o.user_id
                 JOIN order_items oi ON o.id = oi.order_id
                 JOIN products p ON oi.product_id = p.id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    raw_tables.sort();
    assert_eq!(raw_tables, vec!["order_items", "orders", "products", "users"]);
}

/// Test JOIN with USING clause
#[test]
fn it_parses_join_using() {
    let query = "SELECT * FROM users u JOIN user_profiles p USING (user_id)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LATERAL JOIN
#[test]
fn it_parses_lateral_join() {
    let query = "SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id LIMIT 3) AS recent_orders";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    raw_tables.sort();
    assert_eq!(raw_tables, vec!["orders", "users"]);
}
// ============================================================================
// Advanced subquery tests
// ============================================================================

/// Test correlated subquery
#[test]
fn it_parses_correlated_subquery() {
    let query = "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let mut raw_tables = raw_result.tables();
    raw_tables.sort();
    assert_eq!(raw_tables, vec!["orders", "users"]);
}

/// Test NOT EXISTS subquery
#[test]
fn it_parses_not_exists_subquery() {
    let query = "SELECT * FROM users u WHERE NOT EXISTS (SELECT 1 FROM banned b WHERE b.user_id = u.id)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test scalar subquery in SELECT
#[test]
fn it_parses_scalar_subquery() {
    let query = "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test subquery in FROM clause
#[test]
fn it_parses_derived_table() {
    let query = "SELECT * FROM (SELECT id, name FROM users WHERE active = true) AS active_users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ANY/SOME subquery
#[test]
fn it_parses_any_subquery() {
    let query = "SELECT * FROM products WHERE price > ANY (SELECT avg_price FROM categories)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ALL subquery
#[test]
fn it_parses_all_subquery() {
    let query = "SELECT * FROM products WHERE price > ALL (SELECT price FROM discounted_products)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Window function tests
// ============================================================================

/// Test basic window function
#[test]
fn it_parses_window_function() {
    let query = "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rank FROM employees";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test window function with PARTITION BY
#[test]
fn it_parses_window_function_partition() {
    let query = "SELECT department, name, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank FROM employees";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test window function with frame clause
#[test]
fn it_parses_window_function_frame() {
    let query = "SELECT date, amount, SUM(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_sum FROM transactions";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test named window
#[test]
fn it_parses_named_window() {
    let query = "SELECT name, salary, SUM(salary) OVER w, AVG(salary) OVER w FROM employees WINDOW w AS (PARTITION BY department ORDER BY salary)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LAG and LEAD functions
#[test]
fn it_parses_lag_lead() {
    let query =
        "SELECT date, price, LAG(price, 1) OVER (ORDER BY date) AS prev_price, LEAD(price, 1) OVER (ORDER BY date) AS next_price FROM stock_prices";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// CTE variations
// ============================================================================

/// Test multiple CTEs
#[test]
fn it_parses_multiple_ctes() {
    let query = "WITH
        active_users AS (SELECT * FROM users WHERE active = true),
        premium_users AS (SELECT * FROM active_users WHERE plan = 'premium')
        SELECT * FROM premium_users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
    assert!(raw_result.cte_names.contains(&"active_users".to_string()));
    assert!(raw_result.cte_names.contains(&"premium_users".to_string()));
}

/// Test recursive CTE
#[test]
fn it_parses_recursive_cte() {
    let query = "WITH RECURSIVE subordinates AS (
        SELECT id, name, manager_id FROM employees WHERE id = 1
        UNION ALL
        SELECT e.id, e.name, e.manager_id FROM employees e INNER JOIN subordinates s ON e.manager_id = s.id
    ) SELECT * FROM subordinates";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CTE with column list
#[test]
fn it_parses_cte_with_columns() {
    let query = "WITH regional_sales(region, total) AS (SELECT region, SUM(amount) FROM orders GROUP BY region) SELECT * FROM regional_sales";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test CTE with MATERIALIZED
#[test]
fn it_parses_cte_materialized() {
    let query = "WITH t AS MATERIALIZED (SELECT * FROM large_table WHERE x > 100) SELECT * FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Set operations
// ============================================================================

/// Test INTERSECT
#[test]
fn it_parses_intersect() {
    let query = "SELECT id FROM users INTERSECT SELECT user_id FROM orders";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test EXCEPT
#[test]
fn it_parses_except() {
    let query = "SELECT id FROM users EXCEPT SELECT user_id FROM banned_users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test UNION ALL
#[test]
fn it_parses_union_all() {
    let query = "SELECT name FROM users UNION ALL SELECT name FROM admins";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test compound set operations
#[test]
fn it_parses_compound_set_operations() {
    let query = "(SELECT id FROM a UNION SELECT id FROM b) INTERSECT SELECT id FROM c";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// GROUP BY variations
// ============================================================================

/// Test GROUP BY ROLLUP
#[test]
fn it_parses_group_by_rollup() {
    let query = "SELECT region, product, SUM(sales) FROM sales_data GROUP BY ROLLUP(region, product)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test GROUP BY CUBE
#[test]
fn it_parses_group_by_cube() {
    let query = "SELECT region, product, SUM(sales) FROM sales_data GROUP BY CUBE(region, product)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test GROUP BY GROUPING SETS
#[test]
fn it_parses_grouping_sets() {
    let query = "SELECT region, product, SUM(sales) FROM sales_data GROUP BY GROUPING SETS ((region), (product), ())";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// DISTINCT and ORDER BY variations
// ============================================================================

/// Test DISTINCT ON
#[test]
fn it_parses_distinct_on() {
    let query = "SELECT DISTINCT ON (user_id) * FROM orders ORDER BY user_id, created_at DESC";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test ORDER BY with NULLS FIRST/LAST
#[test]
fn it_parses_order_by_nulls() {
    let query = "SELECT * FROM users ORDER BY last_login DESC NULLS LAST";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test FETCH FIRST
#[test]
fn it_parses_fetch_first() {
    let query = "SELECT * FROM users ORDER BY id FETCH FIRST 10 ROWS ONLY";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test OFFSET with FETCH
#[test]
fn it_parses_offset_fetch() {
    let query = "SELECT * FROM users ORDER BY id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Locking clauses
// ============================================================================

/// Test FOR UPDATE
#[test]
fn it_parses_for_update() {
    let query = "SELECT * FROM users WHERE id = 1 FOR UPDATE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test FOR SHARE
#[test]
fn it_parses_for_share() {
    let query = "SELECT * FROM users WHERE id = 1 FOR SHARE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test FOR UPDATE NOWAIT
#[test]
fn it_parses_for_update_nowait() {
    let query = "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test FOR UPDATE SKIP LOCKED
#[test]
fn it_parses_for_update_skip_locked() {
    let query = "SELECT * FROM jobs WHERE status = 'pending' LIMIT 1 FOR UPDATE SKIP LOCKED";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Complex real-world queries
// ============================================================================

/// Test analytics query with window functions
#[test]
fn it_parses_analytics_query() {
    let query = "
        SELECT
            date_trunc('day', created_at) AS day,
            COUNT(*) AS daily_orders,
            SUM(amount) AS daily_revenue,
            AVG(amount) OVER (ORDER BY date_trunc('day', created_at) ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg
        FROM orders
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY date_trunc('day', created_at)
        ORDER BY day";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test hierarchical query with recursive CTE
#[test]
fn it_parses_hierarchy_query() {
    let query = "
        WITH RECURSIVE category_tree AS (
            SELECT id, name, parent_id, 0 AS level, ARRAY[id] AS path
            FROM categories
            WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ct.level + 1, ct.path || c.id
            FROM categories c
            JOIN category_tree ct ON c.parent_id = ct.id
        )
        SELECT * FROM category_tree ORDER BY path";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test complex report query
#[test]
fn it_parses_complex_report_query() {
    let query = "
        WITH monthly_data AS (
            SELECT
                date_trunc('month', o.created_at) AS month,
                u.region,
                p.category,
                SUM(oi.quantity * oi.unit_price) AS revenue,
                COUNT(DISTINCT o.id) AS order_count,
                COUNT(DISTINCT o.user_id) AS customer_count
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.created_at >= '2023-01-01' AND o.status = 'completed'
            GROUP BY 1, 2, 3
        )
        SELECT
            month,
            region,
            category,
            revenue,
            order_count,
            customer_count,
            revenue / NULLIF(order_count, 0) AS avg_order_value,
            SUM(revenue) OVER (PARTITION BY region ORDER BY month) AS cumulative_revenue
        FROM monthly_data
        ORDER BY month DESC, region, revenue DESC";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test query with multiple subqueries and CTEs
#[test]
fn it_parses_mixed_subqueries_and_ctes() {
    let query = "
        WITH high_value_customers AS (
            SELECT user_id FROM orders GROUP BY user_id HAVING SUM(amount) > 1000
        )
        SELECT u.*,
            (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS total_orders,
            (SELECT MAX(created_at) FROM orders o WHERE o.user_id = u.id) AS last_order
        FROM users u
        WHERE u.id IN (SELECT user_id FROM high_value_customers)
            AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.created_at > NOW() - INTERVAL '90 days')
        ORDER BY (SELECT SUM(amount) FROM orders o WHERE o.user_id = u.id) DESC
        LIMIT 100";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// Tests for previously stubbed fields
// ============================================================================

/// Test column with COLLATE clause
#[test]
fn it_parses_column_with_collate() {
    let query = "CREATE TABLE test_collate (
        name TEXT COLLATE \"C\",
        description VARCHAR(255) COLLATE \"en_US.UTF-8\"
    )";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partitioned table with PARTITION BY RANGE
#[test]
fn it_parses_partition_by_range() {
    let query = "CREATE TABLE measurements (
        id SERIAL,
        logdate DATE NOT NULL,
        peaktemp INT
    ) PARTITION BY RANGE (logdate)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partitioned table with PARTITION BY LIST
#[test]
fn it_parses_partition_by_list() {
    let query = "CREATE TABLE orders (
        id SERIAL,
        region TEXT NOT NULL,
        order_date DATE
    ) PARTITION BY LIST (region)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partitioned table with PARTITION BY HASH
#[test]
fn it_parses_partition_by_hash() {
    let query = "CREATE TABLE users_partitioned (
        id SERIAL,
        username TEXT
    ) PARTITION BY HASH (id)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partition with FOR VALUES (range)
#[test]
fn it_parses_partition_for_values_range() {
    let query = "CREATE TABLE measurements_2023 PARTITION OF measurements
        FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partition with FOR VALUES (list)
#[test]
fn it_parses_partition_for_values_list() {
    let query = "CREATE TABLE orders_west PARTITION OF orders
        FOR VALUES IN ('west', 'northwest', 'southwest')";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partition with FOR VALUES (hash)
#[test]
fn it_parses_partition_for_values_hash() {
    let query = "CREATE TABLE users_part_0 PARTITION OF users_partitioned
        FOR VALUES WITH (MODULUS 4, REMAINDER 0)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test partition with DEFAULT
#[test]
fn it_parses_partition_default() {
    let query = "CREATE TABLE orders_other PARTITION OF orders DEFAULT";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test recursive CTE with SEARCH BREADTH FIRST
#[test]
fn it_parses_cte_search_breadth_first() {
    let query = "WITH RECURSIVE search_tree(id, parent_id, data, depth) AS (
        SELECT id, parent_id, data, 0 FROM tree WHERE parent_id IS NULL
        UNION ALL
        SELECT t.id, t.parent_id, t.data, st.depth + 1
        FROM tree t, search_tree st WHERE t.parent_id = st.id
    ) SEARCH BREADTH FIRST BY id SET ordercol
    SELECT * FROM search_tree ORDER BY ordercol";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test recursive CTE with SEARCH DEPTH FIRST
#[test]
fn it_parses_cte_search_depth_first() {
    let query = "WITH RECURSIVE search_tree(id, parent_id, data) AS (
        SELECT id, parent_id, data FROM tree WHERE parent_id IS NULL
        UNION ALL
        SELECT t.id, t.parent_id, t.data
        FROM tree t, search_tree st WHERE t.parent_id = st.id
    ) SEARCH DEPTH FIRST BY id SET ordercol
    SELECT * FROM search_tree ORDER BY ordercol";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test recursive CTE with CYCLE detection
#[test]
fn it_parses_cte_cycle() {
    let query = "WITH RECURSIVE search_graph(id, link, data, depth) AS (
        SELECT g.id, g.link, g.data, 0 FROM graph g
        UNION ALL
        SELECT g.id, g.link, g.data, sg.depth + 1
        FROM graph g, search_graph sg WHERE g.id = sg.link
    ) CYCLE id SET is_cycle USING path
    SELECT * FROM search_graph";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test recursive CTE with both SEARCH and CYCLE
#[test]
fn it_parses_cte_search_and_cycle() {
    let query = "WITH RECURSIVE search_graph(id, link, data, depth) AS (
        SELECT g.id, g.link, g.data, 0 FROM graph g WHERE id = 1
        UNION ALL
        SELECT g.id, g.link, g.data, sg.depth + 1
        FROM graph g, search_graph sg WHERE g.id = sg.link
    ) SEARCH DEPTH FIRST BY id SET ordercol
      CYCLE id SET is_cycle USING path
    SELECT * FROM search_graph";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
