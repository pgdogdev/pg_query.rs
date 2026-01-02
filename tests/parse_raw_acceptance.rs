//! Comprehensive acceptance test suite for parse_raw.
//!
//! This file contains a wide variety of SQL statements to ensure parse_raw
//! produces results equivalent to parse (protobuf-based parsing).
//!
//! The tests are organized by SQL category and cover many edge cases and
//! PostgreSQL-specific features.

use pg_query::{parse, parse_raw, Error};

/// Helper macro for simple parse comparison tests
macro_rules! assert_parse_raw_matches {
    ($query:expr) => {{
        let raw_result = parse_raw($query).expect(&format!("parse_raw failed for: {}", $query));
        let proto_result = parse($query).expect(&format!("parse failed for: {}", $query));
        assert_eq!(raw_result.protobuf, proto_result.protobuf, "Mismatch for query: {}", $query);
    }};
}

/// Helper macro to test multiple queries at once
macro_rules! test_queries {
    ($($query:expr),+ $(,)?) => {{
        $(
            assert_parse_raw_matches!($query);
        )+
    }};
}

// ============================================================================
// SELECT - Basic variations
// ============================================================================

#[test]
fn select_basic_variations() {
    test_queries![
        "SELECT 1",
        "SELECT 1, 2, 3",
        "SELECT *",
        "SELECT * FROM t",
        "SELECT a, b, c FROM t",
        "SELECT t.* FROM t",
        "SELECT t.a, t.b FROM t",
        "SELECT ALL a FROM t",
        "SELECT DISTINCT a FROM t",
        "SELECT DISTINCT ON (a) * FROM t",
        "SELECT DISTINCT ON (a, b) c, d FROM t ORDER BY a, b",
    ];
}

#[test]
fn select_aliases() {
    test_queries![
        "SELECT 1 AS one",
        "SELECT 1 one",
        "SELECT a AS alias FROM t",
        "SELECT a alias FROM t",
        "SELECT t.a AS ta FROM t",
        "SELECT a AS \"Quoted Alias\" FROM t",
        "SELECT * FROM t AS alias",
        "SELECT * FROM t alias",
        "SELECT * FROM t AS alias (col1, col2)",
        "SELECT * FROM schema.table AS t",
    ];
}

#[test]
fn select_where_clause() {
    test_queries![
        "SELECT * FROM t WHERE a = 1",
        "SELECT * FROM t WHERE a <> 1",
        "SELECT * FROM t WHERE a != 1",
        "SELECT * FROM t WHERE a > 1",
        "SELECT * FROM t WHERE a >= 1",
        "SELECT * FROM t WHERE a < 1",
        "SELECT * FROM t WHERE a <= 1",
        "SELECT * FROM t WHERE a = 1 AND b = 2",
        "SELECT * FROM t WHERE a = 1 OR b = 2",
        "SELECT * FROM t WHERE NOT a",
        "SELECT * FROM t WHERE NOT (a = 1)",
        "SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3",
        "SELECT * FROM t WHERE a IS NULL",
        "SELECT * FROM t WHERE a IS NOT NULL",
        "SELECT * FROM t WHERE a IS TRUE",
        "SELECT * FROM t WHERE a IS NOT TRUE",
        "SELECT * FROM t WHERE a IS FALSE",
        "SELECT * FROM t WHERE a IS NOT FALSE",
        "SELECT * FROM t WHERE a IS UNKNOWN",
        "SELECT * FROM t WHERE a IS NOT UNKNOWN",
    ];
}

#[test]
fn select_between() {
    test_queries![
        "SELECT * FROM t WHERE a BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10",
        "SELECT * FROM t WHERE a BETWEEN SYMMETRIC 10 AND 1",
        "SELECT * FROM t WHERE a NOT BETWEEN SYMMETRIC 1 AND 10",
    ];
}

#[test]
fn select_in_clause() {
    test_queries![
        "SELECT * FROM t WHERE a IN (1, 2, 3)",
        "SELECT * FROM t WHERE a NOT IN (1, 2, 3)",
        "SELECT * FROM t WHERE a IN (SELECT b FROM t2)",
        "SELECT * FROM t WHERE a NOT IN (SELECT b FROM t2)",
        "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4))",
        "SELECT * FROM t WHERE (a, b) IN (SELECT c, d FROM t2)",
    ];
}

#[test]
fn select_like_patterns() {
    test_queries![
        "SELECT * FROM t WHERE a LIKE 'foo%'",
        "SELECT * FROM t WHERE a NOT LIKE 'foo%'",
        "SELECT * FROM t WHERE a ILIKE 'foo%'",
        "SELECT * FROM t WHERE a NOT ILIKE 'foo%'",
        "SELECT * FROM t WHERE a LIKE 'foo%' ESCAPE '\\\\'",
        "SELECT * FROM t WHERE a SIMILAR TO '%(foo|bar)%'",
        "SELECT * FROM t WHERE a NOT SIMILAR TO '%(foo|bar)%'",
        "SELECT * FROM t WHERE a ~ '^[a-z]+$'",
        "SELECT * FROM t WHERE a ~* '^[A-Z]+$'",
        "SELECT * FROM t WHERE a !~ '^[a-z]+$'",
        "SELECT * FROM t WHERE a !~* '^[A-Z]+$'",
    ];
}

#[test]
fn select_order_by() {
    test_queries![
        "SELECT * FROM t ORDER BY a",
        "SELECT * FROM t ORDER BY a ASC",
        "SELECT * FROM t ORDER BY a DESC",
        "SELECT * FROM t ORDER BY a NULLS FIRST",
        "SELECT * FROM t ORDER BY a NULLS LAST",
        "SELECT * FROM t ORDER BY a ASC NULLS FIRST",
        "SELECT * FROM t ORDER BY a DESC NULLS LAST",
        "SELECT * FROM t ORDER BY a, b DESC, c ASC NULLS FIRST",
        "SELECT * FROM t ORDER BY 1",
        "SELECT * FROM t ORDER BY 1, 2 DESC",
        "SELECT a + b AS sum FROM t ORDER BY sum",
        r#"SELECT * FROM t ORDER BY a COLLATE "C""#,
        "SELECT * FROM t ORDER BY a USING <",
        "SELECT * FROM t ORDER BY a USING >",
    ];
}

#[test]
fn select_limit_offset() {
    test_queries![
        "SELECT * FROM t LIMIT 10",
        "SELECT * FROM t LIMIT ALL",
        "SELECT * FROM t OFFSET 5",
        "SELECT * FROM t LIMIT 10 OFFSET 5",
        "SELECT * FROM t OFFSET 5 LIMIT 10",
        "SELECT * FROM t FETCH FIRST 10 ROWS ONLY",
        "SELECT * FROM t FETCH FIRST ROW ONLY",
        "SELECT * FROM t FETCH NEXT 10 ROWS ONLY",
        "SELECT * FROM t OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
        // WITH TIES requires ORDER BY clause
        "SELECT * FROM t ORDER BY a FETCH FIRST 10 ROWS WITH TIES",
        // Note: PERCENT is not supported in PostgreSQL
    ];
}

#[test]
fn select_group_by() {
    test_queries![
        "SELECT a, count(*) FROM t GROUP BY a",
        "SELECT a, b, count(*) FROM t GROUP BY a, b",
        "SELECT a, count(*) FROM t GROUP BY 1",
        "SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1",
        "SELECT a, sum(b) FROM t GROUP BY ROLLUP (a)",
        "SELECT a, b, sum(c) FROM t GROUP BY ROLLUP (a, b)",
        "SELECT a, b, sum(c) FROM t GROUP BY CUBE (a, b)",
        "SELECT a, b, sum(c) FROM t GROUP BY GROUPING SETS ((a), (b), ())",
        "SELECT a, b, sum(c) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())",
        "SELECT a, b, c, sum(d) FROM t GROUP BY a, ROLLUP (b, c)",
        "SELECT GROUPING(a, b) FROM t GROUP BY CUBE (a, b)",
        "SELECT a, count(*) FILTER (WHERE b > 0) FROM t GROUP BY a",
    ];
}

// ============================================================================
// SELECT - JOINs
// ============================================================================

#[test]
fn select_joins() {
    test_queries![
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id",
        "SELECT * FROM t1 CROSS JOIN t2",
        "SELECT * FROM t1 NATURAL JOIN t2",
        "SELECT * FROM t1 NATURAL LEFT JOIN t2",
        "SELECT * FROM t1 NATURAL RIGHT JOIN t2",
        "SELECT * FROM t1 NATURAL FULL JOIN t2",
        "SELECT * FROM t1 JOIN t2 USING (id)",
        "SELECT * FROM t1 JOIN t2 USING (id, name)",
        "SELECT * FROM t1, t2",
        "SELECT * FROM t1, t2, t3",
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id",
        "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id",
    ];
}

#[test]
fn select_lateral_join() {
    test_queries![
        "SELECT * FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub",
        "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) AS sub ON true",
        "SELECT * FROM t1 CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id LIMIT 1) AS sub",
        "SELECT * FROM generate_series(1, 3) AS x, LATERAL (SELECT * FROM t WHERE t.n = x) AS sub",
    ];
}

// ============================================================================
// SELECT - Subqueries
// ============================================================================

#[test]
fn select_subqueries() {
    test_queries![
        "SELECT * FROM (SELECT * FROM t) AS sub",
        "SELECT * FROM (SELECT 1 AS a, 2 AS b) AS sub",
        "SELECT (SELECT max(a) FROM t) AS max_a",
        "SELECT a, (SELECT count(*) FROM t2 WHERE t2.id = t1.id) FROM t1",
        "SELECT * FROM t WHERE a = (SELECT max(b) FROM t2)",
        "SELECT * FROM t WHERE a > ALL (SELECT b FROM t2)",
        "SELECT * FROM t WHERE a > ANY (SELECT b FROM t2)",
        "SELECT * FROM t WHERE a > SOME (SELECT b FROM t2)",
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)",
        "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)",
        "SELECT * FROM t WHERE a IN (SELECT b FROM t2)",
        "SELECT ARRAY(SELECT a FROM t)",
    ];
}

// ============================================================================
// SELECT - CTEs (Common Table Expressions)
// ============================================================================

#[test]
fn select_ctes() {
    test_queries![
        "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte",
        "WITH cte (a, b) AS (SELECT 1, 2) SELECT * FROM cte",
        "WITH cte1 AS (SELECT 1 AS a), cte2 AS (SELECT 2 AS b) SELECT * FROM cte1, cte2",
        "WITH cte AS (SELECT * FROM t WHERE active) SELECT * FROM cte WHERE id > 10",
        "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 10) SELECT * FROM cte",
        "WITH cte AS MATERIALIZED (SELECT * FROM t) SELECT * FROM cte",
        "WITH cte AS NOT MATERIALIZED (SELECT * FROM t) SELECT * FROM cte",
    ];
}

#[test]
fn select_cte_search_cycle() {
    test_queries![
        "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 10) SEARCH DEPTH FIRST BY n SET seq SELECT * FROM cte",
        "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 10) SEARCH BREADTH FIRST BY n SET seq SELECT * FROM cte",
        "WITH RECURSIVE cte(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 10) CYCLE n SET is_cycle USING path SELECT * FROM cte",
    ];
}

// ============================================================================
// SELECT - Window functions
// ============================================================================

#[test]
fn select_window_functions() {
    test_queries![
        "SELECT a, ROW_NUMBER() OVER () FROM t",
        "SELECT a, ROW_NUMBER() OVER (ORDER BY a) FROM t",
        "SELECT a, ROW_NUMBER() OVER (PARTITION BY b ORDER BY a) FROM t",
        "SELECT a, RANK() OVER (ORDER BY a) FROM t",
        "SELECT a, DENSE_RANK() OVER (ORDER BY a) FROM t",
        "SELECT a, NTILE(4) OVER (ORDER BY a) FROM t",
        "SELECT a, LAG(a) OVER (ORDER BY a) FROM t",
        "SELECT a, LAG(a, 2) OVER (ORDER BY a) FROM t",
        "SELECT a, LAG(a, 2, 0) OVER (ORDER BY a) FROM t",
        "SELECT a, LEAD(a) OVER (ORDER BY a) FROM t",
        "SELECT a, FIRST_VALUE(a) OVER (ORDER BY a) FROM t",
        "SELECT a, LAST_VALUE(a) OVER (ORDER BY a) FROM t",
        "SELECT a, NTH_VALUE(a, 2) OVER (ORDER BY a) FROM t",
        "SELECT a, PERCENT_RANK() OVER (ORDER BY a) FROM t",
        "SELECT a, CUME_DIST() OVER (ORDER BY a) FROM t",
    ];
}

#[test]
fn select_window_frames() {
    test_queries![
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS UNBOUNDED PRECEDING) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a RANGE UNBOUNDED PRECEDING) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a GROUPS UNBOUNDED PRECEDING) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) FROM t",
        "SELECT a, SUM(a) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) FROM t",
    ];
}

#[test]
fn select_named_windows() {
    test_queries![
        "SELECT a, SUM(a) OVER w FROM t WINDOW w AS (ORDER BY a)",
        "SELECT a, SUM(a) OVER w, AVG(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY a)",
        "SELECT a, SUM(a) OVER w1, AVG(a) OVER w2 FROM t WINDOW w1 AS (ORDER BY a), w2 AS (ORDER BY b)",
    ];
}

// ============================================================================
// SELECT - Set operations
// ============================================================================

#[test]
fn select_set_operations() {
    test_queries![
        "SELECT a FROM t1 UNION SELECT a FROM t2",
        "SELECT a FROM t1 UNION ALL SELECT a FROM t2",
        "SELECT a FROM t1 INTERSECT SELECT a FROM t2",
        "SELECT a FROM t1 INTERSECT ALL SELECT a FROM t2",
        "SELECT a FROM t1 EXCEPT SELECT a FROM t2",
        "SELECT a FROM t1 EXCEPT ALL SELECT a FROM t2",
        "(SELECT a FROM t1) UNION (SELECT a FROM t2) ORDER BY a",
        "SELECT a FROM t1 UNION SELECT a FROM t2 UNION SELECT a FROM t3",
        "(SELECT a FROM t1 UNION SELECT a FROM t2) INTERSECT SELECT a FROM t3",
    ];
}

// ============================================================================
// SELECT - Locking clauses
// ============================================================================

#[test]
fn select_locking() {
    test_queries![
        "SELECT * FROM t FOR UPDATE",
        "SELECT * FROM t FOR NO KEY UPDATE",
        "SELECT * FROM t FOR SHARE",
        "SELECT * FROM t FOR KEY SHARE",
        "SELECT * FROM t FOR UPDATE OF t",
        "SELECT * FROM t1, t2 FOR UPDATE OF t1",
        "SELECT * FROM t FOR UPDATE NOWAIT",
        "SELECT * FROM t FOR UPDATE SKIP LOCKED",
        "SELECT * FROM t FOR UPDATE OF t NOWAIT",
        "SELECT * FROM t1, t2 FOR UPDATE OF t1 FOR SHARE OF t2",
    ];
}

// ============================================================================
// SELECT - Table sampling
// ============================================================================

#[test]
fn select_table_sampling() {
    test_queries![
        "SELECT * FROM t TABLESAMPLE SYSTEM (10)",
        "SELECT * FROM t TABLESAMPLE BERNOULLI (10)",
        "SELECT * FROM t TABLESAMPLE SYSTEM (10) REPEATABLE (42)",
    ];
}

// ============================================================================
// SELECT - Values
// ============================================================================

#[test]
fn select_values() {
    test_queries![
        "VALUES (1)",
        "VALUES (1, 2, 3)",
        "VALUES (1), (2), (3)",
        "VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)",
    ];
}

// ============================================================================
// Literals and constants
// ============================================================================

#[test]
fn literals_numeric() {
    test_queries![
        "SELECT 0",
        "SELECT 42",
        "SELECT -42",
        "SELECT 9223372036854775807",
        "SELECT -9223372036854775808",
        "SELECT 3.14",
        "SELECT -3.14",
        "SELECT .5",
        "SELECT 5.",
        "SELECT 1e10",
        "SELECT 1E10",
        "SELECT 1e+10",
        "SELECT 1e-10",
        "SELECT 1.5e10",
        "SELECT 0.123456789",
    ];
}

#[test]
fn literals_string() {
    test_queries![
        "SELECT 'hello'",
        "SELECT 'hello world'",
        "SELECT 'it''s'",
        "SELECT ''",
        "SELECT E'hello\\nworld'",
        "SELECT E'tab\\there'",
        "SELECT E'quote\\'here'",
        "SELECT $$dollar quoted$$",
        "SELECT $tag$tagged dollar$tag$",
        "SELECT U&'d\\0061t\\+000061'",
    ];
}

#[test]
fn literals_other() {
    test_queries!["SELECT TRUE", "SELECT FALSE", "SELECT NULL", "SELECT B'1010'", "SELECT B'11110000'", "SELECT X'FF'", "SELECT X'DEADBEEF'",];
}

#[test]
fn literals_typed() {
    test_queries![
        "SELECT DATE '2023-01-15'",
        "SELECT TIME '12:30:00'",
        "SELECT TIME WITH TIME ZONE '12:30:00+05'",
        "SELECT TIMESTAMP '2023-01-15 12:30:00'",
        "SELECT TIMESTAMP WITH TIME ZONE '2023-01-15 12:30:00+00'",
        "SELECT INTERVAL '1 year'",
        "SELECT INTERVAL '1 year 2 months 3 days'",
        "SELECT INTERVAL '1' YEAR",
        "SELECT INTERVAL '1-2' YEAR TO MONTH",
        "SELECT INT '42'",
        "SELECT REAL '3.14'",
    ];
}

// ============================================================================
// Type casts
// ============================================================================

#[test]
fn type_casts() {
    test_queries![
        "SELECT 42::integer",
        "SELECT '42'::integer",
        "SELECT 42::text",
        "SELECT 42::numeric(10, 2)",
        "SELECT 'hello'::varchar(100)",
        "SELECT '2023-01-15'::date",
        "SELECT '{1,2,3}'::int[]",
        "SELECT '{{1,2},{3,4}}'::int[][]",
        "SELECT CAST(42 AS text)",
        "SELECT CAST('42' AS integer)",
        "SELECT CAST(x AS numeric(10, 2)) FROM t",
        "SELECT TREAT(x AS text) FROM t",
        "SELECT x::int::text FROM t",
    ];
}

// ============================================================================
// Arrays
// ============================================================================

#[test]
fn arrays() {
    test_queries![
        "SELECT ARRAY[1, 2, 3]",
        "SELECT ARRAY['a', 'b', 'c']",
        "SELECT ARRAY[[1, 2], [3, 4]]",
        "SELECT ARRAY[]::int[]",
        "SELECT arr[1] FROM t",
        "SELECT arr[1][2] FROM t",
        "SELECT arr[1:3] FROM t",
        "SELECT arr[:3] FROM t",
        "SELECT arr[2:] FROM t",
        "SELECT arr || ARRAY[4, 5]",
        "SELECT 1 = ANY (arr) FROM t",
        "SELECT 1 = ALL (arr) FROM t",
        "SELECT array_length(arr, 1) FROM t",
        "SELECT array_dims(arr) FROM t",
        "SELECT array_upper(arr, 1) FROM t",
        "SELECT array_lower(arr, 1) FROM t",
        "SELECT unnest(arr) FROM t",
        "SELECT array_agg(a) FROM t",
        "SELECT array_agg(a ORDER BY b) FROM t",
    ];
}

// ============================================================================
// JSON and JSONB
// ============================================================================

#[test]
fn json_operators() {
    test_queries![
        "SELECT data -> 'key' FROM t",
        "SELECT data ->> 'key' FROM t",
        "SELECT data -> 0 FROM t",
        "SELECT data ->> 0 FROM t",
        "SELECT data #> '{a,b}' FROM t",
        "SELECT data #>> '{a,b}' FROM t",
        "SELECT data @> '{\"a\": 1}' FROM t",
        "SELECT data <@ '{\"a\": 1}' FROM t",
        "SELECT data ? 'key' FROM t",
        "SELECT data ?| array['a', 'b'] FROM t",
        "SELECT data ?& array['a', 'b'] FROM t",
        "SELECT data || '{\"b\": 2}'::jsonb FROM t",
        "SELECT data - 'key' FROM t",
        "SELECT data #- '{a,b}' FROM t",
        "SELECT data @? '$.a' FROM t",
        "SELECT data @@ '$.a == 1' FROM t",
    ];
}

#[test]
fn json_functions() {
    test_queries![
        "SELECT json_build_object('a', 1, 'b', 2)",
        "SELECT jsonb_build_object('a', 1, 'b', 2)",
        "SELECT json_build_array(1, 2, 3)",
        "SELECT jsonb_build_array(1, 2, 3)",
        "SELECT json_object('{a, b}', '{1, 2}')",
        "SELECT to_json(row(1, 'foo'))",
        "SELECT to_jsonb(row(1, 'foo'))",
        "SELECT row_to_json(t) FROM t",
        "SELECT json_agg(t) FROM t",
        "SELECT jsonb_agg(t) FROM t",
        "SELECT json_object_agg(k, v) FROM t",
        "SELECT jsonb_object_agg(k, v) FROM t",
        "SELECT json_typeof(data) FROM t",
        "SELECT jsonb_typeof(data) FROM t",
        "SELECT json_array_length(data) FROM t",
        "SELECT jsonb_array_length(data) FROM t",
        "SELECT json_extract_path(data, 'a', 'b') FROM t",
        "SELECT jsonb_extract_path(data, 'a', 'b') FROM t",
        "SELECT jsonb_set(data, '{a}', '1') FROM t",
        "SELECT jsonb_insert(data, '{a}', '1') FROM t",
        "SELECT jsonb_pretty(data) FROM t",
        "SELECT json_strip_nulls(data) FROM t",
        "SELECT jsonb_path_query(data, '$.a') FROM t",
    ];
}

// ============================================================================
// String functions and operators
// ============================================================================

#[test]
fn string_functions() {
    test_queries![
        "SELECT 'hello' || ' ' || 'world'",
        "SELECT concat('a', 'b', 'c')",
        "SELECT concat_ws(', ', 'a', 'b', 'c')",
        "SELECT length('hello')",
        "SELECT char_length('hello')",
        "SELECT character_length('hello')",
        "SELECT octet_length('hello')",
        "SELECT bit_length('hello')",
        "SELECT upper('hello')",
        "SELECT lower('HELLO')",
        "SELECT initcap('hello world')",
        "SELECT trim('  hello  ')",
        "SELECT trim(leading ' ' from '  hello  ')",
        "SELECT trim(trailing ' ' from '  hello  ')",
        "SELECT trim(both ' ' from '  hello  ')",
        "SELECT ltrim('  hello')",
        "SELECT rtrim('hello  ')",
        "SELECT btrim('  hello  ')",
        "SELECT lpad('hi', 5, '*')",
        "SELECT rpad('hi', 5, '*')",
        "SELECT repeat('ab', 3)",
        "SELECT reverse('hello')",
        "SELECT replace('hello', 'l', 'L')",
        "SELECT translate('hello', 'el', 'ip')",
        "SELECT substring('hello' from 2 for 3)",
        "SELECT substring('hello' from 2)",
        "SELECT substring('hello' for 3)",
        "SELECT substr('hello', 2, 3)",
        "SELECT left('hello', 3)",
        "SELECT right('hello', 3)",
        "SELECT position('ll' in 'hello')",
        "SELECT strpos('hello', 'll')",
        "SELECT overlay('hello' placing 'XX' from 3 for 2)",
        "SELECT split_part('a,b,c', ',', 2)",
        "SELECT string_to_array('a,b,c', ',')",
        "SELECT array_to_string(ARRAY['a','b','c'], ',')",
        "SELECT regexp_replace('hello', 'l+', 'L')",
        "SELECT regexp_match('hello', 'l+')",
        "SELECT regexp_matches('hello', 'l+', 'g')",
        "SELECT regexp_split_to_array('hello world', '\\s+')",
        "SELECT regexp_split_to_table('hello world', '\\s+')",
        "SELECT format('%s %s', 'hello', 'world')",
        "SELECT quote_ident('Column Name')",
        "SELECT quote_literal('it''s')",
        "SELECT quote_nullable(NULL)",
        "SELECT ascii('A')",
        "SELECT chr(65)",
        "SELECT md5('hello')",
        "SELECT encode('hello'::bytea, 'hex')",
        "SELECT decode('68656c6c6f', 'hex')",
    ];
}

// ============================================================================
// Date and time functions
// ============================================================================

#[test]
fn datetime_functions() {
    test_queries![
        "SELECT CURRENT_DATE",
        "SELECT CURRENT_TIME",
        "SELECT CURRENT_TIME(3)",
        "SELECT CURRENT_TIMESTAMP",
        "SELECT CURRENT_TIMESTAMP(3)",
        "SELECT LOCALTIME",
        "SELECT LOCALTIME(3)",
        "SELECT LOCALTIMESTAMP",
        "SELECT LOCALTIMESTAMP(3)",
        "SELECT NOW()",
        "SELECT clock_timestamp()",
        "SELECT statement_timestamp()",
        "SELECT transaction_timestamp()",
        "SELECT timeofday()",
        "SELECT date_part('year', CURRENT_DATE)",
        "SELECT date_trunc('month', CURRENT_TIMESTAMP)",
        "SELECT extract(year from CURRENT_DATE)",
        "SELECT extract(epoch from CURRENT_TIMESTAMP)",
        "SELECT age(TIMESTAMP '2023-01-01', TIMESTAMP '2020-01-01')",
        "SELECT age(TIMESTAMP '2023-01-01')",
        "SELECT date '2023-01-01' + interval '1 month'",
        "SELECT date '2023-01-01' - interval '1 month'",
        "SELECT date '2023-01-15' - date '2023-01-01'",
        "SELECT make_date(2023, 1, 15)",
        "SELECT make_time(12, 30, 0)",
        "SELECT make_timestamp(2023, 1, 15, 12, 30, 0)",
        "SELECT make_timestamptz(2023, 1, 15, 12, 30, 0, 'UTC')",
        "SELECT make_interval(years := 1, months := 2)",
        "SELECT to_char(CURRENT_DATE, 'YYYY-MM-DD')",
        "SELECT to_date('2023-01-15', 'YYYY-MM-DD')",
        "SELECT to_timestamp('2023-01-15 12:30:00', 'YYYY-MM-DD HH24:MI:SS')",
        "SELECT isfinite(date '2023-01-01')",
        "SELECT justify_days(interval '35 days')",
        "SELECT justify_hours(interval '27 hours')",
        "SELECT justify_interval(interval '1 mon -1 hour')",
        "SELECT generate_series(date '2023-01-01', date '2023-01-10', interval '1 day')",
    ];
}

// ============================================================================
// Aggregate functions
// ============================================================================

#[test]
fn aggregate_functions() {
    test_queries![
        "SELECT count(*) FROM t",
        "SELECT count(a) FROM t",
        "SELECT count(DISTINCT a) FROM t",
        "SELECT count(ALL a) FROM t",
        "SELECT sum(a) FROM t",
        "SELECT sum(DISTINCT a) FROM t",
        "SELECT avg(a) FROM t",
        "SELECT min(a) FROM t",
        "SELECT max(a) FROM t",
        "SELECT array_agg(a) FROM t",
        "SELECT array_agg(a ORDER BY b) FROM t",
        "SELECT array_agg(DISTINCT a) FROM t",
        "SELECT string_agg(a, ', ') FROM t",
        "SELECT string_agg(a, ', ' ORDER BY a) FROM t",
        "SELECT bool_and(a) FROM t",
        "SELECT bool_or(a) FROM t",
        "SELECT every(a) FROM t",
        "SELECT bit_and(a) FROM t",
        "SELECT bit_or(a) FROM t",
        "SELECT bit_xor(a) FROM t",
        "SELECT variance(a) FROM t",
        "SELECT var_pop(a) FROM t",
        "SELECT var_samp(a) FROM t",
        "SELECT stddev(a) FROM t",
        "SELECT stddev_pop(a) FROM t",
        "SELECT stddev_samp(a) FROM t",
        "SELECT covar_pop(a, b) FROM t",
        "SELECT covar_samp(a, b) FROM t",
        "SELECT corr(a, b) FROM t",
        "SELECT regr_slope(a, b) FROM t",
        "SELECT regr_intercept(a, b) FROM t",
        "SELECT regr_count(a, b) FROM t",
        "SELECT regr_r2(a, b) FROM t",
        "SELECT mode() WITHIN GROUP (ORDER BY a) FROM t",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) FROM t",
        "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY a) FROM t",
        "SELECT count(*) FILTER (WHERE a > 0) FROM t",
        "SELECT sum(a) FILTER (WHERE b > 0) FROM t",
    ];
}

// ============================================================================
// Mathematical functions
// ============================================================================

#[test]
fn math_functions() {
    test_queries![
        "SELECT 1 + 2",
        "SELECT 5 - 3",
        "SELECT 2 * 3",
        "SELECT 10 / 3",
        "SELECT 10 % 3",
        "SELECT 2 ^ 3",
        "SELECT |/ 16",
        "SELECT ||/ 27",
        "SELECT @ -5",
        // Postfix factorial (!) was removed in PostgreSQL 14, use factorial() function
        "SELECT factorial(5)",
        "SELECT 91 & 15",
        "SELECT 32 | 3",
        "SELECT 17 # 5",
        "SELECT ~1",
        "SELECT 1 << 4",
        "SELECT 8 >> 2",
        "SELECT abs(-5)",
        "SELECT ceil(4.2)",
        "SELECT ceiling(4.2)",
        "SELECT floor(4.8)",
        "SELECT round(4.5)",
        "SELECT round(4.567, 2)",
        "SELECT trunc(4.567)",
        "SELECT trunc(4.567, 2)",
        "SELECT sign(-5)",
        "SELECT sqrt(16)",
        "SELECT cbrt(27)",
        "SELECT power(2, 3)",
        "SELECT exp(1)",
        "SELECT ln(10)",
        "SELECT log(10)",
        "SELECT log(2, 8)",
        "SELECT mod(10, 3)",
        "SELECT div(10, 3)",
        "SELECT gcd(12, 8)",
        "SELECT lcm(12, 8)",
        "SELECT factorial(5)",
        "SELECT degrees(pi())",
        "SELECT radians(180)",
        "SELECT pi()",
        "SELECT random()",
        "SELECT setseed(0.5)",
        "SELECT sin(pi() / 2)",
        "SELECT cos(pi())",
        "SELECT tan(pi() / 4)",
        "SELECT asin(1)",
        "SELECT acos(0)",
        "SELECT atan(1)",
        "SELECT atan2(1, 1)",
        "SELECT sinh(1)",
        "SELECT cosh(1)",
        "SELECT tanh(1)",
        "SELECT width_bucket(5, 0, 10, 5)",
        "SELECT greatest(1, 2, 3)",
        "SELECT least(1, 2, 3)",
    ];
}

// ============================================================================
// Conditional expressions
// ============================================================================

#[test]
fn conditional_expressions() {
    test_queries![
        "SELECT CASE WHEN a = 1 THEN 'one' ELSE 'other' END FROM t",
        "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' END FROM t",
        "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
        "SELECT COALESCE(a, b, c) FROM t",
        "SELECT NULLIF(a, b) FROM t",
        "SELECT GREATEST(a, b, c) FROM t",
        "SELECT LEAST(a, b, c) FROM t",
        "SELECT a IS NULL FROM t",
        "SELECT a IS NOT NULL FROM t",
        "SELECT a IS DISTINCT FROM b FROM t",
        "SELECT a IS NOT DISTINCT FROM b FROM t",
        "SELECT a IS TRUE FROM t",
        "SELECT a IS NOT TRUE FROM t",
        "SELECT a IS FALSE FROM t",
        "SELECT a IS NOT FALSE FROM t",
        "SELECT a IS UNKNOWN FROM t",
        "SELECT a IS NOT UNKNOWN FROM t",
    ];
}

// ============================================================================
// INSERT statements
// ============================================================================

#[test]
fn insert_basic() {
    test_queries![
        "INSERT INTO t DEFAULT VALUES",
        "INSERT INTO t (a) VALUES (1)",
        "INSERT INTO t (a, b) VALUES (1, 2)",
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3)",
        "INSERT INTO t VALUES (1)",
        "INSERT INTO t VALUES (1, 2)",
        "INSERT INTO t VALUES (1, 2, 3), (4, 5, 6)",
        "INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)",
        "INSERT INTO t SELECT * FROM t2",
        "INSERT INTO t (a, b) SELECT c, d FROM t2",
        "INSERT INTO t (a, b) SELECT c, d FROM t2 WHERE e > 10",
    ];
}

#[test]
fn insert_on_conflict() {
    test_queries![
        "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO NOTHING",
        "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b",
        "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = t.b + EXCLUDED.b",
        "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b WHERE t.b < EXCLUDED.b",
        "INSERT INTO t (a, b) VALUES (1, 2) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
        "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c",
    ];
}

#[test]
fn insert_returning() {
    test_queries![
        "INSERT INTO t (a) VALUES (1) RETURNING *",
        "INSERT INTO t (a) VALUES (1) RETURNING id",
        "INSERT INTO t (a, b) VALUES (1, 2) RETURNING id, a, b",
        "INSERT INTO t (a) VALUES (1) RETURNING id AS new_id",
    ];
}

#[test]
fn insert_with_cte() {
    test_queries![
        "WITH data AS (SELECT 1 AS a) INSERT INTO t SELECT * FROM data",
        "WITH ins AS (INSERT INTO t (a) VALUES (1) RETURNING *) SELECT * FROM ins",
    ];
}

// ============================================================================
// UPDATE statements
// ============================================================================

#[test]
fn update_basic() {
    test_queries![
        "UPDATE t SET a = 1",
        "UPDATE t SET a = 1, b = 2",
        "UPDATE t SET a = 1 WHERE id = 1",
        "UPDATE t SET a = a + 1",
        "UPDATE t SET a = b, b = a",
        "UPDATE t SET (a, b) = (1, 2)",
        "UPDATE t SET (a, b) = (SELECT c, d FROM t2 WHERE t2.id = t.id)",
        "UPDATE t SET a = (SELECT max(b) FROM t2)",
    ];
}

#[test]
fn update_from() {
    test_queries![
        "UPDATE t SET a = t2.b FROM t2 WHERE t.id = t2.id",
        "UPDATE t SET a = t2.b, c = t3.d FROM t2, t3 WHERE t.id = t2.id AND t.id = t3.id",
        "UPDATE t SET a = t2.b FROM t2 JOIN t3 ON t2.id = t3.id WHERE t.id = t2.id",
    ];
}

#[test]
fn update_returning() {
    test_queries![
        "UPDATE t SET a = 1 RETURNING *",
        "UPDATE t SET a = 1 RETURNING id",
        "UPDATE t SET a = 1 RETURNING id, a, b",
        "UPDATE t SET a = 1 WHERE id = 1 RETURNING id AS updated_id",
    ];
}

#[test]
fn update_with_cte() {
    test_queries![
        "WITH data AS (SELECT 1 AS id) UPDATE t SET a = 1 WHERE id IN (SELECT id FROM data)",
        "WITH upd AS (UPDATE t SET a = 1 RETURNING *) SELECT * FROM upd",
    ];
}

// ============================================================================
// DELETE statements
// ============================================================================

#[test]
fn delete_basic() {
    test_queries![
        "DELETE FROM t",
        "DELETE FROM t WHERE id = 1",
        "DELETE FROM t WHERE id IN (1, 2, 3)",
        "DELETE FROM t WHERE id IN (SELECT id FROM t2)",
        "DELETE FROM ONLY t",
        "DELETE FROM ONLY t WHERE id = 1",
    ];
}

#[test]
fn delete_using() {
    test_queries![
        "DELETE FROM t USING t2 WHERE t.id = t2.id",
        "DELETE FROM t USING t2, t3 WHERE t.id = t2.id AND t.id = t3.id",
        "DELETE FROM t USING t2 JOIN t3 ON t2.id = t3.id WHERE t.id = t2.id",
    ];
}

#[test]
fn delete_returning() {
    test_queries!["DELETE FROM t RETURNING *", "DELETE FROM t WHERE id = 1 RETURNING id", "DELETE FROM t WHERE id = 1 RETURNING id, a, b",];
}

#[test]
fn delete_with_cte() {
    test_queries![
        "WITH data AS (SELECT 1 AS id) DELETE FROM t WHERE id IN (SELECT id FROM data)",
        "WITH del AS (DELETE FROM t RETURNING *) SELECT * FROM del",
    ];
}

// ============================================================================
// CREATE TABLE statements
// ============================================================================

#[test]
fn create_table_basic() {
    test_queries![
        "CREATE TABLE t (a int)",
        "CREATE TABLE t (a int, b text)",
        "CREATE TABLE t (a int, b text, c boolean)",
        "CREATE TABLE IF NOT EXISTS t (a int)",
        "CREATE TEMP TABLE t (a int)",
        "CREATE TEMPORARY TABLE t (a int)",
        "CREATE UNLOGGED TABLE t (a int)",
        "CREATE TABLE s.t (a int)",
    ];
}

#[test]
fn create_table_data_types() {
    test_queries![
        "CREATE TABLE t (a smallint)",
        "CREATE TABLE t (a integer)",
        "CREATE TABLE t (a bigint)",
        "CREATE TABLE t (a int2)",
        "CREATE TABLE t (a int4)",
        "CREATE TABLE t (a int8)",
        "CREATE TABLE t (a decimal)",
        "CREATE TABLE t (a decimal(10))",
        "CREATE TABLE t (a decimal(10, 2))",
        "CREATE TABLE t (a numeric)",
        "CREATE TABLE t (a numeric(10))",
        "CREATE TABLE t (a numeric(10, 2))",
        "CREATE TABLE t (a real)",
        "CREATE TABLE t (a double precision)",
        "CREATE TABLE t (a float)",
        "CREATE TABLE t (a float(24))",
        "CREATE TABLE t (a float(53))",
        "CREATE TABLE t (a serial)",
        "CREATE TABLE t (a bigserial)",
        "CREATE TABLE t (a smallserial)",
        "CREATE TABLE t (a text)",
        "CREATE TABLE t (a varchar)",
        "CREATE TABLE t (a varchar(100))",
        "CREATE TABLE t (a char)",
        "CREATE TABLE t (a char(10))",
        "CREATE TABLE t (a character)",
        "CREATE TABLE t (a character(10))",
        "CREATE TABLE t (a character varying)",
        "CREATE TABLE t (a character varying(100))",
        "CREATE TABLE t (a bytea)",
        "CREATE TABLE t (a boolean)",
        "CREATE TABLE t (a bool)",
        "CREATE TABLE t (a date)",
        "CREATE TABLE t (a time)",
        "CREATE TABLE t (a time without time zone)",
        "CREATE TABLE t (a time with time zone)",
        "CREATE TABLE t (a timetz)",
        "CREATE TABLE t (a timestamp)",
        "CREATE TABLE t (a timestamp without time zone)",
        "CREATE TABLE t (a timestamp with time zone)",
        "CREATE TABLE t (a timestamptz)",
        "CREATE TABLE t (a interval)",
        "CREATE TABLE t (a uuid)",
        "CREATE TABLE t (a json)",
        "CREATE TABLE t (a jsonb)",
        "CREATE TABLE t (a xml)",
        "CREATE TABLE t (a money)",
        "CREATE TABLE t (a inet)",
        "CREATE TABLE t (a cidr)",
        "CREATE TABLE t (a macaddr)",
        "CREATE TABLE t (a macaddr8)",
        "CREATE TABLE t (a point)",
        "CREATE TABLE t (a line)",
        "CREATE TABLE t (a lseg)",
        "CREATE TABLE t (a box)",
        "CREATE TABLE t (a path)",
        "CREATE TABLE t (a polygon)",
        "CREATE TABLE t (a circle)",
        "CREATE TABLE t (a int4range)",
        "CREATE TABLE t (a int8range)",
        "CREATE TABLE t (a numrange)",
        "CREATE TABLE t (a tsrange)",
        "CREATE TABLE t (a tstzrange)",
        "CREATE TABLE t (a daterange)",
        "CREATE TABLE t (a tsvector)",
        "CREATE TABLE t (a tsquery)",
        "CREATE TABLE t (a bit)",
        "CREATE TABLE t (a bit(8))",
        "CREATE TABLE t (a bit varying)",
        "CREATE TABLE t (a bit varying(8))",
        "CREATE TABLE t (a int[])",
        "CREATE TABLE t (a int[3])",
        "CREATE TABLE t (a int[][])",
        "CREATE TABLE t (a text[])",
    ];
}

#[test]
fn create_table_constraints() {
    test_queries![
        "CREATE TABLE t (a int NOT NULL)",
        "CREATE TABLE t (a int NULL)",
        "CREATE TABLE t (a int DEFAULT 0)",
        "CREATE TABLE t (a int DEFAULT NULL)",
        "CREATE TABLE t (a serial PRIMARY KEY)",
        "CREATE TABLE t (a int UNIQUE)",
        "CREATE TABLE t (a int CHECK (a > 0))",
        "CREATE TABLE t (a int REFERENCES t2 (id))",
        "CREATE TABLE t (a int REFERENCES t2)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON DELETE CASCADE)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON UPDATE CASCADE)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON DELETE SET NULL)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON DELETE SET DEFAULT)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON DELETE RESTRICT)",
        "CREATE TABLE t (a int REFERENCES t2 (id) ON DELETE NO ACTION)",
        "CREATE TABLE t (a int GENERATED ALWAYS AS IDENTITY)",
        "CREATE TABLE t (a int GENERATED BY DEFAULT AS IDENTITY)",
        "CREATE TABLE t (a int GENERATED ALWAYS AS (b + c) STORED)",
        "CREATE TABLE t (a int, b int, PRIMARY KEY (a))",
        "CREATE TABLE t (a int, b int, PRIMARY KEY (a, b))",
        "CREATE TABLE t (a int, b int, UNIQUE (a))",
        "CREATE TABLE t (a int, b int, UNIQUE (a, b))",
        "CREATE TABLE t (a int, b int, CHECK (a > b))",
        "CREATE TABLE t (a int, b int, FOREIGN KEY (a) REFERENCES t2 (id))",
        "CREATE TABLE t (a int, b int, EXCLUDE USING gist (a WITH =))",
        "CREATE TABLE t (a int CONSTRAINT a_positive CHECK (a > 0))",
        "CREATE TABLE t (a int, CONSTRAINT pk PRIMARY KEY (a))",
    ];
}

#[test]
fn create_table_partitioning() {
    test_queries![
        "CREATE TABLE t (a int, b date) PARTITION BY RANGE (b)",
        "CREATE TABLE t (a int, b text) PARTITION BY LIST (b)",
        "CREATE TABLE t (a int, b int) PARTITION BY HASH (b)",
        "CREATE TABLE t (a int, b date, c text) PARTITION BY RANGE (b, c)",
        "CREATE TABLE t_part PARTITION OF t FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')",
        "CREATE TABLE t_part PARTITION OF t FOR VALUES IN ('a', 'b', 'c')",
        "CREATE TABLE t_part PARTITION OF t FOR VALUES WITH (MODULUS 4, REMAINDER 0)",
        "CREATE TABLE t_part PARTITION OF t DEFAULT",
    ];
}

#[test]
fn create_table_as() {
    test_queries![
        "CREATE TABLE t AS SELECT * FROM t2",
        "CREATE TABLE t AS SELECT a, b FROM t2 WHERE c > 10",
        "CREATE TABLE t (x, y) AS SELECT a, b FROM t2",
        "CREATE TABLE IF NOT EXISTS t AS SELECT * FROM t2",
        "CREATE TEMP TABLE t AS SELECT * FROM t2",
        "CREATE TABLE t AS SELECT * FROM t2 WITH DATA",
        "CREATE TABLE t AS SELECT * FROM t2 WITH NO DATA",
    ];
}

// ============================================================================
// CREATE INDEX statements
// ============================================================================

#[test]
fn create_index() {
    test_queries![
        "CREATE INDEX idx ON t (a)",
        "CREATE INDEX idx ON t (a, b)",
        "CREATE INDEX idx ON t (a ASC)",
        "CREATE INDEX idx ON t (a DESC)",
        "CREATE INDEX idx ON t (a NULLS FIRST)",
        "CREATE INDEX idx ON t (a NULLS LAST)",
        "CREATE INDEX idx ON t (a DESC NULLS FIRST)",
        "CREATE UNIQUE INDEX idx ON t (a)",
        "CREATE INDEX IF NOT EXISTS idx ON t (a)",
        "CREATE INDEX CONCURRENTLY idx ON t (a)",
        "CREATE INDEX idx ON t USING btree (a)",
        "CREATE INDEX idx ON t USING hash (a)",
        "CREATE INDEX idx ON t USING gist (a)",
        "CREATE INDEX idx ON t USING gin (a)",
        "CREATE INDEX idx ON t USING spgist (a)",
        "CREATE INDEX idx ON t USING brin (a)",
        "CREATE INDEX idx ON t (lower(a))",
        "CREATE INDEX idx ON t ((a + b))",
        "CREATE INDEX idx ON t (a) WHERE a > 0",
        "CREATE INDEX idx ON t (a) INCLUDE (b, c)",
        r#"CREATE INDEX idx ON t (a COLLATE "C")"#,
        "CREATE INDEX idx ON t (a text_pattern_ops)",
    ];
}

// ============================================================================
// CREATE VIEW statements
// ============================================================================

#[test]
fn create_view() {
    test_queries![
        "CREATE VIEW v AS SELECT * FROM t",
        "CREATE VIEW v (a, b) AS SELECT c, d FROM t",
        "CREATE OR REPLACE VIEW v AS SELECT * FROM t",
        "CREATE TEMP VIEW v AS SELECT * FROM t",
        "CREATE VIEW v AS SELECT * FROM t WITH CHECK OPTION",
        "CREATE VIEW v AS SELECT * FROM t WITH LOCAL CHECK OPTION",
        "CREATE VIEW v AS SELECT * FROM t WITH CASCADED CHECK OPTION",
        "CREATE RECURSIVE VIEW v (n) AS VALUES (1) UNION ALL SELECT n + 1 FROM v WHERE n < 10",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t",
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT * FROM t",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WITH DATA",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WITH NO DATA",
        "REFRESH MATERIALIZED VIEW mv",
        "REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
        "REFRESH MATERIALIZED VIEW mv WITH DATA",
        "REFRESH MATERIALIZED VIEW mv WITH NO DATA",
    ];
}

// ============================================================================
// ALTER TABLE statements
// ============================================================================

#[test]
fn alter_table() {
    test_queries![
        "ALTER TABLE t ADD COLUMN a int",
        "ALTER TABLE t ADD COLUMN IF NOT EXISTS a int",
        "ALTER TABLE t DROP COLUMN a",
        "ALTER TABLE t DROP COLUMN IF EXISTS a",
        "ALTER TABLE t DROP COLUMN a CASCADE",
        "ALTER TABLE t DROP COLUMN a RESTRICT",
        "ALTER TABLE t ALTER COLUMN a SET DATA TYPE text",
        "ALTER TABLE t ALTER COLUMN a TYPE text",
        "ALTER TABLE t ALTER COLUMN a TYPE text USING a::text",
        "ALTER TABLE t ALTER COLUMN a SET DEFAULT 0",
        "ALTER TABLE t ALTER COLUMN a DROP DEFAULT",
        "ALTER TABLE t ALTER COLUMN a SET NOT NULL",
        "ALTER TABLE t ALTER COLUMN a DROP NOT NULL",
        "ALTER TABLE t ALTER COLUMN a ADD GENERATED ALWAYS AS IDENTITY",
        "ALTER TABLE t ALTER COLUMN a SET GENERATED BY DEFAULT",
        "ALTER TABLE t ALTER COLUMN a DROP IDENTITY",
        "ALTER TABLE t ALTER COLUMN a DROP IDENTITY IF EXISTS",
        "ALTER TABLE t ALTER COLUMN a SET STATISTICS 1000",
        "ALTER TABLE t ADD CONSTRAINT c_check CHECK (a > 0)",
        "ALTER TABLE t ADD PRIMARY KEY (a)",
        "ALTER TABLE t ADD UNIQUE (a)",
        "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES t2 (id)",
        "ALTER TABLE t DROP CONSTRAINT c_check",
        "ALTER TABLE t DROP CONSTRAINT IF EXISTS c_check",
        "ALTER TABLE t DROP CONSTRAINT c_check CASCADE",
        "ALTER TABLE t RENAME TO t2",
        "ALTER TABLE t RENAME COLUMN a TO b",
        "ALTER TABLE t RENAME CONSTRAINT c1 TO c2",
        "ALTER TABLE t SET SCHEMA s",
        "ALTER TABLE t OWNER TO new_owner",
        "ALTER TABLE t SET TABLESPACE ts",
        "ALTER TABLE t CLUSTER ON idx",
        "ALTER TABLE t SET WITHOUT CLUSTER",
        "ALTER TABLE t SET LOGGED",
        "ALTER TABLE t SET UNLOGGED",
        "ALTER TABLE t INHERIT parent_t",
        "ALTER TABLE t NO INHERIT parent_t",
        "ALTER TABLE t ENABLE TRIGGER tr",
        "ALTER TABLE t DISABLE TRIGGER tr",
        "ALTER TABLE t ENABLE RULE r",
        "ALTER TABLE t DISABLE RULE r",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE t DISABLE ROW LEVEL SECURITY",
        "ALTER TABLE t FORCE ROW LEVEL SECURITY",
        "ALTER TABLE t NO FORCE ROW LEVEL SECURITY",
        "ALTER TABLE t REPLICA IDENTITY DEFAULT",
        "ALTER TABLE t REPLICA IDENTITY FULL",
        "ALTER TABLE t REPLICA IDENTITY NOTHING",
        "ALTER TABLE t REPLICA IDENTITY USING INDEX idx",
        "ALTER TABLE t ATTACH PARTITION p FOR VALUES FROM (1) TO (10)",
        "ALTER TABLE t DETACH PARTITION p",
        "ALTER TABLE t DETACH PARTITION p CONCURRENTLY",
        "ALTER TABLE t DETACH PARTITION p FINALIZE",
    ];
}

// ============================================================================
// DROP statements
// ============================================================================

#[test]
fn drop_statements() {
    test_queries![
        "DROP TABLE t",
        "DROP TABLE IF EXISTS t",
        "DROP TABLE t CASCADE",
        "DROP TABLE t RESTRICT",
        "DROP TABLE t1, t2, t3",
        "DROP TABLE IF EXISTS t1, t2 CASCADE",
        "DROP VIEW v",
        "DROP VIEW IF EXISTS v",
        "DROP VIEW v CASCADE",
        "DROP MATERIALIZED VIEW mv",
        "DROP MATERIALIZED VIEW IF EXISTS mv CASCADE",
        "DROP INDEX idx",
        "DROP INDEX IF EXISTS idx",
        "DROP INDEX CONCURRENTLY idx",
        "DROP INDEX CONCURRENTLY IF EXISTS idx",
        "DROP SEQUENCE seq",
        "DROP SEQUENCE IF EXISTS seq CASCADE",
        "DROP TYPE typ",
        "DROP TYPE IF EXISTS typ CASCADE",
        "DROP DOMAIN dom",
        "DROP DOMAIN IF EXISTS dom CASCADE",
        "DROP FUNCTION f",
        "DROP FUNCTION IF EXISTS f",
        "DROP FUNCTION f(int, text)",
        "DROP FUNCTION f CASCADE",
        "DROP PROCEDURE p",
        "DROP PROCEDURE IF EXISTS p",
        "DROP AGGREGATE agg(int)",
        "DROP OPERATOR +(int, int)",
        "DROP TRIGGER tr ON t",
        "DROP TRIGGER IF EXISTS tr ON t CASCADE",
        "DROP RULE r ON t",
        "DROP RULE IF EXISTS r ON t CASCADE",
        "DROP SCHEMA s",
        "DROP SCHEMA IF EXISTS s CASCADE",
        "DROP DATABASE db",
        "DROP DATABASE IF EXISTS db",
        "DROP ROLE r",
        "DROP ROLE IF EXISTS r",
        "DROP USER u",
        "DROP USER IF EXISTS u",
        "DROP EXTENSION ext",
        "DROP EXTENSION IF EXISTS ext CASCADE",
        "DROP FOREIGN TABLE ft",
        "DROP SERVER srv",
        "DROP FOREIGN DATA WRAPPER fdw",
        "DROP POLICY pol ON t",
        "DROP PUBLICATION pub",
        "DROP SUBSCRIPTION sub",
    ];
}

// ============================================================================
// CREATE TYPE and DOMAIN statements
// ============================================================================

#[test]
fn create_type() {
    test_queries![
        "CREATE TYPE status AS ENUM ('pending', 'approved', 'rejected')",
        "CREATE TYPE addr AS (street text, city text, zip text)",
        "CREATE TYPE my_range AS RANGE (SUBTYPE = float8)",
        "CREATE DOMAIN positive_int AS int CHECK (VALUE > 0)",
        "CREATE DOMAIN email AS text CHECK (VALUE ~ '^[^@]+@[^@]+$')",
        "CREATE DOMAIN non_empty AS text NOT NULL CHECK (VALUE <> '')",
        "CREATE DOMAIN my_int AS int DEFAULT 0",
        "ALTER TYPE status ADD VALUE 'cancelled'",
        "ALTER TYPE status ADD VALUE IF NOT EXISTS 'cancelled'",
        "ALTER TYPE status ADD VALUE 'processing' BEFORE 'approved'",
        "ALTER TYPE status ADD VALUE 'processing' AFTER 'pending'",
        "ALTER TYPE status RENAME VALUE 'pending' TO 'waiting'",
        "ALTER TYPE addr ADD ATTRIBUTE country text",
        "ALTER TYPE addr DROP ATTRIBUTE zip",
        "ALTER TYPE addr RENAME ATTRIBUTE city TO town",
        "ALTER DOMAIN positive_int SET DEFAULT 1",
        "ALTER DOMAIN positive_int DROP DEFAULT",
        "ALTER DOMAIN positive_int SET NOT NULL",
        "ALTER DOMAIN positive_int DROP NOT NULL",
        "ALTER DOMAIN positive_int ADD CONSTRAINT positive CHECK (VALUE > 0)",
        "ALTER DOMAIN positive_int DROP CONSTRAINT positive",
        "ALTER DOMAIN positive_int RENAME CONSTRAINT positive TO non_negative",
    ];
}

// ============================================================================
// CREATE FUNCTION and PROCEDURE statements
// ============================================================================

#[test]
fn create_function() {
    test_queries![
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
        "CREATE FUNCTION f(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
        "CREATE FUNCTION f(a int, b int) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql",
        "CREATE FUNCTION f(int, int) RETURNS int AS $$ SELECT $1 + $2 $$ LANGUAGE sql",
        "CREATE FUNCTION f(a int DEFAULT 0) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
        "CREATE FUNCTION f(IN a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
        "CREATE FUNCTION f(OUT result int) AS $$ SELECT 1 $$ LANGUAGE sql",
        "CREATE FUNCTION f(INOUT a int) AS $$ SELECT a + 1 $$ LANGUAGE sql",
        "CREATE FUNCTION f(VARIADIC arr int[]) RETURNS int AS $$ SELECT array_length(arr, 1) $$ LANGUAGE sql",
        "CREATE FUNCTION f() RETURNS TABLE (a int, b text) AS $$ SELECT 1, 'one' $$ LANGUAGE sql",
        "CREATE FUNCTION f() RETURNS SETOF int AS $$ SELECT generate_series(1, 10) $$ LANGUAGE sql",
        "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql IMMUTABLE",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql STABLE",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql VOLATILE",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql STRICT",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql RETURNS NULL ON NULL INPUT",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql CALLED ON NULL INPUT",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql SECURITY DEFINER",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql SECURITY INVOKER",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql PARALLEL SAFE",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql PARALLEL UNSAFE",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql PARALLEL RESTRICTED",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql COST 100",
        "CREATE FUNCTION f() RETURNS SETOF int AS $$ SELECT 1 $$ LANGUAGE sql ROWS 100",
        "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql LEAKPROOF",
        "CREATE PROCEDURE p() AS $$ SELECT 1 $$ LANGUAGE sql",
        "CREATE PROCEDURE p(a int) AS $$ SELECT a $$ LANGUAGE sql",
        "CREATE OR REPLACE PROCEDURE p() AS $$ SELECT 1 $$ LANGUAGE sql",
        "CALL p()",
        "CALL p(1, 2, 3)",
    ];
}

// ============================================================================
// CREATE TRIGGER statements
// ============================================================================

#[test]
fn create_trigger() {
    test_queries![
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE DELETE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER DELETE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE TRUNCATE ON t FOR EACH STATEMENT EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE INSERT OR UPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE INSERT OR UPDATE OR DELETE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER UPDATE OF a ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER UPDATE OF a, b ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW WHEN (NEW.a > 0) EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr AFTER UPDATE ON t FOR EACH ROW WHEN (OLD.a IS DISTINCT FROM NEW.a) EXECUTE FUNCTION f()",
        "CREATE TRIGGER tr INSTEAD OF INSERT ON v FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE CONSTRAINT TRIGGER tr AFTER INSERT ON t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE OR REPLACE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "ALTER TRIGGER tr ON t RENAME TO tr2",
        "ALTER TRIGGER tr ON t DEPENDS ON EXTENSION ext",
        // ENABLE/DISABLE TRIGGER are ALTER TABLE subcommands, not standalone statements
        "ALTER TABLE t ENABLE TRIGGER tr",
        "ALTER TABLE t DISABLE TRIGGER tr",
    ];
}

// ============================================================================
// Transaction statements
// ============================================================================

#[test]
fn transaction_statements() {
    test_queries![
        "BEGIN",
        "BEGIN WORK",
        "BEGIN TRANSACTION",
        "START TRANSACTION",
        "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
        "BEGIN ISOLATION LEVEL READ COMMITTED",
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "BEGIN READ ONLY",
        "BEGIN READ WRITE",
        "BEGIN DEFERRABLE",
        "BEGIN NOT DEFERRABLE",
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE",
        "COMMIT",
        "COMMIT WORK",
        "COMMIT TRANSACTION",
        "END",
        "END WORK",
        "END TRANSACTION",
        "ROLLBACK",
        "ROLLBACK WORK",
        "ROLLBACK TRANSACTION",
        "ABORT",
        "ABORT WORK",
        "ABORT TRANSACTION",
        "SAVEPOINT sp",
        "RELEASE SAVEPOINT sp",
        "RELEASE sp",
        "ROLLBACK TO SAVEPOINT sp",
        "ROLLBACK TO sp",
        "PREPARE TRANSACTION 'tx_id'",
        "COMMIT PREPARED 'tx_id'",
        "ROLLBACK PREPARED 'tx_id'",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "SET TRANSACTION READ ONLY",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "LOCK TABLE t",
        "LOCK TABLE t IN ACCESS SHARE MODE",
        "LOCK TABLE t IN ROW SHARE MODE",
        "LOCK TABLE t IN ROW EXCLUSIVE MODE",
        "LOCK TABLE t IN SHARE UPDATE EXCLUSIVE MODE",
        "LOCK TABLE t IN SHARE MODE",
        "LOCK TABLE t IN SHARE ROW EXCLUSIVE MODE",
        "LOCK TABLE t IN EXCLUSIVE MODE",
        "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
        "LOCK TABLE t IN ACCESS EXCLUSIVE MODE NOWAIT",
        "LOCK TABLE t1, t2 IN SHARE MODE",
    ];
}

// ============================================================================
// GRANT and REVOKE statements
// ============================================================================

#[test]
fn grant_revoke() {
    test_queries![
        "GRANT SELECT ON t TO u",
        "GRANT SELECT ON TABLE t TO u",
        "GRANT SELECT, INSERT ON t TO u",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON t TO u",
        "GRANT ALL ON t TO u",
        "GRANT ALL PRIVILEGES ON t TO u",
        "GRANT SELECT ON t TO PUBLIC",
        "GRANT SELECT ON t TO u WITH GRANT OPTION",
        "GRANT SELECT ON ALL TABLES IN SCHEMA s TO u",
        "GRANT USAGE ON SCHEMA s TO u",
        "GRANT CREATE ON SCHEMA s TO u",
        "GRANT ALL ON SCHEMA s TO u",
        "GRANT EXECUTE ON FUNCTION f() TO u",
        "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA s TO u",
        "GRANT USAGE ON SEQUENCE seq TO u",
        "GRANT SELECT, UPDATE ON SEQUENCE seq TO u",
        "GRANT USAGE ON TYPE t TO u",
        "GRANT USAGE ON DOMAIN d TO u",
        "GRANT USAGE ON FOREIGN DATA WRAPPER fdw TO u",
        "GRANT USAGE ON FOREIGN SERVER srv TO u",
        "GRANT CONNECT ON DATABASE db TO u",
        "GRANT CREATE ON DATABASE db TO u",
        "GRANT TEMP ON DATABASE db TO u",
        "GRANT ALL ON DATABASE db TO u",
        "GRANT r TO u",
        "GRANT r TO u WITH ADMIN OPTION",
        "REVOKE SELECT ON t FROM u",
        "REVOKE ALL ON t FROM u",
        "REVOKE SELECT ON t FROM u CASCADE",
        "REVOKE SELECT ON t FROM u RESTRICT",
        "REVOKE GRANT OPTION FOR SELECT ON t FROM u",
        "REVOKE ALL ON ALL TABLES IN SCHEMA s FROM u",
        "REVOKE r FROM u",
        "REVOKE ADMIN OPTION FOR r FROM u",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA s GRANT SELECT ON TABLES TO u",
        "ALTER DEFAULT PRIVILEGES FOR ROLE r IN SCHEMA s GRANT SELECT ON TABLES TO u",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA s REVOKE SELECT ON TABLES FROM u",
    ];
}

// ============================================================================
// EXPLAIN and ANALYZE statements
// ============================================================================

#[test]
fn explain_analyze() {
    test_queries![
        "EXPLAIN SELECT * FROM t",
        "EXPLAIN ANALYZE SELECT * FROM t",
        "EXPLAIN (ANALYZE) SELECT * FROM t",
        "EXPLAIN (ANALYZE true) SELECT * FROM t",
        "EXPLAIN (ANALYZE false) SELECT * FROM t",
        "EXPLAIN (VERBOSE) SELECT * FROM t",
        "EXPLAIN (COSTS) SELECT * FROM t",
        "EXPLAIN (COSTS false) SELECT * FROM t",
        "EXPLAIN (BUFFERS) SELECT * FROM t",
        "EXPLAIN (TIMING) SELECT * FROM t",
        "EXPLAIN (SUMMARY) SELECT * FROM t",
        "EXPLAIN (FORMAT TEXT) SELECT * FROM t",
        "EXPLAIN (FORMAT JSON) SELECT * FROM t",
        "EXPLAIN (FORMAT XML) SELECT * FROM t",
        "EXPLAIN (FORMAT YAML) SELECT * FROM t",
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM t",
        "EXPLAIN (SETTINGS) SELECT * FROM t",
        "EXPLAIN (WAL) SELECT * FROM t",
        "ANALYZE",
        "ANALYZE t",
        "ANALYZE t (a)",
        "ANALYZE t (a, b, c)",
        "ANALYZE VERBOSE t",
        "VACUUM",
        "VACUUM t",
        "VACUUM FULL t",
        "VACUUM FREEZE t",
        "VACUUM ANALYZE t",
        "VACUUM (FULL) t",
        "VACUUM (FREEZE) t",
        "VACUUM (VERBOSE) t",
        "VACUUM (ANALYZE) t",
        "VACUUM (DISABLE_PAGE_SKIPPING) t",
        "VACUUM (SKIP_LOCKED) t",
        "VACUUM (INDEX_CLEANUP) t",
        "VACUUM (TRUNCATE) t",
        "VACUUM (PARALLEL 4) t",
        "VACUUM (FULL, VERBOSE, ANALYZE) t",
    ];
}

// ============================================================================
// COPY statements
// ============================================================================

#[test]
fn copy_statements() {
    test_queries![
        "COPY t FROM STDIN",
        "COPY t TO STDOUT",
        "COPY t FROM '/path/to/file'",
        "COPY t TO '/path/to/file'",
        "COPY t (a, b, c) FROM STDIN",
        "COPY t (a, b, c) TO STDOUT",
        "COPY (SELECT * FROM t) TO STDOUT",
        "COPY t FROM STDIN WITH (FORMAT csv)",
        "COPY t FROM STDIN WITH (FORMAT text)",
        "COPY t FROM STDIN WITH (FORMAT binary)",
        "COPY t FROM STDIN WITH (DELIMITER ',')",
        "COPY t FROM STDIN WITH (NULL 'NULL')",
        "COPY t FROM STDIN WITH (HEADER)",
        "COPY t FROM STDIN WITH (HEADER true)",
        "COPY t FROM STDIN WITH (QUOTE '\"')",
        "COPY t FROM STDIN WITH (ESCAPE '\\\\')",
        "COPY t FROM STDIN WITH (FORCE_QUOTE (a, b))",
        "COPY t FROM STDIN WITH (FORCE_NOT_NULL (a, b))",
        "COPY t FROM STDIN WITH (FORCE_NULL (a, b))",
        "COPY t FROM STDIN WITH (ENCODING 'UTF8')",
        "COPY t FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ',')",
    ];
}

// ============================================================================
// SET and SHOW statements
// ============================================================================

#[test]
fn set_show_statements() {
    test_queries![
        "SET search_path TO public",
        "SET search_path = public",
        "SET search_path TO public, pg_catalog",
        "SET search_path = 'public'",
        "SET LOCAL search_path TO public",
        "SET SESSION search_path TO public",
        "SET timezone TO 'UTC'",
        "SET timezone = 'UTC'",
        "SET TIME ZONE 'UTC'",
        "SET TIME ZONE LOCAL",
        "SET TIME ZONE DEFAULT",
        "SET statement_timeout TO 5000",
        "SET statement_timeout = '5s'",
        "SET client_encoding TO 'UTF8'",
        "SET NAMES 'UTF8'",
        "SET work_mem TO '1GB'",
        "SET enable_seqscan TO off",
        "SET enable_seqscan = false",
        "RESET search_path",
        "RESET ALL",
        "RESET TIME ZONE",
        "SHOW search_path",
        "SHOW ALL",
        "SHOW timezone",
        "SHOW TIME ZONE",
        "SHOW SERVER_VERSION",
        "SHOW transaction_isolation",
    ];
}

// ============================================================================
// PREPARE and EXECUTE statements
// ============================================================================

#[test]
fn prepare_execute() {
    test_queries![
        "PREPARE stmt AS SELECT * FROM t WHERE id = $1",
        "PREPARE stmt (int) AS SELECT * FROM t WHERE id = $1",
        "PREPARE stmt (int, text) AS SELECT * FROM t WHERE id = $1 AND name = $2",
        "EXECUTE stmt",
        "EXECUTE stmt (1)",
        "EXECUTE stmt (1, 'foo')",
        "DEALLOCATE stmt",
        "DEALLOCATE PREPARE stmt",
        "DEALLOCATE ALL",
        "DEALLOCATE PREPARE ALL",
    ];
}

// ============================================================================
// LISTEN, NOTIFY, UNLISTEN statements
// ============================================================================

#[test]
fn listen_notify() {
    test_queries!["LISTEN channel", "UNLISTEN channel", "UNLISTEN *", "NOTIFY channel", "NOTIFY channel, 'payload'",];
}

// ============================================================================
// CURSOR statements
// ============================================================================

#[test]
fn cursor_statements() {
    test_queries![
        "DECLARE cur CURSOR FOR SELECT * FROM t",
        "DECLARE cur CURSOR WITH HOLD FOR SELECT * FROM t",
        "DECLARE cur CURSOR WITHOUT HOLD FOR SELECT * FROM t",
        "DECLARE cur BINARY CURSOR FOR SELECT * FROM t",
        "DECLARE cur INSENSITIVE CURSOR FOR SELECT * FROM t",
        "DECLARE cur SCROLL CURSOR FOR SELECT * FROM t",
        "DECLARE cur NO SCROLL CURSOR FOR SELECT * FROM t",
        "FETCH cur",
        "FETCH NEXT FROM cur",
        "FETCH PRIOR FROM cur",
        "FETCH FIRST FROM cur",
        "FETCH LAST FROM cur",
        "FETCH ABSOLUTE 10 FROM cur",
        "FETCH RELATIVE 5 FROM cur",
        "FETCH RELATIVE -5 FROM cur",
        "FETCH 10 FROM cur",
        "FETCH ALL FROM cur",
        "FETCH FORWARD FROM cur",
        "FETCH FORWARD 10 FROM cur",
        "FETCH FORWARD ALL FROM cur",
        "FETCH BACKWARD FROM cur",
        "FETCH BACKWARD 10 FROM cur",
        "FETCH BACKWARD ALL FROM cur",
        "MOVE cur",
        "MOVE NEXT IN cur",
        "MOVE FORWARD 10 IN cur",
        "CLOSE cur",
        "CLOSE ALL",
    ];
}

// ============================================================================
// DO statements
// ============================================================================

#[test]
fn do_statements() {
    test_queries!["DO $$ BEGIN NULL; END $$", "DO LANGUAGE plpgsql $$ BEGIN NULL; END $$", "DO $$ DECLARE x int; BEGIN x := 1; END $$",];
}

// ============================================================================
// DISCARD statements
// ============================================================================

#[test]
fn discard_statements() {
    test_queries!["DISCARD ALL", "DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP", "DISCARD TEMPORARY",];
}

// ============================================================================
// CLUSTER and REINDEX statements
// ============================================================================

#[test]
fn cluster_reindex() {
    test_queries![
        "CLUSTER",
        "CLUSTER t",
        "CLUSTER t USING idx",
        "CLUSTER VERBOSE t",
        "REINDEX TABLE t",
        "REINDEX INDEX idx",
        "REINDEX SCHEMA s",
        "REINDEX DATABASE db",
        "REINDEX SYSTEM db",
        "REINDEX TABLE CONCURRENTLY t",
        "REINDEX INDEX CONCURRENTLY idx",
        "REINDEX (VERBOSE) TABLE t",
        "REINDEX (TABLESPACE ts) TABLE t",
    ];
}

// ============================================================================
// TRUNCATE statements
// ============================================================================

#[test]
fn truncate_statements() {
    test_queries![
        "TRUNCATE t",
        "TRUNCATE TABLE t",
        "TRUNCATE t1, t2, t3",
        "TRUNCATE ONLY t",
        "TRUNCATE t CASCADE",
        "TRUNCATE t RESTRICT",
        "TRUNCATE t RESTART IDENTITY",
        "TRUNCATE t CONTINUE IDENTITY",
        "TRUNCATE t RESTART IDENTITY CASCADE",
    ];
}

// ============================================================================
// CREATE SCHEMA statements
// ============================================================================

#[test]
fn create_schema() {
    test_queries![
        "CREATE SCHEMA s",
        "CREATE SCHEMA IF NOT EXISTS s",
        "CREATE SCHEMA s AUTHORIZATION u",
        "CREATE SCHEMA AUTHORIZATION u",
        "CREATE SCHEMA IF NOT EXISTS AUTHORIZATION u",
    ];
}

// ============================================================================
// CREATE SEQUENCE statements
// ============================================================================

#[test]
fn create_sequence() {
    test_queries![
        "CREATE SEQUENCE seq",
        "CREATE SEQUENCE IF NOT EXISTS seq",
        "CREATE TEMP SEQUENCE seq",
        "CREATE SEQUENCE seq START WITH 1",
        "CREATE SEQUENCE seq START 1",
        "CREATE SEQUENCE seq INCREMENT BY 1",
        "CREATE SEQUENCE seq INCREMENT 1",
        "CREATE SEQUENCE seq MINVALUE 1",
        "CREATE SEQUENCE seq NO MINVALUE",
        "CREATE SEQUENCE seq MAXVALUE 100",
        "CREATE SEQUENCE seq NO MAXVALUE",
        "CREATE SEQUENCE seq CACHE 10",
        "CREATE SEQUENCE seq CYCLE",
        "CREATE SEQUENCE seq NO CYCLE",
        "CREATE SEQUENCE seq OWNED BY t.a",
        "CREATE SEQUENCE seq OWNED BY NONE",
        "CREATE SEQUENCE seq AS smallint",
        "CREATE SEQUENCE seq AS integer",
        "CREATE SEQUENCE seq AS bigint",
        "CREATE SEQUENCE seq START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 100 CACHE 10 CYCLE",
        "ALTER SEQUENCE seq RESTART",
        "ALTER SEQUENCE seq RESTART WITH 1",
        "ALTER SEQUENCE seq INCREMENT BY 2",
        "ALTER SEQUENCE seq OWNED BY t.b",
        "ALTER SEQUENCE seq SET SCHEMA s",
        "ALTER SEQUENCE seq RENAME TO seq2",
    ];
}

// ============================================================================
// CREATE EXTENSION statements
// ============================================================================

#[test]
fn create_extension() {
    test_queries![
        "CREATE EXTENSION ext",
        "CREATE EXTENSION IF NOT EXISTS ext",
        "CREATE EXTENSION ext WITH SCHEMA s",
        "CREATE EXTENSION ext VERSION '1.0'",
        "CREATE EXTENSION ext CASCADE",
        "ALTER EXTENSION ext UPDATE",
        "ALTER EXTENSION ext UPDATE TO '2.0'",
        "ALTER EXTENSION ext SET SCHEMA s",
        "ALTER EXTENSION ext ADD TABLE t",
        "ALTER EXTENSION ext DROP TABLE t",
    ];
}

// ============================================================================
// CREATE FOREIGN TABLE statements
// ============================================================================

#[test]
fn foreign_tables() {
    test_queries![
        "CREATE SERVER srv FOREIGN DATA WRAPPER fdw",
        "CREATE SERVER srv FOREIGN DATA WRAPPER fdw OPTIONS (host 'localhost', port '5432')",
        "CREATE SERVER IF NOT EXISTS srv FOREIGN DATA WRAPPER fdw",
        "ALTER SERVER srv OPTIONS (SET port '5433')",
        "ALTER SERVER srv OPTIONS (ADD dbname 'mydb')",
        "ALTER SERVER srv OPTIONS (DROP dbname)",
        "CREATE FOREIGN TABLE ft (a int, b text) SERVER srv",
        "CREATE FOREIGN TABLE IF NOT EXISTS ft (a int) SERVER srv",
        "CREATE FOREIGN TABLE ft (a int OPTIONS (column_name 'col_a')) SERVER srv",
        "CREATE FOREIGN TABLE ft (a int) SERVER srv OPTIONS (table_name 'remote_t')",
        "ALTER FOREIGN TABLE ft ADD COLUMN c int",
        "ALTER FOREIGN TABLE ft DROP COLUMN a",
        "ALTER FOREIGN TABLE ft OPTIONS (SET table_name 'new_remote_t')",
        "CREATE USER MAPPING FOR u SERVER srv",
        "CREATE USER MAPPING FOR u SERVER srv OPTIONS (user 'remote_u', password 'secret')",
        "CREATE USER MAPPING FOR PUBLIC SERVER srv",
        "CREATE USER MAPPING FOR CURRENT_USER SERVER srv",
        "ALTER USER MAPPING FOR u SERVER srv OPTIONS (SET password 'new_secret')",
        "DROP USER MAPPING FOR u SERVER srv",
        "DROP USER MAPPING IF EXISTS FOR u SERVER srv",
        "IMPORT FOREIGN SCHEMA s FROM SERVER srv INTO local_s",
        "IMPORT FOREIGN SCHEMA s LIMIT TO (t1, t2) FROM SERVER srv INTO local_s",
        "IMPORT FOREIGN SCHEMA s EXCEPT (t3) FROM SERVER srv INTO local_s",
    ];
}

// ============================================================================
// CREATE POLICY statements
// ============================================================================

#[test]
fn row_level_security() {
    test_queries![
        "CREATE POLICY pol ON t",
        "CREATE POLICY pol ON t FOR SELECT",
        "CREATE POLICY pol ON t FOR INSERT",
        "CREATE POLICY pol ON t FOR UPDATE",
        "CREATE POLICY pol ON t FOR DELETE",
        "CREATE POLICY pol ON t FOR ALL",
        "CREATE POLICY pol ON t TO PUBLIC",
        "CREATE POLICY pol ON t TO u",
        "CREATE POLICY pol ON t TO u, v",
        "CREATE POLICY pol ON t USING (user_id = current_user)",
        "CREATE POLICY pol ON t WITH CHECK (user_id = current_user)",
        "CREATE POLICY pol ON t AS PERMISSIVE",
        "CREATE POLICY pol ON t AS RESTRICTIVE",
        "CREATE POLICY pol ON t FOR SELECT TO u USING (user_id = current_user)",
        "CREATE POLICY pol ON t FOR INSERT TO u WITH CHECK (user_id = current_user)",
        "CREATE POLICY pol ON t FOR UPDATE TO u USING (user_id = current_user) WITH CHECK (user_id = current_user)",
        "ALTER POLICY pol ON t RENAME TO pol2",
        "ALTER POLICY pol ON t TO u",
        "ALTER POLICY pol ON t USING (true)",
        "ALTER POLICY pol ON t WITH CHECK (true)",
    ];
}

// ============================================================================
// CREATE PUBLICATION and SUBSCRIPTION statements
// ============================================================================

#[test]
fn publication_subscription() {
    test_queries![
        "CREATE PUBLICATION pub FOR ALL TABLES",
        "CREATE PUBLICATION pub FOR TABLE t",
        "CREATE PUBLICATION pub FOR TABLE t1, t2, t3",
        "CREATE PUBLICATION pub FOR TABLE t (a, b)",
        "CREATE PUBLICATION pub FOR TABLE t WHERE (a > 0)",
        "CREATE PUBLICATION pub FOR TABLES IN SCHEMA s",
        "CREATE PUBLICATION pub FOR TABLES IN SCHEMA s1, s2",
        "CREATE PUBLICATION pub WITH (publish = 'insert, update')",
        "ALTER PUBLICATION pub ADD TABLE t",
        "ALTER PUBLICATION pub DROP TABLE t",
        "ALTER PUBLICATION pub SET TABLE t1, t2",
        "ALTER PUBLICATION pub SET (publish = 'insert')",
        "ALTER PUBLICATION pub OWNER TO u",
        "ALTER PUBLICATION pub RENAME TO pub2",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=localhost dbname=mydb' PUBLICATION pub",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=localhost' PUBLICATION pub1, pub2",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=localhost' PUBLICATION pub WITH (copy_data = false)",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=localhost' PUBLICATION pub WITH (enabled = false)",
        "ALTER SUBSCRIPTION sub CONNECTION 'host=newhost dbname=mydb'",
        "ALTER SUBSCRIPTION sub SET PUBLICATION pub2",
        "ALTER SUBSCRIPTION sub ADD PUBLICATION pub3",
        "ALTER SUBSCRIPTION sub DROP PUBLICATION pub",
        "ALTER SUBSCRIPTION sub REFRESH PUBLICATION",
        "ALTER SUBSCRIPTION sub ENABLE",
        "ALTER SUBSCRIPTION sub DISABLE",
        "ALTER SUBSCRIPTION sub OWNER TO u",
        "ALTER SUBSCRIPTION sub RENAME TO sub2",
    ];
}

// ============================================================================
// Role and User management
// ============================================================================

#[test]
fn role_management() {
    test_queries![
        "CREATE ROLE r",
        // Note: PostgreSQL doesn't support IF NOT EXISTS for CREATE ROLE
        "CREATE ROLE r WITH LOGIN",
        "CREATE ROLE r WITH SUPERUSER",
        "CREATE ROLE r WITH CREATEDB",
        "CREATE ROLE r WITH CREATEROLE",
        "CREATE ROLE r WITH REPLICATION",
        "CREATE ROLE r WITH BYPASSRLS",
        "CREATE ROLE r WITH PASSWORD 'secret'",
        "CREATE ROLE r WITH PASSWORD NULL",
        "CREATE ROLE r WITH ENCRYPTED PASSWORD 'secret'",
        "CREATE ROLE r WITH VALID UNTIL '2024-01-01'",
        "CREATE ROLE r WITH VALID UNTIL 'infinity'",
        "CREATE ROLE r WITH CONNECTION LIMIT 10",
        "CREATE ROLE r WITH CONNECTION LIMIT -1",
        "CREATE ROLE r IN ROLE r2",
        "CREATE ROLE r IN ROLE r2, r3",
        "CREATE ROLE r ROLE r2",
        "CREATE ROLE r ADMIN r2",
        "CREATE ROLE r WITH LOGIN SUPERUSER CREATEDB PASSWORD 'secret'",
        "CREATE USER u",
        "CREATE USER u WITH PASSWORD 'secret'",
        "CREATE GROUP g",
        "CREATE GROUP g WITH USER u1, u2",
        "ALTER ROLE r WITH PASSWORD 'new_secret'",
        "ALTER ROLE r WITH LOGIN",
        "ALTER ROLE r WITH NOLOGIN",
        "ALTER ROLE r WITH SUPERUSER",
        "ALTER ROLE r WITH NOSUPERUSER",
        "ALTER ROLE r WITH CREATEDB",
        "ALTER ROLE r WITH NOCREATEDB",
        "ALTER ROLE r WITH CREATEROLE",
        "ALTER ROLE r WITH NOCREATEROLE",
        "ALTER ROLE r WITH REPLICATION",
        "ALTER ROLE r WITH NOREPLICATION",
        "ALTER ROLE r WITH BYPASSRLS",
        "ALTER ROLE r WITH NOBYPASSRLS",
        "ALTER ROLE r WITH CONNECTION LIMIT 20",
        "ALTER ROLE r RENAME TO r2",
        "ALTER ROLE r SET search_path TO public",
        "ALTER ROLE r IN DATABASE db SET search_path TO public",
        "ALTER ROLE r RESET search_path",
        "ALTER ROLE r RESET ALL",
        "ALTER USER u WITH PASSWORD 'secret'",
        "ALTER GROUP g ADD USER u",
        "ALTER GROUP g DROP USER u",
        "REASSIGN OWNED BY u TO u2",
        "DROP OWNED BY u",
        "DROP OWNED BY u CASCADE",
        "SET ROLE r",
        "SET ROLE NONE",
        "RESET ROLE",
        "SET SESSION AUTHORIZATION u",
        "SET SESSION AUTHORIZATION DEFAULT",
        "RESET SESSION AUTHORIZATION",
    ];
}

// ============================================================================
// Database management
// ============================================================================

#[test]
fn database_management() {
    test_queries![
        "CREATE DATABASE db",
        // Note: PostgreSQL doesn't support IF NOT EXISTS for CREATE DATABASE
        "CREATE DATABASE db WITH OWNER = u",
        "CREATE DATABASE db WITH TEMPLATE = template0",
        "CREATE DATABASE db WITH ENCODING = 'UTF8'",
        r#"CREATE DATABASE db WITH LC_COLLATE = 'en_US.UTF-8'"#,
        r#"CREATE DATABASE db WITH LC_CTYPE = 'en_US.UTF-8'"#,
        "CREATE DATABASE db WITH TABLESPACE = ts",
        "CREATE DATABASE db WITH ALLOW_CONNECTIONS = false",
        "CREATE DATABASE db WITH CONNECTION LIMIT = 10",
        "CREATE DATABASE db WITH IS_TEMPLATE = true",
        "ALTER DATABASE db WITH CONNECTION LIMIT = 20",
        "ALTER DATABASE db WITH ALLOW_CONNECTIONS = true",
        "ALTER DATABASE db WITH IS_TEMPLATE = false",
        "ALTER DATABASE db RENAME TO db2",
        "ALTER DATABASE db OWNER TO u",
        "ALTER DATABASE db SET TABLESPACE ts",
        "ALTER DATABASE db SET search_path TO public",
        "ALTER DATABASE db RESET search_path",
        "ALTER DATABASE db RESET ALL",
    ];
}

// ============================================================================
// Tablespace management
// ============================================================================

#[test]
fn tablespace_management() {
    test_queries![
        "CREATE TABLESPACE ts LOCATION '/path/to/ts'",
        "CREATE TABLESPACE ts OWNER u LOCATION '/path/to/ts'",
        "ALTER TABLESPACE ts RENAME TO ts2",
        "ALTER TABLESPACE ts OWNER TO u",
        "ALTER TABLESPACE ts SET (seq_page_cost = 1.0)",
        "ALTER TABLESPACE ts RESET (seq_page_cost)",
    ];
}

// ============================================================================
// Comments
// ============================================================================

#[test]
fn comment_statements() {
    test_queries![
        "COMMENT ON TABLE t IS 'This is a table'",
        "COMMENT ON TABLE t IS NULL",
        "COMMENT ON COLUMN t.a IS 'This is a column'",
        "COMMENT ON INDEX idx IS 'This is an index'",
        "COMMENT ON SEQUENCE seq IS 'This is a sequence'",
        "COMMENT ON VIEW v IS 'This is a view'",
        "COMMENT ON MATERIALIZED VIEW mv IS 'This is a materialized view'",
        "COMMENT ON FUNCTION f() IS 'This is a function'",
        "COMMENT ON FUNCTION f(int, text) IS 'This is a function'",
        "COMMENT ON PROCEDURE p() IS 'This is a procedure'",
        "COMMENT ON TRIGGER tr ON t IS 'This is a trigger'",
        "COMMENT ON RULE r ON t IS 'This is a rule'",
        "COMMENT ON TYPE typ IS 'This is a type'",
        "COMMENT ON DOMAIN dom IS 'This is a domain'",
        "COMMENT ON SCHEMA s IS 'This is a schema'",
        "COMMENT ON DATABASE db IS 'This is a database'",
        "COMMENT ON TABLESPACE ts IS 'This is a tablespace'",
        "COMMENT ON ROLE r IS 'This is a role'",
        "COMMENT ON EXTENSION ext IS 'This is an extension'",
        "COMMENT ON CONSTRAINT c ON t IS 'This is a constraint'",
        "COMMENT ON POLICY pol ON t IS 'This is a policy'",
    ];
}

// ============================================================================
// Security labels
// ============================================================================

#[test]
fn security_label_statements() {
    test_queries![
        "SECURITY LABEL ON TABLE t IS 'unclassified'",
        "SECURITY LABEL FOR selinux ON TABLE t IS 'unclassified'",
        "SECURITY LABEL ON COLUMN t.a IS 'unclassified'",
        "SECURITY LABEL ON TABLE t IS NULL",
    ];
}

// ============================================================================
// Error cases - syntax errors should fail
// ============================================================================

#[test]
fn error_cases() {
    // These should fail with parse errors
    let error_queries = [
        "SELEC * FROM t",
        "SELECT * FORM t",
        "SELECT * FROM",
        "INSERT INTO",
        "UPDATE SET a = 1",
        "DELETE WHERE id = 1",
        "CREATE TABLE",
        "DROP",
        "SELECT * FROM t WHERE",
        "SELECT * FROM t ORDER BY",
        "SELECT a, FROM t",
        "(SELECT",
    ];

    for query in error_queries {
        let raw_result = parse_raw(query);
        let proto_result = parse(query);

        assert!(raw_result.is_err(), "Expected parse_raw to fail for: {}", query);
        assert!(proto_result.is_err(), "Expected parse to fail for: {}", query);

        // Both should produce Parse errors
        assert!(matches!(raw_result, Err(Error::Parse(_))), "Expected Parse error from parse_raw for: {}", query);
        assert!(matches!(proto_result, Err(Error::Parse(_))), "Expected Parse error from parse for: {}", query);
    }
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn edge_cases() {
    test_queries![
        // Empty or whitespace-only comments
        "-- comment\nSELECT 1",
        "/* comment */ SELECT 1",
        "SELECT /* inline comment */ 1",
        "SELECT 1 -- trailing comment",
        // Multiple statements
        "SELECT 1; SELECT 2",
        "SELECT 1; SELECT 2; SELECT 3",
        // Quoted identifiers
        r#"SELECT "Column" FROM "Table""#,
        r#"SELECT "select" FROM "from""#,
        r#"SELECT """" FROM t"#,
        // Unicode identifiers
        "SELECT * FROM tbl_日本語",
        // Very long identifiers (up to 63 chars by default)
        "SELECT * FROM a123456789012345678901234567890123456789012345678901234567890123",
        // Operators
        "SELECT 1 <> 2",
        "SELECT 1 != 2",
        "SELECT 'a' || 'b'",
        // Row constructors
        "SELECT ROW(1, 2, 3)",
        "SELECT (1, 2, 3)",
        "SELECT * FROM t WHERE (a, b) = (1, 2)",
        // Table inheritance
        "SELECT * FROM ONLY t",
        "SELECT * FROM t*",
        // Schema-qualified names
        "SELECT * FROM schema.table",
        "SELECT * FROM catalog.schema.table",
        "SELECT schema.function()",
        "SELECT schema.type 'value'",
    ];
}

// ============================================================================
// Misc PostgreSQL features
// ============================================================================

#[test]
fn misc_postgres_features() {
    test_queries![
        // RETURNING with expressions
        "INSERT INTO t (a) VALUES (1) RETURNING a + 1 AS incremented",
        // Table functions
        "SELECT * FROM generate_series(1, 10)",
        "SELECT * FROM generate_series(1, 10) AS n",
        "SELECT * FROM generate_series(1, 10) WITH ORDINALITY",
        "SELECT * FROM generate_series(1, 10) WITH ORDINALITY AS t(n, ord)",
        "SELECT * FROM unnest(ARRAY[1, 2, 3]) WITH ORDINALITY",
        "SELECT * FROM ROWS FROM (generate_series(1, 3), generate_series(1, 4))",
        // XML functions
        "SELECT xmlelement(name foo)",
        "SELECT xmlelement(name foo, 'content')",
        "SELECT xmlforest(a, b, c)",
        "SELECT xmlconcat('<a/>'::xml, '<b/>'::xml)",
        "SELECT xmlagg(x) FROM t",
        "SELECT xpath('/a/b', '<a><b>c</b></a>'::xml)",
        // Full text search
        "SELECT to_tsvector('english', 'The quick brown fox')",
        "SELECT to_tsquery('english', 'quick & fox')",
        "SELECT ts_rank(to_tsvector('english', 'The quick brown fox'), to_tsquery('english', 'fox'))",
        "SELECT * FROM t WHERE tsv @@ to_tsquery('english', 'quick')",
        // Range types
        "SELECT int4range(1, 10)",
        "SELECT int4range(1, 10, '[]')",
        "SELECT '[1,10)'::int4range",
        "SELECT * FROM t WHERE r @> 5",
        "SELECT * FROM t WHERE r && int4range(1, 5)",
        // Geometric operators
        "SELECT point(1, 2)",
        "SELECT box(point(0, 0), point(1, 1))",
        "SELECT circle '<(0,0),1>'",
        // Network operators
        "SELECT '192.168.1.0/24'::inet >> '192.168.1.5'::inet",
        "SELECT '192.168.1.0/24'::inet <<= '192.168.0.0/16'::inet",
        // BETWEEN SYMMETRIC
        "SELECT * FROM t WHERE a BETWEEN SYMMETRIC 10 AND 1",
        // Table sampling
        "SELECT * FROM t TABLESAMPLE SYSTEM (10)",
        "SELECT * FROM t TABLESAMPLE BERNOULLI (10)",
        "SELECT * FROM t TABLESAMPLE SYSTEM (10) REPEATABLE (42)",
        // SELECT INTO
        "SELECT * INTO newtable FROM t",
        "SELECT * INTO TEMP newtable FROM t",
        "SELECT * INTO TEMPORARY newtable FROM t",
        "SELECT * INTO UNLOGGED newtable FROM t",
    ];
}

// ============================================================================
// Implicit row expressions
// ============================================================================

#[test]
fn row_expressions() {
    test_queries![
        "SELECT ROW(1, 2, 3)",
        "SELECT ROW(1, 'a', true)",
        "SELECT (1, 2, 3)",
        "SELECT * FROM t WHERE (a, b) = (1, 2)",
        "SELECT * FROM t WHERE (a, b) <> (1, 2)",
        "SELECT * FROM t WHERE (a, b) < (1, 2)",
        "SELECT * FROM t WHERE (a, b) <= (1, 2)",
        "SELECT * FROM t WHERE (a, b) > (1, 2)",
        "SELECT * FROM t WHERE (a, b) >= (1, 2)",
        "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4))",
        "SELECT * FROM t WHERE ROW(a, b) = ROW(1, 2)",
    ];
}
