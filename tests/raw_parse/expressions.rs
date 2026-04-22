//! Expression tests: literals, type casts, arrays, JSON, operators.
//!
//! These tests verify parse_raw correctly handles various expressions.

use super::*;

// ============================================================================
// Literal value tests
// ============================================================================

/// Test parsing float with leading dot
#[test]
fn it_parses_floats_with_leading_dot() {
    let query = "SELECT .1";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify the float value
    let raw_const = get_first_const(&raw_result.protobuf).expect("should have const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("should have const");
    assert_eq!(raw_const, proto_const);
}

/// Test parsing bit string in hex notation
#[test]
fn it_parses_bit_strings_hex() {
    let query = "SELECT X'EFFF'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    // Verify the bit string value
    let raw_const = get_first_const(&raw_result.protobuf).expect("should have const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("should have const");
    assert_eq!(raw_const, proto_const);
}

/// Test parsing real-world query with multiple joins
#[test]
fn it_parses_real_world_query() {
    let query = "
        SELECT memory_total_bytes, memory_free_bytes, memory_pagecache_bytes,
            (memory_swap_total_bytes - memory_swap_free_bytes) AS swap
        FROM snapshots s JOIN system_snapshots ON (snapshot_id = s.id)
        WHERE s.database_id = 1 AND s.collected_at BETWEEN '2021-01-01' AND '2021-12-31'
        ORDER BY collected_at";
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
    assert_eq!(raw_tables, vec!["snapshots", "system_snapshots"]);
}
// ============================================================================
// A_Const value extraction tests
// ============================================================================

/// Test that parse_raw extracts integer values correctly and matches parse
#[test]
fn it_extracts_integer_const() {
    let query = "SELECT 42";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Ival(int_val)) => {
            assert_eq!(int_val.ival, 42);
        }
        other => panic!("Expected Ival, got {:?}", other),
    }
}

/// Test that parse_raw extracts negative integer values correctly
#[test]
fn it_extracts_negative_integer_const() {
    let query = "SELECT -123";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that parse_raw extracts string values correctly and matches parse
#[test]
fn it_extracts_string_const() {
    let query = "SELECT 'hello world'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Sval(str_val)) => {
            assert_eq!(str_val.sval, "hello world");
        }
        other => panic!("Expected Sval, got {:?}", other),
    }
}

/// Test that parse_raw extracts float values correctly and matches parse
#[test]
fn it_extracts_float_const() {
    let query = "SELECT 3.14159";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Fval(float_val)) => {
            assert_eq!(float_val.fval, "3.14159");
        }
        other => panic!("Expected Fval, got {:?}", other),
    }
}

/// Test that parse_raw extracts boolean TRUE correctly and matches parse
#[test]
fn it_extracts_boolean_true_const() {
    let query = "SELECT TRUE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Boolval(bool_val)) => {
            assert!(bool_val.boolval);
        }
        other => panic!("Expected Boolval(true), got {:?}", other),
    }
}

/// Test that parse_raw extracts boolean FALSE correctly and matches parse
#[test]
fn it_extracts_boolean_false_const() {
    let query = "SELECT FALSE";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Boolval(bool_val)) => {
            assert!(!bool_val.boolval);
        }
        other => panic!("Expected Boolval(false), got {:?}", other),
    }
}

/// Test that parse_raw extracts NULL correctly and matches parse
#[test]
fn it_extracts_null_const() {
    let query = "SELECT NULL";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(raw_const.isnull);
    assert!(raw_const.val.is_none());
}

/// Test that parse_raw extracts bit string values correctly and matches parse
#[test]
fn it_extracts_bit_string_const() {
    let query = "SELECT B'1010'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Bsval(bit_val)) => {
            assert_eq!(bit_val.bsval, "b1010");
        }
        other => panic!("Expected Bsval, got {:?}", other),
    }
}

/// Test that parse_raw extracts hex bit string correctly and matches parse
#[test]
fn it_extracts_hex_bit_string_const() {
    let query = "SELECT X'FF'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);

    let raw_const = get_first_const(&raw_result.protobuf).expect("Should have A_Const");
    let proto_const = get_first_const(&proto_result.protobuf).expect("Should have A_Const");

    assert_eq!(raw_const, proto_const);
    assert!(!raw_const.isnull);
    match &raw_const.val {
        Some(a_const::Val::Bsval(bit_val)) => {
            assert_eq!(bit_val.bsval, "xFF");
        }
        other => panic!("Expected Bsval, got {:?}", other),
    }
}
// ============================================================================
// Expression tests
// ============================================================================

/// Test COALESCE
#[test]
fn it_parses_coalesce() {
    let query = "SELECT COALESCE(nickname, name, 'Unknown') FROM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NULLIF
#[test]
fn it_parses_nullif() {
    let query = "SELECT NULLIF(status, 'deleted') FROM records";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test GREATEST and LEAST
#[test]
fn it_parses_greatest_least() {
    let query = "SELECT GREATEST(a, b, c), LEAST(x, y, z) FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test IS NULL and IS NOT NULL
#[test]
fn it_parses_null_tests() {
    let query = "SELECT * FROM users WHERE deleted_at IS NULL AND email IS NOT NULL";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test IS DISTINCT FROM
#[test]
fn it_parses_is_distinct_from() {
    let query = "SELECT * FROM t WHERE a IS DISTINCT FROM b";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test BETWEEN
#[test]
fn it_parses_between() {
    let query = "SELECT * FROM events WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LIKE and ILIKE
#[test]
fn it_parses_like_ilike() {
    let query = "SELECT * FROM users WHERE name LIKE 'John%' OR email ILIKE '%@EXAMPLE.COM'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SIMILAR TO
#[test]
fn it_parses_similar_to() {
    let query = "SELECT * FROM products WHERE name SIMILAR TO '%(phone|tablet)%'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test complex boolean expressions
#[test]
fn it_parses_complex_boolean() {
    let query = "SELECT * FROM users WHERE (active = true AND verified = true) OR (role = 'admin' AND NOT suspended)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Type cast tests
// ============================================================================

/// Test PostgreSQL-style type cast
#[test]
fn it_parses_pg_type_cast() {
    let query = "SELECT '123'::integer, '2023-01-01'::date, 'true'::boolean";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SQL-style CAST
#[test]
fn it_parses_sql_cast() {
    let query = "SELECT CAST('123' AS integer), CAST(created_at AS date) FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array type cast
#[test]
fn it_parses_array_cast() {
    let query = "SELECT ARRAY[1, 2, 3]::text[]";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Array and JSON tests
// ============================================================================

/// Test array constructor
#[test]
fn it_parses_array_constructor() {
    let query = "SELECT ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array subscript
#[test]
fn it_parses_array_subscript() {
    let query = "SELECT tags[1], matrix[1][2] FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array slice
#[test]
fn it_parses_array_slice() {
    let query = "SELECT arr[2:4], arr[:3], arr[2:] FROM t";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test unnest
#[test]
fn it_parses_unnest() {
    let query = "SELECT unnest(ARRAY[1, 2, 3])";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test JSON operators
#[test]
fn it_parses_json_operators() {
    let query = "SELECT data->'name', data->>'email', data#>'{address,city}' FROM users";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test JSONB containment
#[test]
fn it_parses_jsonb_containment() {
    let query = "SELECT * FROM products WHERE metadata @> '{\"featured\": true}'";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
// ============================================================================
// Parameter placeholder tests
// ============================================================================

/// Test positional parameters
#[test]
fn it_parses_positional_params() {
    let query = "SELECT * FROM users WHERE id = $1 AND status = $2";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test parameters in INSERT
#[test]
fn it_parses_params_in_insert() {
    let query = "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

// ============================================================================
// SQL Value Function tests
// ============================================================================

/// Test CURRENT_TIMESTAMP (was causing infinite recursion)
#[test]
fn it_parses_current_timestamp() {
    let query = "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (6, 1, 37553, -2309, CURRENT_TIMESTAMP)";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test other SQL value functions
#[test]
fn it_parses_sql_value_functions() {
    let query = "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP, CURRENT_USER, CURRENT_CATALOG, CURRENT_SCHEMA";
    let raw_result = parse_raw(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test USER keyword
#[test]
fn it_parses_user_keyword() {
    let query = "SELECT USER";
    parse_test!(query);
}

/// Test SESSION_USER
#[test]
fn it_parses_session_user() {
    let query = "SELECT SESSION_USER";
    parse_test!(query);
}

// ============================================================================
// XML expression tests
// ============================================================================

/// Test XMLELEMENT
#[test]
fn it_parses_xmlelement() {
    let query = "SELECT XMLELEMENT(NAME root, XMLATTRIBUTES(1 AS id), 'text')";
    parse_test!(query);
}

/// Test XMLFOREST
#[test]
fn it_parses_xmlforest() {
    let query = "SELECT XMLFOREST('value' AS elem, 1 AS id) FROM t";
    parse_test!(query);
}

/// Test XMLCONCAT
#[test]
fn it_parses_xmlconcat() {
    let query = "SELECT XMLCONCAT('<a/>', '<b/>')";
    parse_test!(query);
}

/// Test XMLPARSE
#[test]
fn it_parses_xmlparse() {
    let query = "SELECT XMLPARSE(DOCUMENT '<root>data</root>')";
    parse_test!(query);
}

/// Test XMLSERIALIZE
#[test]
fn it_parses_xmlserialize() {
    let query = "SELECT XMLSERIALIZE(DOCUMENT xml_data AS text) FROM t";
    parse_test!(query);
}

/// Test XMLPI
#[test]
fn it_parses_xmlpi() {
    let query = "SELECT XMLPI(NAME php, 'echo hello')";
    parse_test!(query);
}

/// Test XMLROOT
#[test]
fn it_parses_xmlroot() {
    let query = "SELECT XMLROOT(XMLPARSE(DOCUMENT '<a/>'), VERSION '1.0', STANDALONE YES)";
    parse_test!(query);
}

/// Test IS DOCUMENT
#[test]
fn it_parses_is_document() {
    let query = "SELECT * FROM t WHERE col IS DOCUMENT";
    parse_test!(query);
}

/// Test XMLEXISTS
#[test]
fn it_parses_xmlexists() {
    let query = "SELECT XMLEXISTS('//book' PASSING BY REF my_xml)";
    parse_test!(query);
}

// ============================================================================
// JSON expression tests
// ============================================================================

/// Test json_object
#[test]
fn it_parses_json_object_function() {
    let query = "SELECT json_object('{a, 1, b, 2}')";
    parse_test!(query);
}

/// Test JSON object constructor
#[test]
fn it_parses_json_object_constructor() {
    let query = "SELECT JSON_OBJECT('name': 'alice', 'id': 1)";
    parse_test!(query);
}

/// Test JSON array constructor
#[test]
fn it_parses_json_array_constructor() {
    let query = "SELECT JSON_ARRAY(1, 2, 3)";
    parse_test!(query);
}

/// Test JSON array from query
#[test]
fn it_parses_json_array_query() {
    let query = "SELECT JSON_ARRAY(SELECT name FROM users)";
    parse_test!(query);
}

/// Test JSON_OBJECTAGG
#[test]
fn it_parses_json_objectagg() {
    let query = "SELECT JSON_OBJECTAGG(key: value) FROM t";
    parse_test!(query);
}

/// Test JSON_ARRAYAGG
#[test]
fn it_parses_json_arrayagg() {
    let query = "SELECT JSON_ARRAYAGG(name ORDER BY id) FROM users";
    parse_test!(query);
}

/// Test IS JSON predicate
#[test]
fn it_parses_is_json() {
    let query = "SELECT * FROM t WHERE col IS JSON";
    parse_test!(query);
}

/// Test IS JSON OBJECT
#[test]
fn it_parses_is_json_object() {
    let query = "SELECT * FROM t WHERE col IS JSON OBJECT";
    parse_test!(query);
}

/// Test IS NOT JSON
#[test]
fn it_parses_is_not_json() {
    let query = "SELECT * FROM t WHERE col IS NOT JSON";
    parse_test!(query);
}

/// Test JSON_VALUE
#[test]
fn it_parses_json_value() {
    let query = "SELECT JSON_VALUE(col, '$.name') FROM t";
    parse_test!(query);
}

/// Test JSON_QUERY
#[test]
fn it_parses_json_query() {
    let query = "SELECT JSON_QUERY(col, '$.items[*]') FROM t";
    parse_test!(query);
}

/// Test JSON_EXISTS
#[test]
fn it_parses_json_exists() {
    let query = "SELECT * FROM t WHERE JSON_EXISTS(col, '$.items')";
    parse_test!(query);
}

/// Test JSON_SERIALIZE
#[test]
fn it_parses_json_serialize() {
    let query = "SELECT JSON_SERIALIZE(col RETURNING text) FROM t";
    parse_test!(query);
}

/// Test JSON with RETURNING
#[test]
fn it_parses_json_object_returning() {
    let query = "SELECT JSON_OBJECT('a': 1 RETURNING jsonb)";
    parse_test!(query);
}

/// Test JSON_TABLE basic
#[test]
fn it_parses_json_table() {
    let query = "SELECT * FROM JSON_TABLE('{\"a\": 1}', '$' COLUMNS (a int PATH '$.a'))";
    parse_test!(query);
}

// ============================================================================
// GROUPING function and BooleanTest
// ============================================================================

/// Test GROUPING() function
#[test]
fn it_parses_grouping_function() {
    let query = "SELECT region, product, GROUPING(region, product), SUM(sales) FROM sales GROUP BY ROLLUP(region, product)";
    parse_test!(query);
}

/// Regression: GROUPING() in SELECT target list used to SIGABRT because
/// GroupingFunc was routed to the null-returning arm in write_node_inner,
/// which produced a ResTarget with a NULL val and the deparser elog(ERROR)d.
#[test]
fn it_parses_grouping_bare_call() {
    parse_test!("SELECT GROUPING(a, b) FROM t");
}

/// Test IS TRUE
#[test]
fn it_parses_is_true() {
    let query = "SELECT * FROM t WHERE active IS TRUE";
    parse_test!(query);
}

/// Test IS FALSE
#[test]
fn it_parses_is_false() {
    let query = "SELECT * FROM t WHERE flag IS FALSE";
    parse_test!(query);
}

/// Test IS UNKNOWN
#[test]
fn it_parses_is_unknown() {
    let query = "SELECT * FROM t WHERE flag IS UNKNOWN";
    parse_test!(query);
}

/// Test IS NOT TRUE
#[test]
fn it_parses_is_not_true() {
    let query = "SELECT * FROM t WHERE flag IS NOT TRUE";
    parse_test!(query);
}

/// Test IS NOT FALSE
#[test]
fn it_parses_is_not_false() {
    let query = "SELECT * FROM t WHERE flag IS NOT FALSE";
    parse_test!(query);
}

// ============================================================================
// COLLATE expression
// ============================================================================

/// Test COLLATE in WHERE
#[test]
fn it_parses_collate_expression() {
    let query = "SELECT name FROM users WHERE name COLLATE \"C\" < 'M'";
    parse_test!(query);
}

/// Test COLLATE in ORDER BY
#[test]
fn it_parses_collate_order_by() {
    let query = "SELECT name FROM users ORDER BY name COLLATE \"en_US\"";
    parse_test!(query);
}

// ============================================================================
// Named argument expressions and function variations
// ============================================================================

/// Test function call with named arguments
#[test]
fn it_parses_named_args() {
    let query = "SELECT make_timestamp(year => 2023, month => 1, day => 1, hour => 0, min => 0, sec => 0.0)";
    parse_test!(query);
}

/// Test function call with VARIADIC
#[test]
fn it_parses_variadic_func() {
    let query = "SELECT concat_ws(',', VARIADIC ARRAY['a', 'b', 'c'])";
    parse_test!(query);
}

/// Test function call WITHIN GROUP
#[test]
fn it_parses_within_group() {
    let query = "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) FROM employees";
    parse_test!(query);
}

/// Test aggregate with FILTER
#[test]
fn it_parses_aggregate_filter() {
    let query = "SELECT SUM(amount) FILTER (WHERE status = 'completed') FROM orders";
    parse_test!(query);
}

/// Test ORDER BY within aggregate
#[test]
fn it_parses_agg_order_by() {
    let query = "SELECT array_agg(name ORDER BY id DESC) FROM users";
    parse_test!(query);
}

/// Test string_agg with DISTINCT
#[test]
fn it_parses_string_agg_distinct() {
    let query = "SELECT string_agg(DISTINCT category, ', ') FROM products";
    parse_test!(query);
}

// ============================================================================
// Row constructors and row comparisons
// ============================================================================

/// Test ROW constructor
#[test]
fn it_parses_row_constructor() {
    let query = "SELECT ROW(1, 'a', true)";
    parse_test!(query);
}

/// Test row comparison
#[test]
fn it_parses_row_comparison() {
    let query = "SELECT * FROM t WHERE (a, b) = (1, 2)";
    parse_test!(query);
}

/// Test IN with row constructor
#[test]
fn it_parses_in_with_row() {
    let query = "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4))";
    parse_test!(query);
}

// ============================================================================
// String operations, substring, trim, overlay
// ============================================================================

/// Test SUBSTRING with FROM/FOR
#[test]
fn it_parses_substring_from_for() {
    let query = "SELECT SUBSTRING(name FROM 1 FOR 5) FROM users";
    parse_test!(query);
}

/// Test POSITION
#[test]
fn it_parses_position() {
    let query = "SELECT POSITION('b' IN 'abc')";
    parse_test!(query);
}

/// Test TRIM BOTH
#[test]
fn it_parses_trim_both() {
    let query = "SELECT TRIM(BOTH 'x' FROM 'xxhelloxx')";
    parse_test!(query);
}

/// Test TRIM LEADING
#[test]
fn it_parses_trim_leading() {
    let query = "SELECT TRIM(LEADING '0' FROM '000123')";
    parse_test!(query);
}

/// Test OVERLAY
#[test]
fn it_parses_overlay() {
    let query = "SELECT OVERLAY('hello world' PLACING 'PG' FROM 7 FOR 5)";
    parse_test!(query);
}

/// Test EXTRACT
#[test]
fn it_parses_extract() {
    let query = "SELECT EXTRACT(YEAR FROM created_at) FROM events";
    parse_test!(query);
}

/// Test EXTRACT epoch
#[test]
fn it_parses_extract_epoch() {
    let query = "SELECT EXTRACT(EPOCH FROM NOW())";
    parse_test!(query);
}

// ============================================================================
// Interval literals
// ============================================================================

/// Test INTERVAL literal
#[test]
fn it_parses_interval_simple() {
    let query = "SELECT INTERVAL '1 day'";
    parse_test!(query);
}

/// Test INTERVAL with precision
#[test]
fn it_parses_interval_with_fields() {
    let query = "SELECT INTERVAL '1-2' YEAR TO MONTH";
    parse_test!(query);
}

/// Test INTERVAL DAY TO SECOND
#[test]
fn it_parses_interval_day_second() {
    let query = "SELECT INTERVAL '1 12:30:45' DAY TO SECOND";
    parse_test!(query);
}

// ============================================================================
// Additional literal types
// ============================================================================

/// Test escape string
#[test]
fn it_parses_escape_string() {
    let query = "SELECT E'hello\\nworld'";
    parse_test!(query);
}

/// Test unicode string
#[test]
fn it_parses_unicode_string() {
    let query = "SELECT U&'\\00e9'";
    parse_test!(query);
}

/// Test dollar-quoted string
#[test]
fn it_parses_dollar_quoted_string() {
    let query = "SELECT $$it's quoted$$";
    parse_test!(query);
}

/// Test dollar-quoted with tag
#[test]
fn it_parses_dollar_quoted_tag() {
    let query = "SELECT $tag$it's $$quoted$$$tag$";
    parse_test!(query);
}

// ============================================================================
// Operators
// ============================================================================

/// Test concatenation operator
#[test]
fn it_parses_concat_op() {
    let query = "SELECT 'hello' || ' ' || 'world'";
    parse_test!(query);
}

/// Test arithmetic operators
#[test]
fn it_parses_arithmetic() {
    let query = "SELECT 1 + 2 * 3 - 4 / 2, 10 % 3";
    parse_test!(query);
}

/// Test comparison operators
#[test]
fn it_parses_comparison_operators() {
    let query = "SELECT * FROM t WHERE a = 1 AND b != 2 AND c <> 3 AND d < 4 AND e <= 5 AND f > 6 AND g >= 7";
    parse_test!(query);
}

/// Test operator with schema
#[test]
fn it_parses_schema_qualified_operator() {
    let query = "SELECT 1 OPERATOR(pg_catalog.+) 2";
    parse_test!(query);
}

/// Test regular expression operators
#[test]
fn it_parses_regex_operators() {
    let query = "SELECT * FROM t WHERE name ~ '^A' AND email !~ 'test' AND addr ~* 'US' AND tel !~* '555'";
    parse_test!(query);
}

/// Test boolean NOT
#[test]
fn it_parses_not_expression() {
    let query = "SELECT * FROM t WHERE NOT (a = 1)";
    parse_test!(query);
}

/// Test power operator
#[test]
fn it_parses_power_operator() {
    let query = "SELECT 2 ^ 10";
    parse_test!(query);
}
