//! Expression tests: literals, type casts, arrays, JSON, operators.
//!
//! These tests verify parse_raw_iter_iter correctly handles various expressions.

use super::*;

// ============================================================================
// Literal value tests
// ============================================================================

/// Test parsing float with leading dot
#[test]
fn it_parses_floats_with_leading_dot() {
    let query = "SELECT .1";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    // Full structural equality check
    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test that parse_raw extracts string values correctly and matches parse
#[test]
fn it_extracts_string_const() {
    let query = "SELECT 'hello world'";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test NULLIF
#[test]
fn it_parses_nullif() {
    let query = "SELECT NULLIF(status, 'deleted') FROM records";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test GREATEST and LEAST
#[test]
fn it_parses_greatest_least() {
    let query = "SELECT GREATEST(a, b, c), LEAST(x, y, z) FROM t";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test IS NULL and IS NOT NULL
#[test]
fn it_parses_null_tests() {
    let query = "SELECT * FROM users WHERE deleted_at IS NULL AND email IS NOT NULL";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test IS DISTINCT FROM
#[test]
fn it_parses_is_distinct_from() {
    let query = "SELECT * FROM t WHERE a IS DISTINCT FROM b";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test BETWEEN
#[test]
fn it_parses_between() {
    let query = "SELECT * FROM events WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test LIKE and ILIKE
#[test]
fn it_parses_like_ilike() {
    let query = "SELECT * FROM users WHERE name LIKE 'John%' OR email ILIKE '%@EXAMPLE.COM'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SIMILAR TO
#[test]
fn it_parses_similar_to() {
    let query = "SELECT * FROM products WHERE name SIMILAR TO '%(phone|tablet)%'";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test complex boolean expressions
#[test]
fn it_parses_complex_boolean() {
    let query = "SELECT * FROM users WHERE (active = true AND verified = true) OR (role = 'admin' AND NOT suspended)";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test SQL-style CAST
#[test]
fn it_parses_sql_cast() {
    let query = "SELECT CAST('123' AS integer), CAST(created_at AS date) FROM t";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array type cast
#[test]
fn it_parses_array_cast() {
    let query = "SELECT ARRAY[1, 2, 3]::text[]";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array subscript
#[test]
fn it_parses_array_subscript() {
    let query = "SELECT tags[1], matrix[1][2] FROM t";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test array slice
#[test]
fn it_parses_array_slice() {
    let query = "SELECT arr[2:4], arr[:3], arr[2:] FROM t";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test unnest
#[test]
fn it_parses_unnest() {
    let query = "SELECT unnest(ARRAY[1, 2, 3])";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test JSON operators
#[test]
fn it_parses_json_operators() {
    let query = "SELECT data->'name', data->>'email', data#>'{address,city}' FROM users";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test JSONB containment
#[test]
fn it_parses_jsonb_containment() {
    let query = "SELECT * FROM products WHERE metadata @> '{\"featured\": true}'";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test parameters in INSERT
#[test]
fn it_parses_params_in_insert() {
    let query = "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id";
    let raw_result = parse_raw_iter(query).unwrap();
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
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}

/// Test other SQL value functions
#[test]
fn it_parses_sql_value_functions() {
    let query = "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP, CURRENT_USER, CURRENT_CATALOG, CURRENT_SCHEMA";
    let raw_result = parse_raw_iter(query).unwrap();
    let proto_result = parse(query).unwrap();

    assert_eq!(raw_result.protobuf, proto_result.protobuf);
}
