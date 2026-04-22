//! Raw parse tests split into multiple modules for maintainability.
//!
//! This module contains tests that verify parse_raw produces equivalent
//! results to parse (protobuf-based parsing).

pub use pg_query::protobuf::{a_const, node, ParseResult as ProtobufParseResult};
pub use pg_query::{deparse, deparse_raw, parse, parse_raw, Error};

/// Helper to extract AConst from a SELECT statement's first target
pub fn get_first_const(result: &ProtobufParseResult) -> Option<&pg_query::protobuf::AConst> {
    let stmt = result.stmts.first()?;
    let raw_stmt = stmt.stmt.as_ref()?;
    let node = raw_stmt.node.as_ref()?;

    if let node::Node::SelectStmt(select) = node {
        let target = select.target_list.first()?;
        if let Some(node::Node::ResTarget(res_target)) = target.node.as_ref() {
            if let Some(val_node) = res_target.val.as_ref() {
                if let Some(node::Node::AConst(aconst)) = val_node.node.as_ref() {
                    return Some(aconst);
                }
            }
        }
    }
    None
}

/// Helper macro for simple parse comparison tests with deparse verification.
///
/// Asserts:
///   1. parse_raw and parse produce equivalent protobuf trees
///   2. deparse_raw and deparse (both from pg_query) produce the same SQL string
///
/// Comparing the two deparsers against each other (rather than against the
/// original input) lets us verify deparse_raw is correct without fighting the
/// normalization that PostgreSQL's deparser always applies.
#[macro_export]
macro_rules! parse_test {
    ($query:expr) => {{
        let raw_result = parse_raw($query).unwrap();
        let proto_result = parse($query).unwrap();
        assert_eq!(raw_result.protobuf, proto_result.protobuf);
        let deparsed_raw = deparse_raw(&raw_result.protobuf).unwrap();
        let deparsed_proto = deparse(&raw_result.protobuf).unwrap();
        assert_eq!(deparsed_raw, deparsed_proto);
    }};
}

pub mod basic;
pub mod ddl;
pub mod dml;
pub mod expressions;
pub mod select;
pub mod statements;
