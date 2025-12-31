//! Raw parse tests split into multiple modules for maintainability.
//!
//! This module contains tests that verify parse_raw produces equivalent
//! results to parse (protobuf-based parsing).

pub use pg_query::protobuf::{a_const, node, ParseResult as ProtobufParseResult};
pub use pg_query::{parse, parse_raw, Error};

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

/// Helper macro for simple parse comparison tests
#[macro_export]
macro_rules! parse_test {
    ($query:expr) => {{
        let raw_result = parse_raw($query).unwrap();
        let proto_result = parse($query).unwrap();
        assert_eq!(raw_result.protobuf, proto_result.protobuf);
    }};
}

pub mod basic;
pub mod ddl;
pub mod dml;
pub mod expressions;
pub mod select;
pub mod statements;
