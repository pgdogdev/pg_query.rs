//! Direct parsing that bypasses protobuf serialization/deserialization.
//!
//! This module provides a faster alternative to the protobuf-based parsing by
//! directly reading PostgreSQL's internal parse tree structures and converting
//! them to Rust protobuf types.

use crate::bindings;
use crate::bindings_raw;
use crate::parse_result::ParseResult;
use crate::protobuf;
use crate::{Error, Result};
use std::ffi::{CStr, CString};

/// Parses a SQL statement directly into protobuf types without going through protobuf serialization.
///
/// This function is faster than `parse` because it skips the protobuf encode/decode step.
/// The parse tree is read directly from PostgreSQL's internal C structures.
///
/// # Example
///
/// ```rust
/// let result = pg_query::parse_raw("SELECT * FROM users").unwrap();
/// assert_eq!(result.tables(), vec!["users"]);
/// ```
pub fn parse_raw(statement: &str) -> Result<ParseResult> {
    let input = CString::new(statement)?;
    let result = unsafe { bindings_raw::pg_query_parse_raw(input.as_ptr()) };

    let parse_result = if !result.error.is_null() {
        let message = unsafe { CStr::from_ptr((*result.error).message) }.to_string_lossy().to_string();
        Err(Error::Parse(message))
    } else {
        // Convert the C parse tree to protobuf types
        let tree = result.tree;
        let stmts = unsafe { convert_list_to_raw_stmts(tree) };
        let protobuf = protobuf::ParseResult { version: bindings::PG_VERSION_NUM as i32, stmts };
        Ok(ParseResult::new(protobuf, String::new()))
    };

    unsafe { bindings_raw::pg_query_free_raw_parse_result(result) };
    parse_result
}

/// Parses a SQL statement with a custom stack size.
///
/// This function is useful for parsing deeply nested queries that might overflow the stack.
/// It uses the `stacker` crate to grow the stack if needed.
///
/// # Arguments
///
/// * `statement` - The SQL statement to parse
/// * `stack_size` - The stack size in bytes to ensure is available for parsing
///
/// # Example
///
/// ```rust
/// let result = pg_query::parse_raw_with_stack("SELECT * FROM users", 8 * 1024 * 1024).unwrap();
/// assert_eq!(result.tables(), vec!["users"]);
/// ```
pub fn parse_raw_with_stack(statement: &str, stack_size: usize) -> Result<ParseResult> {
    stacker::maybe_grow(32 * 1024, stack_size, || parse_raw(statement))
}

/// Converts a PostgreSQL List of RawStmt nodes to protobuf RawStmt vector.
unsafe fn convert_list_to_raw_stmts(list: *mut bindings_raw::List) -> Vec<protobuf::RawStmt> {
    if list.is_null() {
        return Vec::new();
    }

    let list_ref = &*list;
    let length = list_ref.length as usize;
    let mut stmts = Vec::with_capacity(length);

    for i in 0..length {
        let cell = list_ref.elements.add(i);
        let node_ptr = (*cell).ptr_value as *mut bindings_raw::Node;

        if !node_ptr.is_null() {
            let node_tag = (*node_ptr).type_;
            if node_tag == bindings_raw::NodeTag_T_RawStmt {
                let raw_stmt = node_ptr as *mut bindings_raw::RawStmt;
                stmts.push(convert_raw_stmt(&*raw_stmt));
            }
        }
    }

    stmts
}

/// Converts a C RawStmt to a protobuf RawStmt.
unsafe fn convert_raw_stmt(raw_stmt: &bindings_raw::RawStmt) -> protobuf::RawStmt {
    protobuf::RawStmt { stmt: convert_node_boxed(raw_stmt.stmt), stmt_location: raw_stmt.stmt_location, stmt_len: raw_stmt.stmt_len }
}

/// Converts a C Node pointer to a boxed protobuf Node (for fields that expect Option<Box<Node>>).
unsafe fn convert_node_boxed(node_ptr: *mut bindings_raw::Node) -> Option<Box<protobuf::Node>> {
    convert_node(node_ptr).map(Box::new)
}

/// Converts a C Node pointer to a protobuf Node.
unsafe fn convert_node(node_ptr: *mut bindings_raw::Node) -> Option<protobuf::Node> {
    if node_ptr.is_null() {
        return None;
    }

    let node_tag = (*node_ptr).type_;
    let node = match node_tag {
        // Types that need Box
        bindings_raw::NodeTag_T_SelectStmt => {
            let stmt = node_ptr as *mut bindings_raw::SelectStmt;
            Some(protobuf::node::Node::SelectStmt(Box::new(convert_select_stmt(&*stmt))))
        }
        bindings_raw::NodeTag_T_InsertStmt => {
            let stmt = node_ptr as *mut bindings_raw::InsertStmt;
            Some(protobuf::node::Node::InsertStmt(Box::new(convert_insert_stmt(&*stmt))))
        }
        bindings_raw::NodeTag_T_UpdateStmt => {
            let stmt = node_ptr as *mut bindings_raw::UpdateStmt;
            Some(protobuf::node::Node::UpdateStmt(Box::new(convert_update_stmt(&*stmt))))
        }
        bindings_raw::NodeTag_T_DeleteStmt => {
            let stmt = node_ptr as *mut bindings_raw::DeleteStmt;
            Some(protobuf::node::Node::DeleteStmt(Box::new(convert_delete_stmt(&*stmt))))
        }
        bindings_raw::NodeTag_T_ResTarget => {
            let rt = node_ptr as *mut bindings_raw::ResTarget;
            Some(protobuf::node::Node::ResTarget(Box::new(convert_res_target(&*rt))))
        }
        bindings_raw::NodeTag_T_A_Expr => {
            let expr = node_ptr as *mut bindings_raw::A_Expr;
            Some(protobuf::node::Node::AExpr(Box::new(convert_a_expr(&*expr))))
        }
        bindings_raw::NodeTag_T_A_Const => {
            let aconst = node_ptr as *mut bindings_raw::A_Const;
            Some(protobuf::node::Node::AConst(convert_a_const(&*aconst)))
        }
        bindings_raw::NodeTag_T_FuncCall => {
            let fc = node_ptr as *mut bindings_raw::FuncCall;
            Some(protobuf::node::Node::FuncCall(Box::new(convert_func_call(&*fc))))
        }
        bindings_raw::NodeTag_T_TypeCast => {
            let tc = node_ptr as *mut bindings_raw::TypeCast;
            Some(protobuf::node::Node::TypeCast(Box::new(convert_type_cast(&*tc))))
        }
        bindings_raw::NodeTag_T_JoinExpr => {
            let je = node_ptr as *mut bindings_raw::JoinExpr;
            Some(protobuf::node::Node::JoinExpr(Box::new(convert_join_expr(&*je))))
        }
        bindings_raw::NodeTag_T_SortBy => {
            let sb = node_ptr as *mut bindings_raw::SortBy;
            Some(protobuf::node::Node::SortBy(Box::new(convert_sort_by(&*sb))))
        }
        bindings_raw::NodeTag_T_BoolExpr => {
            let be = node_ptr as *mut bindings_raw::BoolExpr;
            Some(protobuf::node::Node::BoolExpr(Box::new(convert_bool_expr(&*be))))
        }
        bindings_raw::NodeTag_T_SubLink => {
            let sl = node_ptr as *mut bindings_raw::SubLink;
            Some(protobuf::node::Node::SubLink(Box::new(convert_sub_link(&*sl))))
        }
        bindings_raw::NodeTag_T_NullTest => {
            let nt = node_ptr as *mut bindings_raw::NullTest;
            Some(protobuf::node::Node::NullTest(Box::new(convert_null_test(&*nt))))
        }
        bindings_raw::NodeTag_T_CaseExpr => {
            let ce = node_ptr as *mut bindings_raw::CaseExpr;
            Some(protobuf::node::Node::CaseExpr(Box::new(convert_case_expr(&*ce))))
        }
        bindings_raw::NodeTag_T_CaseWhen => {
            let cw = node_ptr as *mut bindings_raw::CaseWhen;
            Some(protobuf::node::Node::CaseWhen(Box::new(convert_case_when(&*cw))))
        }
        bindings_raw::NodeTag_T_CoalesceExpr => {
            let ce = node_ptr as *mut bindings_raw::CoalesceExpr;
            Some(protobuf::node::Node::CoalesceExpr(Box::new(convert_coalesce_expr(&*ce))))
        }
        bindings_raw::NodeTag_T_CommonTableExpr => {
            let cte = node_ptr as *mut bindings_raw::CommonTableExpr;
            Some(protobuf::node::Node::CommonTableExpr(Box::new(convert_common_table_expr(&*cte))))
        }
        bindings_raw::NodeTag_T_ColumnDef => {
            let cd = node_ptr as *mut bindings_raw::ColumnDef;
            Some(protobuf::node::Node::ColumnDef(Box::new(convert_column_def(&*cd))))
        }
        bindings_raw::NodeTag_T_Constraint => {
            let c = node_ptr as *mut bindings_raw::Constraint;
            Some(protobuf::node::Node::Constraint(Box::new(convert_constraint(&*c))))
        }
        bindings_raw::NodeTag_T_DropStmt => {
            let ds = node_ptr as *mut bindings_raw::DropStmt;
            Some(protobuf::node::Node::DropStmt(convert_drop_stmt(&*ds)))
        }
        bindings_raw::NodeTag_T_IndexStmt => {
            let is = node_ptr as *mut bindings_raw::IndexStmt;
            Some(protobuf::node::Node::IndexStmt(Box::new(convert_index_stmt(&*is))))
        }
        bindings_raw::NodeTag_T_IndexElem => {
            let ie = node_ptr as *mut bindings_raw::IndexElem;
            Some(protobuf::node::Node::IndexElem(Box::new(convert_index_elem(&*ie))))
        }
        bindings_raw::NodeTag_T_DefElem => {
            let de = node_ptr as *mut bindings_raw::DefElem;
            Some(protobuf::node::Node::DefElem(Box::new(convert_def_elem(&*de))))
        }
        bindings_raw::NodeTag_T_WindowDef => {
            let wd = node_ptr as *mut bindings_raw::WindowDef;
            Some(protobuf::node::Node::WindowDef(Box::new(convert_window_def(&*wd))))
        }
        // Types that don't need Box
        bindings_raw::NodeTag_T_RangeVar => {
            let rv = node_ptr as *mut bindings_raw::RangeVar;
            Some(protobuf::node::Node::RangeVar(convert_range_var(&*rv)))
        }
        bindings_raw::NodeTag_T_ColumnRef => {
            let cr = node_ptr as *mut bindings_raw::ColumnRef;
            Some(protobuf::node::Node::ColumnRef(convert_column_ref(&*cr)))
        }
        bindings_raw::NodeTag_T_A_Star => Some(protobuf::node::Node::AStar(protobuf::AStar {})),
        bindings_raw::NodeTag_T_TypeName => {
            let tn = node_ptr as *mut bindings_raw::TypeName;
            Some(protobuf::node::Node::TypeName(convert_type_name(&*tn)))
        }
        bindings_raw::NodeTag_T_Alias => {
            let alias = node_ptr as *mut bindings_raw::Alias;
            Some(protobuf::node::Node::Alias(convert_alias(&*alias)))
        }
        bindings_raw::NodeTag_T_String => {
            let s = node_ptr as *mut bindings_raw::String;
            Some(protobuf::node::Node::String(convert_string(&*s)))
        }
        bindings_raw::NodeTag_T_Integer => {
            let i = node_ptr as *mut bindings_raw::Integer;
            Some(protobuf::node::Node::Integer(protobuf::Integer { ival: (*i).ival }))
        }
        bindings_raw::NodeTag_T_Float => {
            let f = node_ptr as *mut bindings_raw::Float;
            let fval = if (*f).fval.is_null() { String::new() } else { CStr::from_ptr((*f).fval).to_string_lossy().to_string() };
            Some(protobuf::node::Node::Float(protobuf::Float { fval }))
        }
        bindings_raw::NodeTag_T_Boolean => {
            let b = node_ptr as *mut bindings_raw::Boolean;
            Some(protobuf::node::Node::Boolean(protobuf::Boolean { boolval: (*b).boolval }))
        }
        bindings_raw::NodeTag_T_ParamRef => {
            let pr = node_ptr as *mut bindings_raw::ParamRef;
            Some(protobuf::node::Node::ParamRef(protobuf::ParamRef { number: (*pr).number, location: (*pr).location }))
        }
        bindings_raw::NodeTag_T_WithClause => {
            let wc = node_ptr as *mut bindings_raw::WithClause;
            Some(protobuf::node::Node::WithClause(convert_with_clause(&*wc)))
        }
        bindings_raw::NodeTag_T_CreateStmt => {
            let cs = node_ptr as *mut bindings_raw::CreateStmt;
            Some(protobuf::node::Node::CreateStmt(convert_create_stmt(&*cs)))
        }
        bindings_raw::NodeTag_T_List => {
            let list = node_ptr as *mut bindings_raw::List;
            Some(protobuf::node::Node::List(convert_list(&*list)))
        }
        bindings_raw::NodeTag_T_LockingClause => {
            let lc = node_ptr as *mut bindings_raw::LockingClause;
            Some(protobuf::node::Node::LockingClause(convert_locking_clause(&*lc)))
        }
        bindings_raw::NodeTag_T_MinMaxExpr => {
            let mme = node_ptr as *mut bindings_raw::MinMaxExpr;
            Some(protobuf::node::Node::MinMaxExpr(Box::new(convert_min_max_expr(&*mme))))
        }
        bindings_raw::NodeTag_T_GroupingSet => {
            let gs = node_ptr as *mut bindings_raw::GroupingSet;
            Some(protobuf::node::Node::GroupingSet(convert_grouping_set(&*gs)))
        }
        bindings_raw::NodeTag_T_RangeSubselect => {
            let rs = node_ptr as *mut bindings_raw::RangeSubselect;
            Some(protobuf::node::Node::RangeSubselect(Box::new(convert_range_subselect(&*rs))))
        }
        bindings_raw::NodeTag_T_A_ArrayExpr => {
            let ae = node_ptr as *mut bindings_raw::A_ArrayExpr;
            Some(protobuf::node::Node::AArrayExpr(convert_a_array_expr(&*ae)))
        }
        bindings_raw::NodeTag_T_A_Indirection => {
            let ai = node_ptr as *mut bindings_raw::A_Indirection;
            Some(protobuf::node::Node::AIndirection(Box::new(convert_a_indirection(&*ai))))
        }
        bindings_raw::NodeTag_T_A_Indices => {
            let ai = node_ptr as *mut bindings_raw::A_Indices;
            Some(protobuf::node::Node::AIndices(Box::new(convert_a_indices(&*ai))))
        }
        bindings_raw::NodeTag_T_AlterTableStmt => {
            let ats = node_ptr as *mut bindings_raw::AlterTableStmt;
            Some(protobuf::node::Node::AlterTableStmt(convert_alter_table_stmt(&*ats)))
        }
        bindings_raw::NodeTag_T_AlterTableCmd => {
            let atc = node_ptr as *mut bindings_raw::AlterTableCmd;
            Some(protobuf::node::Node::AlterTableCmd(Box::new(convert_alter_table_cmd(&*atc))))
        }
        bindings_raw::NodeTag_T_CopyStmt => {
            let cs = node_ptr as *mut bindings_raw::CopyStmt;
            Some(protobuf::node::Node::CopyStmt(Box::new(convert_copy_stmt(&*cs))))
        }
        bindings_raw::NodeTag_T_TruncateStmt => {
            let ts = node_ptr as *mut bindings_raw::TruncateStmt;
            Some(protobuf::node::Node::TruncateStmt(convert_truncate_stmt(&*ts)))
        }
        bindings_raw::NodeTag_T_ViewStmt => {
            let vs = node_ptr as *mut bindings_raw::ViewStmt;
            Some(protobuf::node::Node::ViewStmt(Box::new(convert_view_stmt(&*vs))))
        }
        bindings_raw::NodeTag_T_ExplainStmt => {
            let es = node_ptr as *mut bindings_raw::ExplainStmt;
            Some(protobuf::node::Node::ExplainStmt(Box::new(convert_explain_stmt(&*es))))
        }
        bindings_raw::NodeTag_T_CreateTableAsStmt => {
            let ctas = node_ptr as *mut bindings_raw::CreateTableAsStmt;
            Some(protobuf::node::Node::CreateTableAsStmt(Box::new(convert_create_table_as_stmt(&*ctas))))
        }
        bindings_raw::NodeTag_T_PrepareStmt => {
            let ps = node_ptr as *mut bindings_raw::PrepareStmt;
            Some(protobuf::node::Node::PrepareStmt(Box::new(convert_prepare_stmt(&*ps))))
        }
        bindings_raw::NodeTag_T_ExecuteStmt => {
            let es = node_ptr as *mut bindings_raw::ExecuteStmt;
            Some(protobuf::node::Node::ExecuteStmt(convert_execute_stmt(&*es)))
        }
        bindings_raw::NodeTag_T_DeallocateStmt => {
            let ds = node_ptr as *mut bindings_raw::DeallocateStmt;
            Some(protobuf::node::Node::DeallocateStmt(convert_deallocate_stmt(&*ds)))
        }
        bindings_raw::NodeTag_T_SetToDefault => {
            let std = node_ptr as *mut bindings_raw::SetToDefault;
            Some(protobuf::node::Node::SetToDefault(Box::new(convert_set_to_default(&*std))))
        }
        bindings_raw::NodeTag_T_MultiAssignRef => {
            let mar = node_ptr as *mut bindings_raw::MultiAssignRef;
            Some(protobuf::node::Node::MultiAssignRef(Box::new(convert_multi_assign_ref(&*mar))))
        }
        bindings_raw::NodeTag_T_RowExpr => {
            let re = node_ptr as *mut bindings_raw::RowExpr;
            Some(protobuf::node::Node::RowExpr(Box::new(convert_row_expr(&*re))))
        }
        bindings_raw::NodeTag_T_PartitionElem => {
            let pe = node_ptr as *mut bindings_raw::PartitionElem;
            Some(protobuf::node::Node::PartitionElem(Box::new(convert_partition_elem(&*pe))))
        }
        bindings_raw::NodeTag_T_PartitionRangeDatum => {
            let prd = node_ptr as *mut bindings_raw::PartitionRangeDatum;
            Some(protobuf::node::Node::PartitionRangeDatum(Box::new(convert_partition_range_datum(&*prd))))
        }
        bindings_raw::NodeTag_T_TransactionStmt => {
            let ts = node_ptr as *mut bindings_raw::TransactionStmt;
            Some(protobuf::node::Node::TransactionStmt(convert_transaction_stmt(&*ts)))
        }
        bindings_raw::NodeTag_T_VacuumStmt => {
            let vs = node_ptr as *mut bindings_raw::VacuumStmt;
            Some(protobuf::node::Node::VacuumStmt(convert_vacuum_stmt(&*vs)))
        }
        bindings_raw::NodeTag_T_VacuumRelation => {
            let vr = node_ptr as *mut bindings_raw::VacuumRelation;
            Some(protobuf::node::Node::VacuumRelation(convert_vacuum_relation(&*vr)))
        }
        bindings_raw::NodeTag_T_VariableSetStmt => {
            let vss = node_ptr as *mut bindings_raw::VariableSetStmt;
            Some(protobuf::node::Node::VariableSetStmt(convert_variable_set_stmt(&*vss)))
        }
        bindings_raw::NodeTag_T_VariableShowStmt => {
            let vss = node_ptr as *mut bindings_raw::VariableShowStmt;
            Some(protobuf::node::Node::VariableShowStmt(convert_variable_show_stmt(&*vss)))
        }
        bindings_raw::NodeTag_T_CreateSeqStmt => {
            let css = node_ptr as *mut bindings_raw::CreateSeqStmt;
            Some(protobuf::node::Node::CreateSeqStmt(convert_create_seq_stmt(&*css)))
        }
        bindings_raw::NodeTag_T_DoStmt => {
            let ds = node_ptr as *mut bindings_raw::DoStmt;
            Some(protobuf::node::Node::DoStmt(convert_do_stmt(&*ds)))
        }
        bindings_raw::NodeTag_T_LockStmt => {
            let ls = node_ptr as *mut bindings_raw::LockStmt;
            Some(protobuf::node::Node::LockStmt(convert_lock_stmt(&*ls)))
        }
        bindings_raw::NodeTag_T_CreateSchemaStmt => {
            let css = node_ptr as *mut bindings_raw::CreateSchemaStmt;
            Some(protobuf::node::Node::CreateSchemaStmt(convert_create_schema_stmt(&*css)))
        }
        bindings_raw::NodeTag_T_RenameStmt => {
            let rs = node_ptr as *mut bindings_raw::RenameStmt;
            Some(protobuf::node::Node::RenameStmt(Box::new(convert_rename_stmt(&*rs))))
        }
        bindings_raw::NodeTag_T_CreateFunctionStmt => {
            let cfs = node_ptr as *mut bindings_raw::CreateFunctionStmt;
            Some(protobuf::node::Node::CreateFunctionStmt(Box::new(convert_create_function_stmt(&*cfs))))
        }
        bindings_raw::NodeTag_T_AlterOwnerStmt => {
            let aos = node_ptr as *mut bindings_raw::AlterOwnerStmt;
            Some(protobuf::node::Node::AlterOwnerStmt(Box::new(convert_alter_owner_stmt(&*aos))))
        }
        bindings_raw::NodeTag_T_AlterSeqStmt => {
            let ass = node_ptr as *mut bindings_raw::AlterSeqStmt;
            Some(protobuf::node::Node::AlterSeqStmt(convert_alter_seq_stmt(&*ass)))
        }
        bindings_raw::NodeTag_T_CreateEnumStmt => {
            let ces = node_ptr as *mut bindings_raw::CreateEnumStmt;
            Some(protobuf::node::Node::CreateEnumStmt(convert_create_enum_stmt(&*ces)))
        }
        bindings_raw::NodeTag_T_ObjectWithArgs => {
            let owa = node_ptr as *mut bindings_raw::ObjectWithArgs;
            Some(protobuf::node::Node::ObjectWithArgs(convert_object_with_args(&*owa)))
        }
        bindings_raw::NodeTag_T_FunctionParameter => {
            let fp = node_ptr as *mut bindings_raw::FunctionParameter;
            Some(protobuf::node::Node::FunctionParameter(Box::new(convert_function_parameter(&*fp))))
        }
        bindings_raw::NodeTag_T_NotifyStmt => {
            let ns = node_ptr as *mut bindings_raw::NotifyStmt;
            Some(protobuf::node::Node::NotifyStmt(convert_notify_stmt(&*ns)))
        }
        bindings_raw::NodeTag_T_ListenStmt => {
            let ls = node_ptr as *mut bindings_raw::ListenStmt;
            Some(protobuf::node::Node::ListenStmt(convert_listen_stmt(&*ls)))
        }
        bindings_raw::NodeTag_T_UnlistenStmt => {
            let us = node_ptr as *mut bindings_raw::UnlistenStmt;
            Some(protobuf::node::Node::UnlistenStmt(convert_unlisten_stmt(&*us)))
        }
        bindings_raw::NodeTag_T_DiscardStmt => {
            let ds = node_ptr as *mut bindings_raw::DiscardStmt;
            Some(protobuf::node::Node::DiscardStmt(convert_discard_stmt(&*ds)))
        }
        bindings_raw::NodeTag_T_CollateClause => {
            let cc = node_ptr as *mut bindings_raw::CollateClause;
            Some(protobuf::node::Node::CollateClause(Box::new(convert_collate_clause(&*cc))))
        }
        bindings_raw::NodeTag_T_CoerceToDomain => {
            let ctd = node_ptr as *mut bindings_raw::CoerceToDomain;
            Some(protobuf::node::Node::CoerceToDomain(Box::new(convert_coerce_to_domain(&*ctd))))
        }
        bindings_raw::NodeTag_T_CompositeTypeStmt => {
            let cts = node_ptr as *mut bindings_raw::CompositeTypeStmt;
            Some(protobuf::node::Node::CompositeTypeStmt(convert_composite_type_stmt(&*cts)))
        }
        bindings_raw::NodeTag_T_CreateDomainStmt => {
            let cds = node_ptr as *mut bindings_raw::CreateDomainStmt;
            Some(protobuf::node::Node::CreateDomainStmt(Box::new(convert_create_domain_stmt(&*cds))))
        }
        bindings_raw::NodeTag_T_CreateExtensionStmt => {
            let ces = node_ptr as *mut bindings_raw::CreateExtensionStmt;
            Some(protobuf::node::Node::CreateExtensionStmt(convert_create_extension_stmt(&*ces)))
        }
        bindings_raw::NodeTag_T_CreatePublicationStmt => {
            let cps = node_ptr as *mut bindings_raw::CreatePublicationStmt;
            Some(protobuf::node::Node::CreatePublicationStmt(convert_create_publication_stmt(&*cps)))
        }
        bindings_raw::NodeTag_T_AlterPublicationStmt => {
            let aps = node_ptr as *mut bindings_raw::AlterPublicationStmt;
            Some(protobuf::node::Node::AlterPublicationStmt(convert_alter_publication_stmt(&*aps)))
        }
        bindings_raw::NodeTag_T_CreateSubscriptionStmt => {
            let css = node_ptr as *mut bindings_raw::CreateSubscriptionStmt;
            Some(protobuf::node::Node::CreateSubscriptionStmt(convert_create_subscription_stmt(&*css)))
        }
        bindings_raw::NodeTag_T_AlterSubscriptionStmt => {
            let ass = node_ptr as *mut bindings_raw::AlterSubscriptionStmt;
            Some(protobuf::node::Node::AlterSubscriptionStmt(convert_alter_subscription_stmt(&*ass)))
        }
        bindings_raw::NodeTag_T_CreateTrigStmt => {
            let cts = node_ptr as *mut bindings_raw::CreateTrigStmt;
            Some(protobuf::node::Node::CreateTrigStmt(Box::new(convert_create_trig_stmt(&*cts))))
        }
        bindings_raw::NodeTag_T_PublicationObjSpec => {
            let pos = node_ptr as *mut bindings_raw::PublicationObjSpec;
            Some(protobuf::node::Node::PublicationObjSpec(Box::new(convert_publication_obj_spec(&*pos))))
        }
        bindings_raw::NodeTag_T_PublicationTable => {
            let pt = node_ptr as *mut bindings_raw::PublicationTable;
            Some(protobuf::node::Node::PublicationTable(Box::new(convert_publication_table(&*pt))))
        }
        bindings_raw::NodeTag_T_CheckPointStmt => Some(protobuf::node::Node::CheckPointStmt(protobuf::CheckPointStmt {})),
        bindings_raw::NodeTag_T_CallStmt => {
            let cs = node_ptr as *mut bindings_raw::CallStmt;
            Some(protobuf::node::Node::CallStmt(Box::new(convert_call_stmt(&*cs))))
        }
        bindings_raw::NodeTag_T_RuleStmt => {
            let rs = node_ptr as *mut bindings_raw::RuleStmt;
            Some(protobuf::node::Node::RuleStmt(Box::new(convert_rule_stmt(&*rs))))
        }
        bindings_raw::NodeTag_T_GrantStmt => {
            let gs = node_ptr as *mut bindings_raw::GrantStmt;
            Some(protobuf::node::Node::GrantStmt(convert_grant_stmt(&*gs)))
        }
        bindings_raw::NodeTag_T_GrantRoleStmt => {
            let grs = node_ptr as *mut bindings_raw::GrantRoleStmt;
            Some(protobuf::node::Node::GrantRoleStmt(convert_grant_role_stmt(&*grs)))
        }
        bindings_raw::NodeTag_T_RefreshMatViewStmt => {
            let rmvs = node_ptr as *mut bindings_raw::RefreshMatViewStmt;
            Some(protobuf::node::Node::RefreshMatViewStmt(convert_refresh_mat_view_stmt(&*rmvs)))
        }
        bindings_raw::NodeTag_T_MergeStmt => {
            let ms = node_ptr as *mut bindings_raw::MergeStmt;
            Some(protobuf::node::Node::MergeStmt(Box::new(convert_merge_stmt(&*ms))))
        }
        bindings_raw::NodeTag_T_MergeAction => {
            let ma = node_ptr as *mut bindings_raw::MergeAction;
            Some(protobuf::node::Node::MergeAction(Box::new(convert_merge_action(&*ma))))
        }
        bindings_raw::NodeTag_T_RangeFunction => {
            let rf = node_ptr as *mut bindings_raw::RangeFunction;
            Some(protobuf::node::Node::RangeFunction(convert_range_function(&*rf)))
        }
        bindings_raw::NodeTag_T_MergeWhenClause => {
            let mwc = node_ptr as *mut bindings_raw::MergeWhenClause;
            Some(protobuf::node::Node::MergeWhenClause(Box::new(convert_merge_when_clause(&*mwc))))
        }
        bindings_raw::NodeTag_T_AccessPriv => {
            let ap = node_ptr as *mut bindings_raw::AccessPriv;
            Some(protobuf::node::Node::AccessPriv(convert_access_priv(&*ap)))
        }
        bindings_raw::NodeTag_T_RoleSpec => {
            let rs = node_ptr as *mut bindings_raw::RoleSpec;
            Some(protobuf::node::Node::RoleSpec(convert_role_spec(&*rs)))
        }
        bindings_raw::NodeTag_T_BitString => {
            let bs = node_ptr as *mut bindings_raw::BitString;
            Some(protobuf::node::Node::BitString(convert_bit_string(&*bs)))
        }
        bindings_raw::NodeTag_T_BooleanTest => {
            let bt = node_ptr as *mut bindings_raw::BooleanTest;
            Some(protobuf::node::Node::BooleanTest(Box::new(convert_boolean_test(&*bt))))
        }
        bindings_raw::NodeTag_T_CreateRangeStmt => {
            let crs = node_ptr as *mut bindings_raw::CreateRangeStmt;
            Some(protobuf::node::Node::CreateRangeStmt(convert_create_range_stmt(&*crs)))
        }
        bindings_raw::NodeTag_T_AlterEnumStmt => {
            let aes = node_ptr as *mut bindings_raw::AlterEnumStmt;
            Some(protobuf::node::Node::AlterEnumStmt(convert_alter_enum_stmt(&*aes)))
        }
        bindings_raw::NodeTag_T_ClosePortalStmt => {
            let cps = node_ptr as *mut bindings_raw::ClosePortalStmt;
            Some(protobuf::node::Node::ClosePortalStmt(convert_close_portal_stmt(&*cps)))
        }
        bindings_raw::NodeTag_T_FetchStmt => {
            let fs = node_ptr as *mut bindings_raw::FetchStmt;
            Some(protobuf::node::Node::FetchStmt(convert_fetch_stmt(&*fs)))
        }
        bindings_raw::NodeTag_T_DeclareCursorStmt => {
            let dcs = node_ptr as *mut bindings_raw::DeclareCursorStmt;
            Some(protobuf::node::Node::DeclareCursorStmt(Box::new(convert_declare_cursor_stmt(&*dcs))))
        }
        bindings_raw::NodeTag_T_DefineStmt => {
            let ds = node_ptr as *mut bindings_raw::DefineStmt;
            Some(protobuf::node::Node::DefineStmt(convert_define_stmt(&*ds)))
        }
        bindings_raw::NodeTag_T_CommentStmt => {
            let cs = node_ptr as *mut bindings_raw::CommentStmt;
            Some(protobuf::node::Node::CommentStmt(Box::new(convert_comment_stmt(&*cs))))
        }
        bindings_raw::NodeTag_T_SecLabelStmt => {
            let sls = node_ptr as *mut bindings_raw::SecLabelStmt;
            Some(protobuf::node::Node::SecLabelStmt(Box::new(convert_sec_label_stmt(&*sls))))
        }
        bindings_raw::NodeTag_T_CreateRoleStmt => {
            let crs = node_ptr as *mut bindings_raw::CreateRoleStmt;
            Some(protobuf::node::Node::CreateRoleStmt(convert_create_role_stmt(&*crs)))
        }
        bindings_raw::NodeTag_T_AlterRoleStmt => {
            let ars = node_ptr as *mut bindings_raw::AlterRoleStmt;
            Some(protobuf::node::Node::AlterRoleStmt(convert_alter_role_stmt(&*ars)))
        }
        bindings_raw::NodeTag_T_AlterRoleSetStmt => {
            let arss = node_ptr as *mut bindings_raw::AlterRoleSetStmt;
            Some(protobuf::node::Node::AlterRoleSetStmt(convert_alter_role_set_stmt(&*arss)))
        }
        bindings_raw::NodeTag_T_DropRoleStmt => {
            let drs = node_ptr as *mut bindings_raw::DropRoleStmt;
            Some(protobuf::node::Node::DropRoleStmt(convert_drop_role_stmt(&*drs)))
        }
        bindings_raw::NodeTag_T_CreatePolicyStmt => {
            let cps = node_ptr as *mut bindings_raw::CreatePolicyStmt;
            Some(protobuf::node::Node::CreatePolicyStmt(Box::new(convert_create_policy_stmt(&*cps))))
        }
        bindings_raw::NodeTag_T_AlterPolicyStmt => {
            let aps = node_ptr as *mut bindings_raw::AlterPolicyStmt;
            Some(protobuf::node::Node::AlterPolicyStmt(Box::new(convert_alter_policy_stmt(&*aps))))
        }
        bindings_raw::NodeTag_T_CreateEventTrigStmt => {
            let cets = node_ptr as *mut bindings_raw::CreateEventTrigStmt;
            Some(protobuf::node::Node::CreateEventTrigStmt(convert_create_event_trig_stmt(&*cets)))
        }
        bindings_raw::NodeTag_T_AlterEventTrigStmt => {
            let aets = node_ptr as *mut bindings_raw::AlterEventTrigStmt;
            Some(protobuf::node::Node::AlterEventTrigStmt(convert_alter_event_trig_stmt(&*aets)))
        }
        bindings_raw::NodeTag_T_CreatePLangStmt => {
            let cpls = node_ptr as *mut bindings_raw::CreatePLangStmt;
            Some(protobuf::node::Node::CreatePlangStmt(convert_create_plang_stmt(&*cpls)))
        }
        bindings_raw::NodeTag_T_CreateAmStmt => {
            let cas = node_ptr as *mut bindings_raw::CreateAmStmt;
            Some(protobuf::node::Node::CreateAmStmt(convert_create_am_stmt(&*cas)))
        }
        bindings_raw::NodeTag_T_CreateOpClassStmt => {
            let cocs = node_ptr as *mut bindings_raw::CreateOpClassStmt;
            Some(protobuf::node::Node::CreateOpClassStmt(convert_create_op_class_stmt(&*cocs)))
        }
        bindings_raw::NodeTag_T_CreateOpClassItem => {
            let coci = node_ptr as *mut bindings_raw::CreateOpClassItem;
            Some(protobuf::node::Node::CreateOpClassItem(convert_create_op_class_item(&*coci)))
        }
        bindings_raw::NodeTag_T_CreateOpFamilyStmt => {
            let cofs = node_ptr as *mut bindings_raw::CreateOpFamilyStmt;
            Some(protobuf::node::Node::CreateOpFamilyStmt(convert_create_op_family_stmt(&*cofs)))
        }
        bindings_raw::NodeTag_T_AlterOpFamilyStmt => {
            let aofs = node_ptr as *mut bindings_raw::AlterOpFamilyStmt;
            Some(protobuf::node::Node::AlterOpFamilyStmt(convert_alter_op_family_stmt(&*aofs)))
        }
        bindings_raw::NodeTag_T_CreateFdwStmt => {
            let cfds = node_ptr as *mut bindings_raw::CreateFdwStmt;
            Some(protobuf::node::Node::CreateFdwStmt(convert_create_fdw_stmt(&*cfds)))
        }
        bindings_raw::NodeTag_T_AlterFdwStmt => {
            let afds = node_ptr as *mut bindings_raw::AlterFdwStmt;
            Some(protobuf::node::Node::AlterFdwStmt(convert_alter_fdw_stmt(&*afds)))
        }
        bindings_raw::NodeTag_T_CreateForeignServerStmt => {
            let cfss = node_ptr as *mut bindings_raw::CreateForeignServerStmt;
            Some(protobuf::node::Node::CreateForeignServerStmt(convert_create_foreign_server_stmt(&*cfss)))
        }
        bindings_raw::NodeTag_T_AlterForeignServerStmt => {
            let afss = node_ptr as *mut bindings_raw::AlterForeignServerStmt;
            Some(protobuf::node::Node::AlterForeignServerStmt(convert_alter_foreign_server_stmt(&*afss)))
        }
        bindings_raw::NodeTag_T_CreateForeignTableStmt => {
            let cfts = node_ptr as *mut bindings_raw::CreateForeignTableStmt;
            Some(protobuf::node::Node::CreateForeignTableStmt(convert_create_foreign_table_stmt(&*cfts)))
        }
        bindings_raw::NodeTag_T_CreateUserMappingStmt => {
            let cums = node_ptr as *mut bindings_raw::CreateUserMappingStmt;
            Some(protobuf::node::Node::CreateUserMappingStmt(convert_create_user_mapping_stmt(&*cums)))
        }
        bindings_raw::NodeTag_T_AlterUserMappingStmt => {
            let aums = node_ptr as *mut bindings_raw::AlterUserMappingStmt;
            Some(protobuf::node::Node::AlterUserMappingStmt(convert_alter_user_mapping_stmt(&*aums)))
        }
        bindings_raw::NodeTag_T_DropUserMappingStmt => {
            let dums = node_ptr as *mut bindings_raw::DropUserMappingStmt;
            Some(protobuf::node::Node::DropUserMappingStmt(convert_drop_user_mapping_stmt(&*dums)))
        }
        bindings_raw::NodeTag_T_ImportForeignSchemaStmt => {
            let ifss = node_ptr as *mut bindings_raw::ImportForeignSchemaStmt;
            Some(protobuf::node::Node::ImportForeignSchemaStmt(convert_import_foreign_schema_stmt(&*ifss)))
        }
        bindings_raw::NodeTag_T_CreateTableSpaceStmt => {
            let ctss = node_ptr as *mut bindings_raw::CreateTableSpaceStmt;
            Some(protobuf::node::Node::CreateTableSpaceStmt(convert_create_table_space_stmt(&*ctss)))
        }
        bindings_raw::NodeTag_T_DropTableSpaceStmt => {
            let dtss = node_ptr as *mut bindings_raw::DropTableSpaceStmt;
            Some(protobuf::node::Node::DropTableSpaceStmt(convert_drop_table_space_stmt(&*dtss)))
        }
        bindings_raw::NodeTag_T_AlterTableSpaceOptionsStmt => {
            let atsos = node_ptr as *mut bindings_raw::AlterTableSpaceOptionsStmt;
            Some(protobuf::node::Node::AlterTableSpaceOptionsStmt(convert_alter_table_space_options_stmt(&*atsos)))
        }
        bindings_raw::NodeTag_T_AlterTableMoveAllStmt => {
            let atmas = node_ptr as *mut bindings_raw::AlterTableMoveAllStmt;
            Some(protobuf::node::Node::AlterTableMoveAllStmt(convert_alter_table_move_all_stmt(&*atmas)))
        }
        bindings_raw::NodeTag_T_AlterExtensionStmt => {
            let aes = node_ptr as *mut bindings_raw::AlterExtensionStmt;
            Some(protobuf::node::Node::AlterExtensionStmt(convert_alter_extension_stmt(&*aes)))
        }
        bindings_raw::NodeTag_T_AlterExtensionContentsStmt => {
            let aecs = node_ptr as *mut bindings_raw::AlterExtensionContentsStmt;
            Some(protobuf::node::Node::AlterExtensionContentsStmt(Box::new(convert_alter_extension_contents_stmt(&*aecs))))
        }
        bindings_raw::NodeTag_T_AlterDomainStmt => {
            let ads = node_ptr as *mut bindings_raw::AlterDomainStmt;
            Some(protobuf::node::Node::AlterDomainStmt(Box::new(convert_alter_domain_stmt(&*ads))))
        }
        bindings_raw::NodeTag_T_AlterFunctionStmt => {
            let afs = node_ptr as *mut bindings_raw::AlterFunctionStmt;
            Some(protobuf::node::Node::AlterFunctionStmt(convert_alter_function_stmt(&*afs)))
        }
        bindings_raw::NodeTag_T_AlterOperatorStmt => {
            let aos = node_ptr as *mut bindings_raw::AlterOperatorStmt;
            Some(protobuf::node::Node::AlterOperatorStmt(convert_alter_operator_stmt(&*aos)))
        }
        bindings_raw::NodeTag_T_AlterTypeStmt => {
            let ats = node_ptr as *mut bindings_raw::AlterTypeStmt;
            Some(protobuf::node::Node::AlterTypeStmt(convert_alter_type_stmt(&*ats)))
        }
        bindings_raw::NodeTag_T_AlterObjectSchemaStmt => {
            let aoss = node_ptr as *mut bindings_raw::AlterObjectSchemaStmt;
            Some(protobuf::node::Node::AlterObjectSchemaStmt(Box::new(convert_alter_object_schema_stmt(&*aoss))))
        }
        bindings_raw::NodeTag_T_AlterObjectDependsStmt => {
            let aods = node_ptr as *mut bindings_raw::AlterObjectDependsStmt;
            Some(protobuf::node::Node::AlterObjectDependsStmt(Box::new(convert_alter_object_depends_stmt(&*aods))))
        }
        bindings_raw::NodeTag_T_AlterCollationStmt => {
            let acs = node_ptr as *mut bindings_raw::AlterCollationStmt;
            Some(protobuf::node::Node::AlterCollationStmt(convert_alter_collation_stmt(&*acs)))
        }
        bindings_raw::NodeTag_T_AlterDefaultPrivilegesStmt => {
            let adps = node_ptr as *mut bindings_raw::AlterDefaultPrivilegesStmt;
            Some(protobuf::node::Node::AlterDefaultPrivilegesStmt(convert_alter_default_privileges_stmt(&*adps)))
        }
        bindings_raw::NodeTag_T_CreateCastStmt => {
            let ccs = node_ptr as *mut bindings_raw::CreateCastStmt;
            Some(protobuf::node::Node::CreateCastStmt(convert_create_cast_stmt(&*ccs)))
        }
        bindings_raw::NodeTag_T_CreateTransformStmt => {
            let cts = node_ptr as *mut bindings_raw::CreateTransformStmt;
            Some(protobuf::node::Node::CreateTransformStmt(convert_create_transform_stmt(&*cts)))
        }
        bindings_raw::NodeTag_T_CreateConversionStmt => {
            let ccs = node_ptr as *mut bindings_raw::CreateConversionStmt;
            Some(protobuf::node::Node::CreateConversionStmt(convert_create_conversion_stmt(&*ccs)))
        }
        bindings_raw::NodeTag_T_AlterTSDictionaryStmt => {
            let atds = node_ptr as *mut bindings_raw::AlterTSDictionaryStmt;
            Some(protobuf::node::Node::AlterTsdictionaryStmt(convert_alter_ts_dictionary_stmt(&*atds)))
        }
        bindings_raw::NodeTag_T_AlterTSConfigurationStmt => {
            let atcs = node_ptr as *mut bindings_raw::AlterTSConfigurationStmt;
            Some(protobuf::node::Node::AlterTsconfigurationStmt(convert_alter_ts_configuration_stmt(&*atcs)))
        }
        bindings_raw::NodeTag_T_CreatedbStmt => {
            let cds = node_ptr as *mut bindings_raw::CreatedbStmt;
            Some(protobuf::node::Node::CreatedbStmt(convert_createdb_stmt(&*cds)))
        }
        bindings_raw::NodeTag_T_DropdbStmt => {
            let dds = node_ptr as *mut bindings_raw::DropdbStmt;
            Some(protobuf::node::Node::DropdbStmt(convert_dropdb_stmt(&*dds)))
        }
        bindings_raw::NodeTag_T_AlterDatabaseStmt => {
            let ads = node_ptr as *mut bindings_raw::AlterDatabaseStmt;
            Some(protobuf::node::Node::AlterDatabaseStmt(convert_alter_database_stmt(&*ads)))
        }
        bindings_raw::NodeTag_T_AlterDatabaseSetStmt => {
            let adss = node_ptr as *mut bindings_raw::AlterDatabaseSetStmt;
            Some(protobuf::node::Node::AlterDatabaseSetStmt(convert_alter_database_set_stmt(&*adss)))
        }
        bindings_raw::NodeTag_T_AlterDatabaseRefreshCollStmt => {
            let adrcs = node_ptr as *mut bindings_raw::AlterDatabaseRefreshCollStmt;
            Some(protobuf::node::Node::AlterDatabaseRefreshCollStmt(convert_alter_database_refresh_coll_stmt(&*adrcs)))
        }
        bindings_raw::NodeTag_T_AlterSystemStmt => {
            let ass = node_ptr as *mut bindings_raw::AlterSystemStmt;
            Some(protobuf::node::Node::AlterSystemStmt(convert_alter_system_stmt(&*ass)))
        }
        bindings_raw::NodeTag_T_ClusterStmt => {
            let cs = node_ptr as *mut bindings_raw::ClusterStmt;
            Some(protobuf::node::Node::ClusterStmt(convert_cluster_stmt(&*cs)))
        }
        bindings_raw::NodeTag_T_ReindexStmt => {
            let rs = node_ptr as *mut bindings_raw::ReindexStmt;
            Some(protobuf::node::Node::ReindexStmt(convert_reindex_stmt(&*rs)))
        }
        bindings_raw::NodeTag_T_ConstraintsSetStmt => {
            let css = node_ptr as *mut bindings_raw::ConstraintsSetStmt;
            Some(protobuf::node::Node::ConstraintsSetStmt(convert_constraints_set_stmt(&*css)))
        }
        bindings_raw::NodeTag_T_LoadStmt => {
            let ls = node_ptr as *mut bindings_raw::LoadStmt;
            Some(protobuf::node::Node::LoadStmt(convert_load_stmt(&*ls)))
        }
        bindings_raw::NodeTag_T_DropOwnedStmt => {
            let dos = node_ptr as *mut bindings_raw::DropOwnedStmt;
            Some(protobuf::node::Node::DropOwnedStmt(convert_drop_owned_stmt(&*dos)))
        }
        bindings_raw::NodeTag_T_ReassignOwnedStmt => {
            let ros = node_ptr as *mut bindings_raw::ReassignOwnedStmt;
            Some(protobuf::node::Node::ReassignOwnedStmt(convert_reassign_owned_stmt(&*ros)))
        }
        bindings_raw::NodeTag_T_DropSubscriptionStmt => {
            let dss = node_ptr as *mut bindings_raw::DropSubscriptionStmt;
            Some(protobuf::node::Node::DropSubscriptionStmt(convert_drop_subscription_stmt(&*dss)))
        }
        bindings_raw::NodeTag_T_TableFunc => {
            let tf = node_ptr as *mut bindings_raw::TableFunc;
            Some(protobuf::node::Node::TableFunc(Box::new(convert_table_func(&*tf))))
        }
        bindings_raw::NodeTag_T_IntoClause => {
            let ic = node_ptr as *mut bindings_raw::IntoClause;
            Some(protobuf::node::Node::IntoClause(Box::new(convert_into_clause_node(&*ic))))
        }
        bindings_raw::NodeTag_T_TableLikeClause => {
            let tlc = node_ptr as *mut bindings_raw::TableLikeClause;
            Some(protobuf::node::Node::TableLikeClause(convert_table_like_clause(&*tlc)))
        }
        bindings_raw::NodeTag_T_RangeTableFunc => {
            let rtf = node_ptr as *mut bindings_raw::RangeTableFunc;
            Some(protobuf::node::Node::RangeTableFunc(Box::new(convert_range_table_func(&*rtf))))
        }
        bindings_raw::NodeTag_T_RangeTableFuncCol => {
            let rtfc = node_ptr as *mut bindings_raw::RangeTableFuncCol;
            Some(protobuf::node::Node::RangeTableFuncCol(Box::new(convert_range_table_func_col(&*rtfc))))
        }
        bindings_raw::NodeTag_T_RangeTableSample => {
            let rts = node_ptr as *mut bindings_raw::RangeTableSample;
            Some(protobuf::node::Node::RangeTableSample(Box::new(convert_range_table_sample(&*rts))))
        }
        bindings_raw::NodeTag_T_PartitionSpec => {
            let ps = node_ptr as *mut bindings_raw::PartitionSpec;
            Some(protobuf::node::Node::PartitionSpec(convert_partition_spec(&*ps)))
        }
        bindings_raw::NodeTag_T_PartitionBoundSpec => {
            let pbs = node_ptr as *mut bindings_raw::PartitionBoundSpec;
            Some(protobuf::node::Node::PartitionBoundSpec(convert_partition_bound_spec(&*pbs)))
        }
        bindings_raw::NodeTag_T_PartitionCmd => {
            let pc = node_ptr as *mut bindings_raw::PartitionCmd;
            Some(protobuf::node::Node::PartitionCmd(convert_partition_cmd(&*pc)))
        }
        bindings_raw::NodeTag_T_SinglePartitionSpec => Some(protobuf::node::Node::SinglePartitionSpec(protobuf::SinglePartitionSpec {})),
        bindings_raw::NodeTag_T_InferClause => {
            let ic = node_ptr as *mut bindings_raw::InferClause;
            convert_infer_clause(ic).map(|c| protobuf::node::Node::InferClause(c))
        }
        bindings_raw::NodeTag_T_OnConflictClause => {
            let occ = node_ptr as *mut bindings_raw::OnConflictClause;
            Some(protobuf::node::Node::OnConflictClause(Box::new(convert_on_conflict_clause_node(&*occ))))
        }
        bindings_raw::NodeTag_T_TriggerTransition => {
            let tt = node_ptr as *mut bindings_raw::TriggerTransition;
            Some(protobuf::node::Node::TriggerTransition(convert_trigger_transition(&*tt)))
        }
        bindings_raw::NodeTag_T_CTESearchClause => {
            let csc = node_ptr as *mut bindings_raw::CTESearchClause;
            Some(protobuf::node::Node::CtesearchClause(convert_cte_search_clause(&*csc)))
        }
        bindings_raw::NodeTag_T_CTECycleClause => {
            let ccc = node_ptr as *mut bindings_raw::CTECycleClause;
            Some(protobuf::node::Node::CtecycleClause(Box::new(convert_cte_cycle_clause(&*ccc))))
        }
        bindings_raw::NodeTag_T_CreateStatsStmt => {
            let css = node_ptr as *mut bindings_raw::CreateStatsStmt;
            Some(protobuf::node::Node::CreateStatsStmt(convert_create_stats_stmt(&*css)))
        }
        bindings_raw::NodeTag_T_AlterStatsStmt => {
            let ass = node_ptr as *mut bindings_raw::AlterStatsStmt;
            Some(protobuf::node::Node::AlterStatsStmt(Box::new(convert_alter_stats_stmt(&*ass))))
        }
        bindings_raw::NodeTag_T_StatsElem => {
            let se = node_ptr as *mut bindings_raw::StatsElem;
            Some(protobuf::node::Node::StatsElem(Box::new(convert_stats_elem(&*se))))
        }
        bindings_raw::NodeTag_T_SQLValueFunction => {
            let svf = node_ptr as *mut bindings_raw::SQLValueFunction;
            Some(protobuf::node::Node::SqlvalueFunction(Box::new(convert_sql_value_function(&*svf))))
        }
        bindings_raw::NodeTag_T_XmlExpr => {
            let xe = node_ptr as *mut bindings_raw::XmlExpr;
            Some(protobuf::node::Node::XmlExpr(Box::new(convert_xml_expr(&*xe))))
        }
        bindings_raw::NodeTag_T_XmlSerialize => {
            let xs = node_ptr as *mut bindings_raw::XmlSerialize;
            Some(protobuf::node::Node::XmlSerialize(Box::new(convert_xml_serialize(&*xs))))
        }
        bindings_raw::NodeTag_T_NamedArgExpr => {
            let nae = node_ptr as *mut bindings_raw::NamedArgExpr;
            Some(protobuf::node::Node::NamedArgExpr(Box::new(convert_named_arg_expr(&*nae))))
        }
        // JSON nodes
        bindings_raw::NodeTag_T_JsonFormat => {
            let jf = node_ptr as *mut bindings_raw::JsonFormat;
            Some(protobuf::node::Node::JsonFormat(convert_json_format(&*jf)))
        }
        bindings_raw::NodeTag_T_JsonReturning => {
            let jr = node_ptr as *mut bindings_raw::JsonReturning;
            Some(protobuf::node::Node::JsonReturning(convert_json_returning(&*jr)))
        }
        bindings_raw::NodeTag_T_JsonValueExpr => {
            let jve = node_ptr as *mut bindings_raw::JsonValueExpr;
            Some(protobuf::node::Node::JsonValueExpr(Box::new(convert_json_value_expr(&*jve))))
        }
        bindings_raw::NodeTag_T_JsonConstructorExpr => {
            let jce = node_ptr as *mut bindings_raw::JsonConstructorExpr;
            Some(protobuf::node::Node::JsonConstructorExpr(Box::new(convert_json_constructor_expr(&*jce))))
        }
        bindings_raw::NodeTag_T_JsonIsPredicate => {
            let jip = node_ptr as *mut bindings_raw::JsonIsPredicate;
            Some(protobuf::node::Node::JsonIsPredicate(Box::new(convert_json_is_predicate(&*jip))))
        }
        bindings_raw::NodeTag_T_JsonBehavior => {
            let jb = node_ptr as *mut bindings_raw::JsonBehavior;
            Some(protobuf::node::Node::JsonBehavior(Box::new(convert_json_behavior(&*jb))))
        }
        bindings_raw::NodeTag_T_JsonExpr => {
            let je = node_ptr as *mut bindings_raw::JsonExpr;
            Some(protobuf::node::Node::JsonExpr(Box::new(convert_json_expr(&*je))))
        }
        bindings_raw::NodeTag_T_JsonTablePath => {
            let jtp = node_ptr as *mut bindings_raw::JsonTablePath;
            Some(protobuf::node::Node::JsonTablePath(convert_json_table_path(&*jtp)))
        }
        bindings_raw::NodeTag_T_JsonTablePathScan => {
            let jtps = node_ptr as *mut bindings_raw::JsonTablePathScan;
            Some(protobuf::node::Node::JsonTablePathScan(Box::new(convert_json_table_path_scan(&*jtps))))
        }
        bindings_raw::NodeTag_T_JsonTableSiblingJoin => {
            let jtsj = node_ptr as *mut bindings_raw::JsonTableSiblingJoin;
            Some(protobuf::node::Node::JsonTableSiblingJoin(Box::new(convert_json_table_sibling_join(&*jtsj))))
        }
        bindings_raw::NodeTag_T_JsonOutput => {
            let jo = node_ptr as *mut bindings_raw::JsonOutput;
            Some(protobuf::node::Node::JsonOutput(convert_json_output(&*jo)))
        }
        bindings_raw::NodeTag_T_JsonArgument => {
            let ja = node_ptr as *mut bindings_raw::JsonArgument;
            Some(protobuf::node::Node::JsonArgument(Box::new(convert_json_argument(&*ja))))
        }
        bindings_raw::NodeTag_T_JsonFuncExpr => {
            let jfe = node_ptr as *mut bindings_raw::JsonFuncExpr;
            Some(protobuf::node::Node::JsonFuncExpr(Box::new(convert_json_func_expr(&*jfe))))
        }
        bindings_raw::NodeTag_T_JsonTablePathSpec => {
            let jtps = node_ptr as *mut bindings_raw::JsonTablePathSpec;
            Some(protobuf::node::Node::JsonTablePathSpec(Box::new(convert_json_table_path_spec(&*jtps))))
        }
        bindings_raw::NodeTag_T_JsonTable => {
            let jt = node_ptr as *mut bindings_raw::JsonTable;
            Some(protobuf::node::Node::JsonTable(Box::new(convert_json_table(&*jt))))
        }
        bindings_raw::NodeTag_T_JsonTableColumn => {
            let jtc = node_ptr as *mut bindings_raw::JsonTableColumn;
            Some(protobuf::node::Node::JsonTableColumn(Box::new(convert_json_table_column(&*jtc))))
        }
        bindings_raw::NodeTag_T_JsonKeyValue => {
            let jkv = node_ptr as *mut bindings_raw::JsonKeyValue;
            Some(protobuf::node::Node::JsonKeyValue(Box::new(convert_json_key_value(&*jkv))))
        }
        bindings_raw::NodeTag_T_JsonParseExpr => {
            let jpe = node_ptr as *mut bindings_raw::JsonParseExpr;
            Some(protobuf::node::Node::JsonParseExpr(Box::new(convert_json_parse_expr(&*jpe))))
        }
        bindings_raw::NodeTag_T_JsonScalarExpr => {
            let jse = node_ptr as *mut bindings_raw::JsonScalarExpr;
            Some(protobuf::node::Node::JsonScalarExpr(Box::new(convert_json_scalar_expr(&*jse))))
        }
        bindings_raw::NodeTag_T_JsonSerializeExpr => {
            let jse = node_ptr as *mut bindings_raw::JsonSerializeExpr;
            Some(protobuf::node::Node::JsonSerializeExpr(Box::new(convert_json_serialize_expr(&*jse))))
        }
        bindings_raw::NodeTag_T_JsonObjectConstructor => {
            let joc = node_ptr as *mut bindings_raw::JsonObjectConstructor;
            Some(protobuf::node::Node::JsonObjectConstructor(convert_json_object_constructor(&*joc)))
        }
        bindings_raw::NodeTag_T_JsonArrayConstructor => {
            let jac = node_ptr as *mut bindings_raw::JsonArrayConstructor;
            Some(protobuf::node::Node::JsonArrayConstructor(convert_json_array_constructor(&*jac)))
        }
        bindings_raw::NodeTag_T_JsonArrayQueryConstructor => {
            let jaqc = node_ptr as *mut bindings_raw::JsonArrayQueryConstructor;
            Some(protobuf::node::Node::JsonArrayQueryConstructor(Box::new(convert_json_array_query_constructor(&*jaqc))))
        }
        bindings_raw::NodeTag_T_JsonAggConstructor => {
            let jac = node_ptr as *mut bindings_raw::JsonAggConstructor;
            Some(protobuf::node::Node::JsonAggConstructor(Box::new(convert_json_agg_constructor(&*jac))))
        }
        bindings_raw::NodeTag_T_JsonObjectAgg => {
            let joa = node_ptr as *mut bindings_raw::JsonObjectAgg;
            Some(protobuf::node::Node::JsonObjectAgg(Box::new(convert_json_object_agg(&*joa))))
        }
        bindings_raw::NodeTag_T_JsonArrayAgg => {
            let jaa = node_ptr as *mut bindings_raw::JsonArrayAgg;
            Some(protobuf::node::Node::JsonArrayAgg(Box::new(convert_json_array_agg(&*jaa))))
        }
        _ => {
            // For unhandled node types, return None
            // In the future, we could add more node types here
            None
        }
    };

    node.map(|n| protobuf::Node { node: Some(n) })
}

/// Converts a PostgreSQL List to a protobuf List of Nodes.
unsafe fn convert_list(list: &bindings_raw::List) -> protobuf::List {
    let items = convert_list_to_nodes(list as *const bindings_raw::List as *mut bindings_raw::List);
    protobuf::List { items }
}

/// Converts a PostgreSQL List pointer to a Vec of protobuf Nodes.
/// Note: Preserves placeholder nodes (Node { node: None }) for cases like DISTINCT
/// where the list must retain its structure even if content is not recognized.
unsafe fn convert_list_to_nodes(list: *mut bindings_raw::List) -> Vec<protobuf::Node> {
    if list.is_null() {
        return Vec::new();
    }

    let list_ref = &*list;
    let length = list_ref.length as usize;
    let mut nodes = Vec::with_capacity(length);

    for i in 0..length {
        let cell = list_ref.elements.add(i);
        let node_ptr = (*cell).ptr_value as *mut bindings_raw::Node;

        // Always push the node, even if it's None/unrecognized.
        // This preserves list structure for things like DISTINCT where
        // a placeholder node (Node { node: None }) is meaningful.
        let node = convert_node(node_ptr).unwrap_or_else(|| protobuf::Node { node: None });
        nodes.push(node);
    }

    nodes
}

// ============================================================================
// Statement Conversions
// ============================================================================

unsafe fn convert_select_stmt(stmt: &bindings_raw::SelectStmt) -> protobuf::SelectStmt {
    protobuf::SelectStmt {
        distinct_clause: convert_list_to_nodes(stmt.distinctClause),
        into_clause: convert_into_clause(stmt.intoClause),
        target_list: convert_list_to_nodes(stmt.targetList),
        from_clause: convert_list_to_nodes(stmt.fromClause),
        where_clause: convert_node_boxed(stmt.whereClause),
        group_clause: convert_list_to_nodes(stmt.groupClause),
        group_distinct: stmt.groupDistinct,
        having_clause: convert_node_boxed(stmt.havingClause),
        window_clause: convert_list_to_nodes(stmt.windowClause),
        values_lists: convert_list_to_nodes(stmt.valuesLists),
        sort_clause: convert_list_to_nodes(stmt.sortClause),
        limit_offset: convert_node_boxed(stmt.limitOffset),
        limit_count: convert_node_boxed(stmt.limitCount),
        limit_option: stmt.limitOption as i32 + 1, // Protobuf enums have UNDEFINED=0, so C values need +1
        locking_clause: convert_list_to_nodes(stmt.lockingClause),
        with_clause: convert_with_clause_opt(stmt.withClause),
        op: stmt.op as i32 + 1, // Protobuf SetOperation has UNDEFINED=0, so C values need +1
        all: stmt.all,
        larg: if stmt.larg.is_null() { None } else { Some(Box::new(convert_select_stmt(&*stmt.larg))) },
        rarg: if stmt.rarg.is_null() { None } else { Some(Box::new(convert_select_stmt(&*stmt.rarg))) },
    }
}

unsafe fn convert_insert_stmt(stmt: &bindings_raw::InsertStmt) -> protobuf::InsertStmt {
    protobuf::InsertStmt {
        relation: if stmt.relation.is_null() { None } else { Some(convert_range_var(&*stmt.relation)) },
        cols: convert_list_to_nodes(stmt.cols),
        select_stmt: convert_node_boxed(stmt.selectStmt),
        on_conflict_clause: convert_on_conflict_clause(stmt.onConflictClause),
        returning_list: convert_list_to_nodes(stmt.returningList),
        with_clause: convert_with_clause_opt(stmt.withClause),
        r#override: stmt.override_ as i32 + 1,
    }
}

unsafe fn convert_update_stmt(stmt: &bindings_raw::UpdateStmt) -> protobuf::UpdateStmt {
    protobuf::UpdateStmt {
        relation: if stmt.relation.is_null() { None } else { Some(convert_range_var(&*stmt.relation)) },
        target_list: convert_list_to_nodes(stmt.targetList),
        where_clause: convert_node_boxed(stmt.whereClause),
        from_clause: convert_list_to_nodes(stmt.fromClause),
        returning_list: convert_list_to_nodes(stmt.returningList),
        with_clause: convert_with_clause_opt(stmt.withClause),
    }
}

unsafe fn convert_delete_stmt(stmt: &bindings_raw::DeleteStmt) -> protobuf::DeleteStmt {
    protobuf::DeleteStmt {
        relation: if stmt.relation.is_null() { None } else { Some(convert_range_var(&*stmt.relation)) },
        using_clause: convert_list_to_nodes(stmt.usingClause),
        where_clause: convert_node_boxed(stmt.whereClause),
        returning_list: convert_list_to_nodes(stmt.returningList),
        with_clause: convert_with_clause_opt(stmt.withClause),
    }
}

unsafe fn convert_create_stmt(stmt: &bindings_raw::CreateStmt) -> protobuf::CreateStmt {
    protobuf::CreateStmt {
        relation: if stmt.relation.is_null() { None } else { Some(convert_range_var(&*stmt.relation)) },
        table_elts: convert_list_to_nodes(stmt.tableElts),
        inh_relations: convert_list_to_nodes(stmt.inhRelations),
        partbound: convert_partition_bound_spec_opt(stmt.partbound),
        partspec: convert_partition_spec_opt(stmt.partspec),
        of_typename: if stmt.ofTypename.is_null() { None } else { Some(convert_type_name(&*stmt.ofTypename)) },
        constraints: convert_list_to_nodes(stmt.constraints),
        options: convert_list_to_nodes(stmt.options),
        oncommit: stmt.oncommit as i32 + 1,
        tablespacename: convert_c_string(stmt.tablespacename),
        access_method: convert_c_string(stmt.accessMethod),
        if_not_exists: stmt.if_not_exists,
    }
}

unsafe fn convert_drop_stmt(stmt: &bindings_raw::DropStmt) -> protobuf::DropStmt {
    protobuf::DropStmt {
        objects: convert_list_to_nodes(stmt.objects),
        remove_type: stmt.removeType as i32 + 1,
        behavior: stmt.behavior as i32 + 1,
        missing_ok: stmt.missing_ok,
        concurrent: stmt.concurrent,
    }
}

unsafe fn convert_index_stmt(stmt: &bindings_raw::IndexStmt) -> protobuf::IndexStmt {
    protobuf::IndexStmt {
        idxname: convert_c_string(stmt.idxname),
        relation: if stmt.relation.is_null() { None } else { Some(convert_range_var(&*stmt.relation)) },
        access_method: convert_c_string(stmt.accessMethod),
        table_space: convert_c_string(stmt.tableSpace),
        index_params: convert_list_to_nodes(stmt.indexParams),
        index_including_params: convert_list_to_nodes(stmt.indexIncludingParams),
        options: convert_list_to_nodes(stmt.options),
        where_clause: convert_node_boxed(stmt.whereClause),
        exclude_op_names: convert_list_to_nodes(stmt.excludeOpNames),
        idxcomment: convert_c_string(stmt.idxcomment),
        index_oid: stmt.indexOid,
        old_number: stmt.oldNumber,
        old_create_subid: stmt.oldCreateSubid,
        old_first_relfilelocator_subid: stmt.oldFirstRelfilelocatorSubid,
        unique: stmt.unique,
        nulls_not_distinct: stmt.nulls_not_distinct,
        primary: stmt.primary,
        isconstraint: stmt.isconstraint,
        deferrable: stmt.deferrable,
        initdeferred: stmt.initdeferred,
        transformed: stmt.transformed,
        concurrent: stmt.concurrent,
        if_not_exists: stmt.if_not_exists,
        reset_default_tblspc: stmt.reset_default_tblspc,
    }
}

// ============================================================================
// Expression/Clause Conversions
// ============================================================================

unsafe fn convert_range_var(rv: &bindings_raw::RangeVar) -> protobuf::RangeVar {
    protobuf::RangeVar {
        catalogname: convert_c_string(rv.catalogname),
        schemaname: convert_c_string(rv.schemaname),
        relname: convert_c_string(rv.relname),
        inh: rv.inh,
        relpersistence: String::from_utf8_lossy(&[rv.relpersistence as u8]).to_string(),
        alias: if rv.alias.is_null() { None } else { Some(convert_alias(&*rv.alias)) },
        location: rv.location,
    }
}

unsafe fn convert_column_ref(cr: &bindings_raw::ColumnRef) -> protobuf::ColumnRef {
    protobuf::ColumnRef { fields: convert_list_to_nodes(cr.fields), location: cr.location }
}

unsafe fn convert_res_target(rt: &bindings_raw::ResTarget) -> protobuf::ResTarget {
    protobuf::ResTarget {
        name: convert_c_string(rt.name),
        indirection: convert_list_to_nodes(rt.indirection),
        val: convert_node_boxed(rt.val),
        location: rt.location,
    }
}

unsafe fn convert_a_expr(expr: &bindings_raw::A_Expr) -> protobuf::AExpr {
    protobuf::AExpr {
        kind: expr.kind as i32 + 1,
        name: convert_list_to_nodes(expr.name),
        lexpr: convert_node_boxed(expr.lexpr),
        rexpr: convert_node_boxed(expr.rexpr),
        location: expr.location,
    }
}

unsafe fn convert_a_const(aconst: &bindings_raw::A_Const) -> protobuf::AConst {
    let val = if aconst.isnull {
        None
    } else {
        // Check the node tag in the val union to determine the type
        let node_tag = aconst.val.node.type_;
        match node_tag {
            bindings_raw::NodeTag_T_Integer => Some(protobuf::a_const::Val::Ival(protobuf::Integer { ival: aconst.val.ival.ival })),
            bindings_raw::NodeTag_T_Float => {
                let fval = if aconst.val.fval.fval.is_null() {
                    std::string::String::new()
                } else {
                    CStr::from_ptr(aconst.val.fval.fval).to_string_lossy().to_string()
                };
                Some(protobuf::a_const::Val::Fval(protobuf::Float { fval }))
            }
            bindings_raw::NodeTag_T_Boolean => Some(protobuf::a_const::Val::Boolval(protobuf::Boolean { boolval: aconst.val.boolval.boolval })),
            bindings_raw::NodeTag_T_String => {
                let sval = if aconst.val.sval.sval.is_null() {
                    std::string::String::new()
                } else {
                    CStr::from_ptr(aconst.val.sval.sval).to_string_lossy().to_string()
                };
                Some(protobuf::a_const::Val::Sval(protobuf::String { sval }))
            }
            bindings_raw::NodeTag_T_BitString => {
                let bsval = if aconst.val.bsval.bsval.is_null() {
                    std::string::String::new()
                } else {
                    CStr::from_ptr(aconst.val.bsval.bsval).to_string_lossy().to_string()
                };
                Some(protobuf::a_const::Val::Bsval(protobuf::BitString { bsval }))
            }
            _ => None,
        }
    };

    protobuf::AConst { isnull: aconst.isnull, val, location: aconst.location }
}

unsafe fn convert_func_call(fc: &bindings_raw::FuncCall) -> protobuf::FuncCall {
    protobuf::FuncCall {
        funcname: convert_list_to_nodes(fc.funcname),
        args: convert_list_to_nodes(fc.args),
        agg_order: convert_list_to_nodes(fc.agg_order),
        agg_filter: convert_node_boxed(fc.agg_filter),
        over: if fc.over.is_null() { None } else { Some(Box::new(convert_window_def(&*fc.over))) },
        agg_within_group: fc.agg_within_group,
        agg_star: fc.agg_star,
        agg_distinct: fc.agg_distinct,
        func_variadic: fc.func_variadic,
        funcformat: fc.funcformat as i32 + 1,
        location: fc.location,
    }
}

unsafe fn convert_type_cast(tc: &bindings_raw::TypeCast) -> protobuf::TypeCast {
    protobuf::TypeCast {
        arg: convert_node_boxed(tc.arg),
        type_name: if tc.typeName.is_null() { None } else { Some(convert_type_name(&*tc.typeName)) },
        location: tc.location,
    }
}

unsafe fn convert_type_name(tn: &bindings_raw::TypeName) -> protobuf::TypeName {
    protobuf::TypeName {
        names: convert_list_to_nodes(tn.names),
        type_oid: tn.typeOid,
        setof: tn.setof,
        pct_type: tn.pct_type,
        typmods: convert_list_to_nodes(tn.typmods),
        typemod: tn.typemod,
        array_bounds: convert_list_to_nodes(tn.arrayBounds),
        location: tn.location,
    }
}

unsafe fn convert_alias(alias: &bindings_raw::Alias) -> protobuf::Alias {
    protobuf::Alias { aliasname: convert_c_string(alias.aliasname), colnames: convert_list_to_nodes(alias.colnames) }
}

unsafe fn convert_join_expr(je: &bindings_raw::JoinExpr) -> protobuf::JoinExpr {
    protobuf::JoinExpr {
        jointype: je.jointype as i32 + 1,
        is_natural: je.isNatural,
        larg: convert_node_boxed(je.larg),
        rarg: convert_node_boxed(je.rarg),
        using_clause: convert_list_to_nodes(je.usingClause),
        join_using_alias: if je.join_using_alias.is_null() { None } else { Some(convert_alias(&*je.join_using_alias)) },
        quals: convert_node_boxed(je.quals),
        alias: if je.alias.is_null() { None } else { Some(convert_alias(&*je.alias)) },
        rtindex: je.rtindex,
    }
}

unsafe fn convert_sort_by(sb: &bindings_raw::SortBy) -> protobuf::SortBy {
    protobuf::SortBy {
        node: convert_node_boxed(sb.node),
        sortby_dir: sb.sortby_dir as i32 + 1,
        sortby_nulls: sb.sortby_nulls as i32 + 1,
        use_op: convert_list_to_nodes(sb.useOp),
        location: sb.location,
    }
}

unsafe fn convert_bool_expr(be: &bindings_raw::BoolExpr) -> protobuf::BoolExpr {
    protobuf::BoolExpr {
        xpr: None, // Xpr is internal
        boolop: be.boolop as i32 + 1,
        args: convert_list_to_nodes(be.args),
        location: be.location,
    }
}

unsafe fn convert_sub_link(sl: &bindings_raw::SubLink) -> protobuf::SubLink {
    protobuf::SubLink {
        xpr: None,
        sub_link_type: sl.subLinkType as i32 + 1,
        sub_link_id: sl.subLinkId,
        testexpr: convert_node_boxed(sl.testexpr),
        oper_name: convert_list_to_nodes(sl.operName),
        subselect: convert_node_boxed(sl.subselect),
        location: sl.location,
    }
}

unsafe fn convert_null_test(nt: &bindings_raw::NullTest) -> protobuf::NullTest {
    protobuf::NullTest {
        xpr: None,
        arg: convert_node_boxed(nt.arg as *mut bindings_raw::Node),
        nulltesttype: nt.nulltesttype as i32 + 1,
        argisrow: nt.argisrow,
        location: nt.location,
    }
}

unsafe fn convert_case_expr(ce: &bindings_raw::CaseExpr) -> protobuf::CaseExpr {
    protobuf::CaseExpr {
        xpr: None,
        casetype: ce.casetype,
        casecollid: ce.casecollid,
        arg: convert_node_boxed(ce.arg as *mut bindings_raw::Node),
        args: convert_list_to_nodes(ce.args),
        defresult: convert_node_boxed(ce.defresult as *mut bindings_raw::Node),
        location: ce.location,
    }
}

unsafe fn convert_case_when(cw: &bindings_raw::CaseWhen) -> protobuf::CaseWhen {
    protobuf::CaseWhen {
        xpr: None,
        expr: convert_node_boxed(cw.expr as *mut bindings_raw::Node),
        result: convert_node_boxed(cw.result as *mut bindings_raw::Node),
        location: cw.location,
    }
}

unsafe fn convert_coalesce_expr(ce: &bindings_raw::CoalesceExpr) -> protobuf::CoalesceExpr {
    protobuf::CoalesceExpr {
        xpr: None,
        coalescetype: ce.coalescetype,
        coalescecollid: ce.coalescecollid,
        args: convert_list_to_nodes(ce.args),
        location: ce.location,
    }
}

unsafe fn convert_with_clause(wc: &bindings_raw::WithClause) -> protobuf::WithClause {
    protobuf::WithClause { ctes: convert_list_to_nodes(wc.ctes), recursive: wc.recursive, location: wc.location }
}

unsafe fn convert_with_clause_opt(wc: *mut bindings_raw::WithClause) -> Option<protobuf::WithClause> {
    if wc.is_null() {
        None
    } else {
        Some(convert_with_clause(&*wc))
    }
}

unsafe fn convert_common_table_expr(cte: &bindings_raw::CommonTableExpr) -> protobuf::CommonTableExpr {
    protobuf::CommonTableExpr {
        ctename: convert_c_string(cte.ctename),
        aliascolnames: convert_list_to_nodes(cte.aliascolnames),
        ctematerialized: cte.ctematerialized as i32 + 1,
        ctequery: convert_node_boxed(cte.ctequery),
        search_clause: convert_cte_search_clause_opt(cte.search_clause),
        cycle_clause: convert_cte_cycle_clause_opt(cte.cycle_clause),
        location: cte.location,
        cterecursive: cte.cterecursive,
        cterefcount: cte.cterefcount,
        ctecolnames: convert_list_to_nodes(cte.ctecolnames),
        ctecoltypes: convert_list_to_nodes(cte.ctecoltypes),
        ctecoltypmods: convert_list_to_nodes(cte.ctecoltypmods),
        ctecolcollations: convert_list_to_nodes(cte.ctecolcollations),
    }
}

unsafe fn convert_window_def(wd: &bindings_raw::WindowDef) -> protobuf::WindowDef {
    protobuf::WindowDef {
        name: convert_c_string(wd.name),
        refname: convert_c_string(wd.refname),
        partition_clause: convert_list_to_nodes(wd.partitionClause),
        order_clause: convert_list_to_nodes(wd.orderClause),
        frame_options: wd.frameOptions,
        start_offset: convert_node_boxed(wd.startOffset),
        end_offset: convert_node_boxed(wd.endOffset),
        location: wd.location,
    }
}

unsafe fn convert_into_clause(ic: *mut bindings_raw::IntoClause) -> Option<Box<protobuf::IntoClause>> {
    if ic.is_null() {
        return None;
    }
    let ic_ref = &*ic;
    Some(Box::new(protobuf::IntoClause {
        rel: if ic_ref.rel.is_null() { None } else { Some(convert_range_var(&*ic_ref.rel)) },
        col_names: convert_list_to_nodes(ic_ref.colNames),
        access_method: convert_c_string(ic_ref.accessMethod),
        options: convert_list_to_nodes(ic_ref.options),
        on_commit: ic_ref.onCommit as i32 + 1,
        table_space_name: convert_c_string(ic_ref.tableSpaceName),
        view_query: convert_node_boxed(ic_ref.viewQuery),
        skip_data: ic_ref.skipData,
    }))
}

unsafe fn convert_infer_clause(ic: *mut bindings_raw::InferClause) -> Option<Box<protobuf::InferClause>> {
    if ic.is_null() {
        return None;
    }
    let ic_ref = &*ic;
    Some(Box::new(protobuf::InferClause {
        index_elems: convert_list_to_nodes(ic_ref.indexElems),
        where_clause: convert_node_boxed(ic_ref.whereClause),
        conname: convert_c_string(ic_ref.conname),
        location: ic_ref.location,
    }))
}

unsafe fn convert_on_conflict_clause(oc: *mut bindings_raw::OnConflictClause) -> Option<Box<protobuf::OnConflictClause>> {
    if oc.is_null() {
        return None;
    }
    let oc_ref = &*oc;
    Some(Box::new(protobuf::OnConflictClause {
        action: oc_ref.action as i32 + 1,
        infer: convert_infer_clause(oc_ref.infer),
        target_list: convert_list_to_nodes(oc_ref.targetList),
        where_clause: convert_node_boxed(oc_ref.whereClause),
        location: oc_ref.location,
    }))
}

unsafe fn convert_column_def(cd: &bindings_raw::ColumnDef) -> protobuf::ColumnDef {
    protobuf::ColumnDef {
        colname: convert_c_string(cd.colname),
        type_name: if cd.typeName.is_null() { None } else { Some(convert_type_name(&*cd.typeName)) },
        compression: convert_c_string(cd.compression),
        inhcount: cd.inhcount,
        is_local: cd.is_local,
        is_not_null: cd.is_not_null,
        is_from_type: cd.is_from_type,
        storage: if cd.storage == 0 { String::new() } else { String::from_utf8_lossy(&[cd.storage as u8]).to_string() },
        storage_name: convert_c_string(cd.storage_name),
        raw_default: convert_node_boxed(cd.raw_default),
        cooked_default: convert_node_boxed(cd.cooked_default),
        identity: if cd.identity == 0 { String::new() } else { String::from_utf8_lossy(&[cd.identity as u8]).to_string() },
        identity_sequence: if cd.identitySequence.is_null() { None } else { Some(convert_range_var(&*cd.identitySequence)) },
        generated: if cd.generated == 0 { String::new() } else { String::from_utf8_lossy(&[cd.generated as u8]).to_string() },
        coll_clause: convert_collate_clause_opt(cd.collClause),
        coll_oid: cd.collOid,
        constraints: convert_list_to_nodes(cd.constraints),
        fdwoptions: convert_list_to_nodes(cd.fdwoptions),
        location: cd.location,
    }
}

unsafe fn convert_constraint(c: &bindings_raw::Constraint) -> protobuf::Constraint {
    protobuf::Constraint {
        contype: c.contype as i32 + 1,
        conname: convert_c_string(c.conname),
        deferrable: c.deferrable,
        initdeferred: c.initdeferred,
        location: c.location,
        is_no_inherit: c.is_no_inherit,
        raw_expr: convert_node_boxed(c.raw_expr),
        cooked_expr: convert_c_string(c.cooked_expr),
        generated_when: if c.generated_when == 0 { String::new() } else { String::from_utf8_lossy(&[c.generated_when as u8]).to_string() },
        inhcount: c.inhcount,
        nulls_not_distinct: c.nulls_not_distinct,
        keys: convert_list_to_nodes(c.keys),
        including: convert_list_to_nodes(c.including),
        exclusions: convert_list_to_nodes(c.exclusions),
        options: convert_list_to_nodes(c.options),
        indexname: convert_c_string(c.indexname),
        indexspace: convert_c_string(c.indexspace),
        reset_default_tblspc: c.reset_default_tblspc,
        access_method: convert_c_string(c.access_method),
        where_clause: convert_node_boxed(c.where_clause),
        pktable: if c.pktable.is_null() { None } else { Some(convert_range_var(&*c.pktable)) },
        fk_attrs: convert_list_to_nodes(c.fk_attrs),
        pk_attrs: convert_list_to_nodes(c.pk_attrs),
        fk_matchtype: if c.fk_matchtype == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_matchtype as u8]).to_string() },
        fk_upd_action: if c.fk_upd_action == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_upd_action as u8]).to_string() },
        fk_del_action: if c.fk_del_action == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_del_action as u8]).to_string() },
        fk_del_set_cols: convert_list_to_nodes(c.fk_del_set_cols),
        old_conpfeqop: convert_list_to_nodes(c.old_conpfeqop),
        old_pktable_oid: c.old_pktable_oid,
        skip_validation: c.skip_validation,
        initially_valid: c.initially_valid,
    }
}

unsafe fn convert_index_elem(ie: &bindings_raw::IndexElem) -> protobuf::IndexElem {
    protobuf::IndexElem {
        name: convert_c_string(ie.name),
        expr: convert_node_boxed(ie.expr),
        indexcolname: convert_c_string(ie.indexcolname),
        collation: convert_list_to_nodes(ie.collation),
        opclass: convert_list_to_nodes(ie.opclass),
        opclassopts: convert_list_to_nodes(ie.opclassopts),
        ordering: ie.ordering as i32 + 1,
        nulls_ordering: ie.nulls_ordering as i32 + 1,
    }
}

unsafe fn convert_def_elem(de: &bindings_raw::DefElem) -> protobuf::DefElem {
    protobuf::DefElem {
        defnamespace: convert_c_string(de.defnamespace),
        defname: convert_c_string(de.defname),
        arg: convert_node_boxed(de.arg),
        defaction: de.defaction as i32 + 1,
        location: de.location,
    }
}

unsafe fn convert_string(s: &bindings_raw::String) -> protobuf::String {
    protobuf::String { sval: convert_c_string(s.sval) }
}

unsafe fn convert_locking_clause(lc: &bindings_raw::LockingClause) -> protobuf::LockingClause {
    protobuf::LockingClause {
        locked_rels: convert_list_to_nodes(lc.lockedRels),
        strength: lc.strength as i32 + 1,
        wait_policy: lc.waitPolicy as i32 + 1,
    }
}

unsafe fn convert_min_max_expr(mme: &bindings_raw::MinMaxExpr) -> protobuf::MinMaxExpr {
    protobuf::MinMaxExpr {
        xpr: None, // Expression type info, not needed for parse tree
        minmaxtype: mme.minmaxtype,
        minmaxcollid: mme.minmaxcollid,
        inputcollid: mme.inputcollid,
        op: mme.op as i32 + 1,
        args: convert_list_to_nodes(mme.args),
        location: mme.location,
    }
}

unsafe fn convert_grouping_set(gs: &bindings_raw::GroupingSet) -> protobuf::GroupingSet {
    protobuf::GroupingSet { kind: gs.kind as i32 + 1, content: convert_list_to_nodes(gs.content), location: gs.location }
}

unsafe fn convert_range_subselect(rs: &bindings_raw::RangeSubselect) -> protobuf::RangeSubselect {
    protobuf::RangeSubselect {
        lateral: rs.lateral,
        subquery: convert_node_boxed(rs.subquery),
        alias: if rs.alias.is_null() { None } else { Some(convert_alias(&*rs.alias)) },
    }
}

unsafe fn convert_a_array_expr(ae: &bindings_raw::A_ArrayExpr) -> protobuf::AArrayExpr {
    protobuf::AArrayExpr { elements: convert_list_to_nodes(ae.elements), location: ae.location }
}

unsafe fn convert_a_indirection(ai: &bindings_raw::A_Indirection) -> protobuf::AIndirection {
    protobuf::AIndirection { arg: convert_node_boxed(ai.arg), indirection: convert_list_to_nodes(ai.indirection) }
}

unsafe fn convert_a_indices(ai: &bindings_raw::A_Indices) -> protobuf::AIndices {
    protobuf::AIndices { is_slice: ai.is_slice, lidx: convert_node_boxed(ai.lidx), uidx: convert_node_boxed(ai.uidx) }
}

unsafe fn convert_alter_table_stmt(ats: &bindings_raw::AlterTableStmt) -> protobuf::AlterTableStmt {
    protobuf::AlterTableStmt {
        relation: if ats.relation.is_null() { None } else { Some(convert_range_var(&*ats.relation)) },
        cmds: convert_list_to_nodes(ats.cmds),
        objtype: ats.objtype as i32 + 1,
        missing_ok: ats.missing_ok,
    }
}

unsafe fn convert_alter_table_cmd(atc: &bindings_raw::AlterTableCmd) -> protobuf::AlterTableCmd {
    protobuf::AlterTableCmd {
        subtype: atc.subtype as i32 + 1,
        name: convert_c_string(atc.name),
        num: atc.num as i32,
        newowner: if atc.newowner.is_null() { None } else { Some(convert_role_spec(&*atc.newowner)) },
        def: convert_node_boxed(atc.def),
        behavior: atc.behavior as i32 + 1,
        missing_ok: atc.missing_ok,
        recurse: atc.recurse,
    }
}

unsafe fn convert_role_spec(rs: &bindings_raw::RoleSpec) -> protobuf::RoleSpec {
    protobuf::RoleSpec { roletype: rs.roletype as i32 + 1, rolename: convert_c_string(rs.rolename), location: rs.location }
}

unsafe fn convert_copy_stmt(cs: &bindings_raw::CopyStmt) -> protobuf::CopyStmt {
    protobuf::CopyStmt {
        relation: if cs.relation.is_null() { None } else { Some(convert_range_var(&*cs.relation)) },
        query: convert_node_boxed(cs.query),
        attlist: convert_list_to_nodes(cs.attlist),
        is_from: cs.is_from,
        is_program: cs.is_program,
        filename: convert_c_string(cs.filename),
        options: convert_list_to_nodes(cs.options),
        where_clause: convert_node_boxed(cs.whereClause),
    }
}

unsafe fn convert_truncate_stmt(ts: &bindings_raw::TruncateStmt) -> protobuf::TruncateStmt {
    protobuf::TruncateStmt { relations: convert_list_to_nodes(ts.relations), restart_seqs: ts.restart_seqs, behavior: ts.behavior as i32 + 1 }
}

unsafe fn convert_view_stmt(vs: &bindings_raw::ViewStmt) -> protobuf::ViewStmt {
    protobuf::ViewStmt {
        view: if vs.view.is_null() { None } else { Some(convert_range_var(&*vs.view)) },
        aliases: convert_list_to_nodes(vs.aliases),
        query: convert_node_boxed(vs.query),
        replace: vs.replace,
        options: convert_list_to_nodes(vs.options),
        with_check_option: vs.withCheckOption as i32 + 1,
    }
}

unsafe fn convert_explain_stmt(es: &bindings_raw::ExplainStmt) -> protobuf::ExplainStmt {
    protobuf::ExplainStmt { query: convert_node_boxed(es.query), options: convert_list_to_nodes(es.options) }
}

unsafe fn convert_create_table_as_stmt(ctas: &bindings_raw::CreateTableAsStmt) -> protobuf::CreateTableAsStmt {
    protobuf::CreateTableAsStmt {
        query: convert_node_boxed(ctas.query),
        into: convert_into_clause(ctas.into),
        objtype: ctas.objtype as i32 + 1,
        is_select_into: ctas.is_select_into,
        if_not_exists: ctas.if_not_exists,
    }
}

unsafe fn convert_prepare_stmt(ps: &bindings_raw::PrepareStmt) -> protobuf::PrepareStmt {
    protobuf::PrepareStmt { name: convert_c_string(ps.name), argtypes: convert_list_to_nodes(ps.argtypes), query: convert_node_boxed(ps.query) }
}

unsafe fn convert_execute_stmt(es: &bindings_raw::ExecuteStmt) -> protobuf::ExecuteStmt {
    protobuf::ExecuteStmt { name: convert_c_string(es.name), params: convert_list_to_nodes(es.params) }
}

unsafe fn convert_deallocate_stmt(ds: &bindings_raw::DeallocateStmt) -> protobuf::DeallocateStmt {
    protobuf::DeallocateStmt { name: convert_c_string(ds.name), isall: ds.isall, location: ds.location }
}

unsafe fn convert_set_to_default(std: &bindings_raw::SetToDefault) -> protobuf::SetToDefault {
    protobuf::SetToDefault {
        xpr: None, // Expression type info, not needed for parse tree
        type_id: std.typeId,
        type_mod: std.typeMod,
        collation: std.collation,
        location: std.location,
    }
}

unsafe fn convert_multi_assign_ref(mar: &bindings_raw::MultiAssignRef) -> protobuf::MultiAssignRef {
    protobuf::MultiAssignRef { source: convert_node_boxed(mar.source), colno: mar.colno, ncolumns: mar.ncolumns }
}

unsafe fn convert_row_expr(re: &bindings_raw::RowExpr) -> protobuf::RowExpr {
    protobuf::RowExpr {
        xpr: None, // Expression type info, not needed for parse tree
        args: convert_list_to_nodes(re.args),
        row_typeid: re.row_typeid,
        row_format: re.row_format as i32 + 1,
        colnames: convert_list_to_nodes(re.colnames),
        location: re.location,
    }
}

unsafe fn convert_collate_clause(cc: &bindings_raw::CollateClause) -> protobuf::CollateClause {
    protobuf::CollateClause { arg: convert_node_boxed(cc.arg), collname: convert_list_to_nodes(cc.collname), location: cc.location }
}

unsafe fn convert_collate_clause_opt(cc: *mut bindings_raw::CollateClause) -> Option<Box<protobuf::CollateClause>> {
    if cc.is_null() {
        None
    } else {
        Some(Box::new(convert_collate_clause(&*cc)))
    }
}

unsafe fn convert_partition_spec(ps: &bindings_raw::PartitionSpec) -> protobuf::PartitionSpec {
    // Map from C char values to protobuf enum values
    // C: 'l'=108, 'r'=114, 'h'=104
    // Protobuf: LIST=1, RANGE=2, HASH=3
    let strategy = match ps.strategy as u8 as char {
        'l' => 1, // LIST
        'r' => 2, // RANGE
        'h' => 3, // HASH
        _ => 0,   // UNDEFINED
    };
    protobuf::PartitionSpec { strategy, part_params: convert_list_to_nodes(ps.partParams), location: ps.location }
}

unsafe fn convert_partition_spec_opt(ps: *mut bindings_raw::PartitionSpec) -> Option<protobuf::PartitionSpec> {
    if ps.is_null() {
        None
    } else {
        Some(convert_partition_spec(&*ps))
    }
}

unsafe fn convert_partition_bound_spec(pbs: &bindings_raw::PartitionBoundSpec) -> protobuf::PartitionBoundSpec {
    protobuf::PartitionBoundSpec {
        strategy: if pbs.strategy == 0 { String::new() } else { String::from_utf8_lossy(&[pbs.strategy as u8]).to_string() },
        is_default: pbs.is_default,
        modulus: pbs.modulus,
        remainder: pbs.remainder,
        listdatums: convert_list_to_nodes(pbs.listdatums),
        lowerdatums: convert_list_to_nodes(pbs.lowerdatums),
        upperdatums: convert_list_to_nodes(pbs.upperdatums),
        location: pbs.location,
    }
}

unsafe fn convert_partition_bound_spec_opt(pbs: *mut bindings_raw::PartitionBoundSpec) -> Option<protobuf::PartitionBoundSpec> {
    if pbs.is_null() {
        None
    } else {
        Some(convert_partition_bound_spec(&*pbs))
    }
}

unsafe fn convert_partition_elem(pe: &bindings_raw::PartitionElem) -> protobuf::PartitionElem {
    protobuf::PartitionElem {
        name: convert_c_string(pe.name),
        expr: convert_node_boxed(pe.expr),
        collation: convert_list_to_nodes(pe.collation),
        opclass: convert_list_to_nodes(pe.opclass),
        location: pe.location,
    }
}

unsafe fn convert_partition_range_datum(prd: &bindings_raw::PartitionRangeDatum) -> protobuf::PartitionRangeDatum {
    // Map from C enum to protobuf enum
    // C: PARTITION_RANGE_DATUM_MINVALUE=-1, PARTITION_RANGE_DATUM_VALUE=0, PARTITION_RANGE_DATUM_MAXVALUE=1
    // Protobuf: UNDEFINED=0, MINVALUE=1, VALUE=2, MAXVALUE=3
    let kind = match prd.kind {
        bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_MINVALUE => 1,
        bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_VALUE => 2,
        bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_MAXVALUE => 3,
        _ => 0,
    };
    protobuf::PartitionRangeDatum { kind, value: convert_node_boxed(prd.value), location: prd.location }
}

unsafe fn convert_cte_search_clause(csc: &bindings_raw::CTESearchClause) -> protobuf::CteSearchClause {
    protobuf::CteSearchClause {
        search_col_list: convert_list_to_nodes(csc.search_col_list),
        search_breadth_first: csc.search_breadth_first,
        search_seq_column: convert_c_string(csc.search_seq_column),
        location: csc.location,
    }
}

unsafe fn convert_cte_search_clause_opt(csc: *mut bindings_raw::CTESearchClause) -> Option<protobuf::CteSearchClause> {
    if csc.is_null() {
        None
    } else {
        Some(convert_cte_search_clause(&*csc))
    }
}

unsafe fn convert_cte_cycle_clause(ccc: &bindings_raw::CTECycleClause) -> protobuf::CteCycleClause {
    protobuf::CteCycleClause {
        cycle_col_list: convert_list_to_nodes(ccc.cycle_col_list),
        cycle_mark_column: convert_c_string(ccc.cycle_mark_column),
        cycle_mark_value: convert_node_boxed(ccc.cycle_mark_value),
        cycle_mark_default: convert_node_boxed(ccc.cycle_mark_default),
        cycle_path_column: convert_c_string(ccc.cycle_path_column),
        location: ccc.location,
        cycle_mark_type: ccc.cycle_mark_type,
        cycle_mark_typmod: ccc.cycle_mark_typmod,
        cycle_mark_collation: ccc.cycle_mark_collation,
        cycle_mark_neop: ccc.cycle_mark_neop,
    }
}

unsafe fn convert_cte_cycle_clause_opt(ccc: *mut bindings_raw::CTECycleClause) -> Option<Box<protobuf::CteCycleClause>> {
    if ccc.is_null() {
        None
    } else {
        Some(Box::new(convert_cte_cycle_clause(&*ccc)))
    }
}

// ============================================================================
// Additional Statement Conversions
// ============================================================================

unsafe fn convert_transaction_stmt(ts: &bindings_raw::TransactionStmt) -> protobuf::TransactionStmt {
    protobuf::TransactionStmt {
        kind: ts.kind as i32 + 1, // Protobuf enums have UNDEFINED=0
        options: convert_list_to_nodes(ts.options),
        savepoint_name: convert_c_string(ts.savepoint_name),
        gid: convert_c_string(ts.gid),
        chain: ts.chain,
        location: ts.location,
    }
}

unsafe fn convert_vacuum_stmt(vs: &bindings_raw::VacuumStmt) -> protobuf::VacuumStmt {
    protobuf::VacuumStmt { options: convert_list_to_nodes(vs.options), rels: convert_list_to_nodes(vs.rels), is_vacuumcmd: vs.is_vacuumcmd }
}

unsafe fn convert_vacuum_relation(vr: &bindings_raw::VacuumRelation) -> protobuf::VacuumRelation {
    protobuf::VacuumRelation {
        relation: if vr.relation.is_null() { None } else { Some(convert_range_var(&*vr.relation)) },
        oid: vr.oid,
        va_cols: convert_list_to_nodes(vr.va_cols),
    }
}

unsafe fn convert_variable_set_stmt(vss: &bindings_raw::VariableSetStmt) -> protobuf::VariableSetStmt {
    protobuf::VariableSetStmt {
        kind: vss.kind as i32 + 1, // Protobuf enums have UNDEFINED=0
        name: convert_c_string(vss.name),
        args: convert_list_to_nodes(vss.args),
        is_local: vss.is_local,
    }
}

unsafe fn convert_variable_show_stmt(vss: &bindings_raw::VariableShowStmt) -> protobuf::VariableShowStmt {
    protobuf::VariableShowStmt { name: convert_c_string(vss.name) }
}

unsafe fn convert_create_seq_stmt(css: &bindings_raw::CreateSeqStmt) -> protobuf::CreateSeqStmt {
    protobuf::CreateSeqStmt {
        sequence: if css.sequence.is_null() { None } else { Some(convert_range_var(&*css.sequence)) },
        options: convert_list_to_nodes(css.options),
        owner_id: css.ownerId,
        for_identity: css.for_identity,
        if_not_exists: css.if_not_exists,
    }
}

unsafe fn convert_do_stmt(ds: &bindings_raw::DoStmt) -> protobuf::DoStmt {
    protobuf::DoStmt { args: convert_list_to_nodes(ds.args) }
}

unsafe fn convert_lock_stmt(ls: &bindings_raw::LockStmt) -> protobuf::LockStmt {
    protobuf::LockStmt { relations: convert_list_to_nodes(ls.relations), mode: ls.mode, nowait: ls.nowait }
}

unsafe fn convert_create_schema_stmt(css: &bindings_raw::CreateSchemaStmt) -> protobuf::CreateSchemaStmt {
    protobuf::CreateSchemaStmt {
        schemaname: convert_c_string(css.schemaname),
        authrole: if css.authrole.is_null() { None } else { Some(convert_role_spec(&*css.authrole)) },
        schema_elts: convert_list_to_nodes(css.schemaElts),
        if_not_exists: css.if_not_exists,
    }
}

unsafe fn convert_rename_stmt(rs: &bindings_raw::RenameStmt) -> protobuf::RenameStmt {
    protobuf::RenameStmt {
        rename_type: rs.renameType as i32 + 1, // Protobuf ObjectType has UNDEFINED=0
        relation_type: rs.relationType as i32 + 1,
        relation: if rs.relation.is_null() { None } else { Some(convert_range_var(&*rs.relation)) },
        object: convert_node_boxed(rs.object),
        subname: convert_c_string(rs.subname),
        newname: convert_c_string(rs.newname),
        behavior: rs.behavior as i32 + 1,
        missing_ok: rs.missing_ok,
    }
}

unsafe fn convert_create_function_stmt(cfs: &bindings_raw::CreateFunctionStmt) -> protobuf::CreateFunctionStmt {
    protobuf::CreateFunctionStmt {
        is_procedure: cfs.is_procedure,
        replace: cfs.replace,
        funcname: convert_list_to_nodes(cfs.funcname),
        parameters: convert_list_to_nodes(cfs.parameters),
        return_type: if cfs.returnType.is_null() { None } else { Some(convert_type_name(&*cfs.returnType)) },
        options: convert_list_to_nodes(cfs.options),
        sql_body: convert_node_boxed(cfs.sql_body),
    }
}

unsafe fn convert_alter_owner_stmt(aos: &bindings_raw::AlterOwnerStmt) -> protobuf::AlterOwnerStmt {
    protobuf::AlterOwnerStmt {
        object_type: aos.objectType as i32 + 1, // Protobuf ObjectType has UNDEFINED=0
        relation: if aos.relation.is_null() { None } else { Some(convert_range_var(&*aos.relation)) },
        object: convert_node_boxed(aos.object),
        newowner: if aos.newowner.is_null() { None } else { Some(convert_role_spec(&*aos.newowner)) },
    }
}

unsafe fn convert_alter_seq_stmt(ass: &bindings_raw::AlterSeqStmt) -> protobuf::AlterSeqStmt {
    protobuf::AlterSeqStmt {
        sequence: if ass.sequence.is_null() { None } else { Some(convert_range_var(&*ass.sequence)) },
        options: convert_list_to_nodes(ass.options),
        for_identity: ass.for_identity,
        missing_ok: ass.missing_ok,
    }
}

unsafe fn convert_create_enum_stmt(ces: &bindings_raw::CreateEnumStmt) -> protobuf::CreateEnumStmt {
    protobuf::CreateEnumStmt { type_name: convert_list_to_nodes(ces.typeName), vals: convert_list_to_nodes(ces.vals) }
}

unsafe fn convert_object_with_args(owa: &bindings_raw::ObjectWithArgs) -> protobuf::ObjectWithArgs {
    protobuf::ObjectWithArgs {
        objname: convert_list_to_nodes(owa.objname),
        objargs: convert_list_to_nodes(owa.objargs),
        objfuncargs: convert_list_to_nodes(owa.objfuncargs),
        args_unspecified: owa.args_unspecified,
    }
}

unsafe fn convert_function_parameter(fp: &bindings_raw::FunctionParameter) -> protobuf::FunctionParameter {
    protobuf::FunctionParameter {
        name: convert_c_string(fp.name),
        arg_type: if fp.argType.is_null() { None } else { Some(convert_type_name(&*fp.argType)) },
        mode: convert_function_parameter_mode(fp.mode),
        defexpr: convert_node_boxed(fp.defexpr),
    }
}

/// Converts raw FunctionParameterMode (ASCII char codes) to protobuf enum values
fn convert_function_parameter_mode(mode: bindings_raw::FunctionParameterMode) -> i32 {
    match mode {
        bindings_raw::FunctionParameterMode_FUNC_PARAM_IN => protobuf::FunctionParameterMode::FuncParamIn as i32,
        bindings_raw::FunctionParameterMode_FUNC_PARAM_OUT => protobuf::FunctionParameterMode::FuncParamOut as i32,
        bindings_raw::FunctionParameterMode_FUNC_PARAM_INOUT => protobuf::FunctionParameterMode::FuncParamInout as i32,
        bindings_raw::FunctionParameterMode_FUNC_PARAM_VARIADIC => protobuf::FunctionParameterMode::FuncParamVariadic as i32,
        bindings_raw::FunctionParameterMode_FUNC_PARAM_TABLE => protobuf::FunctionParameterMode::FuncParamTable as i32,
        bindings_raw::FunctionParameterMode_FUNC_PARAM_DEFAULT => protobuf::FunctionParameterMode::FuncParamDefault as i32,
        _ => 0, // Undefined
    }
}

unsafe fn convert_notify_stmt(ns: &bindings_raw::NotifyStmt) -> protobuf::NotifyStmt {
    protobuf::NotifyStmt { conditionname: convert_c_string(ns.conditionname), payload: convert_c_string(ns.payload) }
}

unsafe fn convert_listen_stmt(ls: &bindings_raw::ListenStmt) -> protobuf::ListenStmt {
    protobuf::ListenStmt { conditionname: convert_c_string(ls.conditionname) }
}

unsafe fn convert_unlisten_stmt(us: &bindings_raw::UnlistenStmt) -> protobuf::UnlistenStmt {
    protobuf::UnlistenStmt { conditionname: convert_c_string(us.conditionname) }
}

unsafe fn convert_discard_stmt(ds: &bindings_raw::DiscardStmt) -> protobuf::DiscardStmt {
    protobuf::DiscardStmt {
        target: ds.target as i32 + 1, // DiscardMode enum
    }
}

unsafe fn convert_coerce_to_domain(ctd: &bindings_raw::CoerceToDomain) -> protobuf::CoerceToDomain {
    protobuf::CoerceToDomain {
        xpr: None,
        arg: convert_node_boxed(ctd.arg as *mut bindings_raw::Node),
        resulttype: ctd.resulttype,
        resulttypmod: ctd.resulttypmod,
        resultcollid: ctd.resultcollid,
        coercionformat: ctd.coercionformat as i32 + 1,
        location: ctd.location,
    }
}

unsafe fn convert_composite_type_stmt(cts: &bindings_raw::CompositeTypeStmt) -> protobuf::CompositeTypeStmt {
    protobuf::CompositeTypeStmt {
        typevar: if cts.typevar.is_null() { None } else { Some(convert_range_var(&*cts.typevar)) },
        coldeflist: convert_list_to_nodes(cts.coldeflist),
    }
}

unsafe fn convert_create_domain_stmt(cds: &bindings_raw::CreateDomainStmt) -> protobuf::CreateDomainStmt {
    protobuf::CreateDomainStmt {
        domainname: convert_list_to_nodes(cds.domainname),
        type_name: if cds.typeName.is_null() { None } else { Some(convert_type_name(&*cds.typeName)) },
        coll_clause: convert_collate_clause_opt(cds.collClause),
        constraints: convert_list_to_nodes(cds.constraints),
    }
}

unsafe fn convert_create_extension_stmt(ces: &bindings_raw::CreateExtensionStmt) -> protobuf::CreateExtensionStmt {
    protobuf::CreateExtensionStmt {
        extname: convert_c_string(ces.extname),
        if_not_exists: ces.if_not_exists,
        options: convert_list_to_nodes(ces.options),
    }
}

unsafe fn convert_create_publication_stmt(cps: &bindings_raw::CreatePublicationStmt) -> protobuf::CreatePublicationStmt {
    protobuf::CreatePublicationStmt {
        pubname: convert_c_string(cps.pubname),
        options: convert_list_to_nodes(cps.options),
        pubobjects: convert_list_to_nodes(cps.pubobjects),
        for_all_tables: cps.for_all_tables,
    }
}

unsafe fn convert_alter_publication_stmt(aps: &bindings_raw::AlterPublicationStmt) -> protobuf::AlterPublicationStmt {
    protobuf::AlterPublicationStmt {
        pubname: convert_c_string(aps.pubname),
        options: convert_list_to_nodes(aps.options),
        pubobjects: convert_list_to_nodes(aps.pubobjects),
        for_all_tables: aps.for_all_tables,
        action: aps.action as i32 + 1,
    }
}

unsafe fn convert_create_subscription_stmt(css: &bindings_raw::CreateSubscriptionStmt) -> protobuf::CreateSubscriptionStmt {
    protobuf::CreateSubscriptionStmt {
        subname: convert_c_string(css.subname),
        conninfo: convert_c_string(css.conninfo),
        publication: convert_list_to_nodes(css.publication),
        options: convert_list_to_nodes(css.options),
    }
}

unsafe fn convert_alter_subscription_stmt(ass: &bindings_raw::AlterSubscriptionStmt) -> protobuf::AlterSubscriptionStmt {
    protobuf::AlterSubscriptionStmt {
        kind: ass.kind as i32 + 1,
        subname: convert_c_string(ass.subname),
        conninfo: convert_c_string(ass.conninfo),
        publication: convert_list_to_nodes(ass.publication),
        options: convert_list_to_nodes(ass.options),
    }
}

unsafe fn convert_publication_obj_spec(pos: &bindings_raw::PublicationObjSpec) -> protobuf::PublicationObjSpec {
    let pubtable = if pos.pubtable.is_null() { None } else { Some(Box::new(convert_publication_table(&*pos.pubtable))) };
    protobuf::PublicationObjSpec { pubobjtype: pos.pubobjtype as i32 + 1, name: convert_c_string(pos.name), pubtable, location: pos.location }
}

unsafe fn convert_publication_table(pt: &bindings_raw::PublicationTable) -> protobuf::PublicationTable {
    let relation = if pt.relation.is_null() { None } else { Some(convert_range_var(&*pt.relation)) };
    protobuf::PublicationTable {
        relation,
        where_clause: convert_node_boxed(pt.whereClause as *mut bindings_raw::Node),
        columns: convert_list_to_nodes(pt.columns),
    }
}

unsafe fn convert_create_trig_stmt(cts: &bindings_raw::CreateTrigStmt) -> protobuf::CreateTrigStmt {
    protobuf::CreateTrigStmt {
        replace: cts.replace,
        isconstraint: cts.isconstraint,
        trigname: convert_c_string(cts.trigname),
        relation: if cts.relation.is_null() { None } else { Some(convert_range_var(&*cts.relation)) },
        funcname: convert_list_to_nodes(cts.funcname),
        args: convert_list_to_nodes(cts.args),
        row: cts.row,
        timing: cts.timing as i32,
        events: cts.events as i32,
        columns: convert_list_to_nodes(cts.columns),
        when_clause: convert_node_boxed(cts.whenClause),
        transition_rels: convert_list_to_nodes(cts.transitionRels),
        deferrable: cts.deferrable,
        initdeferred: cts.initdeferred,
        constrrel: if cts.constrrel.is_null() { None } else { Some(convert_range_var(&*cts.constrrel)) },
    }
}

unsafe fn convert_call_stmt(cs: &bindings_raw::CallStmt) -> protobuf::CallStmt {
    protobuf::CallStmt {
        funccall: if cs.funccall.is_null() { None } else { Some(Box::new(convert_func_call(&*cs.funccall))) },
        funcexpr: None, // This is a post-analysis field, not available in raw parse tree
        outargs: convert_list_to_nodes(cs.outargs),
    }
}

unsafe fn convert_rule_stmt(rs: &bindings_raw::RuleStmt) -> protobuf::RuleStmt {
    protobuf::RuleStmt {
        relation: if rs.relation.is_null() { None } else { Some(convert_range_var(&*rs.relation)) },
        rulename: convert_c_string(rs.rulename),
        where_clause: convert_node_boxed(rs.whereClause),
        event: rs.event as i32 + 1, // CmdType enum
        instead: rs.instead,
        actions: convert_list_to_nodes(rs.actions),
        replace: rs.replace,
    }
}

unsafe fn convert_grant_stmt(gs: &bindings_raw::GrantStmt) -> protobuf::GrantStmt {
    protobuf::GrantStmt {
        is_grant: gs.is_grant,
        targtype: gs.targtype as i32 + 1,
        objtype: gs.objtype as i32 + 1,
        objects: convert_list_to_nodes(gs.objects),
        privileges: convert_list_to_nodes(gs.privileges),
        grantees: convert_list_to_nodes(gs.grantees),
        grant_option: gs.grant_option,
        grantor: if gs.grantor.is_null() { None } else { Some(convert_role_spec(&*gs.grantor)) },
        behavior: gs.behavior as i32 + 1,
    }
}

unsafe fn convert_grant_role_stmt(grs: &bindings_raw::GrantRoleStmt) -> protobuf::GrantRoleStmt {
    protobuf::GrantRoleStmt {
        granted_roles: convert_list_to_nodes(grs.granted_roles),
        grantee_roles: convert_list_to_nodes(grs.grantee_roles),
        is_grant: grs.is_grant,
        opt: convert_list_to_nodes(grs.opt),
        grantor: if grs.grantor.is_null() { None } else { Some(convert_role_spec(&*grs.grantor)) },
        behavior: grs.behavior as i32 + 1,
    }
}

unsafe fn convert_refresh_mat_view_stmt(rmvs: &bindings_raw::RefreshMatViewStmt) -> protobuf::RefreshMatViewStmt {
    protobuf::RefreshMatViewStmt {
        concurrent: rmvs.concurrent,
        skip_data: rmvs.skipData,
        relation: if rmvs.relation.is_null() { None } else { Some(convert_range_var(&*rmvs.relation)) },
    }
}

unsafe fn convert_merge_stmt(ms: &bindings_raw::MergeStmt) -> protobuf::MergeStmt {
    protobuf::MergeStmt {
        relation: if ms.relation.is_null() { None } else { Some(convert_range_var(&*ms.relation)) },
        source_relation: convert_node_boxed(ms.sourceRelation),
        join_condition: convert_node_boxed(ms.joinCondition),
        merge_when_clauses: convert_list_to_nodes(ms.mergeWhenClauses),
        returning_list: convert_list_to_nodes(ms.returningList),
        with_clause: convert_with_clause_opt(ms.withClause),
    }
}

unsafe fn convert_merge_action(ma: &bindings_raw::MergeAction) -> protobuf::MergeAction {
    protobuf::MergeAction {
        match_kind: ma.matchKind as i32 + 1,
        command_type: ma.commandType as i32 + 1,
        r#override: ma.override_ as i32 + 1,
        qual: convert_node_boxed(ma.qual),
        target_list: convert_list_to_nodes(ma.targetList),
        update_colnos: convert_list_to_nodes(ma.updateColnos),
    }
}

unsafe fn convert_merge_when_clause(mwc: &bindings_raw::MergeWhenClause) -> protobuf::MergeWhenClause {
    protobuf::MergeWhenClause {
        match_kind: mwc.matchKind as i32 + 1,
        command_type: mwc.commandType as i32 + 1,
        r#override: mwc.override_ as i32 + 1,
        condition: convert_node_boxed(mwc.condition),
        target_list: convert_list_to_nodes(mwc.targetList),
        values: convert_list_to_nodes(mwc.values),
    }
}

unsafe fn convert_range_function(rf: &bindings_raw::RangeFunction) -> protobuf::RangeFunction {
    protobuf::RangeFunction {
        lateral: rf.lateral,
        ordinality: rf.ordinality,
        is_rowsfrom: rf.is_rowsfrom,
        functions: convert_list_to_nodes(rf.functions),
        alias: if rf.alias.is_null() { None } else { Some(convert_alias(&*rf.alias)) },
        coldeflist: convert_list_to_nodes(rf.coldeflist),
    }
}

unsafe fn convert_access_priv(ap: &bindings_raw::AccessPriv) -> protobuf::AccessPriv {
    protobuf::AccessPriv { priv_name: convert_c_string(ap.priv_name), cols: convert_list_to_nodes(ap.cols) }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Converts a C string pointer to a Rust String.
unsafe fn convert_c_string(ptr: *const i8) -> std::string::String {
    if ptr.is_null() {
        std::string::String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().to_string()
    }
}

// ============================================================================
// New Node Conversions (matching raw_deparse.rs)
// ============================================================================

unsafe fn convert_bit_string(bs: &bindings_raw::BitString) -> protobuf::BitString {
    protobuf::BitString { bsval: convert_c_string(bs.bsval) }
}

unsafe fn convert_boolean_test(bt: &bindings_raw::BooleanTest) -> protobuf::BooleanTest {
    protobuf::BooleanTest {
        xpr: None,
        arg: convert_node_boxed(bt.arg as *mut bindings_raw::Node),
        booltesttype: bt.booltesttype as i32 + 1,
        location: bt.location,
    }
}

unsafe fn convert_create_range_stmt(crs: &bindings_raw::CreateRangeStmt) -> protobuf::CreateRangeStmt {
    protobuf::CreateRangeStmt { type_name: convert_list_to_nodes(crs.typeName), params: convert_list_to_nodes(crs.params) }
}

unsafe fn convert_alter_enum_stmt(aes: &bindings_raw::AlterEnumStmt) -> protobuf::AlterEnumStmt {
    protobuf::AlterEnumStmt {
        type_name: convert_list_to_nodes(aes.typeName),
        old_val: convert_c_string(aes.oldVal),
        new_val: convert_c_string(aes.newVal),
        new_val_neighbor: convert_c_string(aes.newValNeighbor),
        new_val_is_after: aes.newValIsAfter,
        skip_if_new_val_exists: aes.skipIfNewValExists,
    }
}

unsafe fn convert_close_portal_stmt(cps: &bindings_raw::ClosePortalStmt) -> protobuf::ClosePortalStmt {
    protobuf::ClosePortalStmt { portalname: convert_c_string(cps.portalname) }
}

unsafe fn convert_fetch_stmt(fs: &bindings_raw::FetchStmt) -> protobuf::FetchStmt {
    protobuf::FetchStmt {
        direction: fs.direction as i32 + 1,
        how_many: fs.howMany as i64,
        portalname: convert_c_string(fs.portalname),
        ismove: fs.ismove,
    }
}

unsafe fn convert_declare_cursor_stmt(dcs: &bindings_raw::DeclareCursorStmt) -> protobuf::DeclareCursorStmt {
    protobuf::DeclareCursorStmt { portalname: convert_c_string(dcs.portalname), options: dcs.options, query: convert_node_boxed(dcs.query) }
}

unsafe fn convert_define_stmt(ds: &bindings_raw::DefineStmt) -> protobuf::DefineStmt {
    protobuf::DefineStmt {
        kind: ds.kind as i32 + 1,
        oldstyle: ds.oldstyle,
        defnames: convert_list_to_nodes(ds.defnames),
        args: convert_list_to_nodes(ds.args),
        definition: convert_list_to_nodes(ds.definition),
        if_not_exists: ds.if_not_exists,
        replace: ds.replace,
    }
}

unsafe fn convert_comment_stmt(cs: &bindings_raw::CommentStmt) -> protobuf::CommentStmt {
    protobuf::CommentStmt { objtype: cs.objtype as i32 + 1, object: convert_node_boxed(cs.object), comment: convert_c_string(cs.comment) }
}

unsafe fn convert_sec_label_stmt(sls: &bindings_raw::SecLabelStmt) -> protobuf::SecLabelStmt {
    protobuf::SecLabelStmt {
        objtype: sls.objtype as i32 + 1,
        object: convert_node_boxed(sls.object),
        provider: convert_c_string(sls.provider),
        label: convert_c_string(sls.label),
    }
}

unsafe fn convert_create_role_stmt(crs: &bindings_raw::CreateRoleStmt) -> protobuf::CreateRoleStmt {
    protobuf::CreateRoleStmt { stmt_type: crs.stmt_type as i32 + 1, role: convert_c_string(crs.role), options: convert_list_to_nodes(crs.options) }
}

unsafe fn convert_alter_role_stmt(ars: &bindings_raw::AlterRoleStmt) -> protobuf::AlterRoleStmt {
    protobuf::AlterRoleStmt {
        role: if ars.role.is_null() { None } else { Some(convert_role_spec(&*ars.role)) },
        options: convert_list_to_nodes(ars.options),
        action: ars.action,
    }
}

unsafe fn convert_alter_role_set_stmt(arss: &bindings_raw::AlterRoleSetStmt) -> protobuf::AlterRoleSetStmt {
    protobuf::AlterRoleSetStmt {
        role: if arss.role.is_null() { None } else { Some(convert_role_spec(&*arss.role)) },
        database: convert_c_string(arss.database),
        setstmt: convert_variable_set_stmt_opt(arss.setstmt),
    }
}

unsafe fn convert_drop_role_stmt(drs: &bindings_raw::DropRoleStmt) -> protobuf::DropRoleStmt {
    protobuf::DropRoleStmt { roles: convert_list_to_nodes(drs.roles), missing_ok: drs.missing_ok }
}

unsafe fn convert_create_policy_stmt(cps: &bindings_raw::CreatePolicyStmt) -> protobuf::CreatePolicyStmt {
    protobuf::CreatePolicyStmt {
        policy_name: convert_c_string(cps.policy_name),
        table: if cps.table.is_null() { None } else { Some(convert_range_var(&*cps.table)) },
        cmd_name: convert_c_string(cps.cmd_name),
        permissive: cps.permissive,
        roles: convert_list_to_nodes(cps.roles),
        qual: convert_node_boxed(cps.qual),
        with_check: convert_node_boxed(cps.with_check),
    }
}

unsafe fn convert_alter_policy_stmt(aps: &bindings_raw::AlterPolicyStmt) -> protobuf::AlterPolicyStmt {
    protobuf::AlterPolicyStmt {
        policy_name: convert_c_string(aps.policy_name),
        table: if aps.table.is_null() { None } else { Some(convert_range_var(&*aps.table)) },
        roles: convert_list_to_nodes(aps.roles),
        qual: convert_node_boxed(aps.qual),
        with_check: convert_node_boxed(aps.with_check),
    }
}

unsafe fn convert_create_event_trig_stmt(cets: &bindings_raw::CreateEventTrigStmt) -> protobuf::CreateEventTrigStmt {
    protobuf::CreateEventTrigStmt {
        trigname: convert_c_string(cets.trigname),
        eventname: convert_c_string(cets.eventname),
        whenclause: convert_list_to_nodes(cets.whenclause),
        funcname: convert_list_to_nodes(cets.funcname),
    }
}

unsafe fn convert_alter_event_trig_stmt(aets: &bindings_raw::AlterEventTrigStmt) -> protobuf::AlterEventTrigStmt {
    protobuf::AlterEventTrigStmt {
        trigname: convert_c_string(aets.trigname),
        tgenabled: String::from_utf8_lossy(&[aets.tgenabled as u8]).to_string(),
    }
}

unsafe fn convert_create_plang_stmt(cpls: &bindings_raw::CreatePLangStmt) -> protobuf::CreatePLangStmt {
    protobuf::CreatePLangStmt {
        replace: cpls.replace,
        plname: convert_c_string(cpls.plname),
        plhandler: convert_list_to_nodes(cpls.plhandler),
        plinline: convert_list_to_nodes(cpls.plinline),
        plvalidator: convert_list_to_nodes(cpls.plvalidator),
        pltrusted: cpls.pltrusted,
    }
}

unsafe fn convert_create_am_stmt(cas: &bindings_raw::CreateAmStmt) -> protobuf::CreateAmStmt {
    protobuf::CreateAmStmt {
        amname: convert_c_string(cas.amname),
        handler_name: convert_list_to_nodes(cas.handler_name),
        amtype: String::from_utf8_lossy(&[cas.amtype as u8]).to_string(),
    }
}

unsafe fn convert_create_op_class_stmt(cocs: &bindings_raw::CreateOpClassStmt) -> protobuf::CreateOpClassStmt {
    protobuf::CreateOpClassStmt {
        opclassname: convert_list_to_nodes(cocs.opclassname),
        opfamilyname: convert_list_to_nodes(cocs.opfamilyname),
        amname: convert_c_string(cocs.amname),
        datatype: if cocs.datatype.is_null() { None } else { Some(convert_type_name(&*cocs.datatype)) },
        items: convert_list_to_nodes(cocs.items),
        is_default: cocs.isDefault,
    }
}

unsafe fn convert_create_op_class_item(coci: &bindings_raw::CreateOpClassItem) -> protobuf::CreateOpClassItem {
    protobuf::CreateOpClassItem {
        itemtype: coci.itemtype,
        name: if coci.name.is_null() { None } else { Some(convert_object_with_args(&*coci.name)) },
        number: coci.number,
        order_family: convert_list_to_nodes(coci.order_family),
        class_args: convert_list_to_nodes(coci.class_args),
        storedtype: if coci.storedtype.is_null() { None } else { Some(convert_type_name(&*coci.storedtype)) },
    }
}

unsafe fn convert_create_op_family_stmt(cofs: &bindings_raw::CreateOpFamilyStmt) -> protobuf::CreateOpFamilyStmt {
    protobuf::CreateOpFamilyStmt { opfamilyname: convert_list_to_nodes(cofs.opfamilyname), amname: convert_c_string(cofs.amname) }
}

unsafe fn convert_alter_op_family_stmt(aofs: &bindings_raw::AlterOpFamilyStmt) -> protobuf::AlterOpFamilyStmt {
    protobuf::AlterOpFamilyStmt {
        opfamilyname: convert_list_to_nodes(aofs.opfamilyname),
        amname: convert_c_string(aofs.amname),
        is_drop: aofs.isDrop,
        items: convert_list_to_nodes(aofs.items),
    }
}

unsafe fn convert_create_fdw_stmt(cfds: &bindings_raw::CreateFdwStmt) -> protobuf::CreateFdwStmt {
    protobuf::CreateFdwStmt {
        fdwname: convert_c_string(cfds.fdwname),
        func_options: convert_list_to_nodes(cfds.func_options),
        options: convert_list_to_nodes(cfds.options),
    }
}

unsafe fn convert_alter_fdw_stmt(afds: &bindings_raw::AlterFdwStmt) -> protobuf::AlterFdwStmt {
    protobuf::AlterFdwStmt {
        fdwname: convert_c_string(afds.fdwname),
        func_options: convert_list_to_nodes(afds.func_options),
        options: convert_list_to_nodes(afds.options),
    }
}

unsafe fn convert_create_foreign_server_stmt(cfss: &bindings_raw::CreateForeignServerStmt) -> protobuf::CreateForeignServerStmt {
    protobuf::CreateForeignServerStmt {
        servername: convert_c_string(cfss.servername),
        servertype: convert_c_string(cfss.servertype),
        version: convert_c_string(cfss.version),
        fdwname: convert_c_string(cfss.fdwname),
        if_not_exists: cfss.if_not_exists,
        options: convert_list_to_nodes(cfss.options),
    }
}

unsafe fn convert_alter_foreign_server_stmt(afss: &bindings_raw::AlterForeignServerStmt) -> protobuf::AlterForeignServerStmt {
    protobuf::AlterForeignServerStmt {
        servername: convert_c_string(afss.servername),
        version: convert_c_string(afss.version),
        options: convert_list_to_nodes(afss.options),
        has_version: afss.has_version,
    }
}

unsafe fn convert_create_foreign_table_stmt(cfts: &bindings_raw::CreateForeignTableStmt) -> protobuf::CreateForeignTableStmt {
    protobuf::CreateForeignTableStmt {
        base_stmt: Some(convert_create_stmt(&cfts.base)),
        servername: convert_c_string(cfts.servername),
        options: convert_list_to_nodes(cfts.options),
    }
}

unsafe fn convert_create_user_mapping_stmt(cums: &bindings_raw::CreateUserMappingStmt) -> protobuf::CreateUserMappingStmt {
    protobuf::CreateUserMappingStmt {
        user: if cums.user.is_null() { None } else { Some(convert_role_spec(&*cums.user)) },
        servername: convert_c_string(cums.servername),
        if_not_exists: cums.if_not_exists,
        options: convert_list_to_nodes(cums.options),
    }
}

unsafe fn convert_alter_user_mapping_stmt(aums: &bindings_raw::AlterUserMappingStmt) -> protobuf::AlterUserMappingStmt {
    protobuf::AlterUserMappingStmt {
        user: if aums.user.is_null() { None } else { Some(convert_role_spec(&*aums.user)) },
        servername: convert_c_string(aums.servername),
        options: convert_list_to_nodes(aums.options),
    }
}

unsafe fn convert_drop_user_mapping_stmt(dums: &bindings_raw::DropUserMappingStmt) -> protobuf::DropUserMappingStmt {
    protobuf::DropUserMappingStmt {
        user: if dums.user.is_null() { None } else { Some(convert_role_spec(&*dums.user)) },
        servername: convert_c_string(dums.servername),
        missing_ok: dums.missing_ok,
    }
}

unsafe fn convert_import_foreign_schema_stmt(ifss: &bindings_raw::ImportForeignSchemaStmt) -> protobuf::ImportForeignSchemaStmt {
    protobuf::ImportForeignSchemaStmt {
        server_name: convert_c_string(ifss.server_name),
        remote_schema: convert_c_string(ifss.remote_schema),
        local_schema: convert_c_string(ifss.local_schema),
        list_type: ifss.list_type as i32 + 1,
        table_list: convert_list_to_nodes(ifss.table_list),
        options: convert_list_to_nodes(ifss.options),
    }
}

unsafe fn convert_create_table_space_stmt(ctss: &bindings_raw::CreateTableSpaceStmt) -> protobuf::CreateTableSpaceStmt {
    protobuf::CreateTableSpaceStmt {
        tablespacename: convert_c_string(ctss.tablespacename),
        owner: if ctss.owner.is_null() { None } else { Some(convert_role_spec(&*ctss.owner)) },
        location: convert_c_string(ctss.location),
        options: convert_list_to_nodes(ctss.options),
    }
}

unsafe fn convert_drop_table_space_stmt(dtss: &bindings_raw::DropTableSpaceStmt) -> protobuf::DropTableSpaceStmt {
    protobuf::DropTableSpaceStmt { tablespacename: convert_c_string(dtss.tablespacename), missing_ok: dtss.missing_ok }
}

unsafe fn convert_alter_table_space_options_stmt(atsos: &bindings_raw::AlterTableSpaceOptionsStmt) -> protobuf::AlterTableSpaceOptionsStmt {
    protobuf::AlterTableSpaceOptionsStmt {
        tablespacename: convert_c_string(atsos.tablespacename),
        options: convert_list_to_nodes(atsos.options),
        is_reset: atsos.isReset,
    }
}

unsafe fn convert_alter_table_move_all_stmt(atmas: &bindings_raw::AlterTableMoveAllStmt) -> protobuf::AlterTableMoveAllStmt {
    protobuf::AlterTableMoveAllStmt {
        orig_tablespacename: convert_c_string(atmas.orig_tablespacename),
        objtype: atmas.objtype as i32 + 1,
        roles: convert_list_to_nodes(atmas.roles),
        new_tablespacename: convert_c_string(atmas.new_tablespacename),
        nowait: atmas.nowait,
    }
}

unsafe fn convert_alter_extension_stmt(aes: &bindings_raw::AlterExtensionStmt) -> protobuf::AlterExtensionStmt {
    protobuf::AlterExtensionStmt { extname: convert_c_string(aes.extname), options: convert_list_to_nodes(aes.options) }
}

unsafe fn convert_alter_extension_contents_stmt(aecs: &bindings_raw::AlterExtensionContentsStmt) -> protobuf::AlterExtensionContentsStmt {
    protobuf::AlterExtensionContentsStmt {
        extname: convert_c_string(aecs.extname),
        action: aecs.action,
        objtype: aecs.objtype as i32 + 1,
        object: convert_node_boxed(aecs.object),
    }
}

unsafe fn convert_alter_domain_stmt(ads: &bindings_raw::AlterDomainStmt) -> protobuf::AlterDomainStmt {
    protobuf::AlterDomainStmt {
        subtype: String::from_utf8_lossy(&[ads.subtype as u8]).to_string(),
        type_name: convert_list_to_nodes(ads.typeName),
        name: convert_c_string(ads.name),
        def: convert_node_boxed(ads.def),
        behavior: ads.behavior as i32 + 1,
        missing_ok: ads.missing_ok,
    }
}

unsafe fn convert_alter_function_stmt(afs: &bindings_raw::AlterFunctionStmt) -> protobuf::AlterFunctionStmt {
    protobuf::AlterFunctionStmt {
        objtype: afs.objtype as i32 + 1,
        func: if afs.func.is_null() { None } else { Some(convert_object_with_args(&*afs.func)) },
        actions: convert_list_to_nodes(afs.actions),
    }
}

unsafe fn convert_alter_operator_stmt(aos: &bindings_raw::AlterOperatorStmt) -> protobuf::AlterOperatorStmt {
    protobuf::AlterOperatorStmt {
        opername: if aos.opername.is_null() { None } else { Some(convert_object_with_args(&*aos.opername)) },
        options: convert_list_to_nodes(aos.options),
    }
}

unsafe fn convert_alter_type_stmt(ats: &bindings_raw::AlterTypeStmt) -> protobuf::AlterTypeStmt {
    protobuf::AlterTypeStmt { type_name: convert_list_to_nodes(ats.typeName), options: convert_list_to_nodes(ats.options) }
}

unsafe fn convert_alter_object_schema_stmt(aoss: &bindings_raw::AlterObjectSchemaStmt) -> protobuf::AlterObjectSchemaStmt {
    protobuf::AlterObjectSchemaStmt {
        object_type: aoss.objectType as i32 + 1,
        relation: if aoss.relation.is_null() { None } else { Some(convert_range_var(&*aoss.relation)) },
        object: convert_node_boxed(aoss.object),
        newschema: convert_c_string(aoss.newschema),
        missing_ok: aoss.missing_ok,
    }
}

unsafe fn convert_alter_object_depends_stmt(aods: &bindings_raw::AlterObjectDependsStmt) -> protobuf::AlterObjectDependsStmt {
    protobuf::AlterObjectDependsStmt {
        object_type: aods.objectType as i32 + 1,
        relation: if aods.relation.is_null() { None } else { Some(convert_range_var(&*aods.relation)) },
        object: convert_node_boxed(aods.object),
        extname: Some(protobuf::String { sval: convert_c_string(aods.extname as *mut i8) }),
        remove: aods.remove,
    }
}

unsafe fn convert_alter_collation_stmt(acs: &bindings_raw::AlterCollationStmt) -> protobuf::AlterCollationStmt {
    protobuf::AlterCollationStmt { collname: convert_list_to_nodes(acs.collname) }
}

unsafe fn convert_alter_default_privileges_stmt(adps: &bindings_raw::AlterDefaultPrivilegesStmt) -> protobuf::AlterDefaultPrivilegesStmt {
    protobuf::AlterDefaultPrivilegesStmt {
        options: convert_list_to_nodes(adps.options),
        action: if adps.action.is_null() { None } else { Some(convert_grant_stmt(&*adps.action)) },
    }
}

unsafe fn convert_create_cast_stmt(ccs: &bindings_raw::CreateCastStmt) -> protobuf::CreateCastStmt {
    protobuf::CreateCastStmt {
        sourcetype: if ccs.sourcetype.is_null() { None } else { Some(convert_type_name(&*ccs.sourcetype)) },
        targettype: if ccs.targettype.is_null() { None } else { Some(convert_type_name(&*ccs.targettype)) },
        func: if ccs.func.is_null() { None } else { Some(convert_object_with_args(&*ccs.func)) },
        context: ccs.context as i32 + 1,
        inout: ccs.inout,
    }
}

unsafe fn convert_create_transform_stmt(cts: &bindings_raw::CreateTransformStmt) -> protobuf::CreateTransformStmt {
    protobuf::CreateTransformStmt {
        replace: cts.replace,
        type_name: if cts.type_name.is_null() { None } else { Some(convert_type_name(&*cts.type_name)) },
        lang: convert_c_string(cts.lang),
        fromsql: if cts.fromsql.is_null() { None } else { Some(convert_object_with_args(&*cts.fromsql)) },
        tosql: if cts.tosql.is_null() { None } else { Some(convert_object_with_args(&*cts.tosql)) },
    }
}

unsafe fn convert_create_conversion_stmt(ccs: &bindings_raw::CreateConversionStmt) -> protobuf::CreateConversionStmt {
    protobuf::CreateConversionStmt {
        conversion_name: convert_list_to_nodes(ccs.conversion_name),
        for_encoding_name: convert_c_string(ccs.for_encoding_name),
        to_encoding_name: convert_c_string(ccs.to_encoding_name),
        func_name: convert_list_to_nodes(ccs.func_name),
        def: ccs.def,
    }
}

unsafe fn convert_alter_ts_dictionary_stmt(atds: &bindings_raw::AlterTSDictionaryStmt) -> protobuf::AlterTsDictionaryStmt {
    protobuf::AlterTsDictionaryStmt { dictname: convert_list_to_nodes(atds.dictname), options: convert_list_to_nodes(atds.options) }
}

unsafe fn convert_alter_ts_configuration_stmt(atcs: &bindings_raw::AlterTSConfigurationStmt) -> protobuf::AlterTsConfigurationStmt {
    protobuf::AlterTsConfigurationStmt {
        kind: atcs.kind as i32 + 1,
        cfgname: convert_list_to_nodes(atcs.cfgname),
        tokentype: convert_list_to_nodes(atcs.tokentype),
        dicts: convert_list_to_nodes(atcs.dicts),
        r#override: atcs.override_,
        replace: atcs.replace,
        missing_ok: atcs.missing_ok,
    }
}

unsafe fn convert_createdb_stmt(cds: &bindings_raw::CreatedbStmt) -> protobuf::CreatedbStmt {
    protobuf::CreatedbStmt { dbname: convert_c_string(cds.dbname), options: convert_list_to_nodes(cds.options) }
}

unsafe fn convert_dropdb_stmt(dds: &bindings_raw::DropdbStmt) -> protobuf::DropdbStmt {
    protobuf::DropdbStmt { dbname: convert_c_string(dds.dbname), missing_ok: dds.missing_ok, options: convert_list_to_nodes(dds.options) }
}

unsafe fn convert_alter_database_stmt(ads: &bindings_raw::AlterDatabaseStmt) -> protobuf::AlterDatabaseStmt {
    protobuf::AlterDatabaseStmt { dbname: convert_c_string(ads.dbname), options: convert_list_to_nodes(ads.options) }
}

unsafe fn convert_alter_database_set_stmt(adss: &bindings_raw::AlterDatabaseSetStmt) -> protobuf::AlterDatabaseSetStmt {
    protobuf::AlterDatabaseSetStmt { dbname: convert_c_string(adss.dbname), setstmt: convert_variable_set_stmt_opt(adss.setstmt) }
}

unsafe fn convert_alter_database_refresh_coll_stmt(adrcs: &bindings_raw::AlterDatabaseRefreshCollStmt) -> protobuf::AlterDatabaseRefreshCollStmt {
    protobuf::AlterDatabaseRefreshCollStmt { dbname: convert_c_string(adrcs.dbname) }
}

unsafe fn convert_alter_system_stmt(ass: &bindings_raw::AlterSystemStmt) -> protobuf::AlterSystemStmt {
    protobuf::AlterSystemStmt { setstmt: convert_variable_set_stmt_opt(ass.setstmt) }
}

unsafe fn convert_cluster_stmt(cs: &bindings_raw::ClusterStmt) -> protobuf::ClusterStmt {
    protobuf::ClusterStmt {
        relation: if cs.relation.is_null() { None } else { Some(convert_range_var(&*cs.relation)) },
        indexname: convert_c_string(cs.indexname),
        params: convert_list_to_nodes(cs.params),
    }
}

unsafe fn convert_reindex_stmt(rs: &bindings_raw::ReindexStmt) -> protobuf::ReindexStmt {
    protobuf::ReindexStmt {
        kind: rs.kind as i32 + 1,
        relation: if rs.relation.is_null() { None } else { Some(convert_range_var(&*rs.relation)) },
        name: convert_c_string(rs.name),
        params: convert_list_to_nodes(rs.params),
    }
}

unsafe fn convert_constraints_set_stmt(css: &bindings_raw::ConstraintsSetStmt) -> protobuf::ConstraintsSetStmt {
    protobuf::ConstraintsSetStmt { constraints: convert_list_to_nodes(css.constraints), deferred: css.deferred }
}

unsafe fn convert_load_stmt(ls: &bindings_raw::LoadStmt) -> protobuf::LoadStmt {
    protobuf::LoadStmt { filename: convert_c_string(ls.filename) }
}

unsafe fn convert_drop_owned_stmt(dos: &bindings_raw::DropOwnedStmt) -> protobuf::DropOwnedStmt {
    protobuf::DropOwnedStmt { roles: convert_list_to_nodes(dos.roles), behavior: dos.behavior as i32 + 1 }
}

unsafe fn convert_reassign_owned_stmt(ros: &bindings_raw::ReassignOwnedStmt) -> protobuf::ReassignOwnedStmt {
    protobuf::ReassignOwnedStmt {
        roles: convert_list_to_nodes(ros.roles),
        newrole: if ros.newrole.is_null() { None } else { Some(convert_role_spec(&*ros.newrole)) },
    }
}

unsafe fn convert_drop_subscription_stmt(dss: &bindings_raw::DropSubscriptionStmt) -> protobuf::DropSubscriptionStmt {
    protobuf::DropSubscriptionStmt { subname: convert_c_string(dss.subname), missing_ok: dss.missing_ok, behavior: dss.behavior as i32 + 1 }
}

unsafe fn convert_table_func(tf: &bindings_raw::TableFunc) -> protobuf::TableFunc {
    protobuf::TableFunc {
        functype: tf.functype as i32,
        ns_uris: convert_list_to_nodes(tf.ns_uris),
        ns_names: convert_list_to_nodes(tf.ns_names),
        docexpr: convert_node_boxed(tf.docexpr),
        rowexpr: convert_node_boxed(tf.rowexpr),
        colnames: convert_list_to_nodes(tf.colnames),
        coltypes: convert_list_to_nodes(tf.coltypes),
        coltypmods: convert_list_to_nodes(tf.coltypmods),
        colcollations: convert_list_to_nodes(tf.colcollations),
        colexprs: convert_list_to_nodes(tf.colexprs),
        coldefexprs: convert_list_to_nodes(tf.coldefexprs),
        colvalexprs: convert_list_to_nodes(tf.colvalexprs),
        passingvalexprs: convert_list_to_nodes(tf.passingvalexprs),
        notnulls: vec![], // Bitmapset conversion not yet supported
        plan: convert_node_boxed(tf.plan),
        ordinalitycol: tf.ordinalitycol,
        location: tf.location,
    }
}

unsafe fn convert_into_clause_node(ic: &bindings_raw::IntoClause) -> protobuf::IntoClause {
    protobuf::IntoClause {
        rel: if ic.rel.is_null() { None } else { Some(convert_range_var(&*ic.rel)) },
        col_names: convert_list_to_nodes(ic.colNames),
        access_method: convert_c_string(ic.accessMethod),
        options: convert_list_to_nodes(ic.options),
        on_commit: ic.onCommit as i32 + 1,
        table_space_name: convert_c_string(ic.tableSpaceName),
        view_query: convert_node_boxed(ic.viewQuery),
        skip_data: ic.skipData,
    }
}

unsafe fn convert_table_like_clause(tlc: &bindings_raw::TableLikeClause) -> protobuf::TableLikeClause {
    protobuf::TableLikeClause {
        relation: if tlc.relation.is_null() { None } else { Some(convert_range_var(&*tlc.relation)) },
        options: tlc.options,
        relation_oid: tlc.relationOid,
    }
}

unsafe fn convert_range_table_func(rtf: &bindings_raw::RangeTableFunc) -> protobuf::RangeTableFunc {
    protobuf::RangeTableFunc {
        lateral: rtf.lateral,
        docexpr: convert_node_boxed(rtf.docexpr),
        rowexpr: convert_node_boxed(rtf.rowexpr),
        namespaces: convert_list_to_nodes(rtf.namespaces),
        columns: convert_list_to_nodes(rtf.columns),
        alias: if rtf.alias.is_null() { None } else { Some(convert_alias(&*rtf.alias)) },
        location: rtf.location,
    }
}

unsafe fn convert_range_table_func_col(rtfc: &bindings_raw::RangeTableFuncCol) -> protobuf::RangeTableFuncCol {
    protobuf::RangeTableFuncCol {
        colname: convert_c_string(rtfc.colname),
        type_name: if rtfc.typeName.is_null() { None } else { Some(convert_type_name(&*rtfc.typeName)) },
        for_ordinality: rtfc.for_ordinality,
        is_not_null: rtfc.is_not_null,
        colexpr: convert_node_boxed(rtfc.colexpr),
        coldefexpr: convert_node_boxed(rtfc.coldefexpr),
        location: rtfc.location,
    }
}

unsafe fn convert_range_table_sample(rts: &bindings_raw::RangeTableSample) -> protobuf::RangeTableSample {
    protobuf::RangeTableSample {
        relation: convert_node_boxed(rts.relation),
        method: convert_list_to_nodes(rts.method),
        args: convert_list_to_nodes(rts.args),
        repeatable: convert_node_boxed(rts.repeatable),
        location: rts.location,
    }
}

unsafe fn convert_partition_cmd(pc: &bindings_raw::PartitionCmd) -> protobuf::PartitionCmd {
    protobuf::PartitionCmd {
        name: if pc.name.is_null() { None } else { Some(convert_range_var(&*pc.name)) },
        bound: convert_partition_bound_spec_opt(pc.bound),
        concurrent: pc.concurrent,
    }
}

unsafe fn convert_on_conflict_clause_node(occ: &bindings_raw::OnConflictClause) -> protobuf::OnConflictClause {
    protobuf::OnConflictClause {
        action: occ.action as i32 + 1,
        infer: convert_infer_clause_opt(occ.infer),
        target_list: convert_list_to_nodes(occ.targetList),
        where_clause: convert_node_boxed(occ.whereClause),
        location: occ.location,
    }
}

unsafe fn convert_trigger_transition(tt: &bindings_raw::TriggerTransition) -> protobuf::TriggerTransition {
    protobuf::TriggerTransition { name: convert_c_string(tt.name), is_new: tt.isNew, is_table: tt.isTable }
}

unsafe fn convert_create_stats_stmt(css: &bindings_raw::CreateStatsStmt) -> protobuf::CreateStatsStmt {
    protobuf::CreateStatsStmt {
        defnames: convert_list_to_nodes(css.defnames),
        stat_types: convert_list_to_nodes(css.stat_types),
        exprs: convert_list_to_nodes(css.exprs),
        relations: convert_list_to_nodes(css.relations),
        stxcomment: convert_c_string(css.stxcomment),
        transformed: css.transformed,
        if_not_exists: css.if_not_exists,
    }
}

unsafe fn convert_alter_stats_stmt(ass: &bindings_raw::AlterStatsStmt) -> protobuf::AlterStatsStmt {
    protobuf::AlterStatsStmt {
        defnames: convert_list_to_nodes(ass.defnames),
        stxstattarget: convert_node_boxed(ass.stxstattarget),
        missing_ok: ass.missing_ok,
    }
}

unsafe fn convert_stats_elem(se: &bindings_raw::StatsElem) -> protobuf::StatsElem {
    protobuf::StatsElem { name: convert_c_string(se.name), expr: convert_node_boxed(se.expr) }
}

unsafe fn convert_sql_value_function(svf: &bindings_raw::SQLValueFunction) -> protobuf::SqlValueFunction {
    protobuf::SqlValueFunction {
        xpr: None,
        op: svf.op as i32 + 1,
        r#type: svf.type_,
        typmod: svf.typmod,
        location: svf.location,
    }
}

unsafe fn convert_xml_expr(xe: &bindings_raw::XmlExpr) -> protobuf::XmlExpr {
    protobuf::XmlExpr {
        xpr: None,
        op: xe.op as i32 + 1,
        name: convert_c_string(xe.name),
        named_args: convert_list_to_nodes(xe.named_args),
        arg_names: convert_list_to_nodes(xe.arg_names),
        args: convert_list_to_nodes(xe.args),
        xmloption: xe.xmloption as i32 + 1,
        indent: xe.indent,
        r#type: xe.type_,
        typmod: xe.typmod,
        location: xe.location,
    }
}

unsafe fn convert_xml_serialize(xs: &bindings_raw::XmlSerialize) -> protobuf::XmlSerialize {
    protobuf::XmlSerialize {
        xmloption: xs.xmloption as i32 + 1,
        expr: convert_node_boxed(xs.expr),
        type_name: if xs.typeName.is_null() { None } else { Some(convert_type_name(&*xs.typeName)) },
        indent: xs.indent,
        location: xs.location,
    }
}

unsafe fn convert_named_arg_expr(nae: &bindings_raw::NamedArgExpr) -> protobuf::NamedArgExpr {
    protobuf::NamedArgExpr {
        xpr: None,
        arg: convert_node_boxed(nae.arg as *mut bindings_raw::Node),
        name: convert_c_string(nae.name),
        argnumber: nae.argnumber,
        location: nae.location,
    }
}

// ============================================================================
// JSON Node Conversions
// ============================================================================

unsafe fn convert_json_format(jf: &bindings_raw::JsonFormat) -> protobuf::JsonFormat {
    protobuf::JsonFormat { format_type: jf.format_type as i32 + 1, encoding: jf.encoding as i32 + 1, location: jf.location }
}

unsafe fn convert_json_returning(jr: &bindings_raw::JsonReturning) -> protobuf::JsonReturning {
    protobuf::JsonReturning {
        format: if jr.format.is_null() { None } else { Some(convert_json_format(&*jr.format)) },
        typid: jr.typid,
        typmod: jr.typmod,
    }
}

unsafe fn convert_json_value_expr(jve: &bindings_raw::JsonValueExpr) -> protobuf::JsonValueExpr {
    protobuf::JsonValueExpr {
        raw_expr: convert_node_boxed(jve.raw_expr as *mut bindings_raw::Node),
        formatted_expr: convert_node_boxed(jve.formatted_expr as *mut bindings_raw::Node),
        format: if jve.format.is_null() { None } else { Some(convert_json_format(&*jve.format)) },
    }
}

unsafe fn convert_json_constructor_expr(jce: &bindings_raw::JsonConstructorExpr) -> protobuf::JsonConstructorExpr {
    protobuf::JsonConstructorExpr {
        xpr: None,
        r#type: jce.type_ as i32 + 1,
        args: convert_list_to_nodes(jce.args),
        func: convert_node_boxed(jce.func as *mut bindings_raw::Node),
        coercion: convert_node_boxed(jce.coercion as *mut bindings_raw::Node),
        returning: if jce.returning.is_null() { None } else { Some(convert_json_returning(&*jce.returning)) },
        absent_on_null: jce.absent_on_null,
        unique: jce.unique,
        location: jce.location,
    }
}

unsafe fn convert_json_is_predicate(jip: &bindings_raw::JsonIsPredicate) -> protobuf::JsonIsPredicate {
    protobuf::JsonIsPredicate {
        expr: convert_node_boxed(jip.expr),
        format: if jip.format.is_null() { None } else { Some(convert_json_format(&*jip.format)) },
        item_type: jip.item_type as i32 + 1,
        unique_keys: jip.unique_keys,
        location: jip.location,
    }
}

unsafe fn convert_json_behavior(jb: &bindings_raw::JsonBehavior) -> protobuf::JsonBehavior {
    protobuf::JsonBehavior { btype: jb.btype as i32 + 1, expr: convert_node_boxed(jb.expr), coerce: jb.coerce, location: jb.location }
}

unsafe fn convert_json_expr(je: &bindings_raw::JsonExpr) -> protobuf::JsonExpr {
    protobuf::JsonExpr {
        xpr: None,
        op: je.op as i32 + 1,
        column_name: convert_c_string(je.column_name),
        formatted_expr: convert_node_boxed(je.formatted_expr as *mut bindings_raw::Node),
        format: if je.format.is_null() { None } else { Some(convert_json_format(&*je.format)) },
        path_spec: convert_node_boxed(je.path_spec),
        returning: if je.returning.is_null() { None } else { Some(convert_json_returning(&*je.returning)) },
        passing_names: convert_list_to_nodes(je.passing_names),
        passing_values: convert_list_to_nodes(je.passing_values),
        on_empty: if je.on_empty.is_null() { None } else { Some(Box::new(convert_json_behavior(&*je.on_empty))) },
        on_error: if je.on_error.is_null() { None } else { Some(Box::new(convert_json_behavior(&*je.on_error))) },
        use_io_coercion: je.use_io_coercion,
        use_json_coercion: je.use_json_coercion,
        wrapper: je.wrapper as i32 + 1,
        omit_quotes: je.omit_quotes,
        collation: je.collation,
        location: je.location,
    }
}

unsafe fn convert_json_table_path(jtp: &bindings_raw::JsonTablePath) -> protobuf::JsonTablePath {
    // In raw parse tree, value is not populated - only name
    protobuf::JsonTablePath { name: convert_c_string(jtp.name) }
}

unsafe fn convert_json_table_path_scan(jtps: &bindings_raw::JsonTablePathScan) -> protobuf::JsonTablePathScan {
    protobuf::JsonTablePathScan {
        plan: convert_node_boxed(&jtps.plan as *const bindings_raw::JsonTablePlan as *mut bindings_raw::Node),
        path: if jtps.path.is_null() { None } else { Some(convert_json_table_path(&*jtps.path)) },
        error_on_error: jtps.errorOnError,
        child: convert_node_boxed(jtps.child as *mut bindings_raw::Node),
        col_min: jtps.colMin,
        col_max: jtps.colMax,
    }
}

unsafe fn convert_json_table_sibling_join(jtsj: &bindings_raw::JsonTableSiblingJoin) -> protobuf::JsonTableSiblingJoin {
    protobuf::JsonTableSiblingJoin {
        plan: convert_node_boxed(&jtsj.plan as *const bindings_raw::JsonTablePlan as *mut bindings_raw::Node),
        lplan: convert_node_boxed(jtsj.lplan as *mut bindings_raw::Node),
        rplan: convert_node_boxed(jtsj.rplan as *mut bindings_raw::Node),
    }
}

unsafe fn convert_json_output(jo: &bindings_raw::JsonOutput) -> protobuf::JsonOutput {
    protobuf::JsonOutput {
        type_name: if jo.typeName.is_null() { None } else { Some(convert_type_name(&*jo.typeName)) },
        returning: if jo.returning.is_null() { None } else { Some(convert_json_returning(&*jo.returning)) },
    }
}

unsafe fn convert_json_argument(ja: &bindings_raw::JsonArgument) -> protobuf::JsonArgument {
    protobuf::JsonArgument {
        val: if ja.val.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*ja.val))) },
        name: convert_c_string(ja.name),
    }
}

unsafe fn convert_json_func_expr(jfe: &bindings_raw::JsonFuncExpr) -> protobuf::JsonFuncExpr {
    protobuf::JsonFuncExpr {
        op: jfe.op as i32 + 1,
        column_name: convert_c_string(jfe.column_name),
        context_item: if jfe.context_item.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jfe.context_item))) },
        pathspec: convert_node_boxed(jfe.pathspec),
        passing: convert_list_to_nodes(jfe.passing),
        output: if jfe.output.is_null() { None } else { Some(convert_json_output(&*jfe.output)) },
        on_empty: if jfe.on_empty.is_null() { None } else { Some(Box::new(convert_json_behavior(&*jfe.on_empty))) },
        on_error: if jfe.on_error.is_null() { None } else { Some(Box::new(convert_json_behavior(&*jfe.on_error))) },
        wrapper: jfe.wrapper as i32 + 1,
        quotes: jfe.quotes as i32 + 1,
        location: jfe.location,
    }
}

unsafe fn convert_json_table_path_spec(jtps: &bindings_raw::JsonTablePathSpec) -> protobuf::JsonTablePathSpec {
    protobuf::JsonTablePathSpec {
        string: convert_node_boxed(jtps.string),
        name: convert_c_string(jtps.name),
        name_location: jtps.name_location,
        location: jtps.location,
    }
}

unsafe fn convert_json_table(jt: &bindings_raw::JsonTable) -> protobuf::JsonTable {
    protobuf::JsonTable {
        context_item: if jt.context_item.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jt.context_item))) },
        pathspec: if jt.pathspec.is_null() { None } else { Some(Box::new(convert_json_table_path_spec(&*jt.pathspec))) },
        passing: convert_list_to_nodes(jt.passing),
        columns: convert_list_to_nodes(jt.columns),
        on_error: if jt.on_error.is_null() { None } else { Some(Box::new(convert_json_behavior(&*jt.on_error))) },
        alias: if jt.alias.is_null() { None } else { Some(convert_alias(&*jt.alias)) },
        lateral: jt.lateral,
        location: jt.location,
    }
}

unsafe fn convert_json_table_column(jtc: &bindings_raw::JsonTableColumn) -> protobuf::JsonTableColumn {
    protobuf::JsonTableColumn {
        coltype: jtc.coltype as i32 + 1,
        name: convert_c_string(jtc.name),
        type_name: if jtc.typeName.is_null() { None } else { Some(convert_type_name(&*jtc.typeName)) },
        pathspec: if jtc.pathspec.is_null() { None } else { Some(Box::new(convert_json_table_path_spec(&*jtc.pathspec))) },
        format: if jtc.format.is_null() { None } else { Some(convert_json_format(&*jtc.format)) },
        wrapper: jtc.wrapper as i32 + 1,
        quotes: jtc.quotes as i32 + 1,
        columns: convert_list_to_nodes(jtc.columns),
        on_empty: if jtc.on_empty.is_null() { None } else { Some(Box::new(convert_json_behavior(&*jtc.on_empty))) },
        on_error: if jtc.on_error.is_null() { None } else { Some(Box::new(convert_json_behavior(&*jtc.on_error))) },
        location: jtc.location,
    }
}

unsafe fn convert_json_key_value(jkv: &bindings_raw::JsonKeyValue) -> protobuf::JsonKeyValue {
    protobuf::JsonKeyValue {
        key: convert_node_boxed(jkv.key as *mut bindings_raw::Node),
        value: if jkv.value.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jkv.value))) },
    }
}

unsafe fn convert_json_parse_expr(jpe: &bindings_raw::JsonParseExpr) -> protobuf::JsonParseExpr {
    protobuf::JsonParseExpr {
        expr: if jpe.expr.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jpe.expr))) },
        output: if jpe.output.is_null() { None } else { Some(convert_json_output(&*jpe.output)) },
        unique_keys: jpe.unique_keys,
        location: jpe.location,
    }
}

unsafe fn convert_json_scalar_expr(jse: &bindings_raw::JsonScalarExpr) -> protobuf::JsonScalarExpr {
    protobuf::JsonScalarExpr {
        expr: convert_node_boxed(jse.expr as *mut bindings_raw::Node),
        output: if jse.output.is_null() { None } else { Some(convert_json_output(&*jse.output)) },
        location: jse.location,
    }
}

unsafe fn convert_json_serialize_expr(jse: &bindings_raw::JsonSerializeExpr) -> protobuf::JsonSerializeExpr {
    protobuf::JsonSerializeExpr {
        expr: if jse.expr.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jse.expr))) },
        output: if jse.output.is_null() { None } else { Some(convert_json_output(&*jse.output)) },
        location: jse.location,
    }
}

unsafe fn convert_json_object_constructor(joc: &bindings_raw::JsonObjectConstructor) -> protobuf::JsonObjectConstructor {
    protobuf::JsonObjectConstructor {
        exprs: convert_list_to_nodes(joc.exprs),
        output: if joc.output.is_null() { None } else { Some(convert_json_output(&*joc.output)) },
        absent_on_null: joc.absent_on_null,
        unique: joc.unique,
        location: joc.location,
    }
}

unsafe fn convert_json_array_constructor(jac: &bindings_raw::JsonArrayConstructor) -> protobuf::JsonArrayConstructor {
    protobuf::JsonArrayConstructor {
        exprs: convert_list_to_nodes(jac.exprs),
        output: if jac.output.is_null() { None } else { Some(convert_json_output(&*jac.output)) },
        absent_on_null: jac.absent_on_null,
        location: jac.location,
    }
}

unsafe fn convert_json_array_query_constructor(jaqc: &bindings_raw::JsonArrayQueryConstructor) -> protobuf::JsonArrayQueryConstructor {
    protobuf::JsonArrayQueryConstructor {
        query: convert_node_boxed(jaqc.query),
        output: if jaqc.output.is_null() { None } else { Some(convert_json_output(&*jaqc.output)) },
        format: if jaqc.format.is_null() { None } else { Some(convert_json_format(&*jaqc.format)) },
        absent_on_null: jaqc.absent_on_null,
        location: jaqc.location,
    }
}

unsafe fn convert_json_agg_constructor(jac: &bindings_raw::JsonAggConstructor) -> protobuf::JsonAggConstructor {
    protobuf::JsonAggConstructor {
        output: if jac.output.is_null() { None } else { Some(convert_json_output(&*jac.output)) },
        agg_filter: convert_node_boxed(jac.agg_filter),
        agg_order: convert_list_to_nodes(jac.agg_order),
        over: if jac.over.is_null() { None } else { Some(Box::new(convert_window_def(&*jac.over))) },
        location: jac.location,
    }
}

unsafe fn convert_json_object_agg(joa: &bindings_raw::JsonObjectAgg) -> protobuf::JsonObjectAgg {
    protobuf::JsonObjectAgg {
        constructor: if joa.constructor.is_null() { None } else { Some(Box::new(convert_json_agg_constructor(&*joa.constructor))) },
        arg: if joa.arg.is_null() { None } else { Some(Box::new(convert_json_key_value(&*joa.arg))) },
        absent_on_null: joa.absent_on_null,
        unique: joa.unique,
    }
}

unsafe fn convert_json_array_agg(jaa: &bindings_raw::JsonArrayAgg) -> protobuf::JsonArrayAgg {
    protobuf::JsonArrayAgg {
        constructor: if jaa.constructor.is_null() { None } else { Some(Box::new(convert_json_agg_constructor(&*jaa.constructor))) },
        arg: if jaa.arg.is_null() { None } else { Some(Box::new(convert_json_value_expr(&*jaa.arg))) },
        absent_on_null: jaa.absent_on_null,
    }
}

// ============================================================================
// Additional Helper Functions
// ============================================================================

unsafe fn convert_variable_set_stmt_opt(stmt: *mut bindings_raw::VariableSetStmt) -> Option<protobuf::VariableSetStmt> {
    if stmt.is_null() {
        None
    } else {
        Some(convert_variable_set_stmt(&*stmt))
    }
}

unsafe fn convert_infer_clause_opt(ic: *mut bindings_raw::InferClause) -> Option<Box<protobuf::InferClause>> {
    if ic.is_null() {
        None
    } else {
        let ic_ref = &*ic;
        Some(Box::new(protobuf::InferClause {
            index_elems: convert_list_to_nodes(ic_ref.indexElems),
            where_clause: convert_node_boxed(ic_ref.whereClause),
            conname: convert_c_string(ic_ref.conname),
            location: ic_ref.location,
        }))
    }
}
