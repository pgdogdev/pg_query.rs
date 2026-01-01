//! Direct deparsing that bypasses protobuf serialization.
//!
//! This module converts Rust protobuf types directly to PostgreSQL's internal
//! C parse tree structures, then deparses them to SQL without going through
//! protobuf serialization.

use crate::bindings_raw;
use crate::protobuf;
use crate::{Error, Result};
use std::ffi::CStr;
use std::os::raw::c_char;

/// Deparses a protobuf ParseResult directly to SQL without protobuf serialization.
///
/// This function is faster than `deparse` because it skips the protobuf encode/decode step.
/// The protobuf types are converted directly to PostgreSQL's internal C structures.
///
/// # Example
///
/// ```rust
/// let result = pg_query::parse("SELECT * FROM users").unwrap();
/// let sql = pg_query::deparse_raw(&result.protobuf).unwrap();
/// assert_eq!(sql, "SELECT * FROM users");
/// ```
pub fn deparse_raw(protobuf: &protobuf::ParseResult) -> Result<String> {
    unsafe {
        // Enter PostgreSQL memory context - this must stay active for the entire operation
        let ctx = bindings_raw::pg_query_deparse_enter_context();

        // Build C nodes from protobuf types (uses palloc which requires active context)
        let stmts = write_stmts(&protobuf.stmts);

        // Deparse the nodes to SQL (also requires active context)
        let result = bindings_raw::pg_query_deparse_nodes(stmts);

        // Exit memory context - this frees all palloc'd memory
        bindings_raw::pg_query_deparse_exit_context(ctx);

        // Handle result (result.query is strdup'd, so it survives context exit)
        if !result.error.is_null() {
            let message = CStr::from_ptr((*result.error).message).to_string_lossy().to_string();
            bindings_raw::pg_query_free_deparse_result(result);
            return Err(Error::Parse(message));
        }

        let query = CStr::from_ptr(result.query).to_string_lossy().to_string();
        bindings_raw::pg_query_free_deparse_result(result);
        Ok(query)
    }
}

/// Allocates a C node of the given type.
unsafe fn alloc_node<T>(tag: u32) -> *mut T {
    bindings_raw::pg_query_alloc_node(std::mem::size_of::<T>(), tag as i32) as *mut T
}

/// Converts a protobuf enum value to a C enum value.
/// Protobuf enums have an extra "Undefined = 0" value, so we subtract 1.
/// If the value is 0 (Undefined), we return 0 (treating it as the first C enum value).
fn proto_enum_to_c(value: i32) -> u32 {
    if value <= 0 {
        0
    } else {
        (value - 1) as u32
    }
}

/// Duplicates a string into PostgreSQL memory context.
unsafe fn pstrdup(s: &str) -> *mut c_char {
    if s.is_empty() {
        return std::ptr::null_mut();
    }
    let cstr = std::ffi::CString::new(s).unwrap();
    bindings_raw::pg_query_pstrdup(cstr.as_ptr())
}

/// Writes a list of RawStmt to a C List.
fn write_stmts(stmts: &[protobuf::RawStmt]) -> *mut std::ffi::c_void {
    if stmts.is_empty() {
        return std::ptr::null_mut();
    }

    let mut list: *mut std::ffi::c_void = std::ptr::null_mut();

    for stmt in stmts {
        let raw_stmt = write_raw_stmt(stmt);
        if list.is_null() {
            list = unsafe { bindings_raw::pg_query_list_make1(raw_stmt as *mut std::ffi::c_void) };
        } else {
            list = unsafe { bindings_raw::pg_query_list_append(list, raw_stmt as *mut std::ffi::c_void) };
        }
    }

    list
}

/// Writes a protobuf RawStmt to a C RawStmt.
fn write_raw_stmt(stmt: &protobuf::RawStmt) -> *mut bindings_raw::RawStmt {
    unsafe {
        let raw_stmt = alloc_node::<bindings_raw::RawStmt>(bindings_raw::NodeTag_T_RawStmt);
        (*raw_stmt).stmt_location = stmt.stmt_location;
        (*raw_stmt).stmt_len = stmt.stmt_len;
        (*raw_stmt).stmt = write_node_boxed(&stmt.stmt);
        raw_stmt
    }
}

/// Writes an Option<Box<Node>> to a C Node pointer.
fn write_node_boxed(node: &Option<Box<protobuf::Node>>) -> *mut bindings_raw::Node {
    match node {
        Some(n) => write_node(n),
        None => std::ptr::null_mut(),
    }
}

/// Writes a protobuf Node to a C Node.
fn write_node(node: &protobuf::Node) -> *mut bindings_raw::Node {
    match &node.node {
        Some(n) => write_node_inner(n),
        None => std::ptr::null_mut(),
    }
}

/// Writes a protobuf node::Node enum to a C Node.
fn write_node_inner(node: &protobuf::node::Node) -> *mut bindings_raw::Node {
    unsafe {
        match node {
            protobuf::node::Node::SelectStmt(stmt) => write_select_stmt(stmt) as *mut bindings_raw::Node,
            protobuf::node::Node::InsertStmt(stmt) => write_insert_stmt(stmt) as *mut bindings_raw::Node,
            protobuf::node::Node::UpdateStmt(stmt) => write_update_stmt(stmt) as *mut bindings_raw::Node,
            protobuf::node::Node::DeleteStmt(stmt) => write_delete_stmt(stmt) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeVar(rv) => write_range_var(rv) as *mut bindings_raw::Node,
            protobuf::node::Node::Alias(alias) => write_alias(alias) as *mut bindings_raw::Node,
            protobuf::node::Node::ResTarget(rt) => write_res_target(rt) as *mut bindings_raw::Node,
            protobuf::node::Node::ColumnRef(cr) => write_column_ref(cr) as *mut bindings_raw::Node,
            protobuf::node::Node::AConst(ac) => write_a_const(ac) as *mut bindings_raw::Node,
            protobuf::node::Node::AExpr(expr) => write_a_expr(expr) as *mut bindings_raw::Node,
            protobuf::node::Node::FuncCall(fc) => write_func_call(fc) as *mut bindings_raw::Node,
            protobuf::node::Node::String(s) => write_string(s) as *mut bindings_raw::Node,
            protobuf::node::Node::Integer(i) => write_integer(i) as *mut bindings_raw::Node,
            protobuf::node::Node::Float(f) => write_float(f) as *mut bindings_raw::Node,
            protobuf::node::Node::Boolean(b) => write_boolean(b) as *mut bindings_raw::Node,
            protobuf::node::Node::List(l) => write_list(l) as *mut bindings_raw::Node,
            protobuf::node::Node::AStar(_) => write_a_star() as *mut bindings_raw::Node,
            protobuf::node::Node::JoinExpr(je) => write_join_expr(je) as *mut bindings_raw::Node,
            protobuf::node::Node::SortBy(sb) => write_sort_by(sb) as *mut bindings_raw::Node,
            protobuf::node::Node::TypeCast(tc) => write_type_cast(tc) as *mut bindings_raw::Node,
            protobuf::node::Node::TypeName(tn) => write_type_name(tn) as *mut bindings_raw::Node,
            protobuf::node::Node::ParamRef(pr) => write_param_ref(pr) as *mut bindings_raw::Node,
            protobuf::node::Node::NullTest(nt) => write_null_test(nt) as *mut bindings_raw::Node,
            protobuf::node::Node::BoolExpr(be) => write_bool_expr(be) as *mut bindings_raw::Node,
            protobuf::node::Node::SubLink(sl) => write_sub_link(sl) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeSubselect(rs) => write_range_subselect(rs) as *mut bindings_raw::Node,
            protobuf::node::Node::CommonTableExpr(cte) => write_common_table_expr(cte) as *mut bindings_raw::Node,
            protobuf::node::Node::WithClause(wc) => write_with_clause(wc) as *mut bindings_raw::Node,
            protobuf::node::Node::GroupingSet(gs) => write_grouping_set(gs) as *mut bindings_raw::Node,
            protobuf::node::Node::WindowDef(wd) => write_window_def(wd) as *mut bindings_raw::Node,
            protobuf::node::Node::CoalesceExpr(ce) => write_coalesce_expr(ce) as *mut bindings_raw::Node,
            protobuf::node::Node::CaseExpr(ce) => write_case_expr(ce) as *mut bindings_raw::Node,
            protobuf::node::Node::CaseWhen(cw) => write_case_when(cw) as *mut bindings_raw::Node,
            protobuf::node::Node::SetToDefault(_) => write_set_to_default() as *mut bindings_raw::Node,
            protobuf::node::Node::LockingClause(lc) => write_locking_clause(lc) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeFunction(rf) => write_range_function(rf) as *mut bindings_raw::Node,
            protobuf::node::Node::BitString(bs) => write_bit_string(bs) as *mut bindings_raw::Node,
            protobuf::node::Node::IndexElem(ie) => write_index_elem(ie) as *mut bindings_raw::Node,
            protobuf::node::Node::DropStmt(ds) => write_drop_stmt(ds) as *mut bindings_raw::Node,
            protobuf::node::Node::ObjectWithArgs(owa) => write_object_with_args(owa) as *mut bindings_raw::Node,
            protobuf::node::Node::FunctionParameter(fp) => write_function_parameter(fp) as *mut bindings_raw::Node,
            protobuf::node::Node::TruncateStmt(ts) => write_truncate_stmt(ts) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateStmt(cs) => write_create_stmt(cs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTableStmt(ats) => write_alter_table_stmt(ats) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTableCmd(atc) => write_alter_table_cmd(atc) as *mut bindings_raw::Node,
            protobuf::node::Node::ColumnDef(cd) => write_column_def(cd) as *mut bindings_raw::Node,
            protobuf::node::Node::Constraint(c) => write_constraint(c) as *mut bindings_raw::Node,
            protobuf::node::Node::IndexStmt(is) => write_index_stmt(is) as *mut bindings_raw::Node,
            protobuf::node::Node::ViewStmt(vs) => write_view_stmt(vs) as *mut bindings_raw::Node,
            protobuf::node::Node::TransactionStmt(ts) => write_transaction_stmt(ts) as *mut bindings_raw::Node,
            protobuf::node::Node::CopyStmt(cs) => write_copy_stmt(cs) as *mut bindings_raw::Node,
            protobuf::node::Node::ExplainStmt(es) => write_explain_stmt(es) as *mut bindings_raw::Node,
            protobuf::node::Node::VacuumStmt(vs) => write_vacuum_stmt(vs) as *mut bindings_raw::Node,
            protobuf::node::Node::LockStmt(ls) => write_lock_stmt(ls) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateSchemaStmt(css) => write_create_schema_stmt(css) as *mut bindings_raw::Node,
            protobuf::node::Node::VariableSetStmt(vss) => write_variable_set_stmt(vss) as *mut bindings_raw::Node,
            protobuf::node::Node::VariableShowStmt(vss) => write_variable_show_stmt(vss) as *mut bindings_raw::Node,
            protobuf::node::Node::RenameStmt(rs) => write_rename_stmt(rs) as *mut bindings_raw::Node,
            protobuf::node::Node::GrantStmt(gs) => write_grant_stmt(gs) as *mut bindings_raw::Node,
            protobuf::node::Node::RoleSpec(rs) => write_role_spec(rs) as *mut bindings_raw::Node,
            protobuf::node::Node::AccessPriv(ap) => write_access_priv(ap) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateFunctionStmt(cfs) => write_create_function_stmt(cfs) as *mut bindings_raw::Node,
            protobuf::node::Node::DefElem(de) => write_def_elem(de) as *mut bindings_raw::Node,
            protobuf::node::Node::RuleStmt(rs) => write_rule_stmt(rs) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateTrigStmt(cts) => write_create_trig_stmt(cts) as *mut bindings_raw::Node,
            protobuf::node::Node::DoStmt(ds) => write_do_stmt(ds) as *mut bindings_raw::Node,
            protobuf::node::Node::CallStmt(cs) => write_call_stmt(cs) as *mut bindings_raw::Node,
            protobuf::node::Node::MergeStmt(ms) => write_merge_stmt(ms) as *mut bindings_raw::Node,
            protobuf::node::Node::MergeWhenClause(mwc) => write_merge_when_clause(mwc) as *mut bindings_raw::Node,
            protobuf::node::Node::GrantRoleStmt(grs) => write_grant_role_stmt(grs) as *mut bindings_raw::Node,
            protobuf::node::Node::PrepareStmt(ps) => write_prepare_stmt(ps) as *mut bindings_raw::Node,
            protobuf::node::Node::ExecuteStmt(es) => write_execute_stmt(es) as *mut bindings_raw::Node,
            protobuf::node::Node::DeallocateStmt(ds) => write_deallocate_stmt(ds) as *mut bindings_raw::Node,
            protobuf::node::Node::AIndirection(ai) => write_a_indirection(ai) as *mut bindings_raw::Node,
            protobuf::node::Node::AIndices(ai) => write_a_indices(ai) as *mut bindings_raw::Node,
            protobuf::node::Node::MinMaxExpr(mme) => write_min_max_expr(mme) as *mut bindings_raw::Node,
            protobuf::node::Node::RowExpr(re) => write_row_expr(re) as *mut bindings_raw::Node,
            protobuf::node::Node::AArrayExpr(ae) => write_a_array_expr(ae) as *mut bindings_raw::Node,
            protobuf::node::Node::BooleanTest(bt) => write_boolean_test(bt) as *mut bindings_raw::Node,
            protobuf::node::Node::CollateClause(cc) => write_collate_clause(cc) as *mut bindings_raw::Node,
            protobuf::node::Node::CheckPointStmt(_) => alloc_node::<bindings_raw::Node>(bindings_raw::NodeTag_T_CheckPointStmt),
            protobuf::node::Node::CreateTableAsStmt(ctas) => write_create_table_as_stmt(ctas) as *mut bindings_raw::Node,
            protobuf::node::Node::RefreshMatViewStmt(rmvs) => write_refresh_mat_view_stmt(rmvs) as *mut bindings_raw::Node,
            protobuf::node::Node::VacuumRelation(vr) => write_vacuum_relation(vr) as *mut bindings_raw::Node,
            // TODO: Add remaining node types as needed
            _ => {
                // For unimplemented nodes, return null and let the deparser handle it
                std::ptr::null_mut()
            }
        }
    }
}

/// Writes a list of protobuf Nodes to a C List.
fn write_node_list(nodes: &[protobuf::Node]) -> *mut bindings_raw::List {
    if nodes.is_empty() {
        return std::ptr::null_mut();
    }

    let mut list: *mut std::ffi::c_void = std::ptr::null_mut();

    for node in nodes {
        let c_node = write_node(node);
        if !c_node.is_null() {
            if list.is_null() {
                list = unsafe { bindings_raw::pg_query_list_make1(c_node as *mut std::ffi::c_void) };
            } else {
                list = unsafe { bindings_raw::pg_query_list_append(list, c_node as *mut std::ffi::c_void) };
            }
        }
    }

    list as *mut bindings_raw::List
}

// =============================================================================
// Individual node type writers
// =============================================================================

unsafe fn write_select_stmt(stmt: &protobuf::SelectStmt) -> *mut bindings_raw::SelectStmt {
    let node = alloc_node::<bindings_raw::SelectStmt>(bindings_raw::NodeTag_T_SelectStmt);
    (*node).distinctClause = write_node_list(&stmt.distinct_clause);
    (*node).intoClause = write_into_clause_opt(&stmt.into_clause);
    (*node).targetList = write_node_list(&stmt.target_list);
    (*node).fromClause = write_node_list(&stmt.from_clause);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).groupClause = write_node_list(&stmt.group_clause);
    (*node).groupDistinct = stmt.group_distinct;
    (*node).havingClause = write_node_boxed(&stmt.having_clause);
    (*node).windowClause = write_node_list(&stmt.window_clause);
    (*node).valuesLists = write_values_lists(&stmt.values_lists);
    (*node).sortClause = write_node_list(&stmt.sort_clause);
    (*node).limitOffset = write_node_boxed(&stmt.limit_offset);
    (*node).limitCount = write_node_boxed(&stmt.limit_count);
    (*node).limitOption = proto_enum_to_c(stmt.limit_option);
    (*node).lockingClause = write_node_list(&stmt.locking_clause);
    (*node).withClause = write_with_clause_ref(&stmt.with_clause);
    (*node).op = proto_enum_to_c(stmt.op);
    (*node).all = stmt.all;
    (*node).larg = write_select_stmt_opt(&stmt.larg);
    (*node).rarg = write_select_stmt_opt(&stmt.rarg);
    node
}

unsafe fn write_select_stmt_opt(stmt: &Option<Box<protobuf::SelectStmt>>) -> *mut bindings_raw::SelectStmt {
    match stmt {
        Some(s) => write_select_stmt(s),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_into_clause_opt(ic: &Option<Box<protobuf::IntoClause>>) -> *mut bindings_raw::IntoClause {
    match ic {
        Some(into) => write_into_clause(into),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_into_clause(ic: &protobuf::IntoClause) -> *mut bindings_raw::IntoClause {
    let node = alloc_node::<bindings_raw::IntoClause>(bindings_raw::NodeTag_T_IntoClause);
    (*node).rel = write_range_var_ref(&ic.rel);
    (*node).colNames = write_node_list(&ic.col_names);
    (*node).accessMethod = pstrdup(&ic.access_method);
    (*node).options = write_node_list(&ic.options);
    (*node).onCommit = proto_enum_to_c(ic.on_commit);
    (*node).tableSpaceName = pstrdup(&ic.table_space_name);
    (*node).viewQuery = write_node_boxed(&ic.view_query);
    (*node).skipData = ic.skip_data;
    node
}

unsafe fn write_insert_stmt(stmt: &protobuf::InsertStmt) -> *mut bindings_raw::InsertStmt {
    let node = alloc_node::<bindings_raw::InsertStmt>(bindings_raw::NodeTag_T_InsertStmt);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).cols = write_node_list(&stmt.cols);
    (*node).selectStmt = write_node_boxed(&stmt.select_stmt);
    (*node).onConflictClause = write_on_conflict_clause_opt(&stmt.on_conflict_clause);
    (*node).returningList = write_node_list(&stmt.returning_list);
    (*node).withClause = write_with_clause_ref(&stmt.with_clause);
    (*node).override_ = proto_enum_to_c(stmt.r#override);
    node
}

unsafe fn write_on_conflict_clause_opt(oc: &Option<Box<protobuf::OnConflictClause>>) -> *mut bindings_raw::OnConflictClause {
    match oc {
        Some(clause) => write_on_conflict_clause(clause),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_on_conflict_clause(oc: &protobuf::OnConflictClause) -> *mut bindings_raw::OnConflictClause {
    let node = alloc_node::<bindings_raw::OnConflictClause>(bindings_raw::NodeTag_T_OnConflictClause);
    (*node).action = proto_enum_to_c(oc.action);
    (*node).infer = write_infer_clause_opt(&oc.infer);
    (*node).targetList = write_node_list(&oc.target_list);
    (*node).whereClause = write_node_boxed(&oc.where_clause);
    (*node).location = oc.location;
    node
}

unsafe fn write_infer_clause_opt(ic: &Option<Box<protobuf::InferClause>>) -> *mut bindings_raw::InferClause {
    match ic {
        Some(infer) => {
            let node = alloc_node::<bindings_raw::InferClause>(bindings_raw::NodeTag_T_InferClause);
            (*node).indexElems = write_node_list(&infer.index_elems);
            (*node).whereClause = write_node_boxed(&infer.where_clause);
            (*node).conname = pstrdup(&infer.conname);
            (*node).location = infer.location;
            node
        }
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_update_stmt(stmt: &protobuf::UpdateStmt) -> *mut bindings_raw::UpdateStmt {
    let node = alloc_node::<bindings_raw::UpdateStmt>(bindings_raw::NodeTag_T_UpdateStmt);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).targetList = write_node_list(&stmt.target_list);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).fromClause = write_node_list(&stmt.from_clause);
    (*node).returningList = write_node_list(&stmt.returning_list);
    (*node).withClause = write_with_clause_ref(&stmt.with_clause);
    node
}

unsafe fn write_delete_stmt(stmt: &protobuf::DeleteStmt) -> *mut bindings_raw::DeleteStmt {
    let node = alloc_node::<bindings_raw::DeleteStmt>(bindings_raw::NodeTag_T_DeleteStmt);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).usingClause = write_node_list(&stmt.using_clause);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).returningList = write_node_list(&stmt.returning_list);
    (*node).withClause = write_with_clause_ref(&stmt.with_clause);
    node
}

unsafe fn write_range_var(rv: &protobuf::RangeVar) -> *mut bindings_raw::RangeVar {
    let node = alloc_node::<bindings_raw::RangeVar>(bindings_raw::NodeTag_T_RangeVar);
    (*node).catalogname = pstrdup(&rv.catalogname);
    (*node).schemaname = pstrdup(&rv.schemaname);
    (*node).relname = pstrdup(&rv.relname);
    (*node).inh = rv.inh;
    (*node).relpersistence = if rv.relpersistence.is_empty() { 'p' as i8 } else { rv.relpersistence.chars().next().unwrap() as i8 };
    (*node).alias = write_alias_ref(&rv.alias);
    (*node).location = rv.location;
    node
}

unsafe fn write_range_var_opt(rv: &Option<Box<protobuf::RangeVar>>) -> *mut bindings_raw::RangeVar {
    match rv {
        Some(r) => write_range_var(r),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_range_var_ref(rv: &Option<protobuf::RangeVar>) -> *mut bindings_raw::RangeVar {
    match rv {
        Some(r) => write_range_var(r),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_alias(alias: &protobuf::Alias) -> *mut bindings_raw::Alias {
    let node = alloc_node::<bindings_raw::Alias>(bindings_raw::NodeTag_T_Alias);
    (*node).aliasname = pstrdup(&alias.aliasname);
    (*node).colnames = write_node_list(&alias.colnames);
    node
}

unsafe fn write_alias_opt(alias: &Option<Box<protobuf::Alias>>) -> *mut bindings_raw::Alias {
    match alias {
        Some(a) => write_alias(a),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_alias_ref(alias: &Option<protobuf::Alias>) -> *mut bindings_raw::Alias {
    match alias {
        Some(a) => write_alias(a),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_res_target(rt: &protobuf::ResTarget) -> *mut bindings_raw::ResTarget {
    let node = alloc_node::<bindings_raw::ResTarget>(bindings_raw::NodeTag_T_ResTarget);
    (*node).name = pstrdup(&rt.name);
    (*node).indirection = write_node_list(&rt.indirection);
    (*node).val = write_node_boxed(&rt.val);
    (*node).location = rt.location;
    node
}

unsafe fn write_column_ref(cr: &protobuf::ColumnRef) -> *mut bindings_raw::ColumnRef {
    let node = alloc_node::<bindings_raw::ColumnRef>(bindings_raw::NodeTag_T_ColumnRef);
    (*node).fields = write_node_list(&cr.fields);
    (*node).location = cr.location;
    node
}

unsafe fn write_a_const(ac: &protobuf::AConst) -> *mut bindings_raw::A_Const {
    let node = alloc_node::<bindings_raw::A_Const>(bindings_raw::NodeTag_T_A_Const);
    (*node).location = ac.location;
    (*node).isnull = ac.isnull;

    if let Some(val) = &ac.val {
        match val {
            protobuf::a_const::Val::Ival(i) => {
                (*node).val.ival.type_ = bindings_raw::NodeTag_T_Integer;
                (*node).val.ival.ival = i.ival;
            }
            protobuf::a_const::Val::Fval(f) => {
                (*node).val.fval.type_ = bindings_raw::NodeTag_T_Float;
                (*node).val.fval.fval = pstrdup(&f.fval);
            }
            protobuf::a_const::Val::Boolval(b) => {
                (*node).val.boolval.type_ = bindings_raw::NodeTag_T_Boolean;
                (*node).val.boolval.boolval = b.boolval;
            }
            protobuf::a_const::Val::Sval(s) => {
                (*node).val.sval.type_ = bindings_raw::NodeTag_T_String;
                (*node).val.sval.sval = pstrdup(&s.sval);
            }
            protobuf::a_const::Val::Bsval(bs) => {
                (*node).val.bsval.type_ = bindings_raw::NodeTag_T_BitString;
                (*node).val.bsval.bsval = pstrdup(&bs.bsval);
            }
        }
    }
    node
}

unsafe fn write_a_expr(expr: &protobuf::AExpr) -> *mut bindings_raw::A_Expr {
    let node = alloc_node::<bindings_raw::A_Expr>(bindings_raw::NodeTag_T_A_Expr);
    (*node).kind = proto_enum_to_c(expr.kind);
    (*node).name = write_node_list(&expr.name);
    (*node).lexpr = write_node_boxed(&expr.lexpr);
    (*node).rexpr = write_node_boxed(&expr.rexpr);
    (*node).location = expr.location;
    node
}

unsafe fn write_func_call(fc: &protobuf::FuncCall) -> *mut bindings_raw::FuncCall {
    let node = alloc_node::<bindings_raw::FuncCall>(bindings_raw::NodeTag_T_FuncCall);
    (*node).funcname = write_node_list(&fc.funcname);
    (*node).args = write_node_list(&fc.args);
    (*node).agg_order = write_node_list(&fc.agg_order);
    (*node).agg_filter = write_node_boxed(&fc.agg_filter);
    (*node).over = write_window_def_opt(&fc.over);
    (*node).agg_within_group = fc.agg_within_group;
    (*node).agg_star = fc.agg_star;
    (*node).agg_distinct = fc.agg_distinct;
    (*node).func_variadic = fc.func_variadic;
    (*node).funcformat = proto_enum_to_c(fc.funcformat);
    (*node).location = fc.location;
    node
}

unsafe fn write_window_def_opt(wd: &Option<Box<protobuf::WindowDef>>) -> *mut bindings_raw::WindowDef {
    match wd {
        Some(w) => write_window_def(w),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_window_def(wd: &protobuf::WindowDef) -> *mut bindings_raw::WindowDef {
    let node = alloc_node::<bindings_raw::WindowDef>(bindings_raw::NodeTag_T_WindowDef);
    (*node).name = pstrdup(&wd.name);
    (*node).refname = pstrdup(&wd.refname);
    (*node).partitionClause = write_node_list(&wd.partition_clause);
    (*node).orderClause = write_node_list(&wd.order_clause);
    (*node).frameOptions = wd.frame_options;
    (*node).startOffset = write_node_boxed(&wd.start_offset);
    (*node).endOffset = write_node_boxed(&wd.end_offset);
    (*node).location = wd.location;
    node
}

unsafe fn write_string(s: &protobuf::String) -> *mut bindings_raw::String {
    let node = alloc_node::<bindings_raw::String>(bindings_raw::NodeTag_T_String);
    (*node).sval = pstrdup(&s.sval);
    node
}

unsafe fn write_integer(i: &protobuf::Integer) -> *mut bindings_raw::Integer {
    let node = alloc_node::<bindings_raw::Integer>(bindings_raw::NodeTag_T_Integer);
    (*node).ival = i.ival;
    node
}

unsafe fn write_float(f: &protobuf::Float) -> *mut bindings_raw::Float {
    let node = alloc_node::<bindings_raw::Float>(bindings_raw::NodeTag_T_Float);
    (*node).fval = pstrdup(&f.fval);
    node
}

unsafe fn write_boolean(b: &protobuf::Boolean) -> *mut bindings_raw::Boolean {
    let node = alloc_node::<bindings_raw::Boolean>(bindings_raw::NodeTag_T_Boolean);
    (*node).boolval = b.boolval;
    node
}

unsafe fn write_bit_string(bs: &protobuf::BitString) -> *mut bindings_raw::BitString {
    let node = alloc_node::<bindings_raw::BitString>(bindings_raw::NodeTag_T_BitString);
    (*node).bsval = pstrdup(&bs.bsval);
    node
}

unsafe fn write_null() -> *mut bindings_raw::Node {
    // A_Const with isnull=true represents NULL
    let node = alloc_node::<bindings_raw::A_Const>(bindings_raw::NodeTag_T_A_Const);
    (*node).isnull = true;
    (*node).location = -1;
    node as *mut bindings_raw::Node
}

unsafe fn write_list(l: &protobuf::List) -> *mut bindings_raw::List {
    write_node_list(&l.items)
}

unsafe fn write_a_star() -> *mut bindings_raw::A_Star {
    alloc_node::<bindings_raw::A_Star>(bindings_raw::NodeTag_T_A_Star)
}

unsafe fn write_join_expr(je: &protobuf::JoinExpr) -> *mut bindings_raw::JoinExpr {
    let node = alloc_node::<bindings_raw::JoinExpr>(bindings_raw::NodeTag_T_JoinExpr);
    (*node).jointype = proto_enum_to_c(je.jointype);
    (*node).isNatural = je.is_natural;
    (*node).larg = write_node_boxed(&je.larg);
    (*node).rarg = write_node_boxed(&je.rarg);
    (*node).usingClause = write_node_list(&je.using_clause);
    (*node).join_using_alias = write_alias_ref(&je.join_using_alias);
    (*node).quals = write_node_boxed(&je.quals);
    (*node).alias = write_alias_ref(&je.alias);
    (*node).rtindex = je.rtindex;
    node
}

unsafe fn write_sort_by(sb: &protobuf::SortBy) -> *mut bindings_raw::SortBy {
    let node = alloc_node::<bindings_raw::SortBy>(bindings_raw::NodeTag_T_SortBy);
    (*node).node = write_node_boxed(&sb.node);
    (*node).sortby_dir = proto_enum_to_c(sb.sortby_dir);
    (*node).sortby_nulls = proto_enum_to_c(sb.sortby_nulls);
    (*node).useOp = write_node_list(&sb.use_op);
    (*node).location = sb.location;
    node
}

unsafe fn write_type_cast(tc: &protobuf::TypeCast) -> *mut bindings_raw::TypeCast {
    let node = alloc_node::<bindings_raw::TypeCast>(bindings_raw::NodeTag_T_TypeCast);
    (*node).arg = write_node_boxed(&tc.arg);
    (*node).typeName = write_type_name_ref(&tc.type_name);
    (*node).location = tc.location;
    node
}

unsafe fn write_type_name_opt(tn: &Option<Box<protobuf::TypeName>>) -> *mut bindings_raw::TypeName {
    match tn {
        Some(t) => write_type_name(t),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_type_name_ref(tn: &Option<protobuf::TypeName>) -> *mut bindings_raw::TypeName {
    match tn {
        Some(t) => write_type_name(t),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_type_name(tn: &protobuf::TypeName) -> *mut bindings_raw::TypeName {
    let node = alloc_node::<bindings_raw::TypeName>(bindings_raw::NodeTag_T_TypeName);
    (*node).names = write_node_list(&tn.names);
    (*node).typeOid = tn.type_oid;
    (*node).setof = tn.setof;
    (*node).pct_type = tn.pct_type;
    (*node).typmods = write_node_list(&tn.typmods);
    (*node).typemod = tn.typemod;
    (*node).arrayBounds = write_node_list(&tn.array_bounds);
    (*node).location = tn.location;
    node
}

unsafe fn write_param_ref(pr: &protobuf::ParamRef) -> *mut bindings_raw::ParamRef {
    let node = alloc_node::<bindings_raw::ParamRef>(bindings_raw::NodeTag_T_ParamRef);
    (*node).number = pr.number;
    (*node).location = pr.location;
    node
}

unsafe fn write_null_test(nt: &protobuf::NullTest) -> *mut bindings_raw::NullTest {
    let node = alloc_node::<bindings_raw::NullTest>(bindings_raw::NodeTag_T_NullTest);
    (*node).arg = write_node_boxed(&nt.arg) as *mut bindings_raw::Expr;
    (*node).nulltesttype = proto_enum_to_c(nt.nulltesttype);
    (*node).argisrow = nt.argisrow;
    (*node).location = nt.location;
    node
}

unsafe fn write_bool_expr(be: &protobuf::BoolExpr) -> *mut bindings_raw::BoolExpr {
    let node = alloc_node::<bindings_raw::BoolExpr>(bindings_raw::NodeTag_T_BoolExpr);
    (*node).boolop = proto_enum_to_c(be.boolop);
    (*node).args = write_node_list(&be.args);
    (*node).location = be.location;
    node
}

unsafe fn write_sub_link(sl: &protobuf::SubLink) -> *mut bindings_raw::SubLink {
    let node = alloc_node::<bindings_raw::SubLink>(bindings_raw::NodeTag_T_SubLink);
    (*node).subLinkType = proto_enum_to_c(sl.sub_link_type);
    (*node).subLinkId = sl.sub_link_id;
    (*node).testexpr = write_node_boxed(&sl.testexpr);
    (*node).operName = write_node_list(&sl.oper_name);
    (*node).subselect = write_node_boxed(&sl.subselect);
    (*node).location = sl.location;
    node
}

unsafe fn write_range_subselect(rs: &protobuf::RangeSubselect) -> *mut bindings_raw::RangeSubselect {
    let node = alloc_node::<bindings_raw::RangeSubselect>(bindings_raw::NodeTag_T_RangeSubselect);
    (*node).lateral = rs.lateral;
    (*node).subquery = write_node_boxed(&rs.subquery);
    (*node).alias = write_alias_ref(&rs.alias);
    node
}

unsafe fn write_common_table_expr(cte: &protobuf::CommonTableExpr) -> *mut bindings_raw::CommonTableExpr {
    let node = alloc_node::<bindings_raw::CommonTableExpr>(bindings_raw::NodeTag_T_CommonTableExpr);
    (*node).ctename = pstrdup(&cte.ctename);
    (*node).aliascolnames = write_node_list(&cte.aliascolnames);
    (*node).ctematerialized = proto_enum_to_c(cte.ctematerialized);
    (*node).ctequery = write_node_boxed(&cte.ctequery);
    (*node).search_clause = write_cte_search_clause_opt(&cte.search_clause);
    (*node).cycle_clause = write_cte_cycle_clause_opt(&cte.cycle_clause);
    (*node).location = cte.location;
    (*node).cterecursive = cte.cterecursive;
    (*node).cterefcount = cte.cterefcount;
    (*node).ctecolnames = write_node_list(&cte.ctecolnames);
    // ctecoltypmods is a list of integers, handle separately if needed
    node
}

unsafe fn write_cte_search_clause_opt(sc: &Option<protobuf::CteSearchClause>) -> *mut bindings_raw::CTESearchClause {
    match sc {
        Some(s) => {
            let node = alloc_node::<bindings_raw::CTESearchClause>(bindings_raw::NodeTag_T_CTESearchClause);
            (*node).search_col_list = write_node_list(&s.search_col_list);
            (*node).search_breadth_first = s.search_breadth_first;
            (*node).search_seq_column = pstrdup(&s.search_seq_column);
            (*node).location = s.location;
            node
        }
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_cte_cycle_clause_opt(cc: &Option<Box<protobuf::CteCycleClause>>) -> *mut bindings_raw::CTECycleClause {
    match cc {
        Some(c) => {
            let node = alloc_node::<bindings_raw::CTECycleClause>(bindings_raw::NodeTag_T_CTECycleClause);
            (*node).cycle_col_list = write_node_list(&c.cycle_col_list);
            (*node).cycle_mark_column = pstrdup(&c.cycle_mark_column);
            (*node).cycle_mark_value = write_node_boxed(&c.cycle_mark_value);
            (*node).cycle_mark_default = write_node_boxed(&c.cycle_mark_default);
            (*node).cycle_path_column = pstrdup(&c.cycle_path_column);
            (*node).location = c.location;
            node
        }
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_with_clause(wc: &protobuf::WithClause) -> *mut bindings_raw::WithClause {
    let node = alloc_node::<bindings_raw::WithClause>(bindings_raw::NodeTag_T_WithClause);
    (*node).ctes = write_node_list(&wc.ctes);
    (*node).recursive = wc.recursive;
    (*node).location = wc.location;
    node
}

unsafe fn write_with_clause_opt(wc: &Option<Box<protobuf::WithClause>>) -> *mut bindings_raw::WithClause {
    match wc {
        Some(w) => write_with_clause(w),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_with_clause_ref(wc: &Option<protobuf::WithClause>) -> *mut bindings_raw::WithClause {
    match wc {
        Some(w) => write_with_clause(w),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_grouping_set(gs: &protobuf::GroupingSet) -> *mut bindings_raw::GroupingSet {
    let node = alloc_node::<bindings_raw::GroupingSet>(bindings_raw::NodeTag_T_GroupingSet);
    (*node).kind = proto_enum_to_c(gs.kind);
    (*node).content = write_node_list(&gs.content);
    (*node).location = gs.location;
    node
}

unsafe fn write_coalesce_expr(ce: &protobuf::CoalesceExpr) -> *mut bindings_raw::CoalesceExpr {
    let node = alloc_node::<bindings_raw::CoalesceExpr>(bindings_raw::NodeTag_T_CoalesceExpr);
    (*node).coalescetype = ce.coalescetype;
    (*node).coalescecollid = ce.coalescecollid;
    (*node).args = write_node_list(&ce.args);
    (*node).location = ce.location;
    node
}

unsafe fn write_case_expr(ce: &protobuf::CaseExpr) -> *mut bindings_raw::CaseExpr {
    let node = alloc_node::<bindings_raw::CaseExpr>(bindings_raw::NodeTag_T_CaseExpr);
    (*node).casetype = ce.casetype;
    (*node).casecollid = ce.casecollid;
    (*node).arg = write_node_boxed(&ce.arg) as *mut bindings_raw::Expr;
    (*node).args = write_node_list(&ce.args);
    (*node).defresult = write_node_boxed(&ce.defresult) as *mut bindings_raw::Expr;
    (*node).location = ce.location;
    node
}

unsafe fn write_case_when(cw: &protobuf::CaseWhen) -> *mut bindings_raw::CaseWhen {
    let node = alloc_node::<bindings_raw::CaseWhen>(bindings_raw::NodeTag_T_CaseWhen);
    (*node).expr = write_node_boxed(&cw.expr) as *mut bindings_raw::Expr;
    (*node).result = write_node_boxed(&cw.result) as *mut bindings_raw::Expr;
    (*node).location = cw.location;
    node
}

unsafe fn write_set_to_default() -> *mut bindings_raw::SetToDefault {
    let node = alloc_node::<bindings_raw::SetToDefault>(bindings_raw::NodeTag_T_SetToDefault);
    (*node).location = -1;
    node
}

unsafe fn write_locking_clause(lc: &protobuf::LockingClause) -> *mut bindings_raw::LockingClause {
    let node = alloc_node::<bindings_raw::LockingClause>(bindings_raw::NodeTag_T_LockingClause);
    (*node).lockedRels = write_node_list(&lc.locked_rels);
    (*node).strength = proto_enum_to_c(lc.strength);
    (*node).waitPolicy = proto_enum_to_c(lc.wait_policy);
    node
}

unsafe fn write_range_function(rf: &protobuf::RangeFunction) -> *mut bindings_raw::RangeFunction {
    let node = alloc_node::<bindings_raw::RangeFunction>(bindings_raw::NodeTag_T_RangeFunction);
    (*node).lateral = rf.lateral;
    (*node).ordinality = rf.ordinality;
    (*node).is_rowsfrom = rf.is_rowsfrom;
    // PostgreSQL expects functions to be a list of 2-element lists: [FuncExpr, coldeflist]
    // The protobuf stores each function as a List node containing just the FuncCall
    // We need to ensure each inner list has exactly 2 elements
    (*node).functions = write_range_function_list(&rf.functions);
    (*node).alias = write_alias_ref(&rf.alias);
    (*node).coldeflist = write_node_list(&rf.coldeflist);
    node
}

/// Writes the functions list for a RangeFunction.
/// PostgreSQL expects a list of 2-element lists: [FuncExpr, coldeflist].
/// The protobuf may store these as List nodes with only the function expression.
fn write_range_function_list(nodes: &[protobuf::Node]) -> *mut bindings_raw::List {
    if nodes.is_empty() {
        return std::ptr::null_mut();
    }

    let mut list: *mut std::ffi::c_void = std::ptr::null_mut();

    for node in nodes {
        // Each node should be a List containing the function expression (and optionally coldeflist)
        // We need to ensure it has exactly 2 elements
        let inner_list = if let Some(protobuf::node::Node::List(l)) = &node.node {
            // It's a List node - ensure it has 2 elements
            let func_expr = if !l.items.is_empty() { write_node(&l.items[0]) } else { std::ptr::null_mut() };
            let coldeflist = if l.items.len() > 1 { write_node(&l.items[1]) } else { std::ptr::null_mut() };
            // Create a 2-element list
            unsafe {
                let inner = bindings_raw::pg_query_list_make1(func_expr as *mut std::ffi::c_void);
                bindings_raw::pg_query_list_append(inner, coldeflist as *mut std::ffi::c_void)
            }
        } else {
            // It's not a List node (shouldn't happen, but handle it)
            // Wrap the node in a 2-element list
            let func_expr = write_node(node);
            unsafe {
                let inner = bindings_raw::pg_query_list_make1(func_expr as *mut std::ffi::c_void);
                bindings_raw::pg_query_list_append(inner, std::ptr::null_mut())
            }
        };

        if list.is_null() {
            list = unsafe { bindings_raw::pg_query_list_make1(inner_list) };
        } else {
            list = unsafe { bindings_raw::pg_query_list_append(list, inner_list) };
        }
    }

    list as *mut bindings_raw::List
}

unsafe fn write_index_elem(ie: &protobuf::IndexElem) -> *mut bindings_raw::IndexElem {
    let node = alloc_node::<bindings_raw::IndexElem>(bindings_raw::NodeTag_T_IndexElem);
    (*node).name = pstrdup(&ie.name);
    (*node).expr = write_node_boxed(&ie.expr);
    (*node).indexcolname = pstrdup(&ie.indexcolname);
    (*node).collation = write_node_list(&ie.collation);
    (*node).opclass = write_node_list(&ie.opclass);
    (*node).opclassopts = write_node_list(&ie.opclassopts);
    (*node).ordering = proto_enum_to_c(ie.ordering);
    (*node).nulls_ordering = proto_enum_to_c(ie.nulls_ordering);
    node
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deparse_raw_empty() {
        let result = protobuf::ParseResult { version: 170007, stmts: vec![] };
        let sql = deparse_raw(&result).unwrap();
        assert_eq!(sql, "");
    }
}

/// Writes values lists (list of lists) for INSERT ... VALUES
unsafe fn write_values_lists(values: &[protobuf::Node]) -> *mut bindings_raw::List {
    if values.is_empty() {
        return std::ptr::null_mut();
    }

    let mut outer_list: *mut std::ffi::c_void = std::ptr::null_mut();

    for value_node in values {
        // Each value_node should be a List node containing the values for one row
        if let Some(protobuf::node::Node::List(inner_list)) = &value_node.node {
            let c_inner_list = write_node_list(&inner_list.items);
            if outer_list.is_null() {
                outer_list = bindings_raw::pg_query_list_make1(c_inner_list as *mut std::ffi::c_void);
            } else {
                outer_list = bindings_raw::pg_query_list_append(outer_list, c_inner_list as *mut std::ffi::c_void);
            }
        }
    }

    outer_list as *mut bindings_raw::List
}

// =============================================================================
// Additional Statement Writers
// =============================================================================

unsafe fn write_drop_stmt(stmt: &protobuf::DropStmt) -> *mut bindings_raw::DropStmt {
    let node = alloc_node::<bindings_raw::DropStmt>(bindings_raw::NodeTag_T_DropStmt);
    (*node).objects = write_node_list(&stmt.objects);
    (*node).removeType = proto_enum_to_c(stmt.remove_type);
    (*node).behavior = proto_enum_to_c(stmt.behavior);
    (*node).missing_ok = stmt.missing_ok;
    (*node).concurrent = stmt.concurrent;
    node
}

unsafe fn write_object_with_args(owa: &protobuf::ObjectWithArgs) -> *mut bindings_raw::ObjectWithArgs {
    let node = alloc_node::<bindings_raw::ObjectWithArgs>(bindings_raw::NodeTag_T_ObjectWithArgs);
    (*node).objname = write_node_list(&owa.objname);
    (*node).objargs = write_node_list(&owa.objargs);
    (*node).objfuncargs = write_node_list(&owa.objfuncargs);
    (*node).args_unspecified = owa.args_unspecified;
    node
}

unsafe fn write_function_parameter(fp: &protobuf::FunctionParameter) -> *mut bindings_raw::FunctionParameter {
    let node = alloc_node::<bindings_raw::FunctionParameter>(bindings_raw::NodeTag_T_FunctionParameter);
    (*node).name = pstrdup(&fp.name);
    (*node).argType = write_type_name_ptr(&fp.arg_type);
    (*node).mode = proto_function_param_mode(fp.mode);
    (*node).defexpr = write_node_boxed(&fp.defexpr);
    node
}

fn proto_function_param_mode(mode: i32) -> bindings_raw::FunctionParameterMode {
    match mode {
        1 => bindings_raw::FunctionParameterMode_FUNC_PARAM_IN,
        2 => bindings_raw::FunctionParameterMode_FUNC_PARAM_OUT,
        3 => bindings_raw::FunctionParameterMode_FUNC_PARAM_INOUT,
        4 => bindings_raw::FunctionParameterMode_FUNC_PARAM_VARIADIC,
        5 => bindings_raw::FunctionParameterMode_FUNC_PARAM_TABLE,
        6 => bindings_raw::FunctionParameterMode_FUNC_PARAM_DEFAULT,
        _ => bindings_raw::FunctionParameterMode_FUNC_PARAM_IN,
    }
}

unsafe fn write_type_name_ptr(tn: &Option<protobuf::TypeName>) -> *mut bindings_raw::TypeName {
    match tn {
        Some(tn) => write_type_name(tn),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_truncate_stmt(stmt: &protobuf::TruncateStmt) -> *mut bindings_raw::TruncateStmt {
    let node = alloc_node::<bindings_raw::TruncateStmt>(bindings_raw::NodeTag_T_TruncateStmt);
    (*node).relations = write_node_list(&stmt.relations);
    (*node).restart_seqs = stmt.restart_seqs;
    (*node).behavior = proto_enum_to_c(stmt.behavior);
    node
}

unsafe fn write_create_stmt(stmt: &protobuf::CreateStmt) -> *mut bindings_raw::CreateStmt {
    let node = alloc_node::<bindings_raw::CreateStmt>(bindings_raw::NodeTag_T_CreateStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).tableElts = write_node_list(&stmt.table_elts);
    (*node).inhRelations = write_node_list(&stmt.inh_relations);
    (*node).partbound = std::ptr::null_mut(); // Complex type, skip for now
    (*node).partspec = std::ptr::null_mut(); // Complex type, skip for now
    (*node).ofTypename = write_type_name_ptr(&stmt.of_typename);
    (*node).constraints = write_node_list(&stmt.constraints);
    (*node).options = write_node_list(&stmt.options);
    (*node).oncommit = proto_enum_to_c(stmt.oncommit);
    (*node).tablespacename = pstrdup(&stmt.tablespacename);
    (*node).accessMethod = pstrdup(&stmt.access_method);
    (*node).if_not_exists = stmt.if_not_exists;
    node
}

unsafe fn write_range_var_ptr(rv: &Option<protobuf::RangeVar>) -> *mut bindings_raw::RangeVar {
    match rv {
        Some(rv) => write_range_var(rv),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_alter_table_stmt(stmt: &protobuf::AlterTableStmt) -> *mut bindings_raw::AlterTableStmt {
    let node = alloc_node::<bindings_raw::AlterTableStmt>(bindings_raw::NodeTag_T_AlterTableStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).cmds = write_node_list(&stmt.cmds);
    (*node).objtype = proto_enum_to_c(stmt.objtype);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_alter_table_cmd(cmd: &protobuf::AlterTableCmd) -> *mut bindings_raw::AlterTableCmd {
    let node = alloc_node::<bindings_raw::AlterTableCmd>(bindings_raw::NodeTag_T_AlterTableCmd);
    (*node).subtype = proto_enum_to_c(cmd.subtype);
    (*node).name = pstrdup(&cmd.name);
    (*node).num = cmd.num as i16;
    (*node).newowner = std::ptr::null_mut(); // RoleSpec, complex
    (*node).def = write_node_boxed(&cmd.def);
    (*node).behavior = proto_enum_to_c(cmd.behavior);
    (*node).missing_ok = cmd.missing_ok;
    (*node).recurse = cmd.recurse;
    node
}

unsafe fn write_column_def(cd: &protobuf::ColumnDef) -> *mut bindings_raw::ColumnDef {
    let node = alloc_node::<bindings_raw::ColumnDef>(bindings_raw::NodeTag_T_ColumnDef);
    (*node).colname = pstrdup(&cd.colname);
    (*node).typeName = write_type_name_ptr(&cd.type_name);
    (*node).compression = pstrdup(&cd.compression);
    (*node).inhcount = cd.inhcount;
    (*node).is_local = cd.is_local;
    (*node).is_not_null = cd.is_not_null;
    (*node).is_from_type = cd.is_from_type;
    (*node).storage = if cd.storage.is_empty() { 0 } else { cd.storage.as_bytes()[0] as i8 };
    (*node).raw_default = write_node_boxed(&cd.raw_default);
    (*node).cooked_default = write_node_boxed(&cd.cooked_default);
    (*node).identity = if cd.identity.is_empty() { 0 } else { cd.identity.as_bytes()[0] as i8 };
    (*node).identitySequence = std::ptr::null_mut();
    (*node).generated = if cd.generated.is_empty() { 0 } else { cd.generated.as_bytes()[0] as i8 };
    (*node).collClause = std::ptr::null_mut();
    (*node).collOid = cd.coll_oid;
    (*node).constraints = write_node_list(&cd.constraints);
    (*node).fdwoptions = write_node_list(&cd.fdwoptions);
    (*node).location = cd.location;
    node
}

unsafe fn write_constraint(c: &protobuf::Constraint) -> *mut bindings_raw::Constraint {
    let node = alloc_node::<bindings_raw::Constraint>(bindings_raw::NodeTag_T_Constraint);
    (*node).contype = proto_enum_to_c(c.contype);
    (*node).conname = pstrdup(&c.conname);
    (*node).deferrable = c.deferrable;
    (*node).initdeferred = c.initdeferred;
    (*node).skip_validation = c.skip_validation;
    (*node).initially_valid = c.initially_valid;
    (*node).is_no_inherit = c.is_no_inherit;
    (*node).raw_expr = write_node_boxed(&c.raw_expr);
    (*node).cooked_expr = pstrdup(&c.cooked_expr);
    (*node).generated_when = if c.generated_when.is_empty() { 0 } else { c.generated_when.as_bytes()[0] as i8 };
    (*node).nulls_not_distinct = c.nulls_not_distinct;
    (*node).keys = write_node_list(&c.keys);
    (*node).including = write_node_list(&c.including);
    (*node).exclusions = write_node_list(&c.exclusions);
    (*node).options = write_node_list(&c.options);
    (*node).indexname = pstrdup(&c.indexname);
    (*node).indexspace = pstrdup(&c.indexspace);
    (*node).reset_default_tblspc = c.reset_default_tblspc;
    (*node).access_method = pstrdup(&c.access_method);
    (*node).where_clause = write_node_boxed(&c.where_clause);
    (*node).pktable = write_range_var_ptr(&c.pktable);
    (*node).fk_attrs = write_node_list(&c.fk_attrs);
    (*node).pk_attrs = write_node_list(&c.pk_attrs);
    (*node).fk_matchtype = if c.fk_matchtype.is_empty() { 0 } else { c.fk_matchtype.as_bytes()[0] as i8 };
    (*node).fk_upd_action = if c.fk_upd_action.is_empty() { 0 } else { c.fk_upd_action.as_bytes()[0] as i8 };
    (*node).fk_del_action = if c.fk_del_action.is_empty() { 0 } else { c.fk_del_action.as_bytes()[0] as i8 };
    (*node).fk_del_set_cols = write_node_list(&c.fk_del_set_cols);
    (*node).old_conpfeqop = write_node_list(&c.old_conpfeqop);
    (*node).old_pktable_oid = c.old_pktable_oid;
    (*node).location = c.location;
    node
}

unsafe fn write_index_stmt(stmt: &protobuf::IndexStmt) -> *mut bindings_raw::IndexStmt {
    let node = alloc_node::<bindings_raw::IndexStmt>(bindings_raw::NodeTag_T_IndexStmt);
    (*node).idxname = pstrdup(&stmt.idxname);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).accessMethod = pstrdup(&stmt.access_method);
    (*node).tableSpace = pstrdup(&stmt.table_space);
    (*node).indexParams = write_node_list(&stmt.index_params);
    (*node).indexIncludingParams = write_node_list(&stmt.index_including_params);
    (*node).options = write_node_list(&stmt.options);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).excludeOpNames = write_node_list(&stmt.exclude_op_names);
    (*node).idxcomment = pstrdup(&stmt.idxcomment);
    (*node).indexOid = stmt.index_oid;
    (*node).oldNumber = stmt.old_number;
    (*node).oldCreateSubid = stmt.old_create_subid;
    (*node).oldFirstRelfilelocatorSubid = stmt.old_first_relfilelocator_subid;
    (*node).unique = stmt.unique;
    (*node).nulls_not_distinct = stmt.nulls_not_distinct;
    (*node).primary = stmt.primary;
    (*node).isconstraint = stmt.isconstraint;
    (*node).deferrable = stmt.deferrable;
    (*node).initdeferred = stmt.initdeferred;
    (*node).transformed = stmt.transformed;
    (*node).concurrent = stmt.concurrent;
    (*node).if_not_exists = stmt.if_not_exists;
    (*node).reset_default_tblspc = stmt.reset_default_tblspc;
    node
}

unsafe fn write_view_stmt(stmt: &protobuf::ViewStmt) -> *mut bindings_raw::ViewStmt {
    let node = alloc_node::<bindings_raw::ViewStmt>(bindings_raw::NodeTag_T_ViewStmt);
    (*node).view = write_range_var_ptr(&stmt.view);
    (*node).aliases = write_node_list(&stmt.aliases);
    (*node).query = write_node_boxed(&stmt.query);
    (*node).replace = stmt.replace;
    (*node).options = write_node_list(&stmt.options);
    (*node).withCheckOption = proto_enum_to_c(stmt.with_check_option);
    node
}

unsafe fn write_transaction_stmt(stmt: &protobuf::TransactionStmt) -> *mut bindings_raw::TransactionStmt {
    let node = alloc_node::<bindings_raw::TransactionStmt>(bindings_raw::NodeTag_T_TransactionStmt);
    (*node).kind = proto_enum_to_c(stmt.kind);
    (*node).options = write_node_list(&stmt.options);
    (*node).savepoint_name = pstrdup(&stmt.savepoint_name);
    (*node).gid = pstrdup(&stmt.gid);
    (*node).chain = stmt.chain;
    (*node).location = stmt.location;
    node
}

unsafe fn write_copy_stmt(stmt: &protobuf::CopyStmt) -> *mut bindings_raw::CopyStmt {
    let node = alloc_node::<bindings_raw::CopyStmt>(bindings_raw::NodeTag_T_CopyStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).query = write_node_boxed(&stmt.query);
    (*node).attlist = write_node_list(&stmt.attlist);
    (*node).is_from = stmt.is_from;
    (*node).is_program = stmt.is_program;
    (*node).filename = pstrdup(&stmt.filename);
    (*node).options = write_node_list(&stmt.options);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    node
}

unsafe fn write_explain_stmt(stmt: &protobuf::ExplainStmt) -> *mut bindings_raw::ExplainStmt {
    let node = alloc_node::<bindings_raw::ExplainStmt>(bindings_raw::NodeTag_T_ExplainStmt);
    (*node).query = write_node_boxed(&stmt.query);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_create_table_as_stmt(stmt: &protobuf::CreateTableAsStmt) -> *mut bindings_raw::CreateTableAsStmt {
    let node = alloc_node::<bindings_raw::CreateTableAsStmt>(bindings_raw::NodeTag_T_CreateTableAsStmt);
    (*node).query = write_node_boxed(&stmt.query);
    (*node).into = if let Some(ref into) = stmt.into { write_into_clause(into) } else { std::ptr::null_mut() };
    (*node).objtype = proto_enum_to_c(stmt.objtype);
    (*node).is_select_into = stmt.is_select_into;
    (*node).if_not_exists = stmt.if_not_exists;
    node
}

unsafe fn write_refresh_mat_view_stmt(stmt: &protobuf::RefreshMatViewStmt) -> *mut bindings_raw::RefreshMatViewStmt {
    let node = alloc_node::<bindings_raw::RefreshMatViewStmt>(bindings_raw::NodeTag_T_RefreshMatViewStmt);
    (*node).concurrent = stmt.concurrent;
    (*node).skipData = stmt.skip_data;
    (*node).relation = write_range_var_ref(&stmt.relation);
    node
}

unsafe fn write_vacuum_relation(vr: &protobuf::VacuumRelation) -> *mut bindings_raw::VacuumRelation {
    let node = alloc_node::<bindings_raw::VacuumRelation>(bindings_raw::NodeTag_T_VacuumRelation);
    (*node).relation = write_range_var_ref(&vr.relation);
    (*node).oid = vr.oid;
    (*node).va_cols = write_node_list(&vr.va_cols);
    node
}

unsafe fn write_vacuum_stmt(stmt: &protobuf::VacuumStmt) -> *mut bindings_raw::VacuumStmt {
    let node = alloc_node::<bindings_raw::VacuumStmt>(bindings_raw::NodeTag_T_VacuumStmt);
    (*node).options = write_node_list(&stmt.options);
    (*node).rels = write_node_list(&stmt.rels);
    (*node).is_vacuumcmd = stmt.is_vacuumcmd;
    node
}

unsafe fn write_lock_stmt(stmt: &protobuf::LockStmt) -> *mut bindings_raw::LockStmt {
    let node = alloc_node::<bindings_raw::LockStmt>(bindings_raw::NodeTag_T_LockStmt);
    (*node).relations = write_node_list(&stmt.relations);
    (*node).mode = stmt.mode;
    (*node).nowait = stmt.nowait;
    node
}

unsafe fn write_create_schema_stmt(stmt: &protobuf::CreateSchemaStmt) -> *mut bindings_raw::CreateSchemaStmt {
    let node = alloc_node::<bindings_raw::CreateSchemaStmt>(bindings_raw::NodeTag_T_CreateSchemaStmt);
    (*node).schemaname = pstrdup(&stmt.schemaname);
    (*node).authrole = if let Some(ref role) = stmt.authrole { write_role_spec(role) } else { std::ptr::null_mut() };
    (*node).schemaElts = write_node_list(&stmt.schema_elts);
    (*node).if_not_exists = stmt.if_not_exists;
    node
}

unsafe fn write_variable_set_stmt(stmt: &protobuf::VariableSetStmt) -> *mut bindings_raw::VariableSetStmt {
    let node = alloc_node::<bindings_raw::VariableSetStmt>(bindings_raw::NodeTag_T_VariableSetStmt);
    (*node).kind = proto_enum_to_c(stmt.kind);
    (*node).name = pstrdup(&stmt.name);
    (*node).args = write_node_list(&stmt.args);
    (*node).is_local = stmt.is_local;
    node
}

unsafe fn write_variable_show_stmt(stmt: &protobuf::VariableShowStmt) -> *mut bindings_raw::VariableShowStmt {
    let node = alloc_node::<bindings_raw::VariableShowStmt>(bindings_raw::NodeTag_T_VariableShowStmt);
    (*node).name = pstrdup(&stmt.name);
    node
}

unsafe fn write_rename_stmt(stmt: &protobuf::RenameStmt) -> *mut bindings_raw::RenameStmt {
    let node = alloc_node::<bindings_raw::RenameStmt>(bindings_raw::NodeTag_T_RenameStmt);
    (*node).renameType = proto_enum_to_c(stmt.rename_type);
    (*node).relationType = proto_enum_to_c(stmt.relation_type);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).object = write_node_boxed(&stmt.object);
    (*node).subname = pstrdup(&stmt.subname);
    (*node).newname = pstrdup(&stmt.newname);
    (*node).behavior = proto_enum_to_c(stmt.behavior);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_grant_stmt(stmt: &protobuf::GrantStmt) -> *mut bindings_raw::GrantStmt {
    let node = alloc_node::<bindings_raw::GrantStmt>(bindings_raw::NodeTag_T_GrantStmt);
    (*node).is_grant = stmt.is_grant;
    (*node).targtype = proto_enum_to_c(stmt.targtype);
    (*node).objtype = proto_enum_to_c(stmt.objtype);
    (*node).objects = write_node_list(&stmt.objects);
    (*node).privileges = write_node_list(&stmt.privileges);
    (*node).grantees = write_node_list(&stmt.grantees);
    (*node).grant_option = stmt.grant_option;
    (*node).grantor = std::ptr::null_mut(); // RoleSpec, complex
    (*node).behavior = proto_enum_to_c(stmt.behavior);
    node
}

unsafe fn write_role_spec(rs: &protobuf::RoleSpec) -> *mut bindings_raw::RoleSpec {
    let node = alloc_node::<bindings_raw::RoleSpec>(bindings_raw::NodeTag_T_RoleSpec);
    (*node).roletype = proto_enum_to_c(rs.roletype);
    (*node).rolename = pstrdup(&rs.rolename);
    (*node).location = rs.location;
    node
}

unsafe fn write_access_priv(ap: &protobuf::AccessPriv) -> *mut bindings_raw::AccessPriv {
    let node = alloc_node::<bindings_raw::AccessPriv>(bindings_raw::NodeTag_T_AccessPriv);
    (*node).priv_name = pstrdup(&ap.priv_name);
    (*node).cols = write_node_list(&ap.cols);
    node
}

unsafe fn write_create_function_stmt(stmt: &protobuf::CreateFunctionStmt) -> *mut bindings_raw::CreateFunctionStmt {
    let node = alloc_node::<bindings_raw::CreateFunctionStmt>(bindings_raw::NodeTag_T_CreateFunctionStmt);
    (*node).is_procedure = stmt.is_procedure;
    (*node).replace = stmt.replace;
    (*node).funcname = write_node_list(&stmt.funcname);
    (*node).parameters = write_node_list(&stmt.parameters);
    (*node).returnType = write_type_name_ptr(&stmt.return_type);
    (*node).options = write_node_list(&stmt.options);
    (*node).sql_body = write_node_boxed(&stmt.sql_body);
    node
}

unsafe fn write_def_elem(de: &protobuf::DefElem) -> *mut bindings_raw::DefElem {
    let node = alloc_node::<bindings_raw::DefElem>(bindings_raw::NodeTag_T_DefElem);
    (*node).defnamespace = pstrdup(&de.defnamespace);
    (*node).defname = pstrdup(&de.defname);
    (*node).arg = write_node_boxed(&de.arg);
    (*node).defaction = proto_enum_to_c(de.defaction);
    (*node).location = de.location;
    node
}

unsafe fn write_rule_stmt(stmt: &protobuf::RuleStmt) -> *mut bindings_raw::RuleStmt {
    let node = alloc_node::<bindings_raw::RuleStmt>(bindings_raw::NodeTag_T_RuleStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).rulename = pstrdup(&stmt.rulename);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).event = proto_enum_to_c(stmt.event);
    (*node).instead = stmt.instead;
    (*node).actions = write_node_list(&stmt.actions);
    (*node).replace = stmt.replace;
    node
}

unsafe fn write_create_trig_stmt(stmt: &protobuf::CreateTrigStmt) -> *mut bindings_raw::CreateTrigStmt {
    let node = alloc_node::<bindings_raw::CreateTrigStmt>(bindings_raw::NodeTag_T_CreateTrigStmt);
    (*node).replace = stmt.replace;
    (*node).isconstraint = stmt.isconstraint;
    (*node).trigname = pstrdup(&stmt.trigname);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).funcname = write_node_list(&stmt.funcname);
    (*node).args = write_node_list(&stmt.args);
    (*node).row = stmt.row;
    (*node).timing = stmt.timing as i16;
    (*node).events = stmt.events as i16;
    (*node).columns = write_node_list(&stmt.columns);
    (*node).whenClause = write_node_boxed(&stmt.when_clause);
    (*node).transitionRels = write_node_list(&stmt.transition_rels);
    (*node).deferrable = stmt.deferrable;
    (*node).initdeferred = stmt.initdeferred;
    (*node).constrrel = write_range_var_ptr(&stmt.constrrel);
    node
}

unsafe fn write_do_stmt(stmt: &protobuf::DoStmt) -> *mut bindings_raw::DoStmt {
    let node = alloc_node::<bindings_raw::DoStmt>(bindings_raw::NodeTag_T_DoStmt);
    (*node).args = write_node_list(&stmt.args);
    node
}

unsafe fn write_call_stmt(stmt: &protobuf::CallStmt) -> *mut bindings_raw::CallStmt {
    let node = alloc_node::<bindings_raw::CallStmt>(bindings_raw::NodeTag_T_CallStmt);
    (*node).funccall = match &stmt.funccall {
        Some(fc) => write_func_call(fc),
        None => std::ptr::null_mut(),
    };
    (*node).funcexpr = std::ptr::null_mut(); // Post-analysis field
    (*node).outargs = write_node_list(&stmt.outargs);
    node
}

unsafe fn write_merge_stmt(stmt: &protobuf::MergeStmt) -> *mut bindings_raw::MergeStmt {
    let node = alloc_node::<bindings_raw::MergeStmt>(bindings_raw::NodeTag_T_MergeStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).sourceRelation = write_node_boxed(&stmt.source_relation);
    (*node).joinCondition = write_node_boxed(&stmt.join_condition);
    (*node).mergeWhenClauses = write_node_list(&stmt.merge_when_clauses);
    (*node).returningList = write_node_list(&stmt.returning_list);
    (*node).withClause = match &stmt.with_clause {
        Some(wc) => write_with_clause(wc),
        None => std::ptr::null_mut(),
    };
    node
}

unsafe fn write_merge_when_clause(mwc: &protobuf::MergeWhenClause) -> *mut bindings_raw::MergeWhenClause {
    let node = alloc_node::<bindings_raw::MergeWhenClause>(bindings_raw::NodeTag_T_MergeWhenClause);
    (*node).matchKind = proto_enum_to_c(mwc.match_kind);
    (*node).commandType = proto_enum_to_c(mwc.command_type);
    (*node).override_ = proto_enum_to_c(mwc.r#override);
    (*node).condition = write_node_boxed(&mwc.condition);
    (*node).targetList = write_node_list(&mwc.target_list);
    (*node).values = write_node_list(&mwc.values);
    node
}

unsafe fn write_grant_role_stmt(stmt: &protobuf::GrantRoleStmt) -> *mut bindings_raw::GrantRoleStmt {
    let node = alloc_node::<bindings_raw::GrantRoleStmt>(bindings_raw::NodeTag_T_GrantRoleStmt);
    (*node).granted_roles = write_node_list(&stmt.granted_roles);
    (*node).grantee_roles = write_node_list(&stmt.grantee_roles);
    (*node).is_grant = stmt.is_grant;
    (*node).opt = write_node_list(&stmt.opt);
    (*node).grantor = std::ptr::null_mut();
    (*node).behavior = proto_enum_to_c(stmt.behavior);
    node
}

unsafe fn write_prepare_stmt(stmt: &protobuf::PrepareStmt) -> *mut bindings_raw::PrepareStmt {
    let node = alloc_node::<bindings_raw::PrepareStmt>(bindings_raw::NodeTag_T_PrepareStmt);
    (*node).name = pstrdup(&stmt.name);
    (*node).argtypes = write_node_list(&stmt.argtypes);
    (*node).query = write_node_boxed(&stmt.query);
    node
}

unsafe fn write_execute_stmt(stmt: &protobuf::ExecuteStmt) -> *mut bindings_raw::ExecuteStmt {
    let node = alloc_node::<bindings_raw::ExecuteStmt>(bindings_raw::NodeTag_T_ExecuteStmt);
    (*node).name = pstrdup(&stmt.name);
    (*node).params = write_node_list(&stmt.params);
    node
}

unsafe fn write_deallocate_stmt(stmt: &protobuf::DeallocateStmt) -> *mut bindings_raw::DeallocateStmt {
    let node = alloc_node::<bindings_raw::DeallocateStmt>(bindings_raw::NodeTag_T_DeallocateStmt);
    (*node).name = pstrdup(&stmt.name);
    (*node).isall = stmt.isall;
    (*node).location = stmt.location;
    node
}

unsafe fn write_a_indirection(ai: &protobuf::AIndirection) -> *mut bindings_raw::A_Indirection {
    let node = alloc_node::<bindings_raw::A_Indirection>(bindings_raw::NodeTag_T_A_Indirection);
    (*node).arg = write_node_boxed(&ai.arg);
    (*node).indirection = write_node_list(&ai.indirection);
    node
}

unsafe fn write_a_indices(ai: &protobuf::AIndices) -> *mut bindings_raw::A_Indices {
    let node = alloc_node::<bindings_raw::A_Indices>(bindings_raw::NodeTag_T_A_Indices);
    (*node).is_slice = ai.is_slice;
    (*node).lidx = write_node_boxed(&ai.lidx);
    (*node).uidx = write_node_boxed(&ai.uidx);
    node
}

unsafe fn write_min_max_expr(mme: &protobuf::MinMaxExpr) -> *mut bindings_raw::MinMaxExpr {
    let node = alloc_node::<bindings_raw::MinMaxExpr>(bindings_raw::NodeTag_T_MinMaxExpr);
    (*node).minmaxtype = mme.minmaxtype;
    (*node).minmaxcollid = mme.minmaxcollid;
    (*node).inputcollid = mme.inputcollid;
    (*node).op = proto_enum_to_c(mme.op);
    (*node).args = write_node_list(&mme.args);
    (*node).location = mme.location;
    node
}

unsafe fn write_row_expr(re: &protobuf::RowExpr) -> *mut bindings_raw::RowExpr {
    let node = alloc_node::<bindings_raw::RowExpr>(bindings_raw::NodeTag_T_RowExpr);
    (*node).args = write_node_list(&re.args);
    (*node).row_typeid = re.row_typeid;
    (*node).row_format = proto_enum_to_c(re.row_format);
    (*node).colnames = write_node_list(&re.colnames);
    (*node).location = re.location;
    node
}

unsafe fn write_a_array_expr(ae: &protobuf::AArrayExpr) -> *mut bindings_raw::A_ArrayExpr {
    let node = alloc_node::<bindings_raw::A_ArrayExpr>(bindings_raw::NodeTag_T_A_ArrayExpr);
    (*node).elements = write_node_list(&ae.elements);
    (*node).location = ae.location;
    node
}

unsafe fn write_boolean_test(bt: &protobuf::BooleanTest) -> *mut bindings_raw::BooleanTest {
    let node = alloc_node::<bindings_raw::BooleanTest>(bindings_raw::NodeTag_T_BooleanTest);
    (*node).arg = write_node_boxed(&bt.arg) as *mut bindings_raw::Expr;
    (*node).booltesttype = proto_enum_to_c(bt.booltesttype);
    (*node).location = bt.location;
    node
}

unsafe fn write_collate_clause(cc: &protobuf::CollateClause) -> *mut bindings_raw::CollateClause {
    let node = alloc_node::<bindings_raw::CollateClause>(bindings_raw::NodeTag_T_CollateClause);
    (*node).arg = write_node_boxed(&cc.arg);
    (*node).collname = write_node_list(&cc.collname);
    (*node).location = cc.location;
    node
}
