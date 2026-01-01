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
unsafe fn alloc_node<T>(tag: bindings_raw::NodeTag) -> *mut T {
    bindings_raw::pg_query_alloc_node(std::mem::size_of::<T>(), tag as i32) as *mut T
}

/// Converts a protobuf enum value to a C enum value.
/// Protobuf enums have an extra "Undefined = 0" value, so we subtract 1.
/// If the value is 0 (Undefined), we return 0 (treating it as the first C enum value).
fn proto_enum_to_c(value: i32) -> i32 {
    if value <= 0 {
        0
    } else {
        value - 1
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
            // Simple statement nodes
            protobuf::node::Node::ListenStmt(ls) => write_listen_stmt(ls) as *mut bindings_raw::Node,
            protobuf::node::Node::UnlistenStmt(us) => write_unlisten_stmt(us) as *mut bindings_raw::Node,
            protobuf::node::Node::NotifyStmt(ns) => write_notify_stmt(ns) as *mut bindings_raw::Node,
            protobuf::node::Node::DiscardStmt(ds) => write_discard_stmt(ds) as *mut bindings_raw::Node,
            // Type definition nodes
            protobuf::node::Node::CompositeTypeStmt(cts) => write_composite_type_stmt(cts) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateEnumStmt(ces) => write_create_enum_stmt(ces) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateRangeStmt(crs) => write_create_range_stmt(crs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterEnumStmt(aes) => write_alter_enum_stmt(aes) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateDomainStmt(cds) => write_create_domain_stmt(cds) as *mut bindings_raw::Node,
            // Extension nodes
            protobuf::node::Node::CreateExtensionStmt(ces) => write_create_extension_stmt(ces) as *mut bindings_raw::Node,
            // Publication/Subscription nodes
            protobuf::node::Node::CreatePublicationStmt(cps) => write_create_publication_stmt(cps) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterPublicationStmt(aps) => write_alter_publication_stmt(aps) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateSubscriptionStmt(css) => write_create_subscription_stmt(css) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterSubscriptionStmt(ass) => write_alter_subscription_stmt(ass) as *mut bindings_raw::Node,
            // Expression nodes
            protobuf::node::Node::CoerceToDomain(ctd) => write_coerce_to_domain(ctd) as *mut bindings_raw::Node,
            // Sequence nodes
            protobuf::node::Node::CreateSeqStmt(css) => write_create_seq_stmt(css) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterSeqStmt(ass) => write_alter_seq_stmt(ass) as *mut bindings_raw::Node,
            // Cursor nodes
            protobuf::node::Node::ClosePortalStmt(cps) => write_close_portal_stmt(cps) as *mut bindings_raw::Node,
            protobuf::node::Node::FetchStmt(fs) => write_fetch_stmt(fs) as *mut bindings_raw::Node,
            protobuf::node::Node::DeclareCursorStmt(dcs) => write_declare_cursor_stmt(dcs) as *mut bindings_raw::Node,
            // Additional DDL statements
            protobuf::node::Node::DefineStmt(ds) => write_define_stmt(ds) as *mut bindings_raw::Node,
            protobuf::node::Node::CommentStmt(cs) => write_comment_stmt(cs) as *mut bindings_raw::Node,
            protobuf::node::Node::SecLabelStmt(sls) => write_sec_label_stmt(sls) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateRoleStmt(crs) => write_create_role_stmt(crs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterRoleStmt(ars) => write_alter_role_stmt(ars) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterRoleSetStmt(arss) => write_alter_role_set_stmt(arss) as *mut bindings_raw::Node,
            protobuf::node::Node::DropRoleStmt(drs) => write_drop_role_stmt(drs) as *mut bindings_raw::Node,
            protobuf::node::Node::CreatePolicyStmt(cps) => write_create_policy_stmt(cps) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterPolicyStmt(aps) => write_alter_policy_stmt(aps) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateEventTrigStmt(cets) => write_create_event_trig_stmt(cets) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterEventTrigStmt(aets) => write_alter_event_trig_stmt(aets) as *mut bindings_raw::Node,
            protobuf::node::Node::CreatePlangStmt(cpls) => write_create_plang_stmt(cpls) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateAmStmt(cas) => write_create_am_stmt(cas) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateOpClassStmt(cocs) => write_create_op_class_stmt(cocs) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateOpClassItem(coci) => write_create_op_class_item(coci) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateOpFamilyStmt(cofs) => write_create_op_family_stmt(cofs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterOpFamilyStmt(aofs) => write_alter_op_family_stmt(aofs) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateFdwStmt(cfds) => write_create_fdw_stmt(cfds) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterFdwStmt(afds) => write_alter_fdw_stmt(afds) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateForeignServerStmt(cfss) => write_create_foreign_server_stmt(cfss) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterForeignServerStmt(afss) => write_alter_foreign_server_stmt(afss) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateForeignTableStmt(cfts) => write_create_foreign_table_stmt(cfts) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateUserMappingStmt(cums) => write_create_user_mapping_stmt(cums) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterUserMappingStmt(aums) => write_alter_user_mapping_stmt(aums) as *mut bindings_raw::Node,
            protobuf::node::Node::DropUserMappingStmt(dums) => write_drop_user_mapping_stmt(dums) as *mut bindings_raw::Node,
            protobuf::node::Node::ImportForeignSchemaStmt(ifss) => write_import_foreign_schema_stmt(ifss) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateTableSpaceStmt(ctss) => write_create_table_space_stmt(ctss) as *mut bindings_raw::Node,
            protobuf::node::Node::DropTableSpaceStmt(dtss) => write_drop_table_space_stmt(dtss) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTableSpaceOptionsStmt(atsos) => write_alter_table_space_options_stmt(atsos) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTableMoveAllStmt(atmas) => write_alter_table_move_all_stmt(atmas) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterExtensionStmt(aes) => write_alter_extension_stmt(aes) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterExtensionContentsStmt(aecs) => write_alter_extension_contents_stmt(aecs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterDomainStmt(ads) => write_alter_domain_stmt(ads) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterFunctionStmt(afs) => write_alter_function_stmt(afs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterOperatorStmt(aos) => write_alter_operator_stmt(aos) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTypeStmt(ats) => write_alter_type_stmt(ats) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterOwnerStmt(aos) => write_alter_owner_stmt(aos) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterObjectSchemaStmt(aoss) => write_alter_object_schema_stmt(aoss) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterObjectDependsStmt(aods) => write_alter_object_depends_stmt(aods) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterCollationStmt(acs) => write_alter_collation_stmt(acs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterDefaultPrivilegesStmt(adps) => write_alter_default_privileges_stmt(adps) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateCastStmt(ccs) => write_create_cast_stmt(ccs) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateTransformStmt(cts) => write_create_transform_stmt(cts) as *mut bindings_raw::Node,
            protobuf::node::Node::CreateConversionStmt(ccs) => write_create_conversion_stmt(ccs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTsdictionaryStmt(atds) => write_alter_ts_dictionary_stmt(atds) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterTsconfigurationStmt(atcs) => write_alter_ts_configuration_stmt(atcs) as *mut bindings_raw::Node,
            // Database statements
            protobuf::node::Node::CreatedbStmt(cds) => write_createdb_stmt(cds) as *mut bindings_raw::Node,
            protobuf::node::Node::DropdbStmt(dds) => write_dropdb_stmt(dds) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterDatabaseStmt(ads) => write_alter_database_stmt(ads) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterDatabaseSetStmt(adss) => write_alter_database_set_stmt(adss) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterDatabaseRefreshCollStmt(adrcs) => write_alter_database_refresh_coll_stmt(adrcs) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterSystemStmt(ass) => write_alter_system_stmt(ass) as *mut bindings_raw::Node,
            protobuf::node::Node::ClusterStmt(cs) => write_cluster_stmt(cs) as *mut bindings_raw::Node,
            protobuf::node::Node::ReindexStmt(rs) => write_reindex_stmt(rs) as *mut bindings_raw::Node,
            protobuf::node::Node::ConstraintsSetStmt(css) => write_constraints_set_stmt(css) as *mut bindings_raw::Node,
            protobuf::node::Node::LoadStmt(ls) => write_load_stmt(ls) as *mut bindings_raw::Node,
            protobuf::node::Node::DropOwnedStmt(dos) => write_drop_owned_stmt(dos) as *mut bindings_raw::Node,
            protobuf::node::Node::ReassignOwnedStmt(ros) => write_reassign_owned_stmt(ros) as *mut bindings_raw::Node,
            protobuf::node::Node::DropSubscriptionStmt(dss) => write_drop_subscription_stmt(dss) as *mut bindings_raw::Node,
            // Table-related nodes
            protobuf::node::Node::TableFunc(tf) => write_table_func(tf) as *mut bindings_raw::Node,
            protobuf::node::Node::IntoClause(ic) => write_into_clause(ic) as *mut bindings_raw::Node,
            protobuf::node::Node::TableLikeClause(tlc) => write_table_like_clause(tlc) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeTableFunc(rtf) => write_range_table_func(rtf) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeTableFuncCol(rtfc) => write_range_table_func_col(rtfc) as *mut bindings_raw::Node,
            protobuf::node::Node::RangeTableSample(rts) => write_range_table_sample(rts) as *mut bindings_raw::Node,
            protobuf::node::Node::PartitionSpec(ps) => write_partition_spec(ps) as *mut bindings_raw::Node,
            protobuf::node::Node::PartitionBoundSpec(pbs) => write_partition_bound_spec(pbs) as *mut bindings_raw::Node,
            protobuf::node::Node::PartitionRangeDatum(prd) => write_partition_range_datum(prd) as *mut bindings_raw::Node,
            protobuf::node::Node::PartitionElem(pe) => write_partition_elem(pe) as *mut bindings_raw::Node,
            protobuf::node::Node::PartitionCmd(pc) => write_partition_cmd(pc) as *mut bindings_raw::Node,
            protobuf::node::Node::SinglePartitionSpec(sps) => write_single_partition_spec(sps) as *mut bindings_raw::Node,
            protobuf::node::Node::InferClause(ic) => write_infer_clause(ic) as *mut bindings_raw::Node,
            protobuf::node::Node::OnConflictClause(occ) => write_on_conflict_clause(occ) as *mut bindings_raw::Node,
            protobuf::node::Node::MultiAssignRef(mar) => write_multi_assign_ref(mar) as *mut bindings_raw::Node,
            protobuf::node::Node::TriggerTransition(tt) => write_trigger_transition(tt) as *mut bindings_raw::Node,
            // CTE-related nodes
            protobuf::node::Node::CtesearchClause(csc) => write_cte_search_clause(csc) as *mut bindings_raw::Node,
            protobuf::node::Node::CtecycleClause(ccc) => write_cte_cycle_clause(ccc) as *mut bindings_raw::Node,
            // Statistics nodes
            protobuf::node::Node::CreateStatsStmt(css) => write_create_stats_stmt(css) as *mut bindings_raw::Node,
            protobuf::node::Node::AlterStatsStmt(ass) => write_alter_stats_stmt(ass) as *mut bindings_raw::Node,
            protobuf::node::Node::StatsElem(se) => write_stats_elem(se) as *mut bindings_raw::Node,
            // Publication nodes
            protobuf::node::Node::PublicationObjSpec(pos) => write_publication_obj_spec(pos) as *mut bindings_raw::Node,
            protobuf::node::Node::PublicationTable(pt) => write_publication_table(pt) as *mut bindings_raw::Node,
            // Expression nodes (internal/executor - return null as they shouldn't appear in raw parse trees)
            protobuf::node::Node::Var(_)
            | protobuf::node::Node::Aggref(_)
            | protobuf::node::Node::WindowFunc(_)
            | protobuf::node::Node::WindowFuncRunCondition(_)
            | protobuf::node::Node::MergeSupportFunc(_)
            | protobuf::node::Node::SubscriptingRef(_)
            | protobuf::node::Node::FuncExpr(_)
            | protobuf::node::Node::OpExpr(_)
            | protobuf::node::Node::DistinctExpr(_)
            | protobuf::node::Node::NullIfExpr(_)
            | protobuf::node::Node::ScalarArrayOpExpr(_)
            | protobuf::node::Node::FieldSelect(_)
            | protobuf::node::Node::FieldStore(_)
            | protobuf::node::Node::RelabelType(_)
            | protobuf::node::Node::CoerceViaIo(_)
            | protobuf::node::Node::ArrayCoerceExpr(_)
            | protobuf::node::Node::ConvertRowtypeExpr(_)
            | protobuf::node::Node::CollateExpr(_)
            | protobuf::node::Node::CaseTestExpr(_)
            | protobuf::node::Node::ArrayExpr(_)
            | protobuf::node::Node::RowCompareExpr(_)
            | protobuf::node::Node::CoerceToDomainValue(_)
            | protobuf::node::Node::CurrentOfExpr(_)
            | protobuf::node::Node::NextValueExpr(_)
            | protobuf::node::Node::InferenceElem(_)
            | protobuf::node::Node::SubPlan(_)
            | protobuf::node::Node::AlternativeSubPlan(_)
            | protobuf::node::Node::TargetEntry(_)
            | protobuf::node::Node::RangeTblRef(_)
            | protobuf::node::Node::FromExpr(_)
            | protobuf::node::Node::OnConflictExpr(_)
            | protobuf::node::Node::Query(_)
            | protobuf::node::Node::MergeAction(_)
            | protobuf::node::Node::SortGroupClause(_)
            | protobuf::node::Node::WindowClause(_)
            | protobuf::node::Node::RowMarkClause(_)
            | protobuf::node::Node::WithCheckOption(_)
            | protobuf::node::Node::RangeTblEntry(_)
            | protobuf::node::Node::RangeTblFunction(_)
            | protobuf::node::Node::TableSampleClause(_)
            | protobuf::node::Node::RtepermissionInfo(_)
            | protobuf::node::Node::GroupingFunc(_)
            | protobuf::node::Node::Param(_)
            | protobuf::node::Node::IntList(_)
            | protobuf::node::Node::OidList(_)
            | protobuf::node::Node::RawStmt(_)
            | protobuf::node::Node::SetOperationStmt(_)
            | protobuf::node::Node::ReturnStmt(_)
            | protobuf::node::Node::PlassignStmt(_)
            | protobuf::node::Node::ReplicaIdentityStmt(_)
            | protobuf::node::Node::CallContext(_)
            | protobuf::node::Node::InlineCodeBlock(_) => {
                // These are internal/executor nodes that shouldn't appear in raw parse trees,
                // or are handled specially elsewhere
                std::ptr::null_mut()
            }
            // SQL Value function
            protobuf::node::Node::SqlvalueFunction(svf) => write_sql_value_function(svf) as *mut bindings_raw::Node,
            // XML nodes
            protobuf::node::Node::XmlExpr(xe) => write_xml_expr(xe) as *mut bindings_raw::Node,
            protobuf::node::Node::XmlSerialize(xs) => write_xml_serialize(xs) as *mut bindings_raw::Node,
            // Named argument
            protobuf::node::Node::NamedArgExpr(nae) => write_named_arg_expr(nae) as *mut bindings_raw::Node,
            // JSON nodes
            protobuf::node::Node::JsonFormat(jf) => write_json_format(jf) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonReturning(jr) => write_json_returning(jr) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonValueExpr(jve) => write_json_value_expr(jve) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonConstructorExpr(jce) => write_json_constructor_expr(jce) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonIsPredicate(jip) => write_json_is_predicate(jip) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonBehavior(jb) => write_json_behavior(jb) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonExpr(je) => write_json_expr(je) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTablePath(jtp) => write_json_table_path(jtp) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTablePathScan(jtps) => write_json_table_path_scan(jtps) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTableSiblingJoin(jtsj) => write_json_table_sibling_join(jtsj) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonOutput(jo) => write_json_output(jo) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonArgument(ja) => write_json_argument(ja) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonFuncExpr(jfe) => write_json_func_expr(jfe) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTablePathSpec(jtps) => write_json_table_path_spec(jtps) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTable(jt) => write_json_table(jt) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonTableColumn(jtc) => write_json_table_column(jtc) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonKeyValue(jkv) => write_json_key_value(jkv) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonParseExpr(jpe) => write_json_parse_expr(jpe) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonScalarExpr(jse) => write_json_scalar_expr(jse) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonSerializeExpr(jse) => write_json_serialize_expr(jse) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonObjectConstructor(joc) => write_json_object_constructor(joc) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonArrayConstructor(jac) => write_json_array_constructor(jac) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonArrayQueryConstructor(jaqc) => write_json_array_query_constructor(jaqc) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonAggConstructor(jac) => write_json_agg_constructor(jac) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonObjectAgg(joa) => write_json_object_agg(joa) as *mut bindings_raw::Node,
            protobuf::node::Node::JsonArrayAgg(jaa) => write_json_array_agg(jaa) as *mut bindings_raw::Node,
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
    (*node).limitOption = proto_enum_to_c(stmt.limit_option) as _;
    (*node).lockingClause = write_node_list(&stmt.locking_clause);
    (*node).withClause = write_with_clause_ref(&stmt.with_clause);
    (*node).op = proto_enum_to_c(stmt.op) as _;
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
    (*node).onCommit = proto_enum_to_c(ic.on_commit) as _;
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
    (*node).override_ = proto_enum_to_c(stmt.r#override) as _;
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
    (*node).action = proto_enum_to_c(oc.action) as _;
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
    (*node).kind = proto_enum_to_c(expr.kind) as _;
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
    (*node).funcformat = proto_enum_to_c(fc.funcformat) as _;
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

unsafe fn write_list(l: &protobuf::List) -> *mut bindings_raw::List {
    write_node_list(&l.items)
}

unsafe fn write_a_star() -> *mut bindings_raw::A_Star {
    alloc_node::<bindings_raw::A_Star>(bindings_raw::NodeTag_T_A_Star)
}

unsafe fn write_join_expr(je: &protobuf::JoinExpr) -> *mut bindings_raw::JoinExpr {
    let node = alloc_node::<bindings_raw::JoinExpr>(bindings_raw::NodeTag_T_JoinExpr);
    (*node).jointype = proto_enum_to_c(je.jointype) as _;
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
    (*node).sortby_dir = proto_enum_to_c(sb.sortby_dir) as _;
    (*node).sortby_nulls = proto_enum_to_c(sb.sortby_nulls) as _;
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
    (*node).nulltesttype = proto_enum_to_c(nt.nulltesttype) as _;
    (*node).argisrow = nt.argisrow;
    (*node).location = nt.location;
    node
}

unsafe fn write_bool_expr(be: &protobuf::BoolExpr) -> *mut bindings_raw::BoolExpr {
    let node = alloc_node::<bindings_raw::BoolExpr>(bindings_raw::NodeTag_T_BoolExpr);
    (*node).boolop = proto_enum_to_c(be.boolop) as _;
    (*node).args = write_node_list(&be.args);
    (*node).location = be.location;
    node
}

unsafe fn write_sub_link(sl: &protobuf::SubLink) -> *mut bindings_raw::SubLink {
    let node = alloc_node::<bindings_raw::SubLink>(bindings_raw::NodeTag_T_SubLink);
    (*node).subLinkType = proto_enum_to_c(sl.sub_link_type) as _;
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
    (*node).ctematerialized = proto_enum_to_c(cte.ctematerialized) as _;
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

unsafe fn write_with_clause_ref(wc: &Option<protobuf::WithClause>) -> *mut bindings_raw::WithClause {
    match wc {
        Some(w) => write_with_clause(w),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_grouping_set(gs: &protobuf::GroupingSet) -> *mut bindings_raw::GroupingSet {
    let node = alloc_node::<bindings_raw::GroupingSet>(bindings_raw::NodeTag_T_GroupingSet);
    (*node).kind = proto_enum_to_c(gs.kind) as _;
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
    (*node).strength = proto_enum_to_c(lc.strength) as _;
    (*node).waitPolicy = proto_enum_to_c(lc.wait_policy) as _;
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
    (*node).ordering = proto_enum_to_c(ie.ordering) as _;
    (*node).nulls_ordering = proto_enum_to_c(ie.nulls_ordering) as _;
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
    (*node).removeType = proto_enum_to_c(stmt.remove_type) as _;
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
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
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
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
    (*node).oncommit = proto_enum_to_c(stmt.oncommit) as _;
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
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_alter_table_cmd(cmd: &protobuf::AlterTableCmd) -> *mut bindings_raw::AlterTableCmd {
    let node = alloc_node::<bindings_raw::AlterTableCmd>(bindings_raw::NodeTag_T_AlterTableCmd);
    (*node).subtype = proto_enum_to_c(cmd.subtype) as _;
    (*node).name = pstrdup(&cmd.name);
    (*node).num = cmd.num as i16;
    (*node).newowner = std::ptr::null_mut(); // RoleSpec, complex
    (*node).def = write_node_boxed(&cmd.def);
    (*node).behavior = proto_enum_to_c(cmd.behavior) as _;
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
    (*node).contype = proto_enum_to_c(c.contype) as _;
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
    (*node).withCheckOption = proto_enum_to_c(stmt.with_check_option) as _;
    node
}

unsafe fn write_transaction_stmt(stmt: &protobuf::TransactionStmt) -> *mut bindings_raw::TransactionStmt {
    let node = alloc_node::<bindings_raw::TransactionStmt>(bindings_raw::NodeTag_T_TransactionStmt);
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
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
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
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
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
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
    (*node).renameType = proto_enum_to_c(stmt.rename_type) as _;
    (*node).relationType = proto_enum_to_c(stmt.relation_type) as _;
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).object = write_node_boxed(&stmt.object);
    (*node).subname = pstrdup(&stmt.subname);
    (*node).newname = pstrdup(&stmt.newname);
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_grant_stmt(stmt: &protobuf::GrantStmt) -> *mut bindings_raw::GrantStmt {
    let node = alloc_node::<bindings_raw::GrantStmt>(bindings_raw::NodeTag_T_GrantStmt);
    (*node).is_grant = stmt.is_grant;
    (*node).targtype = proto_enum_to_c(stmt.targtype) as _;
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).objects = write_node_list(&stmt.objects);
    (*node).privileges = write_node_list(&stmt.privileges);
    (*node).grantees = write_node_list(&stmt.grantees);
    (*node).grant_option = stmt.grant_option;
    (*node).grantor = std::ptr::null_mut(); // RoleSpec, complex
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
    node
}

unsafe fn write_role_spec(rs: &protobuf::RoleSpec) -> *mut bindings_raw::RoleSpec {
    let node = alloc_node::<bindings_raw::RoleSpec>(bindings_raw::NodeTag_T_RoleSpec);
    (*node).roletype = proto_enum_to_c(rs.roletype) as _;
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
    (*node).defaction = proto_enum_to_c(de.defaction) as _;
    (*node).location = de.location;
    node
}

unsafe fn write_rule_stmt(stmt: &protobuf::RuleStmt) -> *mut bindings_raw::RuleStmt {
    let node = alloc_node::<bindings_raw::RuleStmt>(bindings_raw::NodeTag_T_RuleStmt);
    (*node).relation = write_range_var_ptr(&stmt.relation);
    (*node).rulename = pstrdup(&stmt.rulename);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).event = proto_enum_to_c(stmt.event) as _;
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
    (*node).matchKind = proto_enum_to_c(mwc.match_kind) as _;
    (*node).commandType = proto_enum_to_c(mwc.command_type) as _;
    (*node).override_ = proto_enum_to_c(mwc.r#override) as _;
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
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
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
    (*node).op = proto_enum_to_c(mme.op) as _;
    (*node).args = write_node_list(&mme.args);
    (*node).location = mme.location;
    node
}

unsafe fn write_row_expr(re: &protobuf::RowExpr) -> *mut bindings_raw::RowExpr {
    let node = alloc_node::<bindings_raw::RowExpr>(bindings_raw::NodeTag_T_RowExpr);
    (*node).args = write_node_list(&re.args);
    (*node).row_typeid = re.row_typeid;
    (*node).row_format = proto_enum_to_c(re.row_format) as _;
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
    (*node).booltesttype = proto_enum_to_c(bt.booltesttype) as _;
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

// =============================================================================
// Simple statement nodes
// =============================================================================

unsafe fn write_listen_stmt(stmt: &protobuf::ListenStmt) -> *mut bindings_raw::ListenStmt {
    let node = alloc_node::<bindings_raw::ListenStmt>(bindings_raw::NodeTag_T_ListenStmt);
    (*node).conditionname = pstrdup(&stmt.conditionname);
    node
}

unsafe fn write_unlisten_stmt(stmt: &protobuf::UnlistenStmt) -> *mut bindings_raw::UnlistenStmt {
    let node = alloc_node::<bindings_raw::UnlistenStmt>(bindings_raw::NodeTag_T_UnlistenStmt);
    (*node).conditionname = pstrdup(&stmt.conditionname);
    node
}

unsafe fn write_notify_stmt(stmt: &protobuf::NotifyStmt) -> *mut bindings_raw::NotifyStmt {
    let node = alloc_node::<bindings_raw::NotifyStmt>(bindings_raw::NodeTag_T_NotifyStmt);
    (*node).conditionname = pstrdup(&stmt.conditionname);
    (*node).payload = pstrdup(&stmt.payload);
    node
}

unsafe fn write_discard_stmt(stmt: &protobuf::DiscardStmt) -> *mut bindings_raw::DiscardStmt {
    let node = alloc_node::<bindings_raw::DiscardStmt>(bindings_raw::NodeTag_T_DiscardStmt);
    (*node).target = proto_enum_to_c(stmt.target) as _;
    node
}

// =============================================================================
// Type definition nodes
// =============================================================================

unsafe fn write_composite_type_stmt(stmt: &protobuf::CompositeTypeStmt) -> *mut bindings_raw::CompositeTypeStmt {
    let node = alloc_node::<bindings_raw::CompositeTypeStmt>(bindings_raw::NodeTag_T_CompositeTypeStmt);
    (*node).typevar = write_range_var_ref(&stmt.typevar);
    (*node).coldeflist = write_node_list(&stmt.coldeflist);
    node
}

unsafe fn write_create_enum_stmt(stmt: &protobuf::CreateEnumStmt) -> *mut bindings_raw::CreateEnumStmt {
    let node = alloc_node::<bindings_raw::CreateEnumStmt>(bindings_raw::NodeTag_T_CreateEnumStmt);
    (*node).typeName = write_node_list(&stmt.type_name);
    (*node).vals = write_node_list(&stmt.vals);
    node
}

unsafe fn write_create_range_stmt(stmt: &protobuf::CreateRangeStmt) -> *mut bindings_raw::CreateRangeStmt {
    let node = alloc_node::<bindings_raw::CreateRangeStmt>(bindings_raw::NodeTag_T_CreateRangeStmt);
    (*node).typeName = write_node_list(&stmt.type_name);
    (*node).params = write_node_list(&stmt.params);
    node
}

unsafe fn write_alter_enum_stmt(stmt: &protobuf::AlterEnumStmt) -> *mut bindings_raw::AlterEnumStmt {
    let node = alloc_node::<bindings_raw::AlterEnumStmt>(bindings_raw::NodeTag_T_AlterEnumStmt);
    (*node).typeName = write_node_list(&stmt.type_name);
    (*node).oldVal = pstrdup(&stmt.old_val);
    (*node).newVal = pstrdup(&stmt.new_val);
    (*node).newValNeighbor = pstrdup(&stmt.new_val_neighbor);
    (*node).newValIsAfter = stmt.new_val_is_after;
    (*node).skipIfNewValExists = stmt.skip_if_new_val_exists;
    node
}

unsafe fn write_create_domain_stmt(stmt: &protobuf::CreateDomainStmt) -> *mut bindings_raw::CreateDomainStmt {
    let node = alloc_node::<bindings_raw::CreateDomainStmt>(bindings_raw::NodeTag_T_CreateDomainStmt);
    (*node).domainname = write_node_list(&stmt.domainname);
    (*node).typeName = write_type_name_ref(&stmt.type_name);
    (*node).collClause = match &stmt.coll_clause {
        Some(cc) => write_collate_clause(cc) as *mut bindings_raw::CollateClause,
        None => std::ptr::null_mut(),
    };
    (*node).constraints = write_node_list(&stmt.constraints);
    node
}

// =============================================================================
// Extension nodes
// =============================================================================

unsafe fn write_create_extension_stmt(stmt: &protobuf::CreateExtensionStmt) -> *mut bindings_raw::CreateExtensionStmt {
    let node = alloc_node::<bindings_raw::CreateExtensionStmt>(bindings_raw::NodeTag_T_CreateExtensionStmt);
    (*node).extname = pstrdup(&stmt.extname);
    (*node).if_not_exists = stmt.if_not_exists;
    (*node).options = write_node_list(&stmt.options);
    node
}

// =============================================================================
// Publication/Subscription nodes
// =============================================================================

unsafe fn write_create_publication_stmt(stmt: &protobuf::CreatePublicationStmt) -> *mut bindings_raw::CreatePublicationStmt {
    let node = alloc_node::<bindings_raw::CreatePublicationStmt>(bindings_raw::NodeTag_T_CreatePublicationStmt);
    (*node).pubname = pstrdup(&stmt.pubname);
    (*node).options = write_node_list(&stmt.options);
    (*node).pubobjects = write_node_list(&stmt.pubobjects);
    (*node).for_all_tables = stmt.for_all_tables;
    node
}

unsafe fn write_alter_publication_stmt(stmt: &protobuf::AlterPublicationStmt) -> *mut bindings_raw::AlterPublicationStmt {
    let node = alloc_node::<bindings_raw::AlterPublicationStmt>(bindings_raw::NodeTag_T_AlterPublicationStmt);
    (*node).pubname = pstrdup(&stmt.pubname);
    (*node).options = write_node_list(&stmt.options);
    (*node).pubobjects = write_node_list(&stmt.pubobjects);
    (*node).for_all_tables = stmt.for_all_tables;
    (*node).action = proto_enum_to_c(stmt.action) as _;
    node
}

unsafe fn write_create_subscription_stmt(stmt: &protobuf::CreateSubscriptionStmt) -> *mut bindings_raw::CreateSubscriptionStmt {
    let node = alloc_node::<bindings_raw::CreateSubscriptionStmt>(bindings_raw::NodeTag_T_CreateSubscriptionStmt);
    (*node).subname = pstrdup(&stmt.subname);
    (*node).conninfo = pstrdup(&stmt.conninfo);
    (*node).publication = write_node_list(&stmt.publication);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_subscription_stmt(stmt: &protobuf::AlterSubscriptionStmt) -> *mut bindings_raw::AlterSubscriptionStmt {
    let node = alloc_node::<bindings_raw::AlterSubscriptionStmt>(bindings_raw::NodeTag_T_AlterSubscriptionStmt);
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
    (*node).subname = pstrdup(&stmt.subname);
    (*node).conninfo = pstrdup(&stmt.conninfo);
    (*node).publication = write_node_list(&stmt.publication);
    (*node).options = write_node_list(&stmt.options);
    node
}

// =============================================================================
// Expression nodes
// =============================================================================

unsafe fn write_coerce_to_domain(ctd: &protobuf::CoerceToDomain) -> *mut bindings_raw::CoerceToDomain {
    let node = alloc_node::<bindings_raw::CoerceToDomain>(bindings_raw::NodeTag_T_CoerceToDomain);
    (*node).arg = write_node_boxed(&ctd.arg) as *mut bindings_raw::Expr;
    (*node).resulttype = ctd.resulttype;
    (*node).resulttypmod = ctd.resulttypmod;
    (*node).resultcollid = ctd.resultcollid;
    (*node).coercionformat = proto_enum_to_c(ctd.coercionformat) as _;
    (*node).location = ctd.location;
    node
}

// =============================================================================
// Sequence nodes
// =============================================================================

unsafe fn write_create_seq_stmt(stmt: &protobuf::CreateSeqStmt) -> *mut bindings_raw::CreateSeqStmt {
    let node = alloc_node::<bindings_raw::CreateSeqStmt>(bindings_raw::NodeTag_T_CreateSeqStmt);
    (*node).sequence = write_range_var_ref(&stmt.sequence);
    (*node).options = write_node_list(&stmt.options);
    (*node).ownerId = stmt.owner_id;
    (*node).for_identity = stmt.for_identity;
    (*node).if_not_exists = stmt.if_not_exists;
    node
}

unsafe fn write_alter_seq_stmt(stmt: &protobuf::AlterSeqStmt) -> *mut bindings_raw::AlterSeqStmt {
    let node = alloc_node::<bindings_raw::AlterSeqStmt>(bindings_raw::NodeTag_T_AlterSeqStmt);
    (*node).sequence = write_range_var_ref(&stmt.sequence);
    (*node).options = write_node_list(&stmt.options);
    (*node).for_identity = stmt.for_identity;
    (*node).missing_ok = stmt.missing_ok;
    node
}

// =============================================================================
// Cursor nodes
// =============================================================================

unsafe fn write_close_portal_stmt(stmt: &protobuf::ClosePortalStmt) -> *mut bindings_raw::ClosePortalStmt {
    let node = alloc_node::<bindings_raw::ClosePortalStmt>(bindings_raw::NodeTag_T_ClosePortalStmt);
    (*node).portalname = pstrdup(&stmt.portalname);
    node
}

unsafe fn write_fetch_stmt(stmt: &protobuf::FetchStmt) -> *mut bindings_raw::FetchStmt {
    let node = alloc_node::<bindings_raw::FetchStmt>(bindings_raw::NodeTag_T_FetchStmt);
    (*node).direction = proto_enum_to_c(stmt.direction) as _;
    (*node).howMany = stmt.how_many as _;
    (*node).portalname = pstrdup(&stmt.portalname);
    (*node).ismove = stmt.ismove;
    node
}

unsafe fn write_declare_cursor_stmt(stmt: &protobuf::DeclareCursorStmt) -> *mut bindings_raw::DeclareCursorStmt {
    let node = alloc_node::<bindings_raw::DeclareCursorStmt>(bindings_raw::NodeTag_T_DeclareCursorStmt);
    (*node).portalname = pstrdup(&stmt.portalname);
    (*node).options = stmt.options;
    (*node).query = write_node_boxed(&stmt.query);
    node
}

// =============================================================================
// Additional DDL statements
// =============================================================================

unsafe fn write_define_stmt(stmt: &protobuf::DefineStmt) -> *mut bindings_raw::DefineStmt {
    let node = alloc_node::<bindings_raw::DefineStmt>(bindings_raw::NodeTag_T_DefineStmt);
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
    (*node).oldstyle = stmt.oldstyle;
    (*node).defnames = write_node_list(&stmt.defnames);
    (*node).args = write_node_list(&stmt.args);
    (*node).definition = write_node_list(&stmt.definition);
    (*node).if_not_exists = stmt.if_not_exists;
    (*node).replace = stmt.replace;
    node
}

unsafe fn write_comment_stmt(stmt: &protobuf::CommentStmt) -> *mut bindings_raw::CommentStmt {
    let node = alloc_node::<bindings_raw::CommentStmt>(bindings_raw::NodeTag_T_CommentStmt);
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).object = write_node_boxed(&stmt.object);
    (*node).comment = pstrdup(&stmt.comment);
    node
}

unsafe fn write_sec_label_stmt(stmt: &protobuf::SecLabelStmt) -> *mut bindings_raw::SecLabelStmt {
    let node = alloc_node::<bindings_raw::SecLabelStmt>(bindings_raw::NodeTag_T_SecLabelStmt);
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).object = write_node_boxed(&stmt.object);
    (*node).provider = pstrdup(&stmt.provider);
    (*node).label = pstrdup(&stmt.label);
    node
}

unsafe fn write_create_role_stmt(stmt: &protobuf::CreateRoleStmt) -> *mut bindings_raw::CreateRoleStmt {
    let node = alloc_node::<bindings_raw::CreateRoleStmt>(bindings_raw::NodeTag_T_CreateRoleStmt);
    (*node).stmt_type = proto_enum_to_c(stmt.stmt_type) as _;
    (*node).role = pstrdup(&stmt.role);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_role_stmt(stmt: &protobuf::AlterRoleStmt) -> *mut bindings_raw::AlterRoleStmt {
    let node = alloc_node::<bindings_raw::AlterRoleStmt>(bindings_raw::NodeTag_T_AlterRoleStmt);
    (*node).role = write_role_spec_ref(&stmt.role);
    (*node).options = write_node_list(&stmt.options);
    (*node).action = stmt.action;
    node
}

unsafe fn write_alter_role_set_stmt(stmt: &protobuf::AlterRoleSetStmt) -> *mut bindings_raw::AlterRoleSetStmt {
    let node = alloc_node::<bindings_raw::AlterRoleSetStmt>(bindings_raw::NodeTag_T_AlterRoleSetStmt);
    (*node).role = write_role_spec_ref(&stmt.role);
    (*node).database = pstrdup(&stmt.database);
    (*node).setstmt = write_variable_set_stmt_ref(&stmt.setstmt);
    node
}

unsafe fn write_drop_role_stmt(stmt: &protobuf::DropRoleStmt) -> *mut bindings_raw::DropRoleStmt {
    let node = alloc_node::<bindings_raw::DropRoleStmt>(bindings_raw::NodeTag_T_DropRoleStmt);
    (*node).roles = write_node_list(&stmt.roles);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_create_policy_stmt(stmt: &protobuf::CreatePolicyStmt) -> *mut bindings_raw::CreatePolicyStmt {
    let node = alloc_node::<bindings_raw::CreatePolicyStmt>(bindings_raw::NodeTag_T_CreatePolicyStmt);
    (*node).policy_name = pstrdup(&stmt.policy_name);
    (*node).table = write_range_var_ref(&stmt.table);
    (*node).cmd_name = pstrdup(&stmt.cmd_name);
    (*node).permissive = stmt.permissive;
    (*node).roles = write_node_list(&stmt.roles);
    (*node).qual = write_node_boxed(&stmt.qual);
    (*node).with_check = write_node_boxed(&stmt.with_check);
    node
}

unsafe fn write_alter_policy_stmt(stmt: &protobuf::AlterPolicyStmt) -> *mut bindings_raw::AlterPolicyStmt {
    let node = alloc_node::<bindings_raw::AlterPolicyStmt>(bindings_raw::NodeTag_T_AlterPolicyStmt);
    (*node).policy_name = pstrdup(&stmt.policy_name);
    (*node).table = write_range_var_ref(&stmt.table);
    (*node).roles = write_node_list(&stmt.roles);
    (*node).qual = write_node_boxed(&stmt.qual);
    (*node).with_check = write_node_boxed(&stmt.with_check);
    node
}

unsafe fn write_create_event_trig_stmt(stmt: &protobuf::CreateEventTrigStmt) -> *mut bindings_raw::CreateEventTrigStmt {
    let node = alloc_node::<bindings_raw::CreateEventTrigStmt>(bindings_raw::NodeTag_T_CreateEventTrigStmt);
    (*node).trigname = pstrdup(&stmt.trigname);
    (*node).eventname = pstrdup(&stmt.eventname);
    (*node).whenclause = write_node_list(&stmt.whenclause);
    (*node).funcname = write_node_list(&stmt.funcname);
    node
}

unsafe fn write_alter_event_trig_stmt(stmt: &protobuf::AlterEventTrigStmt) -> *mut bindings_raw::AlterEventTrigStmt {
    let node = alloc_node::<bindings_raw::AlterEventTrigStmt>(bindings_raw::NodeTag_T_AlterEventTrigStmt);
    (*node).trigname = pstrdup(&stmt.trigname);
    (*node).tgenabled = if stmt.tgenabled.is_empty() { 0 } else { stmt.tgenabled.as_bytes()[0] as i8 };
    node
}

unsafe fn write_create_plang_stmt(stmt: &protobuf::CreatePLangStmt) -> *mut bindings_raw::CreatePLangStmt {
    let node = alloc_node::<bindings_raw::CreatePLangStmt>(bindings_raw::NodeTag_T_CreatePLangStmt);
    (*node).replace = stmt.replace;
    (*node).plname = pstrdup(&stmt.plname);
    (*node).plhandler = write_node_list(&stmt.plhandler);
    (*node).plinline = write_node_list(&stmt.plinline);
    (*node).plvalidator = write_node_list(&stmt.plvalidator);
    (*node).pltrusted = stmt.pltrusted;
    node
}

unsafe fn write_create_am_stmt(stmt: &protobuf::CreateAmStmt) -> *mut bindings_raw::CreateAmStmt {
    let node = alloc_node::<bindings_raw::CreateAmStmt>(bindings_raw::NodeTag_T_CreateAmStmt);
    (*node).amname = pstrdup(&stmt.amname);
    (*node).handler_name = write_node_list(&stmt.handler_name);
    (*node).amtype = if stmt.amtype.is_empty() { 0 } else { stmt.amtype.as_bytes()[0] as i8 };
    node
}

unsafe fn write_create_op_class_stmt(stmt: &protobuf::CreateOpClassStmt) -> *mut bindings_raw::CreateOpClassStmt {
    let node = alloc_node::<bindings_raw::CreateOpClassStmt>(bindings_raw::NodeTag_T_CreateOpClassStmt);
    (*node).opclassname = write_node_list(&stmt.opclassname);
    (*node).opfamilyname = write_node_list(&stmt.opfamilyname);
    (*node).amname = pstrdup(&stmt.amname);
    (*node).datatype = write_type_name_ref(&stmt.datatype);
    (*node).items = write_node_list(&stmt.items);
    (*node).isDefault = stmt.is_default;
    node
}

unsafe fn write_create_op_class_item(stmt: &protobuf::CreateOpClassItem) -> *mut bindings_raw::CreateOpClassItem {
    let node = alloc_node::<bindings_raw::CreateOpClassItem>(bindings_raw::NodeTag_T_CreateOpClassItem);
    (*node).itemtype = stmt.itemtype;
    (*node).name = write_object_with_args_ref(&stmt.name);
    (*node).number = stmt.number;
    (*node).order_family = write_node_list(&stmt.order_family);
    (*node).class_args = write_node_list(&stmt.class_args);
    (*node).storedtype = write_type_name_ref(&stmt.storedtype);
    node
}

unsafe fn write_create_op_family_stmt(stmt: &protobuf::CreateOpFamilyStmt) -> *mut bindings_raw::CreateOpFamilyStmt {
    let node = alloc_node::<bindings_raw::CreateOpFamilyStmt>(bindings_raw::NodeTag_T_CreateOpFamilyStmt);
    (*node).opfamilyname = write_node_list(&stmt.opfamilyname);
    (*node).amname = pstrdup(&stmt.amname);
    node
}

unsafe fn write_alter_op_family_stmt(stmt: &protobuf::AlterOpFamilyStmt) -> *mut bindings_raw::AlterOpFamilyStmt {
    let node = alloc_node::<bindings_raw::AlterOpFamilyStmt>(bindings_raw::NodeTag_T_AlterOpFamilyStmt);
    (*node).opfamilyname = write_node_list(&stmt.opfamilyname);
    (*node).amname = pstrdup(&stmt.amname);
    (*node).isDrop = stmt.is_drop;
    (*node).items = write_node_list(&stmt.items);
    node
}

unsafe fn write_create_fdw_stmt(stmt: &protobuf::CreateFdwStmt) -> *mut bindings_raw::CreateFdwStmt {
    let node = alloc_node::<bindings_raw::CreateFdwStmt>(bindings_raw::NodeTag_T_CreateFdwStmt);
    (*node).fdwname = pstrdup(&stmt.fdwname);
    (*node).func_options = write_node_list(&stmt.func_options);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_fdw_stmt(stmt: &protobuf::AlterFdwStmt) -> *mut bindings_raw::AlterFdwStmt {
    let node = alloc_node::<bindings_raw::AlterFdwStmt>(bindings_raw::NodeTag_T_AlterFdwStmt);
    (*node).fdwname = pstrdup(&stmt.fdwname);
    (*node).func_options = write_node_list(&stmt.func_options);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_create_foreign_server_stmt(stmt: &protobuf::CreateForeignServerStmt) -> *mut bindings_raw::CreateForeignServerStmt {
    let node = alloc_node::<bindings_raw::CreateForeignServerStmt>(bindings_raw::NodeTag_T_CreateForeignServerStmt);
    (*node).servername = pstrdup(&stmt.servername);
    (*node).servertype = pstrdup(&stmt.servertype);
    (*node).version = pstrdup(&stmt.version);
    (*node).fdwname = pstrdup(&stmt.fdwname);
    (*node).if_not_exists = stmt.if_not_exists;
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_foreign_server_stmt(stmt: &protobuf::AlterForeignServerStmt) -> *mut bindings_raw::AlterForeignServerStmt {
    let node = alloc_node::<bindings_raw::AlterForeignServerStmt>(bindings_raw::NodeTag_T_AlterForeignServerStmt);
    (*node).servername = pstrdup(&stmt.servername);
    (*node).version = pstrdup(&stmt.version);
    (*node).options = write_node_list(&stmt.options);
    (*node).has_version = stmt.has_version;
    node
}

unsafe fn write_create_foreign_table_stmt(stmt: &protobuf::CreateForeignTableStmt) -> *mut bindings_raw::CreateForeignTableStmt {
    let node = alloc_node::<bindings_raw::CreateForeignTableStmt>(bindings_raw::NodeTag_T_CreateForeignTableStmt);
    // CreateForeignTableStmt extends CreateStmt
    (*node).base.type_ = bindings_raw::NodeTag_T_CreateForeignTableStmt;
    if let Some(ref base) = stmt.base_stmt {
        (*node).base.relation = write_range_var_ref(&base.relation);
        (*node).base.tableElts = write_node_list(&base.table_elts);
        (*node).base.inhRelations = write_node_list(&base.inh_relations);
        (*node).base.partbound = write_partition_bound_spec_ref(&base.partbound);
        (*node).base.partspec = write_partition_spec_ref(&base.partspec);
        (*node).base.ofTypename = write_type_name_ref(&base.of_typename);
        (*node).base.constraints = write_node_list(&base.constraints);
        (*node).base.options = write_node_list(&base.options);
        (*node).base.oncommit = proto_enum_to_c(base.oncommit) as _;
        (*node).base.tablespacename = pstrdup(&base.tablespacename);
        (*node).base.accessMethod = pstrdup(&base.access_method);
        (*node).base.if_not_exists = base.if_not_exists;
    }
    (*node).servername = pstrdup(&stmt.servername);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_create_user_mapping_stmt(stmt: &protobuf::CreateUserMappingStmt) -> *mut bindings_raw::CreateUserMappingStmt {
    let node = alloc_node::<bindings_raw::CreateUserMappingStmt>(bindings_raw::NodeTag_T_CreateUserMappingStmt);
    (*node).user = write_role_spec_ref(&stmt.user);
    (*node).servername = pstrdup(&stmt.servername);
    (*node).if_not_exists = stmt.if_not_exists;
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_user_mapping_stmt(stmt: &protobuf::AlterUserMappingStmt) -> *mut bindings_raw::AlterUserMappingStmt {
    let node = alloc_node::<bindings_raw::AlterUserMappingStmt>(bindings_raw::NodeTag_T_AlterUserMappingStmt);
    (*node).user = write_role_spec_ref(&stmt.user);
    (*node).servername = pstrdup(&stmt.servername);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_drop_user_mapping_stmt(stmt: &protobuf::DropUserMappingStmt) -> *mut bindings_raw::DropUserMappingStmt {
    let node = alloc_node::<bindings_raw::DropUserMappingStmt>(bindings_raw::NodeTag_T_DropUserMappingStmt);
    (*node).user = write_role_spec_ref(&stmt.user);
    (*node).servername = pstrdup(&stmt.servername);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_import_foreign_schema_stmt(stmt: &protobuf::ImportForeignSchemaStmt) -> *mut bindings_raw::ImportForeignSchemaStmt {
    let node = alloc_node::<bindings_raw::ImportForeignSchemaStmt>(bindings_raw::NodeTag_T_ImportForeignSchemaStmt);
    (*node).server_name = pstrdup(&stmt.server_name);
    (*node).remote_schema = pstrdup(&stmt.remote_schema);
    (*node).local_schema = pstrdup(&stmt.local_schema);
    (*node).list_type = proto_enum_to_c(stmt.list_type) as _;
    (*node).table_list = write_node_list(&stmt.table_list);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_create_table_space_stmt(stmt: &protobuf::CreateTableSpaceStmt) -> *mut bindings_raw::CreateTableSpaceStmt {
    let node = alloc_node::<bindings_raw::CreateTableSpaceStmt>(bindings_raw::NodeTag_T_CreateTableSpaceStmt);
    (*node).tablespacename = pstrdup(&stmt.tablespacename);
    (*node).owner = write_role_spec_ref(&stmt.owner);
    (*node).location = pstrdup(&stmt.location);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_drop_table_space_stmt(stmt: &protobuf::DropTableSpaceStmt) -> *mut bindings_raw::DropTableSpaceStmt {
    let node = alloc_node::<bindings_raw::DropTableSpaceStmt>(bindings_raw::NodeTag_T_DropTableSpaceStmt);
    (*node).tablespacename = pstrdup(&stmt.tablespacename);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_alter_table_space_options_stmt(stmt: &protobuf::AlterTableSpaceOptionsStmt) -> *mut bindings_raw::AlterTableSpaceOptionsStmt {
    let node = alloc_node::<bindings_raw::AlterTableSpaceOptionsStmt>(bindings_raw::NodeTag_T_AlterTableSpaceOptionsStmt);
    (*node).tablespacename = pstrdup(&stmt.tablespacename);
    (*node).options = write_node_list(&stmt.options);
    (*node).isReset = stmt.is_reset;
    node
}

unsafe fn write_alter_table_move_all_stmt(stmt: &protobuf::AlterTableMoveAllStmt) -> *mut bindings_raw::AlterTableMoveAllStmt {
    let node = alloc_node::<bindings_raw::AlterTableMoveAllStmt>(bindings_raw::NodeTag_T_AlterTableMoveAllStmt);
    (*node).orig_tablespacename = pstrdup(&stmt.orig_tablespacename);
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).roles = write_node_list(&stmt.roles);
    (*node).new_tablespacename = pstrdup(&stmt.new_tablespacename);
    (*node).nowait = stmt.nowait;
    node
}

unsafe fn write_alter_extension_stmt(stmt: &protobuf::AlterExtensionStmt) -> *mut bindings_raw::AlterExtensionStmt {
    let node = alloc_node::<bindings_raw::AlterExtensionStmt>(bindings_raw::NodeTag_T_AlterExtensionStmt);
    (*node).extname = pstrdup(&stmt.extname);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_extension_contents_stmt(stmt: &protobuf::AlterExtensionContentsStmt) -> *mut bindings_raw::AlterExtensionContentsStmt {
    let node = alloc_node::<bindings_raw::AlterExtensionContentsStmt>(bindings_raw::NodeTag_T_AlterExtensionContentsStmt);
    (*node).extname = pstrdup(&stmt.extname);
    (*node).action = stmt.action;
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).object = write_node_boxed(&stmt.object);
    node
}

unsafe fn write_alter_domain_stmt(stmt: &protobuf::AlterDomainStmt) -> *mut bindings_raw::AlterDomainStmt {
    let node = alloc_node::<bindings_raw::AlterDomainStmt>(bindings_raw::NodeTag_T_AlterDomainStmt);
    (*node).subtype = if stmt.subtype.is_empty() { 0 } else { stmt.subtype.as_bytes()[0] as i8 };
    (*node).typeName = write_node_list(&stmt.type_name);
    (*node).name = pstrdup(&stmt.name);
    (*node).def = write_node_boxed(&stmt.def);
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_alter_function_stmt(stmt: &protobuf::AlterFunctionStmt) -> *mut bindings_raw::AlterFunctionStmt {
    let node = alloc_node::<bindings_raw::AlterFunctionStmt>(bindings_raw::NodeTag_T_AlterFunctionStmt);
    (*node).objtype = proto_enum_to_c(stmt.objtype) as _;
    (*node).func = write_object_with_args_ref(&stmt.func);
    (*node).actions = write_node_list(&stmt.actions);
    node
}

unsafe fn write_alter_operator_stmt(stmt: &protobuf::AlterOperatorStmt) -> *mut bindings_raw::AlterOperatorStmt {
    let node = alloc_node::<bindings_raw::AlterOperatorStmt>(bindings_raw::NodeTag_T_AlterOperatorStmt);
    (*node).opername = write_object_with_args_ref(&stmt.opername);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_type_stmt(stmt: &protobuf::AlterTypeStmt) -> *mut bindings_raw::AlterTypeStmt {
    let node = alloc_node::<bindings_raw::AlterTypeStmt>(bindings_raw::NodeTag_T_AlterTypeStmt);
    (*node).typeName = write_node_list(&stmt.type_name);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_owner_stmt(stmt: &protobuf::AlterOwnerStmt) -> *mut bindings_raw::AlterOwnerStmt {
    let node = alloc_node::<bindings_raw::AlterOwnerStmt>(bindings_raw::NodeTag_T_AlterOwnerStmt);
    (*node).objectType = proto_enum_to_c(stmt.object_type) as _;
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).object = write_node_boxed(&stmt.object);
    (*node).newowner = write_role_spec_ref(&stmt.newowner);
    node
}

unsafe fn write_alter_object_schema_stmt(stmt: &protobuf::AlterObjectSchemaStmt) -> *mut bindings_raw::AlterObjectSchemaStmt {
    let node = alloc_node::<bindings_raw::AlterObjectSchemaStmt>(bindings_raw::NodeTag_T_AlterObjectSchemaStmt);
    (*node).objectType = proto_enum_to_c(stmt.object_type) as _;
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).object = write_node_boxed(&stmt.object);
    (*node).newschema = pstrdup(&stmt.newschema);
    (*node).missing_ok = stmt.missing_ok;
    node
}

unsafe fn write_alter_object_depends_stmt(stmt: &protobuf::AlterObjectDependsStmt) -> *mut bindings_raw::AlterObjectDependsStmt {
    let node = alloc_node::<bindings_raw::AlterObjectDependsStmt>(bindings_raw::NodeTag_T_AlterObjectDependsStmt);
    (*node).objectType = proto_enum_to_c(stmt.object_type) as _;
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).object = write_node_boxed(&stmt.object);
    (*node).extname = write_string_ref(&stmt.extname);
    (*node).remove = stmt.remove;
    node
}

unsafe fn write_alter_collation_stmt(stmt: &protobuf::AlterCollationStmt) -> *mut bindings_raw::AlterCollationStmt {
    let node = alloc_node::<bindings_raw::AlterCollationStmt>(bindings_raw::NodeTag_T_AlterCollationStmt);
    (*node).collname = write_node_list(&stmt.collname);
    node
}

unsafe fn write_alter_default_privileges_stmt(stmt: &protobuf::AlterDefaultPrivilegesStmt) -> *mut bindings_raw::AlterDefaultPrivilegesStmt {
    let node = alloc_node::<bindings_raw::AlterDefaultPrivilegesStmt>(bindings_raw::NodeTag_T_AlterDefaultPrivilegesStmt);
    (*node).options = write_node_list(&stmt.options);
    (*node).action = write_grant_stmt_ref(&stmt.action);
    node
}

unsafe fn write_create_cast_stmt(stmt: &protobuf::CreateCastStmt) -> *mut bindings_raw::CreateCastStmt {
    let node = alloc_node::<bindings_raw::CreateCastStmt>(bindings_raw::NodeTag_T_CreateCastStmt);
    (*node).sourcetype = write_type_name_ref(&stmt.sourcetype);
    (*node).targettype = write_type_name_ref(&stmt.targettype);
    (*node).func = write_object_with_args_ref(&stmt.func);
    (*node).context = proto_enum_to_c(stmt.context) as _;
    (*node).inout = stmt.inout;
    node
}

unsafe fn write_create_transform_stmt(stmt: &protobuf::CreateTransformStmt) -> *mut bindings_raw::CreateTransformStmt {
    let node = alloc_node::<bindings_raw::CreateTransformStmt>(bindings_raw::NodeTag_T_CreateTransformStmt);
    (*node).replace = stmt.replace;
    (*node).type_name = write_type_name_ref(&stmt.type_name);
    (*node).lang = pstrdup(&stmt.lang);
    (*node).fromsql = write_object_with_args_ref(&stmt.fromsql);
    (*node).tosql = write_object_with_args_ref(&stmt.tosql);
    node
}

unsafe fn write_create_conversion_stmt(stmt: &protobuf::CreateConversionStmt) -> *mut bindings_raw::CreateConversionStmt {
    let node = alloc_node::<bindings_raw::CreateConversionStmt>(bindings_raw::NodeTag_T_CreateConversionStmt);
    (*node).conversion_name = write_node_list(&stmt.conversion_name);
    (*node).for_encoding_name = pstrdup(&stmt.for_encoding_name);
    (*node).to_encoding_name = pstrdup(&stmt.to_encoding_name);
    (*node).func_name = write_node_list(&stmt.func_name);
    (*node).def = stmt.def;
    node
}

unsafe fn write_alter_ts_dictionary_stmt(stmt: &protobuf::AlterTsDictionaryStmt) -> *mut bindings_raw::AlterTSDictionaryStmt {
    let node = alloc_node::<bindings_raw::AlterTSDictionaryStmt>(bindings_raw::NodeTag_T_AlterTSDictionaryStmt);
    (*node).dictname = write_node_list(&stmt.dictname);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_ts_configuration_stmt(stmt: &protobuf::AlterTsConfigurationStmt) -> *mut bindings_raw::AlterTSConfigurationStmt {
    let node = alloc_node::<bindings_raw::AlterTSConfigurationStmt>(bindings_raw::NodeTag_T_AlterTSConfigurationStmt);
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
    (*node).cfgname = write_node_list(&stmt.cfgname);
    (*node).tokentype = write_node_list(&stmt.tokentype);
    (*node).dicts = write_node_list(&stmt.dicts);
    (*node).override_ = stmt.r#override;
    (*node).replace = stmt.replace;
    (*node).missing_ok = stmt.missing_ok;
    node
}

// =============================================================================
// Database statements
// =============================================================================

unsafe fn write_createdb_stmt(stmt: &protobuf::CreatedbStmt) -> *mut bindings_raw::CreatedbStmt {
    let node = alloc_node::<bindings_raw::CreatedbStmt>(bindings_raw::NodeTag_T_CreatedbStmt);
    (*node).dbname = pstrdup(&stmt.dbname);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_dropdb_stmt(stmt: &protobuf::DropdbStmt) -> *mut bindings_raw::DropdbStmt {
    let node = alloc_node::<bindings_raw::DropdbStmt>(bindings_raw::NodeTag_T_DropdbStmt);
    (*node).dbname = pstrdup(&stmt.dbname);
    (*node).missing_ok = stmt.missing_ok;
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_database_stmt(stmt: &protobuf::AlterDatabaseStmt) -> *mut bindings_raw::AlterDatabaseStmt {
    let node = alloc_node::<bindings_raw::AlterDatabaseStmt>(bindings_raw::NodeTag_T_AlterDatabaseStmt);
    (*node).dbname = pstrdup(&stmt.dbname);
    (*node).options = write_node_list(&stmt.options);
    node
}

unsafe fn write_alter_database_set_stmt(stmt: &protobuf::AlterDatabaseSetStmt) -> *mut bindings_raw::AlterDatabaseSetStmt {
    let node = alloc_node::<bindings_raw::AlterDatabaseSetStmt>(bindings_raw::NodeTag_T_AlterDatabaseSetStmt);
    (*node).dbname = pstrdup(&stmt.dbname);
    (*node).setstmt = write_variable_set_stmt_ref(&stmt.setstmt);
    node
}

unsafe fn write_alter_database_refresh_coll_stmt(stmt: &protobuf::AlterDatabaseRefreshCollStmt) -> *mut bindings_raw::AlterDatabaseRefreshCollStmt {
    let node = alloc_node::<bindings_raw::AlterDatabaseRefreshCollStmt>(bindings_raw::NodeTag_T_AlterDatabaseRefreshCollStmt);
    (*node).dbname = pstrdup(&stmt.dbname);
    node
}

unsafe fn write_alter_system_stmt(stmt: &protobuf::AlterSystemStmt) -> *mut bindings_raw::AlterSystemStmt {
    let node = alloc_node::<bindings_raw::AlterSystemStmt>(bindings_raw::NodeTag_T_AlterSystemStmt);
    (*node).setstmt = write_variable_set_stmt_ref(&stmt.setstmt);
    node
}

unsafe fn write_cluster_stmt(stmt: &protobuf::ClusterStmt) -> *mut bindings_raw::ClusterStmt {
    let node = alloc_node::<bindings_raw::ClusterStmt>(bindings_raw::NodeTag_T_ClusterStmt);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).indexname = pstrdup(&stmt.indexname);
    (*node).params = write_node_list(&stmt.params);
    node
}

unsafe fn write_reindex_stmt(stmt: &protobuf::ReindexStmt) -> *mut bindings_raw::ReindexStmt {
    let node = alloc_node::<bindings_raw::ReindexStmt>(bindings_raw::NodeTag_T_ReindexStmt);
    (*node).kind = proto_enum_to_c(stmt.kind) as _;
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).name = pstrdup(&stmt.name);
    (*node).params = write_node_list(&stmt.params);
    node
}

unsafe fn write_constraints_set_stmt(stmt: &protobuf::ConstraintsSetStmt) -> *mut bindings_raw::ConstraintsSetStmt {
    let node = alloc_node::<bindings_raw::ConstraintsSetStmt>(bindings_raw::NodeTag_T_ConstraintsSetStmt);
    (*node).constraints = write_node_list(&stmt.constraints);
    (*node).deferred = stmt.deferred;
    node
}

unsafe fn write_load_stmt(stmt: &protobuf::LoadStmt) -> *mut bindings_raw::LoadStmt {
    let node = alloc_node::<bindings_raw::LoadStmt>(bindings_raw::NodeTag_T_LoadStmt);
    (*node).filename = pstrdup(&stmt.filename);
    node
}

unsafe fn write_drop_owned_stmt(stmt: &protobuf::DropOwnedStmt) -> *mut bindings_raw::DropOwnedStmt {
    let node = alloc_node::<bindings_raw::DropOwnedStmt>(bindings_raw::NodeTag_T_DropOwnedStmt);
    (*node).roles = write_node_list(&stmt.roles);
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
    node
}

unsafe fn write_reassign_owned_stmt(stmt: &protobuf::ReassignOwnedStmt) -> *mut bindings_raw::ReassignOwnedStmt {
    let node = alloc_node::<bindings_raw::ReassignOwnedStmt>(bindings_raw::NodeTag_T_ReassignOwnedStmt);
    (*node).roles = write_node_list(&stmt.roles);
    (*node).newrole = write_role_spec_ref(&stmt.newrole);
    node
}

unsafe fn write_drop_subscription_stmt(stmt: &protobuf::DropSubscriptionStmt) -> *mut bindings_raw::DropSubscriptionStmt {
    let node = alloc_node::<bindings_raw::DropSubscriptionStmt>(bindings_raw::NodeTag_T_DropSubscriptionStmt);
    (*node).subname = pstrdup(&stmt.subname);
    (*node).missing_ok = stmt.missing_ok;
    (*node).behavior = proto_enum_to_c(stmt.behavior) as _;
    node
}

// =============================================================================
// Table-related nodes
// =============================================================================

unsafe fn write_table_func(stmt: &protobuf::TableFunc) -> *mut bindings_raw::TableFunc {
    let node = alloc_node::<bindings_raw::TableFunc>(bindings_raw::NodeTag_T_TableFunc);
    (*node).ns_uris = write_node_list(&stmt.ns_uris);
    (*node).ns_names = write_node_list(&stmt.ns_names);
    (*node).docexpr = write_node_boxed(&stmt.docexpr);
    (*node).rowexpr = write_node_boxed(&stmt.rowexpr);
    (*node).colnames = write_node_list(&stmt.colnames);
    (*node).coltypes = write_node_list(&stmt.coltypes);
    (*node).coltypmods = write_node_list(&stmt.coltypmods);
    (*node).colcollations = write_node_list(&stmt.colcollations);
    (*node).colexprs = write_node_list(&stmt.colexprs);
    (*node).coldefexprs = write_node_list(&stmt.coldefexprs);
    (*node).ordinalitycol = stmt.ordinalitycol;
    (*node).location = stmt.location;
    node
}

unsafe fn write_table_like_clause(stmt: &protobuf::TableLikeClause) -> *mut bindings_raw::TableLikeClause {
    let node = alloc_node::<bindings_raw::TableLikeClause>(bindings_raw::NodeTag_T_TableLikeClause);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).options = stmt.options;
    (*node).relationOid = stmt.relation_oid;
    node
}

unsafe fn write_range_table_func(stmt: &protobuf::RangeTableFunc) -> *mut bindings_raw::RangeTableFunc {
    let node = alloc_node::<bindings_raw::RangeTableFunc>(bindings_raw::NodeTag_T_RangeTableFunc);
    (*node).lateral = stmt.lateral;
    (*node).docexpr = write_node_boxed(&stmt.docexpr);
    (*node).rowexpr = write_node_boxed(&stmt.rowexpr);
    (*node).namespaces = write_node_list(&stmt.namespaces);
    (*node).columns = write_node_list(&stmt.columns);
    (*node).alias = write_alias_ref(&stmt.alias);
    (*node).location = stmt.location;
    node
}

unsafe fn write_range_table_func_col(stmt: &protobuf::RangeTableFuncCol) -> *mut bindings_raw::RangeTableFuncCol {
    let node = alloc_node::<bindings_raw::RangeTableFuncCol>(bindings_raw::NodeTag_T_RangeTableFuncCol);
    (*node).colname = pstrdup(&stmt.colname);
    (*node).typeName = write_type_name_ref(&stmt.type_name);
    (*node).for_ordinality = stmt.for_ordinality;
    (*node).is_not_null = stmt.is_not_null;
    (*node).colexpr = write_node_boxed(&stmt.colexpr);
    (*node).coldefexpr = write_node_boxed(&stmt.coldefexpr);
    (*node).location = stmt.location;
    node
}

unsafe fn write_range_table_sample(stmt: &protobuf::RangeTableSample) -> *mut bindings_raw::RangeTableSample {
    let node = alloc_node::<bindings_raw::RangeTableSample>(bindings_raw::NodeTag_T_RangeTableSample);
    (*node).relation = write_node_boxed(&stmt.relation);
    (*node).method = write_node_list(&stmt.method);
    (*node).args = write_node_list(&stmt.args);
    (*node).repeatable = write_node_boxed(&stmt.repeatable);
    (*node).location = stmt.location;
    node
}

unsafe fn write_partition_spec(stmt: &protobuf::PartitionSpec) -> *mut bindings_raw::PartitionSpec {
    let node = alloc_node::<bindings_raw::PartitionSpec>(bindings_raw::NodeTag_T_PartitionSpec);
    (*node).strategy = proto_enum_to_c(stmt.strategy) as _;
    (*node).partParams = write_node_list(&stmt.part_params);
    (*node).location = stmt.location;
    node
}

unsafe fn write_partition_bound_spec(stmt: &protobuf::PartitionBoundSpec) -> *mut bindings_raw::PartitionBoundSpec {
    let node = alloc_node::<bindings_raw::PartitionBoundSpec>(bindings_raw::NodeTag_T_PartitionBoundSpec);
    (*node).strategy = if stmt.strategy.is_empty() { 0 } else { stmt.strategy.as_bytes()[0] as i8 };
    (*node).is_default = stmt.is_default;
    (*node).modulus = stmt.modulus;
    (*node).remainder = stmt.remainder;
    (*node).listdatums = write_node_list(&stmt.listdatums);
    (*node).lowerdatums = write_node_list(&stmt.lowerdatums);
    (*node).upperdatums = write_node_list(&stmt.upperdatums);
    (*node).location = stmt.location;
    node
}

unsafe fn write_partition_range_datum(stmt: &protobuf::PartitionRangeDatum) -> *mut bindings_raw::PartitionRangeDatum {
    let node = alloc_node::<bindings_raw::PartitionRangeDatum>(bindings_raw::NodeTag_T_PartitionRangeDatum);
    (*node).kind = proto_enum_to_c(stmt.kind) as i32;
    (*node).value = write_node_boxed(&stmt.value);
    (*node).location = stmt.location;
    node
}

unsafe fn write_partition_elem(stmt: &protobuf::PartitionElem) -> *mut bindings_raw::PartitionElem {
    let node = alloc_node::<bindings_raw::PartitionElem>(bindings_raw::NodeTag_T_PartitionElem);
    (*node).name = pstrdup(&stmt.name);
    (*node).expr = write_node_boxed(&stmt.expr);
    (*node).collation = write_node_list(&stmt.collation);
    (*node).opclass = write_node_list(&stmt.opclass);
    (*node).location = stmt.location;
    node
}

unsafe fn write_partition_cmd(stmt: &protobuf::PartitionCmd) -> *mut bindings_raw::PartitionCmd {
    let node = alloc_node::<bindings_raw::PartitionCmd>(bindings_raw::NodeTag_T_PartitionCmd);
    (*node).name = write_range_var_ref(&stmt.name);
    (*node).bound = write_partition_bound_spec_ref(&stmt.bound);
    (*node).concurrent = stmt.concurrent;
    node
}

unsafe fn write_single_partition_spec(_stmt: &protobuf::SinglePartitionSpec) -> *mut bindings_raw::SinglePartitionSpec {
    // SinglePartitionSpec is an empty struct in protobuf
    let node = alloc_node::<bindings_raw::SinglePartitionSpec>(bindings_raw::NodeTag_T_SinglePartitionSpec);
    node
}

unsafe fn write_infer_clause(stmt: &protobuf::InferClause) -> *mut bindings_raw::InferClause {
    let node = alloc_node::<bindings_raw::InferClause>(bindings_raw::NodeTag_T_InferClause);
    (*node).indexElems = write_node_list(&stmt.index_elems);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).conname = pstrdup(&stmt.conname);
    (*node).location = stmt.location;
    node
}

unsafe fn write_multi_assign_ref(stmt: &protobuf::MultiAssignRef) -> *mut bindings_raw::MultiAssignRef {
    let node = alloc_node::<bindings_raw::MultiAssignRef>(bindings_raw::NodeTag_T_MultiAssignRef);
    (*node).source = write_node_boxed(&stmt.source);
    (*node).colno = stmt.colno;
    (*node).ncolumns = stmt.ncolumns;
    node
}

unsafe fn write_trigger_transition(stmt: &protobuf::TriggerTransition) -> *mut bindings_raw::TriggerTransition {
    let node = alloc_node::<bindings_raw::TriggerTransition>(bindings_raw::NodeTag_T_TriggerTransition);
    (*node).name = pstrdup(&stmt.name);
    (*node).isNew = stmt.is_new;
    (*node).isTable = stmt.is_table;
    node
}

// =============================================================================
// CTE-related nodes
// =============================================================================

unsafe fn write_cte_search_clause(stmt: &protobuf::CteSearchClause) -> *mut bindings_raw::CTESearchClause {
    let node = alloc_node::<bindings_raw::CTESearchClause>(bindings_raw::NodeTag_T_CTESearchClause);
    (*node).search_col_list = write_node_list(&stmt.search_col_list);
    (*node).search_breadth_first = stmt.search_breadth_first;
    (*node).search_seq_column = pstrdup(&stmt.search_seq_column);
    (*node).location = stmt.location;
    node
}

unsafe fn write_cte_cycle_clause(stmt: &protobuf::CteCycleClause) -> *mut bindings_raw::CTECycleClause {
    let node = alloc_node::<bindings_raw::CTECycleClause>(bindings_raw::NodeTag_T_CTECycleClause);
    (*node).cycle_col_list = write_node_list(&stmt.cycle_col_list);
    (*node).cycle_mark_column = pstrdup(&stmt.cycle_mark_column);
    (*node).cycle_mark_value = write_node_boxed(&stmt.cycle_mark_value);
    (*node).cycle_mark_default = write_node_boxed(&stmt.cycle_mark_default);
    (*node).cycle_path_column = pstrdup(&stmt.cycle_path_column);
    (*node).location = stmt.location;
    node
}

// =============================================================================
// Statistics nodes
// =============================================================================

unsafe fn write_create_stats_stmt(stmt: &protobuf::CreateStatsStmt) -> *mut bindings_raw::CreateStatsStmt {
    let node = alloc_node::<bindings_raw::CreateStatsStmt>(bindings_raw::NodeTag_T_CreateStatsStmt);
    (*node).defnames = write_node_list(&stmt.defnames);
    (*node).stat_types = write_node_list(&stmt.stat_types);
    (*node).exprs = write_node_list(&stmt.exprs);
    (*node).relations = write_node_list(&stmt.relations);
    (*node).stxcomment = pstrdup(&stmt.stxcomment);
    (*node).transformed = stmt.transformed;
    (*node).if_not_exists = stmt.if_not_exists;
    node
}

unsafe fn write_alter_stats_stmt(stmt: &protobuf::AlterStatsStmt) -> *mut bindings_raw::AlterStatsStmt {
    let node = alloc_node::<bindings_raw::AlterStatsStmt>(bindings_raw::NodeTag_T_AlterStatsStmt);
    (*node).defnames = write_node_list(&stmt.defnames);
    (*node).missing_ok = stmt.missing_ok;
    (*node).stxstattarget = write_node_boxed(&stmt.stxstattarget);
    node
}

unsafe fn write_stats_elem(stmt: &protobuf::StatsElem) -> *mut bindings_raw::StatsElem {
    let node = alloc_node::<bindings_raw::StatsElem>(bindings_raw::NodeTag_T_StatsElem);
    (*node).name = pstrdup(&stmt.name);
    (*node).expr = write_node_boxed(&stmt.expr);
    node
}

// =============================================================================
// Publication nodes
// =============================================================================

unsafe fn write_publication_obj_spec(stmt: &protobuf::PublicationObjSpec) -> *mut bindings_raw::PublicationObjSpec {
    let node = alloc_node::<bindings_raw::PublicationObjSpec>(bindings_raw::NodeTag_T_PublicationObjSpec);
    (*node).pubobjtype = proto_enum_to_c(stmt.pubobjtype) as _;
    (*node).name = pstrdup(&stmt.name);
    (*node).pubtable = write_publication_table_ref(&stmt.pubtable);
    (*node).location = stmt.location;
    node
}

unsafe fn write_publication_table(stmt: &protobuf::PublicationTable) -> *mut bindings_raw::PublicationTable {
    let node = alloc_node::<bindings_raw::PublicationTable>(bindings_raw::NodeTag_T_PublicationTable);
    (*node).relation = write_range_var_ref(&stmt.relation);
    (*node).whereClause = write_node_boxed(&stmt.where_clause);
    (*node).columns = write_node_list(&stmt.columns);
    node
}

// =============================================================================
// SQL Value function
// =============================================================================

unsafe fn write_sql_value_function(stmt: &protobuf::SqlValueFunction) -> *mut bindings_raw::SQLValueFunction {
    let node = alloc_node::<bindings_raw::SQLValueFunction>(bindings_raw::NodeTag_T_SQLValueFunction);
    (*node).op = proto_enum_to_c(stmt.op) as _;
    (*node).type_ = stmt.r#type;
    (*node).typmod = stmt.typmod;
    (*node).location = stmt.location;
    node
}

// =============================================================================
// XML nodes
// =============================================================================

unsafe fn write_xml_expr(stmt: &protobuf::XmlExpr) -> *mut bindings_raw::XmlExpr {
    let node = alloc_node::<bindings_raw::XmlExpr>(bindings_raw::NodeTag_T_XmlExpr);
    (*node).op = proto_enum_to_c(stmt.op) as _;
    (*node).name = pstrdup(&stmt.name);
    (*node).named_args = write_node_list(&stmt.named_args);
    (*node).arg_names = write_node_list(&stmt.arg_names);
    (*node).args = write_node_list(&stmt.args);
    (*node).xmloption = proto_enum_to_c(stmt.xmloption) as _;
    (*node).indent = stmt.indent;
    (*node).type_ = stmt.r#type;
    (*node).typmod = stmt.typmod;
    (*node).location = stmt.location;
    node
}

unsafe fn write_xml_serialize(stmt: &protobuf::XmlSerialize) -> *mut bindings_raw::XmlSerialize {
    let node = alloc_node::<bindings_raw::XmlSerialize>(bindings_raw::NodeTag_T_XmlSerialize);
    (*node).xmloption = proto_enum_to_c(stmt.xmloption) as _;
    (*node).expr = write_node_boxed(&stmt.expr);
    (*node).typeName = write_type_name_ref(&stmt.type_name);
    (*node).indent = stmt.indent;
    (*node).location = stmt.location;
    node
}

// =============================================================================
// Named argument
// =============================================================================

unsafe fn write_named_arg_expr(stmt: &protobuf::NamedArgExpr) -> *mut bindings_raw::NamedArgExpr {
    let node = alloc_node::<bindings_raw::NamedArgExpr>(bindings_raw::NodeTag_T_NamedArgExpr);
    (*node).arg = write_node_boxed(&stmt.arg) as *mut bindings_raw::Expr;
    (*node).name = pstrdup(&stmt.name);
    (*node).argnumber = stmt.argnumber;
    (*node).location = stmt.location;
    node
}

// =============================================================================
// JSON nodes
// =============================================================================

unsafe fn write_json_format(stmt: &protobuf::JsonFormat) -> *mut bindings_raw::JsonFormat {
    let node = alloc_node::<bindings_raw::JsonFormat>(bindings_raw::NodeTag_T_JsonFormat);
    (*node).format_type = proto_enum_to_c(stmt.format_type) as _;
    (*node).encoding = proto_enum_to_c(stmt.encoding) as _;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_returning(stmt: &protobuf::JsonReturning) -> *mut bindings_raw::JsonReturning {
    let node = alloc_node::<bindings_raw::JsonReturning>(bindings_raw::NodeTag_T_JsonReturning);
    (*node).format = write_json_format_ref(&stmt.format);
    (*node).typid = stmt.typid;
    (*node).typmod = stmt.typmod;
    node
}

unsafe fn write_json_value_expr(stmt: &protobuf::JsonValueExpr) -> *mut bindings_raw::JsonValueExpr {
    let node = alloc_node::<bindings_raw::JsonValueExpr>(bindings_raw::NodeTag_T_JsonValueExpr);
    (*node).raw_expr = write_node_boxed(&stmt.raw_expr) as *mut bindings_raw::Expr;
    (*node).formatted_expr = write_node_boxed(&stmt.formatted_expr) as *mut bindings_raw::Expr;
    (*node).format = write_json_format_ref(&stmt.format);
    node
}

unsafe fn write_json_constructor_expr(stmt: &protobuf::JsonConstructorExpr) -> *mut bindings_raw::JsonConstructorExpr {
    let node = alloc_node::<bindings_raw::JsonConstructorExpr>(bindings_raw::NodeTag_T_JsonConstructorExpr);
    (*node).type_ = proto_enum_to_c(stmt.r#type) as _;
    (*node).args = write_node_list(&stmt.args);
    (*node).func = write_node_boxed(&stmt.func) as *mut bindings_raw::Expr;
    (*node).coercion = write_node_boxed(&stmt.coercion) as *mut bindings_raw::Expr;
    (*node).returning = write_json_returning_ref(&stmt.returning);
    (*node).absent_on_null = stmt.absent_on_null;
    (*node).unique = stmt.unique;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_is_predicate(stmt: &protobuf::JsonIsPredicate) -> *mut bindings_raw::JsonIsPredicate {
    let node = alloc_node::<bindings_raw::JsonIsPredicate>(bindings_raw::NodeTag_T_JsonIsPredicate);
    (*node).expr = write_node_boxed(&stmt.expr);
    (*node).format = write_json_format_ref(&stmt.format);
    (*node).item_type = proto_enum_to_c(stmt.item_type) as _;
    (*node).unique_keys = stmt.unique_keys;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_behavior(stmt: &protobuf::JsonBehavior) -> *mut bindings_raw::JsonBehavior {
    let node = alloc_node::<bindings_raw::JsonBehavior>(bindings_raw::NodeTag_T_JsonBehavior);
    (*node).btype = proto_enum_to_c(stmt.btype) as _;
    (*node).expr = write_node_boxed(&stmt.expr);
    (*node).coerce = stmt.coerce;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_expr(stmt: &protobuf::JsonExpr) -> *mut bindings_raw::JsonExpr {
    let node = alloc_node::<bindings_raw::JsonExpr>(bindings_raw::NodeTag_T_JsonExpr);
    (*node).op = proto_enum_to_c(stmt.op) as _;
    (*node).column_name = pstrdup(&stmt.column_name);
    (*node).formatted_expr = write_node_boxed(&stmt.formatted_expr);
    (*node).format = write_json_format_ref(&stmt.format);
    (*node).path_spec = write_node_boxed(&stmt.path_spec);
    (*node).returning = write_json_returning_ref(&stmt.returning);
    (*node).passing_names = write_node_list(&stmt.passing_names);
    (*node).passing_values = write_node_list(&stmt.passing_values);
    (*node).on_empty = write_json_behavior_ref(&stmt.on_empty);
    (*node).on_error = write_json_behavior_ref(&stmt.on_error);
    (*node).use_io_coercion = stmt.use_io_coercion;
    (*node).use_json_coercion = stmt.use_json_coercion;
    (*node).wrapper = proto_enum_to_c(stmt.wrapper) as _;
    (*node).omit_quotes = stmt.omit_quotes;
    (*node).collation = stmt.collation;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_table_path(stmt: &protobuf::JsonTablePath) -> *mut bindings_raw::JsonTablePath {
    let node = alloc_node::<bindings_raw::JsonTablePath>(bindings_raw::NodeTag_T_JsonTablePath);
    // value is only populated after semantic analysis, not in raw parse tree
    (*node).value = std::ptr::null_mut();
    (*node).name = pstrdup(&stmt.name);
    node
}

unsafe fn write_json_table_path_scan(stmt: &protobuf::JsonTablePathScan) -> *mut bindings_raw::JsonTablePathScan {
    let node = alloc_node::<bindings_raw::JsonTablePathScan>(bindings_raw::NodeTag_T_JsonTablePathScan);
    (*node).path = write_json_table_path_opt(&stmt.path);
    (*node).errorOnError = stmt.error_on_error;
    (*node).child = write_node_boxed(&stmt.child) as *mut bindings_raw::JsonTablePlan;
    (*node).colMin = stmt.col_min;
    (*node).colMax = stmt.col_max;
    node
}

unsafe fn write_json_table_sibling_join(stmt: &protobuf::JsonTableSiblingJoin) -> *mut bindings_raw::JsonTableSiblingJoin {
    let node = alloc_node::<bindings_raw::JsonTableSiblingJoin>(bindings_raw::NodeTag_T_JsonTableSiblingJoin);
    (*node).lplan = write_node_boxed(&stmt.lplan) as *mut bindings_raw::JsonTablePlan;
    (*node).rplan = write_node_boxed(&stmt.rplan) as *mut bindings_raw::JsonTablePlan;
    node
}

unsafe fn write_json_output(stmt: &protobuf::JsonOutput) -> *mut bindings_raw::JsonOutput {
    let node = alloc_node::<bindings_raw::JsonOutput>(bindings_raw::NodeTag_T_JsonOutput);
    (*node).typeName = write_type_name_ref(&stmt.type_name);
    (*node).returning = write_json_returning_ref(&stmt.returning);
    node
}

unsafe fn write_json_argument(stmt: &protobuf::JsonArgument) -> *mut bindings_raw::JsonArgument {
    let node = alloc_node::<bindings_raw::JsonArgument>(bindings_raw::NodeTag_T_JsonArgument);
    (*node).val = write_json_value_expr_ref(&stmt.val);
    (*node).name = pstrdup(&stmt.name);
    node
}

unsafe fn write_json_func_expr(stmt: &protobuf::JsonFuncExpr) -> *mut bindings_raw::JsonFuncExpr {
    let node = alloc_node::<bindings_raw::JsonFuncExpr>(bindings_raw::NodeTag_T_JsonFuncExpr);
    (*node).op = proto_enum_to_c(stmt.op) as _;
    (*node).column_name = pstrdup(&stmt.column_name);
    (*node).context_item = write_json_value_expr_ref(&stmt.context_item);
    (*node).pathspec = write_node_boxed(&stmt.pathspec);
    (*node).passing = write_node_list(&stmt.passing);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).on_empty = write_json_behavior_ref(&stmt.on_empty);
    (*node).on_error = write_json_behavior_ref(&stmt.on_error);
    (*node).wrapper = proto_enum_to_c(stmt.wrapper) as _;
    (*node).quotes = proto_enum_to_c(stmt.quotes) as _;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_table_path_spec(stmt: &protobuf::JsonTablePathSpec) -> *mut bindings_raw::JsonTablePathSpec {
    let node = alloc_node::<bindings_raw::JsonTablePathSpec>(bindings_raw::NodeTag_T_JsonTablePathSpec);
    (*node).string = write_node_boxed(&stmt.string);
    (*node).name = pstrdup(&stmt.name);
    (*node).name_location = stmt.name_location;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_table(stmt: &protobuf::JsonTable) -> *mut bindings_raw::JsonTable {
    let node = alloc_node::<bindings_raw::JsonTable>(bindings_raw::NodeTag_T_JsonTable);
    (*node).context_item = write_json_value_expr_ref(&stmt.context_item);
    (*node).pathspec = write_json_table_path_spec_ref(&stmt.pathspec);
    (*node).passing = write_node_list(&stmt.passing);
    (*node).columns = write_node_list(&stmt.columns);
    (*node).on_error = write_json_behavior_ref(&stmt.on_error);
    (*node).alias = write_alias_ref(&stmt.alias);
    (*node).lateral = stmt.lateral;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_table_column(stmt: &protobuf::JsonTableColumn) -> *mut bindings_raw::JsonTableColumn {
    let node = alloc_node::<bindings_raw::JsonTableColumn>(bindings_raw::NodeTag_T_JsonTableColumn);
    (*node).coltype = proto_enum_to_c(stmt.coltype) as _;
    (*node).name = pstrdup(&stmt.name);
    (*node).typeName = write_type_name_ref(&stmt.type_name);
    (*node).pathspec = write_json_table_path_spec_ref(&stmt.pathspec);
    (*node).format = write_json_format_ref(&stmt.format);
    (*node).wrapper = proto_enum_to_c(stmt.wrapper) as _;
    (*node).quotes = proto_enum_to_c(stmt.quotes) as _;
    (*node).columns = write_node_list(&stmt.columns);
    (*node).on_empty = write_json_behavior_ref(&stmt.on_empty);
    (*node).on_error = write_json_behavior_ref(&stmt.on_error);
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_key_value(stmt: &protobuf::JsonKeyValue) -> *mut bindings_raw::JsonKeyValue {
    let node = alloc_node::<bindings_raw::JsonKeyValue>(bindings_raw::NodeTag_T_JsonKeyValue);
    (*node).key = write_node_boxed(&stmt.key) as *mut bindings_raw::Expr;
    (*node).value = write_json_value_expr_ref(&stmt.value);
    node
}

unsafe fn write_json_parse_expr(stmt: &protobuf::JsonParseExpr) -> *mut bindings_raw::JsonParseExpr {
    let node = alloc_node::<bindings_raw::JsonParseExpr>(bindings_raw::NodeTag_T_JsonParseExpr);
    (*node).expr = write_json_value_expr_ref(&stmt.expr);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).unique_keys = stmt.unique_keys;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_scalar_expr(stmt: &protobuf::JsonScalarExpr) -> *mut bindings_raw::JsonScalarExpr {
    let node = alloc_node::<bindings_raw::JsonScalarExpr>(bindings_raw::NodeTag_T_JsonScalarExpr);
    (*node).expr = write_node_boxed(&stmt.expr) as *mut bindings_raw::Expr;
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_serialize_expr(stmt: &protobuf::JsonSerializeExpr) -> *mut bindings_raw::JsonSerializeExpr {
    let node = alloc_node::<bindings_raw::JsonSerializeExpr>(bindings_raw::NodeTag_T_JsonSerializeExpr);
    (*node).expr = write_json_value_expr_ref(&stmt.expr);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_object_constructor(stmt: &protobuf::JsonObjectConstructor) -> *mut bindings_raw::JsonObjectConstructor {
    let node = alloc_node::<bindings_raw::JsonObjectConstructor>(bindings_raw::NodeTag_T_JsonObjectConstructor);
    (*node).exprs = write_node_list(&stmt.exprs);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).absent_on_null = stmt.absent_on_null;
    (*node).unique = stmt.unique;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_array_constructor(stmt: &protobuf::JsonArrayConstructor) -> *mut bindings_raw::JsonArrayConstructor {
    let node = alloc_node::<bindings_raw::JsonArrayConstructor>(bindings_raw::NodeTag_T_JsonArrayConstructor);
    (*node).exprs = write_node_list(&stmt.exprs);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).absent_on_null = stmt.absent_on_null;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_array_query_constructor(stmt: &protobuf::JsonArrayQueryConstructor) -> *mut bindings_raw::JsonArrayQueryConstructor {
    let node = alloc_node::<bindings_raw::JsonArrayQueryConstructor>(bindings_raw::NodeTag_T_JsonArrayQueryConstructor);
    (*node).query = write_node_boxed(&stmt.query);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).format = write_json_format_ref(&stmt.format);
    (*node).absent_on_null = stmt.absent_on_null;
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_agg_constructor(stmt: &protobuf::JsonAggConstructor) -> *mut bindings_raw::JsonAggConstructor {
    let node = alloc_node::<bindings_raw::JsonAggConstructor>(bindings_raw::NodeTag_T_JsonAggConstructor);
    (*node).output = write_json_output_ref(&stmt.output);
    (*node).agg_filter = write_node_boxed(&stmt.agg_filter);
    (*node).agg_order = write_node_list(&stmt.agg_order);
    (*node).over = write_window_def_boxed_ref(&stmt.over);
    (*node).location = stmt.location;
    node
}

unsafe fn write_json_object_agg(stmt: &protobuf::JsonObjectAgg) -> *mut bindings_raw::JsonObjectAgg {
    let node = alloc_node::<bindings_raw::JsonObjectAgg>(bindings_raw::NodeTag_T_JsonObjectAgg);
    (*node).constructor = write_json_agg_constructor_ref(&stmt.constructor);
    (*node).arg = write_json_key_value_ref(&stmt.arg);
    (*node).absent_on_null = stmt.absent_on_null;
    (*node).unique = stmt.unique;
    node
}

unsafe fn write_json_array_agg(stmt: &protobuf::JsonArrayAgg) -> *mut bindings_raw::JsonArrayAgg {
    let node = alloc_node::<bindings_raw::JsonArrayAgg>(bindings_raw::NodeTag_T_JsonArrayAgg);
    (*node).constructor = write_json_agg_constructor_ref(&stmt.constructor);
    (*node).arg = write_json_value_expr_ref(&stmt.arg);
    (*node).absent_on_null = stmt.absent_on_null;
    node
}

// =============================================================================
// Additional helper functions for optional refs
// =============================================================================

unsafe fn write_role_spec_ref(role: &Option<protobuf::RoleSpec>) -> *mut bindings_raw::RoleSpec {
    match role {
        Some(r) => write_role_spec(r),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_variable_set_stmt_ref(stmt: &Option<protobuf::VariableSetStmt>) -> *mut bindings_raw::VariableSetStmt {
    match stmt {
        Some(s) => write_variable_set_stmt(s),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_object_with_args_ref(owa: &Option<protobuf::ObjectWithArgs>) -> *mut bindings_raw::ObjectWithArgs {
    match owa {
        Some(o) => write_object_with_args(o),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_partition_bound_spec_ref(pbs: &Option<protobuf::PartitionBoundSpec>) -> *mut bindings_raw::PartitionBoundSpec {
    match pbs {
        Some(p) => write_partition_bound_spec(p),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_partition_spec_ref(ps: &Option<protobuf::PartitionSpec>) -> *mut bindings_raw::PartitionSpec {
    match ps {
        Some(p) => write_partition_spec(p),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_grant_stmt_ref(gs: &Option<protobuf::GrantStmt>) -> *mut bindings_raw::GrantStmt {
    match gs {
        Some(g) => write_grant_stmt(g),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_string_ref(s: &Option<protobuf::String>) -> *mut bindings_raw::String {
    match s {
        Some(str_val) => write_string(str_val),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_publication_table_ref(pt: &Option<Box<protobuf::PublicationTable>>) -> *mut bindings_raw::PublicationTable {
    match pt {
        Some(p) => write_publication_table(p),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_format_ref(jf: &Option<protobuf::JsonFormat>) -> *mut bindings_raw::JsonFormat {
    match jf {
        Some(f) => write_json_format(f),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_returning_ref(jr: &Option<protobuf::JsonReturning>) -> *mut bindings_raw::JsonReturning {
    match jr {
        Some(r) => write_json_returning(r),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_behavior_ref(jb: &Option<Box<protobuf::JsonBehavior>>) -> *mut bindings_raw::JsonBehavior {
    match jb {
        Some(b) => write_json_behavior(b),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_output_ref(jo: &Option<protobuf::JsonOutput>) -> *mut bindings_raw::JsonOutput {
    match jo {
        Some(o) => write_json_output(o),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_value_expr_ref(jve: &Option<Box<protobuf::JsonValueExpr>>) -> *mut bindings_raw::JsonValueExpr {
    match jve {
        Some(v) => write_json_value_expr(v),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_table_path_spec_ref(jtps: &Option<Box<protobuf::JsonTablePathSpec>>) -> *mut bindings_raw::JsonTablePathSpec {
    match jtps {
        Some(p) => write_json_table_path_spec(p),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_window_def_boxed_ref(wd: &Option<Box<protobuf::WindowDef>>) -> *mut bindings_raw::WindowDef {
    match wd {
        Some(w) => write_window_def(w),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_table_path_opt(jtp: &Option<protobuf::JsonTablePath>) -> *mut bindings_raw::JsonTablePath {
    match jtp {
        Some(p) => write_json_table_path(p),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_agg_constructor_ref(jac: &Option<Box<protobuf::JsonAggConstructor>>) -> *mut bindings_raw::JsonAggConstructor {
    match jac {
        Some(c) => write_json_agg_constructor(c),
        None => std::ptr::null_mut(),
    }
}

unsafe fn write_json_key_value_ref(jkv: &Option<Box<protobuf::JsonKeyValue>>) -> *mut bindings_raw::JsonKeyValue {
    match jkv {
        Some(k) => write_json_key_value(k),
        None => std::ptr::null_mut(),
    }
}
