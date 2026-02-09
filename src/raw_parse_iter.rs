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
use std::os::raw::c_char;

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
pub fn parse_raw_iter(statement: &str) -> Result<ParseResult> {
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
    let mut processor = Processor::new(raw_stmt.stmt);

    processor.process();

    protobuf::RawStmt { stmt: processor.get_result(), stmt_location: raw_stmt.stmt_location, stmt_len: raw_stmt.stmt_len }
}

struct ProcessingNode {
    collect: bool,
    node: *const bindings_raw::Node,
}

impl ProcessingNode {
    #[inline]
    fn with_node(node: *const bindings_raw::Node) -> Self {
        Self { collect: false, node }
    }

    #[inline]
    fn with_collect(node: *const bindings_raw::Node) -> Self {
        Self { collect: true, node }
    }
}

struct Processor {
    stack: Vec<ProcessingNode>,
    result_stack: Vec<protobuf::Node>,
}

impl Processor {
    fn new(root: *const bindings_raw::Node) -> Self {
        Self { stack: vec![ProcessingNode { collect: false, node: root }], result_stack: Default::default() }
    }

    fn get_result(mut self) -> Option<Box<protobuf::Node>> {
        let result = self.result_stack.pop();

        assert!(self.result_stack.is_empty(), "Result stack should be empty after processing, but has {} items left", self.result_stack.len());

        result.map(Box::new)
    }

    unsafe fn process(&mut self) {
        while let Some(entry) = self.stack.pop() {
            let collect = entry.collect;
            let node_ptr = entry.node;

            if node_ptr.is_null() {
                continue;
            }

            let node_tag = (*node_ptr).type_;

            match node_tag {
                // === SelectStmt (boxed) ===
                bindings_raw::NodeTag_T_SelectStmt => {
                    let stmt = node_ptr as *const bindings_raw::SelectStmt;

                    if collect {
                        let node = self.collect_select_stmt(&*stmt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_select_stmt(&*stmt);
                    }
                }

                // === ResTarget (boxed) ===
                bindings_raw::NodeTag_T_ResTarget => {
                    let rt = node_ptr as *const bindings_raw::ResTarget;

                    if collect {
                        let node = self.collect_res_target(&*rt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_res_target(&*rt);
                    }
                }

                // === ColumnRef (has fields list) ===
                bindings_raw::NodeTag_T_ColumnRef => {
                    let cr = node_ptr as *const bindings_raw::ColumnRef;

                    if collect {
                        let node = self.collect_column_ref(&*cr);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_column_ref(&*cr);
                    }
                }

                // === RangeVar (iterative — alias.colnames queued) ===
                bindings_raw::NodeTag_T_RangeVar => {
                    let rv = node_ptr as *const bindings_raw::RangeVar;
                    if collect {
                        let node = self.collect_range_var(&*rv);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_range_var(&*rv);
                    }
                }

                // === JoinExpr (iterative) ===
                bindings_raw::NodeTag_T_JoinExpr => {
                    let je = node_ptr as *const bindings_raw::JoinExpr;
                    if collect {
                        let node = self.collect_join_expr(&*je);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_join_expr(&*je);
                    }
                }

                // === A_Expr (iterative) ===
                bindings_raw::NodeTag_T_A_Expr => {
                    let expr = node_ptr as *const bindings_raw::A_Expr;
                    if collect {
                        let node = self.collect_a_expr(&*expr);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_a_expr(&*expr);
                    }
                }

                // === FuncCall (iterative) ===
                bindings_raw::NodeTag_T_FuncCall => {
                    let fc = node_ptr as *const bindings_raw::FuncCall;
                    if collect {
                        let node = self.collect_func_call(&*fc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_func_call(&*fc);
                    }
                }

                // === WindowDef (iterative) ===
                bindings_raw::NodeTag_T_WindowDef => {
                    let wd = node_ptr as *const bindings_raw::WindowDef;
                    if collect {
                        let node = self.collect_window_def(&*wd);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_window_def(&*wd);
                    }
                }

                // === InsertStmt (iterative) ===
                bindings_raw::NodeTag_T_InsertStmt => {
                    let stmt = node_ptr as *const bindings_raw::InsertStmt;
                    if collect {
                        let node = self.collect_insert_stmt(&*stmt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_insert_stmt(&*stmt);
                    }
                }

                // === UpdateStmt (iterative) ===
                bindings_raw::NodeTag_T_UpdateStmt => {
                    let stmt = node_ptr as *const bindings_raw::UpdateStmt;
                    if collect {
                        let node = self.collect_update_stmt(&*stmt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_update_stmt(&*stmt);
                    }
                }

                // === DeleteStmt (iterative) ===
                bindings_raw::NodeTag_T_DeleteStmt => {
                    let stmt = node_ptr as *const bindings_raw::DeleteStmt;
                    if collect {
                        let node = self.collect_delete_stmt(&*stmt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_delete_stmt(&*stmt);
                    }
                }

                // === List (iterative — items queued) ===
                bindings_raw::NodeTag_T_List => {
                    let list = node_ptr as *mut bindings_raw::List;
                    if collect {
                        let items = self.fetch_list_results(list);
                        self.push_result(protobuf::node::Node::List(protobuf::List { items }));
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_list_nodes(list);
                    }
                }

                // === SortBy (iterative) ===
                bindings_raw::NodeTag_T_SortBy => {
                    let sb = node_ptr as *const bindings_raw::SortBy;
                    if collect {
                        let node = self.collect_sort_by(&*sb);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_sort_by(&*sb);
                    }
                }

                // === TypeCast (iterative) ===
                bindings_raw::NodeTag_T_TypeCast => {
                    let tc = node_ptr as *const bindings_raw::TypeCast;
                    if collect {
                        let node = self.collect_type_cast(&*tc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_type_cast(&*tc);
                    }
                }

                // === TypeName (iterative) ===
                bindings_raw::NodeTag_T_TypeName => {
                    let tn = node_ptr as *const bindings_raw::TypeName;
                    if collect {
                        let node = self.collect_type_name(&*tn);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_type_name(&*tn);
                    }
                }

                // === A_Const (leaf — no children) ===
                bindings_raw::NodeTag_T_A_Const => {
                    let aconst = node_ptr as *const bindings_raw::A_Const;
                    let converted = convert_a_const(&*aconst);
                    self.push_result(protobuf::node::Node::AConst(converted));
                }

                // === A_Star (leaf) ===
                bindings_raw::NodeTag_T_A_Star => {
                    self.push_result(protobuf::node::Node::AStar(protobuf::AStar {}));
                }

                // === String (leaf) ===
                bindings_raw::NodeTag_T_String => {
                    let s = node_ptr as *const bindings_raw::String;
                    self.push_result(protobuf::node::Node::String(convert_string(&*s)));
                }

                // === Integer (leaf) ===
                bindings_raw::NodeTag_T_Integer => {
                    let i = node_ptr as *const bindings_raw::Integer;
                    self.push_result(protobuf::node::Node::Integer(protobuf::Integer { ival: (*i).ival }));
                }

                // === Float (leaf) ===
                bindings_raw::NodeTag_T_Float => {
                    let f = node_ptr as *const bindings_raw::Float;
                    let fval = if (*f).fval.is_null() { std::string::String::new() } else { CStr::from_ptr((*f).fval).to_string_lossy().to_string() };
                    self.push_result(protobuf::node::Node::Float(protobuf::Float { fval }));
                }

                // === Boolean (leaf) ===
                bindings_raw::NodeTag_T_Boolean => {
                    let b = node_ptr as *const bindings_raw::Boolean;
                    self.push_result(protobuf::node::Node::Boolean(protobuf::Boolean { boolval: (*b).boolval }));
                }

                // === BitString (leaf) ===
                bindings_raw::NodeTag_T_BitString => {
                    let bs = node_ptr as *const bindings_raw::BitString;
                    self.push_result(protobuf::node::Node::BitString(convert_bit_string(&*bs)));
                }

                // === ParamRef (leaf) ===
                bindings_raw::NodeTag_T_ParamRef => {
                    let pr = node_ptr as *const bindings_raw::ParamRef;
                    self.push_result(protobuf::node::Node::ParamRef(protobuf::ParamRef { number: (*pr).number, location: (*pr).location }));
                }

                // === BoolExpr (iterative) ===
                bindings_raw::NodeTag_T_BoolExpr => {
                    let be = node_ptr as *const bindings_raw::BoolExpr;
                    if collect {
                        let node = self.collect_bool_expr(&*be);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_bool_expr(&*be);
                    }
                }

                // === NullTest (iterative) ===
                bindings_raw::NodeTag_T_NullTest => {
                    let nt = node_ptr as *const bindings_raw::NullTest;
                    if collect {
                        let node = self.collect_null_test(&*nt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_null_test(&*nt);
                    }
                }

                // === SubLink (iterative) ===
                bindings_raw::NodeTag_T_SubLink => {
                    let sl = node_ptr as *const bindings_raw::SubLink;
                    if collect {
                        let node = self.collect_sub_link(&*sl);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_sub_link(&*sl);
                    }
                }

                // === CaseExpr (iterative) ===
                bindings_raw::NodeTag_T_CaseExpr => {
                    let ce = node_ptr as *const bindings_raw::CaseExpr;
                    if collect {
                        let node = self.collect_case_expr(&*ce);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_case_expr(&*ce);
                    }
                }

                // === CaseWhen (iterative) ===
                bindings_raw::NodeTag_T_CaseWhen => {
                    let cw = node_ptr as *const bindings_raw::CaseWhen;
                    if collect {
                        let node = self.collect_case_when(&*cw);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_case_when(&*cw);
                    }
                }

                // === CoalesceExpr (iterative) ===
                bindings_raw::NodeTag_T_CoalesceExpr => {
                    let ce = node_ptr as *const bindings_raw::CoalesceExpr;
                    if collect {
                        let node = self.collect_coalesce_expr(&*ce);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_coalesce_expr(&*ce);
                    }
                }

                // === MinMaxExpr (iterative) ===
                bindings_raw::NodeTag_T_MinMaxExpr => {
                    let mme = node_ptr as *const bindings_raw::MinMaxExpr;
                    if collect {
                        let node = self.collect_min_max_expr(&*mme);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_min_max_expr(&*mme);
                    }
                }

                // === SQLValueFunction (leaf) ===
                bindings_raw::NodeTag_T_SQLValueFunction => {
                    let svf = node_ptr as *const bindings_raw::SQLValueFunction;
                    self.push_result(protobuf::node::Node::SqlvalueFunction(Box::new(protobuf::SqlValueFunction {
                        xpr: None,
                        op: (*svf).op as i32 + 1,
                        r#type: (*svf).type_,
                        typmod: (*svf).typmod,
                        location: (*svf).location,
                    })));
                }

                // === SetToDefault (leaf) ===
                bindings_raw::NodeTag_T_SetToDefault => {
                    let std_ = node_ptr as *const bindings_raw::SetToDefault;
                    self.push_result(protobuf::node::Node::SetToDefault(Box::new(protobuf::SetToDefault {
                        xpr: None,
                        type_id: (*std_).typeId,
                        type_mod: (*std_).typeMod,
                        collation: (*std_).collation,
                        location: (*std_).location,
                    })));
                }

                // === BooleanTest (iterative) ===
                bindings_raw::NodeTag_T_BooleanTest => {
                    let bt = node_ptr as *const bindings_raw::BooleanTest;
                    if collect {
                        let node = self.collect_boolean_test(&*bt);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_boolean_test(&*bt);
                    }
                }

                // === A_ArrayExpr (iterative) ===
                bindings_raw::NodeTag_T_A_ArrayExpr => {
                    let ae = node_ptr as *const bindings_raw::A_ArrayExpr;
                    if collect {
                        let node = self.collect_a_array_expr(&*ae);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_a_array_expr(&*ae);
                    }
                }

                // === A_Indirection (iterative) ===
                bindings_raw::NodeTag_T_A_Indirection => {
                    let ai = node_ptr as *const bindings_raw::A_Indirection;
                    if collect {
                        let node = self.collect_a_indirection(&*ai);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_a_indirection(&*ai);
                    }
                }

                // === A_Indices (iterative) ===
                bindings_raw::NodeTag_T_A_Indices => {
                    let ai = node_ptr as *const bindings_raw::A_Indices;
                    if collect {
                        let node = self.collect_a_indices(&*ai);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_a_indices(&*ai);
                    }
                }

                // === CollateClause (iterative) ===
                bindings_raw::NodeTag_T_CollateClause => {
                    let cc = node_ptr as *const bindings_raw::CollateClause;
                    if collect {
                        let node = self.collect_collate_clause(&*cc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_collate_clause(&*cc);
                    }
                }

                // === RangeSubselect (iterative) ===
                bindings_raw::NodeTag_T_RangeSubselect => {
                    let rs = node_ptr as *const bindings_raw::RangeSubselect;
                    if collect {
                        let node = self.collect_range_subselect(&*rs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_range_subselect(&*rs);
                    }
                }

                // === CommonTableExpr (iterative) ===
                bindings_raw::NodeTag_T_CommonTableExpr => {
                    let cte = node_ptr as *const bindings_raw::CommonTableExpr;
                    if collect {
                        let node = self.collect_common_table_expr(&*cte);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_common_table_expr(&*cte);
                    }
                }

                // === GroupingSet (iterative) ===
                bindings_raw::NodeTag_T_GroupingSet => {
                    let gs = node_ptr as *const bindings_raw::GroupingSet;
                    if collect {
                        let node = self.collect_grouping_set(&*gs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_grouping_set(&*gs);
                    }
                }

                // === LockingClause (iterative) ===
                bindings_raw::NodeTag_T_LockingClause => {
                    let lc = node_ptr as *const bindings_raw::LockingClause;
                    if collect {
                        let node = self.collect_locking_clause(&*lc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_locking_clause(&*lc);
                    }
                }

                // === RowExpr (iterative) ===
                bindings_raw::NodeTag_T_RowExpr => {
                    let re = node_ptr as *const bindings_raw::RowExpr;
                    if collect {
                        let node = self.collect_row_expr(&*re);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_row_expr(&*re);
                    }
                }

                // === MultiAssignRef (iterative) ===
                bindings_raw::NodeTag_T_MultiAssignRef => {
                    let mar = node_ptr as *const bindings_raw::MultiAssignRef;
                    if collect {
                        let node = self.collect_multi_assign_ref(&*mar);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_multi_assign_ref(&*mar);
                    }
                }

                // === CTESearchClause (iterative) ===
                bindings_raw::NodeTag_T_CTESearchClause => {
                    let csc = node_ptr as *const bindings_raw::CTESearchClause;
                    if collect {
                        let node = self.collect_cte_search_clause(&*csc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_cte_search_clause(&*csc);
                    }
                }

                // === CTECycleClause (iterative) ===
                bindings_raw::NodeTag_T_CTECycleClause => {
                    let ccc = node_ptr as *const bindings_raw::CTECycleClause;
                    if collect {
                        let node = self.collect_cte_cycle_clause(&*ccc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_cte_cycle_clause(&*ccc);
                    }
                }

                // === Alias (iterative) ===
                bindings_raw::NodeTag_T_Alias => {
                    let alias = node_ptr as *const bindings_raw::Alias;
                    if collect {
                        let node = self.collect_alias(&*alias);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alias(&*alias);
                    }
                }

                // === GroupingFunc (iterative) ===
                bindings_raw::NodeTag_T_GroupingFunc => {
                    let gf = node_ptr as *const bindings_raw::GroupingFunc;
                    if collect {
                        let node = self.collect_grouping_func(&*gf);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_grouping_func(&*gf);
                    }
                }

                // === IndexElem (iterative) ===
                bindings_raw::NodeTag_T_IndexElem => {
                    let ie = node_ptr as *const bindings_raw::IndexElem;
                    if collect {
                        let node = self.collect_index_elem(&*ie);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_index_elem(&*ie);
                    }
                }

                // === DefElem (iterative) ===
                bindings_raw::NodeTag_T_DefElem => {
                    let de = node_ptr as *const bindings_raw::DefElem;
                    if collect {
                        let node = self.collect_def_elem(&*de);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_def_elem(&*de);
                    }
                }

                // === ColumnDef (iterative) ===
                bindings_raw::NodeTag_T_ColumnDef => {
                    let cd = node_ptr as *const bindings_raw::ColumnDef;
                    if collect {
                        let node = self.collect_column_def(&*cd);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_column_def(&*cd);
                    }
                }

                // === Constraint (iterative) ===
                bindings_raw::NodeTag_T_Constraint => {
                    let c = node_ptr as *const bindings_raw::Constraint;
                    if collect {
                        let node = self.collect_constraint(&*c);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_constraint(&*c);
                    }
                }

                // === RoleSpec (leaf-like) ===
                bindings_raw::NodeTag_T_RoleSpec => {
                    let rs = node_ptr as *const bindings_raw::RoleSpec;
                    self.push_result(protobuf::node::Node::RoleSpec(protobuf::RoleSpec {
                        roletype: (*rs).roletype as i32 + 1,
                        rolename: convert_c_string((*rs).rolename),
                        location: (*rs).location,
                    }));
                }

                // === CreateStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateStmt => {
                    let cs = node_ptr as *const bindings_raw::CreateStmt;
                    if collect {
                        let node = self.collect_create_stmt(&*cs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_stmt(&*cs);
                    }
                }

                // === DropStmt (iterative) ===
                bindings_raw::NodeTag_T_DropStmt => {
                    let ds = node_ptr as *const bindings_raw::DropStmt;
                    if collect {
                        let node = self.collect_drop_stmt(&*ds);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_drop_stmt(&*ds);
                    }
                }

                // === IndexStmt (iterative) ===
                bindings_raw::NodeTag_T_IndexStmt => {
                    let is_ = node_ptr as *const bindings_raw::IndexStmt;
                    if collect {
                        let node = self.collect_index_stmt(&*is_);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_index_stmt(&*is_);
                    }
                }

                // === AlterTableStmt (iterative) ===
                bindings_raw::NodeTag_T_AlterTableStmt => {
                    let ats = node_ptr as *const bindings_raw::AlterTableStmt;
                    if collect {
                        let node = self.collect_alter_table_stmt(&*ats);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_table_stmt(&*ats);
                    }
                }

                // === AlterTableCmd (iterative) ===
                bindings_raw::NodeTag_T_AlterTableCmd => {
                    let atc = node_ptr as *const bindings_raw::AlterTableCmd;
                    if collect {
                        let node = self.collect_alter_table_cmd(&*atc);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_table_cmd(&*atc);
                    }
                }

                // === RenameStmt (iterative) ===
                bindings_raw::NodeTag_T_RenameStmt => {
                    let rs = node_ptr as *const bindings_raw::RenameStmt;
                    if collect {
                        let node = self.collect_rename_stmt(&*rs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_rename_stmt(&*rs);
                    }
                }

                // === ViewStmt (iterative) ===
                bindings_raw::NodeTag_T_ViewStmt => {
                    let vs = node_ptr as *const bindings_raw::ViewStmt;
                    if collect {
                        let node = self.collect_view_stmt(&*vs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_view_stmt(&*vs);
                    }
                }

                // === CreateTableAsStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateTableAsStmt => {
                    let ctas = node_ptr as *const bindings_raw::CreateTableAsStmt;
                    if collect {
                        let node = self.collect_create_table_as_stmt(&*ctas);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_table_as_stmt(&*ctas);
                    }
                }

                // === TruncateStmt (iterative) ===
                bindings_raw::NodeTag_T_TruncateStmt => {
                    let ts = node_ptr as *const bindings_raw::TruncateStmt;
                    if collect {
                        let node = self.collect_truncate_stmt(&*ts);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_truncate_stmt(&*ts);
                    }
                }

                // === AlterOwnerStmt (iterative) ===
                bindings_raw::NodeTag_T_AlterOwnerStmt => {
                    let aos = node_ptr as *const bindings_raw::AlterOwnerStmt;
                    if collect {
                        let node = self.collect_alter_owner_stmt(&*aos);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_owner_stmt(&*aos);
                    }
                }

                // === CreateSeqStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateSeqStmt => {
                    let css = node_ptr as *const bindings_raw::CreateSeqStmt;
                    if collect {
                        let node = self.collect_create_seq_stmt(&*css);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_seq_stmt(&*css);
                    }
                }

                // === AlterSeqStmt (iterative) ===
                bindings_raw::NodeTag_T_AlterSeqStmt => {
                    let ass_ = node_ptr as *const bindings_raw::AlterSeqStmt;
                    if collect {
                        let node = self.collect_alter_seq_stmt(&*ass_);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_seq_stmt(&*ass_);
                    }
                }

                // === CreateDomainStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateDomainStmt => {
                    let cds = node_ptr as *const bindings_raw::CreateDomainStmt;
                    if collect {
                        let node = self.collect_create_domain_stmt(&*cds);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_domain_stmt(&*cds);
                    }
                }

                // === CompositeTypeStmt (iterative) ===
                bindings_raw::NodeTag_T_CompositeTypeStmt => {
                    let cts = node_ptr as *const bindings_raw::CompositeTypeStmt;
                    if collect {
                        let node = self.collect_composite_type_stmt(&*cts);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_composite_type_stmt(&*cts);
                    }
                }

                // === CreateEnumStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateEnumStmt => {
                    let ces = node_ptr as *const bindings_raw::CreateEnumStmt;
                    if collect {
                        let node = self.collect_create_enum_stmt(&*ces);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_enum_stmt(&*ces);
                    }
                }

                // === CreateExtensionStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateExtensionStmt => {
                    let ces = node_ptr as *const bindings_raw::CreateExtensionStmt;
                    if collect {
                        let node = self.collect_create_extension_stmt(&*ces);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_extension_stmt(&*ces);
                    }
                }

                // === CreatePublicationStmt (iterative) ===
                bindings_raw::NodeTag_T_CreatePublicationStmt => {
                    let cps = node_ptr as *const bindings_raw::CreatePublicationStmt;
                    if collect {
                        let node = self.collect_create_publication_stmt(&*cps);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_publication_stmt(&*cps);
                    }
                }

                // === AlterPublicationStmt (iterative) ===
                bindings_raw::NodeTag_T_AlterPublicationStmt => {
                    let aps = node_ptr as *const bindings_raw::AlterPublicationStmt;
                    if collect {
                        let node = self.collect_alter_publication_stmt(&*aps);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_publication_stmt(&*aps);
                    }
                }

                // === CreateSubscriptionStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateSubscriptionStmt => {
                    let css = node_ptr as *const bindings_raw::CreateSubscriptionStmt;
                    if collect {
                        let node = self.collect_create_subscription_stmt(&*css);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_subscription_stmt(&*css);
                    }
                }

                // === AlterSubscriptionStmt (iterative) ===
                bindings_raw::NodeTag_T_AlterSubscriptionStmt => {
                    let ass_ = node_ptr as *const bindings_raw::AlterSubscriptionStmt;
                    if collect {
                        let node = self.collect_alter_subscription_stmt(&*ass_);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_alter_subscription_stmt(&*ass_);
                    }
                }

                // === CreateTrigStmt (iterative) ===
                bindings_raw::NodeTag_T_CreateTrigStmt => {
                    let cts = node_ptr as *const bindings_raw::CreateTrigStmt;
                    if collect {
                        let node = self.collect_create_trig_stmt(&*cts);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_create_trig_stmt(&*cts);
                    }
                }

                // === PublicationObjSpec (iterative) ===
                bindings_raw::NodeTag_T_PublicationObjSpec => {
                    let pos = node_ptr as *const bindings_raw::PublicationObjSpec;
                    if collect {
                        let node = self.collect_publication_obj_spec(&*pos);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_publication_obj_spec(&*pos);
                    }
                }

                // === TriggerTransition (leaf) ===
                bindings_raw::NodeTag_T_TriggerTransition => {
                    let tt = node_ptr as *const bindings_raw::TriggerTransition;
                    self.push_result(protobuf::node::Node::TriggerTransition(protobuf::TriggerTransition {
                        name: convert_c_string((*tt).name),
                        is_new: (*tt).isNew,
                        is_table: (*tt).isTable,
                    }));
                }

                // === PartitionElem (iterative) ===
                bindings_raw::NodeTag_T_PartitionElem => {
                    let pe = node_ptr as *const bindings_raw::PartitionElem;
                    if collect {
                        let node = self.collect_partition_elem(&*pe);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_partition_elem(&*pe);
                    }
                }

                // === PartitionSpec (iterative) ===
                bindings_raw::NodeTag_T_PartitionSpec => {
                    let ps = node_ptr as *const bindings_raw::PartitionSpec;
                    if collect {
                        let node = self.collect_partition_spec(&*ps);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_partition_spec(&*ps);
                    }
                }

                // === PartitionBoundSpec (iterative) ===
                bindings_raw::NodeTag_T_PartitionBoundSpec => {
                    let pbs = node_ptr as *const bindings_raw::PartitionBoundSpec;
                    if collect {
                        let node = self.collect_partition_bound_spec(&*pbs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_partition_bound_spec(&*pbs);
                    }
                }

                // === PartitionRangeDatum (iterative) ===
                bindings_raw::NodeTag_T_PartitionRangeDatum => {
                    let prd = node_ptr as *const bindings_raw::PartitionRangeDatum;
                    if collect {
                        let node = self.collect_partition_range_datum(&*prd);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_partition_range_datum(&*prd);
                    }
                }

                // === ExplainStmt (iterative) ===
                bindings_raw::NodeTag_T_ExplainStmt => {
                    let es = node_ptr as *const bindings_raw::ExplainStmt;
                    if collect {
                        let node = self.collect_explain_stmt(&*es);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_explain_stmt(&*es);
                    }
                }

                // === CopyStmt (iterative) ===
                bindings_raw::NodeTag_T_CopyStmt => {
                    let cs = node_ptr as *const bindings_raw::CopyStmt;
                    if collect {
                        let node = self.collect_copy_stmt(&*cs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_copy_stmt(&*cs);
                    }
                }

                // === PrepareStmt (iterative) ===
                bindings_raw::NodeTag_T_PrepareStmt => {
                    let ps = node_ptr as *const bindings_raw::PrepareStmt;
                    if collect {
                        let node = self.collect_prepare_stmt(&*ps);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_prepare_stmt(&*ps);
                    }
                }

                // === ExecuteStmt (iterative) ===
                bindings_raw::NodeTag_T_ExecuteStmt => {
                    let es = node_ptr as *const bindings_raw::ExecuteStmt;
                    if collect {
                        let node = self.collect_execute_stmt(&*es);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_execute_stmt(&*es);
                    }
                }

                // === DeallocateStmt (leaf) ===
                bindings_raw::NodeTag_T_DeallocateStmt => {
                    let ds = node_ptr as *const bindings_raw::DeallocateStmt;
                    self.push_result(protobuf::node::Node::DeallocateStmt(protobuf::DeallocateStmt {
                        name: convert_c_string((*ds).name),
                        isall: (*ds).isall,
                        location: (*ds).location,
                    }));
                }

                // === TransactionStmt (iterative) ===
                bindings_raw::NodeTag_T_TransactionStmt => {
                    let ts = node_ptr as *const bindings_raw::TransactionStmt;
                    if collect {
                        let node = self.collect_transaction_stmt(&*ts);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_transaction_stmt(&*ts);
                    }
                }

                // === VacuumStmt (iterative) ===
                bindings_raw::NodeTag_T_VacuumStmt => {
                    let vs = node_ptr as *const bindings_raw::VacuumStmt;
                    if collect {
                        let node = self.collect_vacuum_stmt(&*vs);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_vacuum_stmt(&*vs);
                    }
                }

                // === VacuumRelation (iterative) ===
                bindings_raw::NodeTag_T_VacuumRelation => {
                    let vr = node_ptr as *const bindings_raw::VacuumRelation;
                    if collect {
                        let node = self.collect_vacuum_relation(&*vr);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_vacuum_relation(&*vr);
                    }
                }

                // === VariableSetStmt (iterative) ===
                bindings_raw::NodeTag_T_VariableSetStmt => {
                    let vss = node_ptr as *const bindings_raw::VariableSetStmt;
                    if collect {
                        let node = self.collect_variable_set_stmt(&*vss);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_variable_set_stmt(&*vss);
                    }
                }

                // === VariableShowStmt (leaf) ===
                bindings_raw::NodeTag_T_VariableShowStmt => {
                    let vss = node_ptr as *const bindings_raw::VariableShowStmt;
                    self.push_result(protobuf::node::Node::VariableShowStmt(protobuf::VariableShowStmt { name: convert_c_string((*vss).name) }));
                }

                // === NotifyStmt (leaf) ===
                bindings_raw::NodeTag_T_NotifyStmt => {
                    let ns = node_ptr as *const bindings_raw::NotifyStmt;
                    self.push_result(protobuf::node::Node::NotifyStmt(protobuf::NotifyStmt {
                        conditionname: convert_c_string((*ns).conditionname),
                        payload: convert_c_string((*ns).payload),
                    }));
                }

                // === ListenStmt (leaf) ===
                bindings_raw::NodeTag_T_ListenStmt => {
                    let ls = node_ptr as *const bindings_raw::ListenStmt;
                    self.push_result(protobuf::node::Node::ListenStmt(protobuf::ListenStmt { conditionname: convert_c_string((*ls).conditionname) }));
                }

                // === UnlistenStmt (leaf) ===
                bindings_raw::NodeTag_T_UnlistenStmt => {
                    let us = node_ptr as *const bindings_raw::UnlistenStmt;
                    self.push_result(protobuf::node::Node::UnlistenStmt(protobuf::UnlistenStmt {
                        conditionname: convert_c_string((*us).conditionname),
                    }));
                }

                // === DiscardStmt (leaf) ===
                bindings_raw::NodeTag_T_DiscardStmt => {
                    let ds = node_ptr as *const bindings_raw::DiscardStmt;
                    self.push_result(protobuf::node::Node::DiscardStmt(protobuf::DiscardStmt { target: (*ds).target as i32 + 1 }));
                }

                // === LockStmt (iterative) ===
                bindings_raw::NodeTag_T_LockStmt => {
                    let ls = node_ptr as *const bindings_raw::LockStmt;
                    if collect {
                        let node = self.collect_lock_stmt(&*ls);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_lock_stmt(&*ls);
                    }
                }

                // === DoStmt (iterative) ===
                bindings_raw::NodeTag_T_DoStmt => {
                    let ds = node_ptr as *const bindings_raw::DoStmt;
                    if collect {
                        let node = self.collect_do_stmt(&*ds);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_do_stmt(&*ds);
                    }
                }

                // === ObjectWithArgs (iterative) ===
                bindings_raw::NodeTag_T_ObjectWithArgs => {
                    let owa = node_ptr as *const bindings_raw::ObjectWithArgs;
                    if collect {
                        let node = self.collect_object_with_args(&*owa);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_object_with_args(&*owa);
                    }
                }

                // === CoerceToDomain (iterative) ===
                bindings_raw::NodeTag_T_CoerceToDomain => {
                    let ctd = node_ptr as *const bindings_raw::CoerceToDomain;
                    if collect {
                        let node = self.collect_coerce_to_domain(&*ctd);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_coerce_to_domain(&*ctd);
                    }
                }

                // === FunctionParameter (iterative) ===
                bindings_raw::NodeTag_T_FunctionParameter => {
                    let fp = node_ptr as *const bindings_raw::FunctionParameter;
                    if collect {
                        let node = self.collect_function_parameter(&*fp);
                        self.push_result(node);
                    } else {
                        self.queue_collect(node_ptr);
                        self.queue_function_parameter(&*fp);
                    }
                }

                _ => {
                    panic!("[ERROR] Unhandled node tag={} in iterative processor. This type needs to be migrated.", node_tag,);
                }
            }
        }
    }

    fn queue_node(&mut self, node: *const bindings_raw::Node) {
        self.stack.push(ProcessingNode::with_node(node));
    }

    fn queue_collect(&mut self, node: *const bindings_raw::Node) {
        self.stack.push(ProcessingNode::with_collect(node))
    }

    fn push_result(&mut self, node: protobuf::node::Node) {
        self.result_stack.push(protobuf::Node { node: Some(node) });
    }

    fn single_result(&mut self, node: *const bindings_raw::Node) -> Option<protobuf::Node> {
        if node.is_null() {
            return None;
        }

        let result = self.result_stack.pop().expect("result stack should not be empty while processing");
        Some(result)
    }

    fn single_result_box(&mut self, node: *const bindings_raw::Node) -> Option<Box<protobuf::Node>> {
        self.single_result(node).map(Box::new)
    }

    fn fetch_results(&mut self, count: usize) -> Vec<protobuf::Node> {
        if count > self.result_stack.len() {
            panic!(
                "fetch_results: count ({}) > result_stack.len ({}) — stack contents: {:?}",
                count,
                self.result_stack.len(),
                self.result_stack.iter().map(|n| format!("{:?}", n.node.as_ref().map(|n| std::mem::discriminant(n)))).collect::<Vec<_>>(),
            );
        }
        let start = self.result_stack.len() - count;
        self.result_stack.drain(start..).rev().collect()
    }

    unsafe fn queue_list_nodes(&mut self, list: *mut bindings_raw::List) {
        if list.is_null() {
            return;
        }
        let list = &*list;
        let length = list.length as usize;

        self.stack.reserve(length);

        for i in 0..length {
            let cell = list.elements.add(i);
            let node = (*cell).ptr_value as *const bindings_raw::Node;
            if !node.is_null() {
            } else {
            }
            self.stack.push(ProcessingNode::with_node(node));
        }
    }

    unsafe fn fetch_list_results(&mut self, list: *const bindings_raw::List) -> Vec<protobuf::Node> {
        if list.is_null() {
            return Vec::new();
        }
        let list = &*list;
        let length = list.length as usize;

        self.fetch_results(length)
    }

    unsafe fn queue_into_clause(&mut self, ic: *const bindings_raw::IntoClause) {
        if ic.is_null() {
            return;
        }
        let ic_ref = &*ic;
        if !ic_ref.rel.is_null() {
            self.queue_node(ic_ref.rel as *const bindings_raw::Node);
        }
        self.queue_list_nodes(ic_ref.colNames);
        self.queue_list_nodes(ic_ref.options);
        self.queue_node(ic_ref.viewQuery);
    }

    unsafe fn fetch_into_clause(&mut self, ic: *const bindings_raw::IntoClause) -> Option<Box<protobuf::IntoClause>> {
        if ic.is_null() {
            return None;
        }
        let ic_ref = &*ic;
        let rel = if ic_ref.rel.is_null() { None } else { self.pop_range_var() };
        let col_names = self.fetch_list_results(ic_ref.colNames);
        let options = self.fetch_list_results(ic_ref.options);
        let view_query = self.single_result_box(ic_ref.viewQuery);
        Some(Box::new(protobuf::IntoClause {
            rel,
            col_names,
            access_method: convert_c_string(ic_ref.accessMethod),
            options,
            on_commit: ic_ref.onCommit as i32 + 1,
            table_space_name: convert_c_string(ic_ref.tableSpaceName),
            view_query,
            skip_data: ic_ref.skipData,
        }))
    }

    unsafe fn queue_with_clause(&mut self, wc: *mut bindings_raw::WithClause) {
        if wc.is_null() {
            return;
        }

        let wc = &*wc;

        self.queue_list_nodes(wc.ctes);
    }

    unsafe fn fetch_with_clause(&mut self, wc: *mut bindings_raw::WithClause) -> Option<protobuf::WithClause> {
        if wc.is_null() {
            return None;
        }

        let wc = &*wc;

        Some(protobuf::WithClause { ctes: self.fetch_list_results(wc.ctes), recursive: wc.recursive, location: wc.location })
    }

    unsafe fn queue_select_stmt(&mut self, stmt: &bindings_raw::SelectStmt) {
        self.queue_list_nodes(stmt.distinctClause);
        self.queue_into_clause(stmt.intoClause);
        self.queue_list_nodes(stmt.targetList);
        self.queue_list_nodes(stmt.fromClause);
        self.queue_node(stmt.whereClause);
        self.queue_list_nodes(stmt.groupClause);
        self.queue_node(stmt.havingClause);
        self.queue_list_nodes(stmt.windowClause);
        self.queue_list_nodes(stmt.valuesLists);
        self.queue_list_nodes(stmt.sortClause);
        self.queue_node(stmt.limitOffset);
        self.queue_node(stmt.limitCount);
        self.queue_list_nodes(stmt.lockingClause);
        self.queue_with_clause(stmt.withClause);

        if !stmt.larg.is_null() {
            self.queue_node(stmt.larg as *const bindings_raw::Node);
        }

        if !stmt.rarg.is_null() {
            self.queue_node(stmt.rarg as *const bindings_raw::Node);
        }
    }

    fn pop_select_stmt(&mut self) -> Option<Box<protobuf::SelectStmt>> {
        self.result_stack.pop().and_then(|n| match n.node {
            Some(protobuf::node::Node::SelectStmt(s)) => Some(s),
            _ => None,
        })
    }

    unsafe fn collect_select_stmt(&mut self, stmt: &bindings_raw::SelectStmt) -> protobuf::node::Node {
        let a = protobuf::SelectStmt {
            distinct_clause: self.fetch_list_results(stmt.distinctClause),
            into_clause: self.fetch_into_clause(stmt.intoClause),
            target_list: self.fetch_list_results(stmt.targetList),
            from_clause: self.fetch_list_results(stmt.fromClause),
            where_clause: self.single_result_box(stmt.whereClause),
            group_clause: self.fetch_list_results(stmt.groupClause),
            group_distinct: stmt.groupDistinct,
            having_clause: self.single_result_box(stmt.havingClause),
            window_clause: self.fetch_list_results(stmt.windowClause),
            values_lists: self.fetch_list_results(stmt.valuesLists),
            sort_clause: self.fetch_list_results(stmt.sortClause),
            limit_offset: self.single_result_box(stmt.limitOffset),
            limit_count: self.single_result_box(stmt.limitCount),
            limit_option: stmt.limitOption as i32 + 1, // Protobuf enums have UNDEFINED=0, so C values need +1
            locking_clause: self.fetch_list_results(stmt.lockingClause),
            with_clause: self.fetch_with_clause(stmt.withClause),
            op: stmt.op as i32 + 1, // Protobuf SetOperation has UNDEFINED=0, so C values need +1
            all: stmt.all,
            larg: if stmt.larg.is_null() { None } else { self.pop_select_stmt() },
            rarg: if stmt.rarg.is_null() { None } else { self.pop_select_stmt() },
        };

        protobuf::node::Node::SelectStmt(Box::new(a))
    }

    // ====================================================================
    // ResTarget
    // ====================================================================

    unsafe fn queue_res_target(&mut self, rt: &bindings_raw::ResTarget) {
        // Queue children that need recursive processing:
        //   indirection (list of nodes) and val (single node)
        self.queue_list_nodes(rt.indirection);
        self.queue_node(rt.val);
    }

    unsafe fn collect_res_target(&mut self, rt: &bindings_raw::ResTarget) -> protobuf::node::Node {
        let indirection = self.fetch_list_results(rt.indirection);
        let val = self.single_result_box(rt.val);

        let res = protobuf::ResTarget { name: convert_c_string(rt.name), indirection, val, location: rt.location };
        protobuf::node::Node::ResTarget(Box::new(res))
    }

    // ====================================================================
    // ColumnRef
    // ====================================================================

    unsafe fn queue_column_ref(&mut self, cr: &bindings_raw::ColumnRef) {
        self.queue_list_nodes(cr.fields);
    }

    unsafe fn collect_column_ref(&mut self, cr: &bindings_raw::ColumnRef) -> protobuf::node::Node {
        let fields = self.fetch_list_results(cr.fields);
        protobuf::node::Node::ColumnRef(protobuf::ColumnRef { fields, location: cr.location })
    }

    // ====================================================================
    // RangeVar (iterative — queues alias.colnames)
    // ====================================================================

    unsafe fn queue_range_var(&mut self, rv: &bindings_raw::RangeVar) {
        if !rv.alias.is_null() {
            self.queue_list_nodes((*rv.alias).colnames);
        }
    }

    unsafe fn collect_range_var(&mut self, rv: &bindings_raw::RangeVar) -> protobuf::node::Node {
        let alias = if rv.alias.is_null() {
            None
        } else {
            let alias = &*rv.alias;
            Some(protobuf::Alias { aliasname: convert_c_string(alias.aliasname), colnames: self.fetch_list_results(alias.colnames) })
        };
        protobuf::node::Node::RangeVar(protobuf::RangeVar {
            catalogname: convert_c_string(rv.catalogname),
            schemaname: convert_c_string(rv.schemaname),
            relname: convert_c_string(rv.relname),
            inh: rv.inh,
            relpersistence: std::string::String::from_utf8_lossy(&[rv.relpersistence as u8]).to_string(),
            alias,
            location: rv.location,
        })
    }

    fn pop_range_var(&mut self) -> Option<protobuf::RangeVar> {
        self.result_stack.pop().and_then(|n| match n.node {
            Some(protobuf::node::Node::RangeVar(rv)) => Some(rv),
            other => panic!("[ERROR] Expected RangeVar on result stack, got {:?}", other.as_ref().map(|n| std::mem::discriminant(n))),
        })
    }

    // ====================================================================
    // JoinExpr
    // ====================================================================

    unsafe fn queue_join_expr(&mut self, je: &bindings_raw::JoinExpr) {
        self.queue_list_nodes(je.usingClause);
        self.queue_node(je.larg);
        self.queue_node(je.rarg);
        self.queue_node(je.quals);
        if !je.alias.is_null() {
            self.queue_list_nodes((*je.alias).colnames);
        }
        if !je.join_using_alias.is_null() {
            self.queue_list_nodes((*je.join_using_alias).colnames);
        }
    }

    unsafe fn collect_join_expr(&mut self, je: &bindings_raw::JoinExpr) -> protobuf::node::Node {
        let using_clause = self.fetch_list_results(je.usingClause);
        let larg = self.single_result_box(je.larg);
        let rarg = self.single_result_box(je.rarg);
        let quals = self.single_result_box(je.quals);
        let alias = if je.alias.is_null() {
            None
        } else {
            Some(protobuf::Alias { aliasname: convert_c_string((*je.alias).aliasname), colnames: self.fetch_list_results((*je.alias).colnames) })
        };
        let join_using_alias = if je.join_using_alias.is_null() {
            None
        } else {
            Some(protobuf::Alias {
                aliasname: convert_c_string((*je.join_using_alias).aliasname),
                colnames: self.fetch_list_results((*je.join_using_alias).colnames),
            })
        };
        protobuf::node::Node::JoinExpr(Box::new(protobuf::JoinExpr {
            jointype: je.jointype as i32 + 1,
            is_natural: je.isNatural,
            larg,
            rarg,
            using_clause,
            join_using_alias,
            quals,
            alias,
            rtindex: je.rtindex,
        }))
    }

    // ====================================================================
    // A_Expr
    // ====================================================================

    unsafe fn queue_a_expr(&mut self, expr: &bindings_raw::A_Expr) {
        self.queue_list_nodes(expr.name);
        self.queue_node(expr.lexpr);
        self.queue_node(expr.rexpr);
    }

    unsafe fn collect_a_expr(&mut self, expr: &bindings_raw::A_Expr) -> protobuf::node::Node {
        let name = self.fetch_list_results(expr.name);
        let lexpr = self.single_result_box(expr.lexpr);
        let rexpr = self.single_result_box(expr.rexpr);
        protobuf::node::Node::AExpr(Box::new(protobuf::AExpr { kind: expr.kind as i32 + 1, name, lexpr, rexpr, location: expr.location }))
    }

    // ====================================================================
    // FuncCall
    // ====================================================================

    unsafe fn queue_func_call(&mut self, fc: &bindings_raw::FuncCall) {
        self.queue_list_nodes(fc.funcname);
        self.queue_list_nodes(fc.args);
        self.queue_list_nodes(fc.agg_order);
        self.queue_node(fc.agg_filter);
        if !fc.over.is_null() {
            self.queue_node(fc.over as *const bindings_raw::Node);
        }
    }

    unsafe fn collect_func_call(&mut self, fc: &bindings_raw::FuncCall) -> protobuf::node::Node {
        let funcname = self.fetch_list_results(fc.funcname);
        let args = self.fetch_list_results(fc.args);
        let agg_order = self.fetch_list_results(fc.agg_order);
        let agg_filter = self.single_result_box(fc.agg_filter);
        let over = if fc.over.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::WindowDef(wd)) => Some(wd),
                _ => None,
            })
        };
        protobuf::node::Node::FuncCall(Box::new(protobuf::FuncCall {
            funcname,
            args,
            agg_order,
            agg_filter,
            over,
            agg_within_group: fc.agg_within_group,
            agg_star: fc.agg_star,
            agg_distinct: fc.agg_distinct,
            func_variadic: fc.func_variadic,
            funcformat: fc.funcformat as i32 + 1,
            location: fc.location,
        }))
    }

    // ====================================================================
    // WindowDef
    // ====================================================================

    unsafe fn queue_window_def(&mut self, wd: &bindings_raw::WindowDef) {
        self.queue_list_nodes(wd.partitionClause);
        self.queue_list_nodes(wd.orderClause);
        self.queue_node(wd.startOffset);
        self.queue_node(wd.endOffset);
    }

    unsafe fn collect_window_def(&mut self, wd: &bindings_raw::WindowDef) -> protobuf::node::Node {
        let partition_clause = self.fetch_list_results(wd.partitionClause);
        let order_clause = self.fetch_list_results(wd.orderClause);
        let start_offset = self.single_result_box(wd.startOffset);
        let end_offset = self.single_result_box(wd.endOffset);
        protobuf::node::Node::WindowDef(Box::new(protobuf::WindowDef {
            name: convert_c_string(wd.name),
            refname: convert_c_string(wd.refname),
            partition_clause,
            order_clause,
            frame_options: wd.frameOptions,
            start_offset,
            end_offset,
            location: wd.location,
        }))
    }

    // ====================================================================
    // InsertStmt
    // ====================================================================

    unsafe fn queue_insert_stmt(&mut self, stmt: &bindings_raw::InsertStmt) {
        if !stmt.relation.is_null() {
            self.queue_node(stmt.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(stmt.cols);
        self.queue_node(stmt.selectStmt);
        self.queue_on_conflict_clause(stmt.onConflictClause);
        self.queue_list_nodes(stmt.returningList);
        self.queue_with_clause(stmt.withClause);
    }

    unsafe fn collect_insert_stmt(&mut self, stmt: &bindings_raw::InsertStmt) -> protobuf::node::Node {
        let relation = if stmt.relation.is_null() { None } else { self.pop_range_var() };
        let cols = self.fetch_list_results(stmt.cols);
        let select_stmt = self.single_result_box(stmt.selectStmt);
        let on_conflict_clause = self.fetch_on_conflict_clause(stmt.onConflictClause);
        let returning_list = self.fetch_list_results(stmt.returningList);
        let with_clause = self.fetch_with_clause(stmt.withClause);
        protobuf::node::Node::InsertStmt(Box::new(protobuf::InsertStmt {
            relation,
            cols,
            select_stmt,
            on_conflict_clause,
            returning_list,
            with_clause,
            r#override: stmt.override_ as i32 + 1,
        }))
    }

    // ====================================================================
    // UpdateStmt
    // ====================================================================

    unsafe fn queue_update_stmt(&mut self, stmt: &bindings_raw::UpdateStmt) {
        if !stmt.relation.is_null() {
            self.queue_node(stmt.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(stmt.targetList);
        self.queue_node(stmt.whereClause);
        self.queue_list_nodes(stmt.fromClause);
        self.queue_list_nodes(stmt.returningList);
        self.queue_with_clause(stmt.withClause);
    }

    unsafe fn collect_update_stmt(&mut self, stmt: &bindings_raw::UpdateStmt) -> protobuf::node::Node {
        let relation = if stmt.relation.is_null() { None } else { self.pop_range_var() };
        let target_list = self.fetch_list_results(stmt.targetList);
        let where_clause = self.single_result_box(stmt.whereClause);
        let from_clause = self.fetch_list_results(stmt.fromClause);
        let returning_list = self.fetch_list_results(stmt.returningList);
        let with_clause = self.fetch_with_clause(stmt.withClause);
        protobuf::node::Node::UpdateStmt(Box::new(protobuf::UpdateStmt {
            relation,
            target_list,
            where_clause,
            from_clause,
            returning_list,
            with_clause,
        }))
    }

    // ====================================================================
    // DeleteStmt
    // ====================================================================

    unsafe fn queue_delete_stmt(&mut self, stmt: &bindings_raw::DeleteStmt) {
        if !stmt.relation.is_null() {
            self.queue_node(stmt.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(stmt.usingClause);
        self.queue_node(stmt.whereClause);
        self.queue_list_nodes(stmt.returningList);
        self.queue_with_clause(stmt.withClause);
    }

    unsafe fn collect_delete_stmt(&mut self, stmt: &bindings_raw::DeleteStmt) -> protobuf::node::Node {
        let relation = if stmt.relation.is_null() { None } else { self.pop_range_var() };
        let using_clause = self.fetch_list_results(stmt.usingClause);
        let where_clause = self.single_result_box(stmt.whereClause);
        let returning_list = self.fetch_list_results(stmt.returningList);
        let with_clause = self.fetch_with_clause(stmt.withClause);
        protobuf::node::Node::DeleteStmt(Box::new(protobuf::DeleteStmt { relation, using_clause, where_clause, returning_list, with_clause }))
    }

    // ====================================================================
    // SortBy
    // ====================================================================

    unsafe fn queue_sort_by(&mut self, sb: &bindings_raw::SortBy) {
        self.queue_node(sb.node);
        self.queue_list_nodes(sb.useOp);
    }

    unsafe fn collect_sort_by(&mut self, sb: &bindings_raw::SortBy) -> protobuf::node::Node {
        let node = self.single_result_box(sb.node);
        let use_op = self.fetch_list_results(sb.useOp);
        protobuf::node::Node::SortBy(Box::new(protobuf::SortBy {
            node,
            sortby_dir: sb.sortby_dir as i32 + 1,
            sortby_nulls: sb.sortby_nulls as i32 + 1,
            use_op,
            location: sb.location,
        }))
    }

    // ====================================================================
    // TypeCast
    // ====================================================================

    unsafe fn queue_type_cast(&mut self, tc: &bindings_raw::TypeCast) {
        self.queue_node(tc.arg);
        if !tc.typeName.is_null() {
            self.queue_node(tc.typeName as *const bindings_raw::Node);
        }
    }

    unsafe fn collect_type_cast(&mut self, tc: &bindings_raw::TypeCast) -> protobuf::node::Node {
        let arg = self.single_result_box(tc.arg);
        let type_name = if tc.typeName.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::TypeName(tn)) => Some(tn),
                _ => None,
            })
        };
        protobuf::node::Node::TypeCast(Box::new(protobuf::TypeCast { arg, type_name, location: tc.location }))
    }

    // ====================================================================
    // TypeName
    // ====================================================================

    unsafe fn queue_type_name(&mut self, tn: &bindings_raw::TypeName) {
        self.queue_list_nodes(tn.names);
        self.queue_list_nodes(tn.typmods);
        self.queue_list_nodes(tn.arrayBounds);
    }

    unsafe fn collect_type_name(&mut self, tn: &bindings_raw::TypeName) -> protobuf::node::Node {
        let names = self.fetch_list_results(tn.names);
        let typmods = self.fetch_list_results(tn.typmods);
        let array_bounds = self.fetch_list_results(tn.arrayBounds);
        protobuf::node::Node::TypeName(protobuf::TypeName {
            names,
            type_oid: tn.typeOid,
            setof: tn.setof,
            pct_type: tn.pct_type,
            typmods,
            typemod: tn.typemod,
            array_bounds,
            location: tn.location,
        })
    }

    // ====================================================================
    // OnConflictClause (helper struct — not a Node)
    // ====================================================================

    unsafe fn queue_on_conflict_clause(&mut self, oc: *mut bindings_raw::OnConflictClause) {
        if oc.is_null() {
            return;
        }
        let oc = &*oc;
        self.queue_infer_clause(oc.infer);
        self.queue_list_nodes(oc.targetList);
        self.queue_node(oc.whereClause);
    }

    unsafe fn fetch_on_conflict_clause(&mut self, oc: *mut bindings_raw::OnConflictClause) -> Option<Box<protobuf::OnConflictClause>> {
        if oc.is_null() {
            return None;
        }
        let oc = &*oc;
        let infer = self.fetch_infer_clause(oc.infer);
        let target_list = self.fetch_list_results(oc.targetList);
        let where_clause = self.single_result_box(oc.whereClause);
        Some(Box::new(protobuf::OnConflictClause { action: oc.action as i32 + 1, infer, target_list, where_clause, location: oc.location }))
    }

    // ====================================================================
    // InferClause (helper struct — not a Node)
    // ====================================================================

    unsafe fn queue_infer_clause(&mut self, ic: *mut bindings_raw::InferClause) {
        if ic.is_null() {
            return;
        }
        let ic = &*ic;
        self.queue_list_nodes(ic.indexElems);
        self.queue_node(ic.whereClause);
    }

    unsafe fn fetch_infer_clause(&mut self, ic: *mut bindings_raw::InferClause) -> Option<Box<protobuf::InferClause>> {
        if ic.is_null() {
            return None;
        }
        let ic = &*ic;
        let index_elems = self.fetch_list_results(ic.indexElems);
        let where_clause = self.single_result_box(ic.whereClause);
        Some(Box::new(protobuf::InferClause { index_elems, where_clause, conname: convert_c_string(ic.conname), location: ic.location }))
    }

    // ====================================================================
    // BoolExpr
    // ====================================================================

    unsafe fn queue_bool_expr(&mut self, be: &bindings_raw::BoolExpr) {
        self.queue_list_nodes(be.args);
    }

    unsafe fn collect_bool_expr(&mut self, be: &bindings_raw::BoolExpr) -> protobuf::node::Node {
        let args = self.fetch_list_results(be.args);
        protobuf::node::Node::BoolExpr(Box::new(protobuf::BoolExpr { xpr: None, boolop: be.boolop as i32 + 1, args, location: be.location }))
    }

    // ====================================================================
    // NullTest
    // ====================================================================

    unsafe fn queue_null_test(&mut self, nt: &bindings_raw::NullTest) {
        self.queue_node(nt.arg as *const bindings_raw::Node);
    }

    unsafe fn collect_null_test(&mut self, nt: &bindings_raw::NullTest) -> protobuf::node::Node {
        let arg = self.single_result_box(nt.arg as *const bindings_raw::Node);
        protobuf::node::Node::NullTest(Box::new(protobuf::NullTest {
            xpr: None,
            arg,
            nulltesttype: nt.nulltesttype as i32 + 1,
            argisrow: nt.argisrow,
            location: nt.location,
        }))
    }

    // ====================================================================
    // SubLink
    // ====================================================================

    unsafe fn queue_sub_link(&mut self, sl: &bindings_raw::SubLink) {
        self.queue_node(sl.testexpr);
        self.queue_list_nodes(sl.operName);
        self.queue_node(sl.subselect);
    }

    unsafe fn collect_sub_link(&mut self, sl: &bindings_raw::SubLink) -> protobuf::node::Node {
        let testexpr = self.single_result_box(sl.testexpr);
        let oper_name = self.fetch_list_results(sl.operName);
        let subselect = self.single_result_box(sl.subselect);
        protobuf::node::Node::SubLink(Box::new(protobuf::SubLink {
            xpr: None,
            sub_link_type: sl.subLinkType as i32 + 1,
            sub_link_id: sl.subLinkId,
            testexpr,
            oper_name,
            subselect,
            location: sl.location,
        }))
    }

    // ====================================================================
    // CaseExpr
    // ====================================================================

    unsafe fn queue_case_expr(&mut self, ce: &bindings_raw::CaseExpr) {
        self.queue_node(ce.arg as *const bindings_raw::Node);
        self.queue_list_nodes(ce.args);
        self.queue_node(ce.defresult as *const bindings_raw::Node);
    }

    unsafe fn collect_case_expr(&mut self, ce: &bindings_raw::CaseExpr) -> protobuf::node::Node {
        let arg = self.single_result_box(ce.arg as *const bindings_raw::Node);
        let args = self.fetch_list_results(ce.args);
        let defresult = self.single_result_box(ce.defresult as *const bindings_raw::Node);
        protobuf::node::Node::CaseExpr(Box::new(protobuf::CaseExpr {
            xpr: None,
            casetype: ce.casetype,
            casecollid: ce.casecollid,
            arg,
            args,
            defresult,
            location: ce.location,
        }))
    }

    // ====================================================================
    // CaseWhen
    // ====================================================================

    unsafe fn queue_case_when(&mut self, cw: &bindings_raw::CaseWhen) {
        self.queue_node(cw.expr as *const bindings_raw::Node);
        self.queue_node(cw.result as *const bindings_raw::Node);
    }

    unsafe fn collect_case_when(&mut self, cw: &bindings_raw::CaseWhen) -> protobuf::node::Node {
        let expr = self.single_result_box(cw.expr as *const bindings_raw::Node);
        let result = self.single_result_box(cw.result as *const bindings_raw::Node);
        protobuf::node::Node::CaseWhen(Box::new(protobuf::CaseWhen { xpr: None, expr, result, location: cw.location }))
    }

    // ====================================================================
    // CoalesceExpr
    // ====================================================================

    unsafe fn queue_coalesce_expr(&mut self, ce: &bindings_raw::CoalesceExpr) {
        self.queue_list_nodes(ce.args);
    }

    unsafe fn collect_coalesce_expr(&mut self, ce: &bindings_raw::CoalesceExpr) -> protobuf::node::Node {
        let args = self.fetch_list_results(ce.args);
        protobuf::node::Node::CoalesceExpr(Box::new(protobuf::CoalesceExpr {
            xpr: None,
            coalescetype: ce.coalescetype,
            coalescecollid: ce.coalescecollid,
            args,
            location: ce.location,
        }))
    }

    // ====================================================================
    // MinMaxExpr
    // ====================================================================

    unsafe fn queue_min_max_expr(&mut self, mme: &bindings_raw::MinMaxExpr) {
        self.queue_list_nodes(mme.args);
    }

    unsafe fn collect_min_max_expr(&mut self, mme: &bindings_raw::MinMaxExpr) -> protobuf::node::Node {
        let args = self.fetch_list_results(mme.args);
        protobuf::node::Node::MinMaxExpr(Box::new(protobuf::MinMaxExpr {
            xpr: None,
            minmaxtype: mme.minmaxtype,
            minmaxcollid: mme.minmaxcollid,
            inputcollid: mme.inputcollid,
            op: mme.op as i32 + 1,
            args,
            location: mme.location,
        }))
    }

    // ====================================================================
    // BooleanTest
    // ====================================================================

    unsafe fn queue_boolean_test(&mut self, bt: &bindings_raw::BooleanTest) {
        self.queue_node(bt.arg as *const bindings_raw::Node);
    }

    unsafe fn collect_boolean_test(&mut self, bt: &bindings_raw::BooleanTest) -> protobuf::node::Node {
        let arg = self.single_result_box(bt.arg as *const bindings_raw::Node);
        protobuf::node::Node::BooleanTest(Box::new(protobuf::BooleanTest {
            xpr: None,
            arg,
            booltesttype: bt.booltesttype as i32 + 1,
            location: bt.location,
        }))
    }

    // ====================================================================
    // A_ArrayExpr
    // ====================================================================

    unsafe fn queue_a_array_expr(&mut self, ae: &bindings_raw::A_ArrayExpr) {
        self.queue_list_nodes(ae.elements);
    }

    unsafe fn collect_a_array_expr(&mut self, ae: &bindings_raw::A_ArrayExpr) -> protobuf::node::Node {
        let elements = self.fetch_list_results(ae.elements);
        protobuf::node::Node::AArrayExpr(protobuf::AArrayExpr { elements, location: ae.location })
    }

    // ====================================================================
    // A_Indirection
    // ====================================================================

    unsafe fn queue_a_indirection(&mut self, ai: &bindings_raw::A_Indirection) {
        self.queue_node(ai.arg);
        self.queue_list_nodes(ai.indirection);
    }

    unsafe fn collect_a_indirection(&mut self, ai: &bindings_raw::A_Indirection) -> protobuf::node::Node {
        let arg = self.single_result_box(ai.arg);
        let indirection = self.fetch_list_results(ai.indirection);
        protobuf::node::Node::AIndirection(Box::new(protobuf::AIndirection { arg, indirection }))
    }

    // ====================================================================
    // A_Indices
    // ====================================================================

    unsafe fn queue_a_indices(&mut self, ai: &bindings_raw::A_Indices) {
        self.queue_node(ai.lidx);
        self.queue_node(ai.uidx);
    }

    unsafe fn collect_a_indices(&mut self, ai: &bindings_raw::A_Indices) -> protobuf::node::Node {
        let lidx = self.single_result_box(ai.lidx);
        let uidx = self.single_result_box(ai.uidx);
        protobuf::node::Node::AIndices(Box::new(protobuf::AIndices { is_slice: ai.is_slice, lidx, uidx }))
    }

    // ====================================================================
    // CollateClause
    // ====================================================================

    unsafe fn queue_collate_clause(&mut self, cc: &bindings_raw::CollateClause) {
        self.queue_node(cc.arg);
        self.queue_list_nodes(cc.collname);
    }

    unsafe fn collect_collate_clause(&mut self, cc: &bindings_raw::CollateClause) -> protobuf::node::Node {
        let arg = self.single_result_box(cc.arg);
        let collname = self.fetch_list_results(cc.collname);
        protobuf::node::Node::CollateClause(Box::new(protobuf::CollateClause { arg, collname, location: cc.location }))
    }

    // ====================================================================
    // RangeSubselect
    // ====================================================================

    unsafe fn queue_range_subselect(&mut self, rs: &bindings_raw::RangeSubselect) {
        self.queue_node(rs.subquery);
        if !rs.alias.is_null() {
            self.queue_node(rs.alias as *const bindings_raw::Node);
        }
    }

    unsafe fn collect_range_subselect(&mut self, rs: &bindings_raw::RangeSubselect) -> protobuf::node::Node {
        let subquery = self.single_result_box(rs.subquery);
        let alias = if rs.alias.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::Alias(a)) => Some(a),
                _ => None,
            })
        };
        protobuf::node::Node::RangeSubselect(Box::new(protobuf::RangeSubselect { lateral: rs.lateral, subquery, alias }))
    }

    // ====================================================================
    // CommonTableExpr
    // ====================================================================

    unsafe fn queue_common_table_expr(&mut self, cte: &bindings_raw::CommonTableExpr) {
        self.queue_list_nodes(cte.aliascolnames);
        self.queue_node(cte.ctequery);
        if !cte.search_clause.is_null() {
            self.queue_node(cte.search_clause as *const bindings_raw::Node);
        }
        if !cte.cycle_clause.is_null() {
            self.queue_node(cte.cycle_clause as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cte.ctecolnames);
        self.queue_list_nodes(cte.ctecoltypes);
        self.queue_list_nodes(cte.ctecoltypmods);
        self.queue_list_nodes(cte.ctecolcollations);
    }

    unsafe fn collect_common_table_expr(&mut self, cte: &bindings_raw::CommonTableExpr) -> protobuf::node::Node {
        let aliascolnames = self.fetch_list_results(cte.aliascolnames);
        let ctequery = self.single_result_box(cte.ctequery);
        let search_clause = if cte.search_clause.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::CtesearchClause(sc)) => Some(sc),
                _ => None,
            })
        };
        let cycle_clause = if cte.cycle_clause.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::CtecycleClause(cc)) => Some(cc),
                _ => None,
            })
        };
        let ctecolnames = self.fetch_list_results(cte.ctecolnames);
        let ctecoltypes = self.fetch_list_results(cte.ctecoltypes);
        let ctecoltypmods = self.fetch_list_results(cte.ctecoltypmods);
        let ctecolcollations = self.fetch_list_results(cte.ctecolcollations);
        protobuf::node::Node::CommonTableExpr(Box::new(protobuf::CommonTableExpr {
            ctename: convert_c_string(cte.ctename),
            aliascolnames,
            ctematerialized: cte.ctematerialized as i32 + 1,
            ctequery,
            search_clause,
            cycle_clause,
            location: cte.location,
            cterecursive: cte.cterecursive,
            cterefcount: cte.cterefcount,
            ctecolnames,
            ctecoltypes,
            ctecoltypmods,
            ctecolcollations,
        }))
    }

    // ====================================================================
    // CTESearchClause
    // ====================================================================

    unsafe fn queue_cte_search_clause(&mut self, csc: &bindings_raw::CTESearchClause) {
        self.queue_list_nodes(csc.search_col_list);
    }

    unsafe fn collect_cte_search_clause(&mut self, csc: &bindings_raw::CTESearchClause) -> protobuf::node::Node {
        let search_col_list = self.fetch_list_results(csc.search_col_list);
        protobuf::node::Node::CtesearchClause(protobuf::CteSearchClause {
            search_col_list,
            search_breadth_first: csc.search_breadth_first,
            search_seq_column: convert_c_string(csc.search_seq_column),
            location: csc.location,
        })
    }

    // ====================================================================
    // CTECycleClause
    // ====================================================================

    unsafe fn queue_cte_cycle_clause(&mut self, ccc: &bindings_raw::CTECycleClause) {
        self.queue_list_nodes(ccc.cycle_col_list);
        self.queue_node(ccc.cycle_mark_value);
        self.queue_node(ccc.cycle_mark_default);
    }

    unsafe fn collect_cte_cycle_clause(&mut self, ccc: &bindings_raw::CTECycleClause) -> protobuf::node::Node {
        let cycle_col_list = self.fetch_list_results(ccc.cycle_col_list);
        let cycle_mark_value = self.single_result_box(ccc.cycle_mark_value);
        let cycle_mark_default = self.single_result_box(ccc.cycle_mark_default);
        protobuf::node::Node::CtecycleClause(Box::new(protobuf::CteCycleClause {
            cycle_col_list,
            cycle_mark_column: convert_c_string(ccc.cycle_mark_column),
            cycle_mark_value,
            cycle_mark_default,
            cycle_path_column: convert_c_string(ccc.cycle_path_column),
            location: ccc.location,
            cycle_mark_type: ccc.cycle_mark_type,
            cycle_mark_typmod: ccc.cycle_mark_typmod,
            cycle_mark_collation: ccc.cycle_mark_collation,
            cycle_mark_neop: ccc.cycle_mark_neop,
        }))
    }

    // ====================================================================
    // GroupingSet
    // ====================================================================

    unsafe fn queue_grouping_set(&mut self, gs: &bindings_raw::GroupingSet) {
        self.queue_list_nodes(gs.content);
    }

    unsafe fn collect_grouping_set(&mut self, gs: &bindings_raw::GroupingSet) -> protobuf::node::Node {
        let content = self.fetch_list_results(gs.content);
        protobuf::node::Node::GroupingSet(protobuf::GroupingSet { kind: gs.kind as i32 + 1, content, location: gs.location })
    }

    // ====================================================================
    // LockingClause
    // ====================================================================

    unsafe fn queue_locking_clause(&mut self, lc: &bindings_raw::LockingClause) {
        self.queue_list_nodes(lc.lockedRels);
    }

    unsafe fn collect_locking_clause(&mut self, lc: &bindings_raw::LockingClause) -> protobuf::node::Node {
        let locked_rels = self.fetch_list_results(lc.lockedRels);
        protobuf::node::Node::LockingClause(protobuf::LockingClause {
            locked_rels,
            strength: lc.strength as i32 + 1,
            wait_policy: lc.waitPolicy as i32 + 1,
        })
    }

    // ====================================================================
    // RowExpr
    // ====================================================================

    unsafe fn queue_row_expr(&mut self, re: &bindings_raw::RowExpr) {
        self.queue_list_nodes(re.args);
        self.queue_list_nodes(re.colnames);
    }

    unsafe fn collect_row_expr(&mut self, re: &bindings_raw::RowExpr) -> protobuf::node::Node {
        let args = self.fetch_list_results(re.args);
        let colnames = self.fetch_list_results(re.colnames);
        protobuf::node::Node::RowExpr(Box::new(protobuf::RowExpr {
            xpr: None,
            args,
            row_typeid: re.row_typeid,
            row_format: re.row_format as i32 + 1,
            colnames,
            location: re.location,
        }))
    }

    // ====================================================================
    // MultiAssignRef
    // ====================================================================

    unsafe fn queue_multi_assign_ref(&mut self, mar: &bindings_raw::MultiAssignRef) {
        self.queue_node(mar.source);
    }

    unsafe fn collect_multi_assign_ref(&mut self, mar: &bindings_raw::MultiAssignRef) -> protobuf::node::Node {
        let source = self.single_result_box(mar.source);
        protobuf::node::Node::MultiAssignRef(Box::new(protobuf::MultiAssignRef { source, colno: mar.colno, ncolumns: mar.ncolumns }))
    }

    // ====================================================================
    // Alias (as a standalone node)
    // ====================================================================

    unsafe fn queue_alias(&mut self, alias: &bindings_raw::Alias) {
        self.queue_list_nodes(alias.colnames);
    }

    unsafe fn collect_alias(&mut self, alias: &bindings_raw::Alias) -> protobuf::node::Node {
        let colnames = self.fetch_list_results(alias.colnames);
        protobuf::node::Node::Alias(protobuf::Alias { aliasname: convert_c_string(alias.aliasname), colnames })
    }

    // ====================================================================
    // GroupingFunc
    // ====================================================================

    unsafe fn queue_grouping_func(&mut self, gf: &bindings_raw::GroupingFunc) {
        self.queue_list_nodes(gf.args);
        self.queue_list_nodes(gf.refs);
    }

    unsafe fn collect_grouping_func(&mut self, gf: &bindings_raw::GroupingFunc) -> protobuf::node::Node {
        let args = self.fetch_list_results(gf.args);
        let refs = self.fetch_list_results(gf.refs);
        protobuf::node::Node::GroupingFunc(Box::new(protobuf::GroupingFunc {
            xpr: None,
            args,
            refs,
            agglevelsup: gf.agglevelsup,
            location: gf.location,
        }))
    }

    // ====================================================================
    // IndexElem
    // ====================================================================
    unsafe fn queue_index_elem(&mut self, ie: &bindings_raw::IndexElem) {
        self.queue_node(ie.expr);
        self.queue_list_nodes(ie.collation);
        self.queue_list_nodes(ie.opclass);
        self.queue_list_nodes(ie.opclassopts);
    }
    unsafe fn collect_index_elem(&mut self, ie: &bindings_raw::IndexElem) -> protobuf::node::Node {
        let expr = self.single_result_box(ie.expr);
        let collation = self.fetch_list_results(ie.collation);
        let opclass = self.fetch_list_results(ie.opclass);
        let opclassopts = self.fetch_list_results(ie.opclassopts);
        protobuf::node::Node::IndexElem(Box::new(protobuf::IndexElem {
            name: convert_c_string(ie.name),
            expr,
            indexcolname: convert_c_string(ie.indexcolname),
            collation,
            opclass,
            opclassopts,
            ordering: ie.ordering as i32 + 1,
            nulls_ordering: ie.nulls_ordering as i32 + 1,
        }))
    }

    // ====================================================================
    // DefElem
    // ====================================================================
    unsafe fn queue_def_elem(&mut self, de: &bindings_raw::DefElem) {
        self.queue_node(de.arg);
    }
    unsafe fn collect_def_elem(&mut self, de: &bindings_raw::DefElem) -> protobuf::node::Node {
        let arg = self.single_result_box(de.arg);
        protobuf::node::Node::DefElem(Box::new(protobuf::DefElem {
            defnamespace: convert_c_string(de.defnamespace),
            defname: convert_c_string(de.defname),
            arg,
            defaction: de.defaction as i32 + 1,
            location: de.location,
        }))
    }

    // ====================================================================
    // ColumnDef
    // ====================================================================
    unsafe fn queue_column_def(&mut self, cd: &bindings_raw::ColumnDef) {
        if !cd.typeName.is_null() {
            self.queue_node(cd.typeName as *const bindings_raw::Node);
        }
        self.queue_node(cd.raw_default);
        self.queue_node(cd.cooked_default);
        if !cd.collClause.is_null() {
            self.queue_node(cd.collClause as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cd.constraints);
        self.queue_list_nodes(cd.fdwoptions);
    }
    unsafe fn collect_column_def(&mut self, cd: &bindings_raw::ColumnDef) -> protobuf::node::Node {
        let type_name = if cd.typeName.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::TypeName(tn)) => Some(tn),
                _ => None,
            })
        };
        let raw_default = self.single_result_box(cd.raw_default);
        let cooked_default = self.single_result_box(cd.cooked_default);
        let coll_clause = if cd.collClause.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::CollateClause(cc)) => Some(cc),
                _ => None,
            })
        };
        let constraints = self.fetch_list_results(cd.constraints);
        let fdwoptions = self.fetch_list_results(cd.fdwoptions);
        protobuf::node::Node::ColumnDef(Box::new(protobuf::ColumnDef {
            colname: convert_c_string(cd.colname),
            type_name,
            compression: convert_c_string(cd.compression),
            inhcount: cd.inhcount,
            is_local: cd.is_local,
            is_not_null: cd.is_not_null,
            is_from_type: cd.is_from_type,
            storage: if cd.storage == 0 { String::new() } else { String::from_utf8_lossy(&[cd.storage as u8]).to_string() },
            storage_name: convert_c_string(cd.storage_name),
            raw_default,
            cooked_default,
            identity: if cd.identity == 0 { String::new() } else { String::from_utf8_lossy(&[cd.identity as u8]).to_string() },
            identity_sequence: None, // post-analysis
            generated: if cd.generated == 0 { String::new() } else { String::from_utf8_lossy(&[cd.generated as u8]).to_string() },
            coll_clause,
            coll_oid: cd.collOid,
            constraints,
            fdwoptions,
            location: cd.location,
        }))
    }

    // ====================================================================
    // Constraint
    // ====================================================================
    unsafe fn queue_constraint(&mut self, c: &bindings_raw::Constraint) {
        self.queue_node(c.raw_expr);
        self.queue_list_nodes(c.keys);
        self.queue_list_nodes(c.including);
        self.queue_list_nodes(c.exclusions);
        self.queue_list_nodes(c.options);
        self.queue_node(c.where_clause);
        if !c.pktable.is_null() {
            self.queue_node(c.pktable as *const bindings_raw::Node);
        }
        self.queue_list_nodes(c.fk_attrs);
        self.queue_list_nodes(c.pk_attrs);
        self.queue_list_nodes(c.fk_del_set_cols);
        self.queue_list_nodes(c.old_conpfeqop);
    }
    unsafe fn collect_constraint(&mut self, c: &bindings_raw::Constraint) -> protobuf::node::Node {
        let raw_expr = self.single_result_box(c.raw_expr);
        let keys = self.fetch_list_results(c.keys);
        let including = self.fetch_list_results(c.including);
        let exclusions = self.fetch_list_results(c.exclusions);
        let options = self.fetch_list_results(c.options);
        let where_clause = self.single_result_box(c.where_clause);
        let pktable = if c.pktable.is_null() { None } else { self.pop_range_var() };
        let fk_attrs = self.fetch_list_results(c.fk_attrs);
        let pk_attrs = self.fetch_list_results(c.pk_attrs);
        let fk_del_set_cols = self.fetch_list_results(c.fk_del_set_cols);
        let old_conpfeqop = self.fetch_list_results(c.old_conpfeqop);
        protobuf::node::Node::Constraint(Box::new(protobuf::Constraint {
            contype: c.contype as i32 + 1,
            conname: convert_c_string(c.conname),
            deferrable: c.deferrable,
            initdeferred: c.initdeferred,
            location: c.location,
            is_no_inherit: c.is_no_inherit,
            raw_expr,
            cooked_expr: convert_c_string(c.cooked_expr),
            generated_when: if c.generated_when == 0 { String::new() } else { String::from_utf8_lossy(&[c.generated_when as u8]).to_string() },
            inhcount: c.inhcount,
            nulls_not_distinct: c.nulls_not_distinct,
            keys,
            including,
            exclusions,
            options,
            indexname: convert_c_string(c.indexname),
            indexspace: convert_c_string(c.indexspace),
            reset_default_tblspc: c.reset_default_tblspc,
            access_method: convert_c_string(c.access_method),
            where_clause,
            pktable,
            fk_attrs,
            pk_attrs,
            fk_matchtype: if c.fk_matchtype == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_matchtype as u8]).to_string() },
            fk_upd_action: if c.fk_upd_action == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_upd_action as u8]).to_string() },
            fk_del_action: if c.fk_del_action == 0 { String::new() } else { String::from_utf8_lossy(&[c.fk_del_action as u8]).to_string() },
            fk_del_set_cols,
            old_conpfeqop,
            old_pktable_oid: c.old_pktable_oid,
            skip_validation: c.skip_validation,
            initially_valid: c.initially_valid,
        }))
    }

    // ====================================================================
    // CreateStmt
    // ====================================================================
    unsafe fn queue_create_stmt(&mut self, cs: &bindings_raw::CreateStmt) {
        if !cs.relation.is_null() {
            self.queue_node(cs.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cs.tableElts);
        self.queue_list_nodes(cs.inhRelations);
        if !cs.partbound.is_null() {
            self.queue_node(cs.partbound as *const bindings_raw::Node);
        }
        if !cs.partspec.is_null() {
            self.queue_node(cs.partspec as *const bindings_raw::Node);
        }
        if !cs.ofTypename.is_null() {
            self.queue_node(cs.ofTypename as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cs.constraints);
        self.queue_list_nodes(cs.options);
    }
    unsafe fn collect_create_stmt(&mut self, cs: &bindings_raw::CreateStmt) -> protobuf::node::Node {
        let relation = if cs.relation.is_null() { None } else { self.pop_range_var() };
        let table_elts = self.fetch_list_results(cs.tableElts);
        let inh_relations = self.fetch_list_results(cs.inhRelations);
        let partbound = if cs.partbound.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::PartitionBoundSpec(pbs)) => Some(pbs),
                _ => None,
            })
        };
        let partspec = if cs.partspec.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::PartitionSpec(ps)) => Some(ps),
                _ => None,
            })
        };
        let of_typename = if cs.ofTypename.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::TypeName(tn)) => Some(tn),
                _ => None,
            })
        };
        let constraints = self.fetch_list_results(cs.constraints);
        let options = self.fetch_list_results(cs.options);
        protobuf::node::Node::CreateStmt(protobuf::CreateStmt {
            relation,
            table_elts,
            inh_relations,
            partbound,
            partspec,
            of_typename,
            constraints,
            options,
            oncommit: cs.oncommit as i32 + 1,
            tablespacename: convert_c_string(cs.tablespacename),
            access_method: convert_c_string(cs.accessMethod),
            if_not_exists: cs.if_not_exists,
        })
    }

    // ====================================================================
    // DropStmt
    // ====================================================================
    unsafe fn queue_drop_stmt(&mut self, ds: &bindings_raw::DropStmt) {
        self.queue_list_nodes(ds.objects);
    }
    unsafe fn collect_drop_stmt(&mut self, ds: &bindings_raw::DropStmt) -> protobuf::node::Node {
        let objects = self.fetch_list_results(ds.objects);
        protobuf::node::Node::DropStmt(protobuf::DropStmt {
            objects,
            remove_type: ds.removeType as i32 + 1,
            behavior: ds.behavior as i32 + 1,
            missing_ok: ds.missing_ok,
            concurrent: ds.concurrent,
        })
    }

    // ====================================================================
    // IndexStmt
    // ====================================================================
    unsafe fn queue_index_stmt(&mut self, is_: &bindings_raw::IndexStmt) {
        if !is_.relation.is_null() {
            self.queue_node(is_.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(is_.indexParams);
        self.queue_list_nodes(is_.indexIncludingParams);
        self.queue_list_nodes(is_.options);
        self.queue_node(is_.whereClause);
        self.queue_list_nodes(is_.excludeOpNames);
    }
    unsafe fn collect_index_stmt(&mut self, is_: &bindings_raw::IndexStmt) -> protobuf::node::Node {
        let relation = if is_.relation.is_null() { None } else { self.pop_range_var() };
        let index_params = self.fetch_list_results(is_.indexParams);
        let index_including_params = self.fetch_list_results(is_.indexIncludingParams);
        let options = self.fetch_list_results(is_.options);
        let where_clause = self.single_result_box(is_.whereClause);
        let exclude_op_names = self.fetch_list_results(is_.excludeOpNames);
        protobuf::node::Node::IndexStmt(Box::new(protobuf::IndexStmt {
            idxname: convert_c_string(is_.idxname),
            relation,
            access_method: convert_c_string(is_.accessMethod),
            table_space: convert_c_string(is_.tableSpace),
            index_params,
            index_including_params,
            options,
            where_clause,
            exclude_op_names,
            idxcomment: convert_c_string(is_.idxcomment),
            index_oid: is_.indexOid,
            old_number: is_.oldNumber,
            old_create_subid: is_.oldCreateSubid,
            old_first_relfilelocator_subid: is_.oldFirstRelfilelocatorSubid,
            unique: is_.unique,
            nulls_not_distinct: is_.nulls_not_distinct,
            primary: is_.primary,
            isconstraint: is_.isconstraint,
            deferrable: is_.deferrable,
            initdeferred: is_.initdeferred,
            transformed: is_.transformed,
            concurrent: is_.concurrent,
            if_not_exists: is_.if_not_exists,
            reset_default_tblspc: is_.reset_default_tblspc,
        }))
    }

    // ====================================================================
    // AlterTableStmt
    // ====================================================================
    unsafe fn queue_alter_table_stmt(&mut self, ats: &bindings_raw::AlterTableStmt) {
        if !ats.relation.is_null() {
            self.queue_node(ats.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(ats.cmds);
    }
    unsafe fn collect_alter_table_stmt(&mut self, ats: &bindings_raw::AlterTableStmt) -> protobuf::node::Node {
        let relation = if ats.relation.is_null() { None } else { self.pop_range_var() };
        let cmds = self.fetch_list_results(ats.cmds);
        protobuf::node::Node::AlterTableStmt(protobuf::AlterTableStmt { relation, cmds, objtype: ats.objtype as i32 + 1, missing_ok: ats.missing_ok })
    }

    // ====================================================================
    // AlterTableCmd
    // ====================================================================
    unsafe fn queue_alter_table_cmd(&mut self, atc: &bindings_raw::AlterTableCmd) {
        self.queue_node(atc.def);
    }
    unsafe fn collect_alter_table_cmd(&mut self, atc: &bindings_raw::AlterTableCmd) -> protobuf::node::Node {
        let def = self.single_result_box(atc.def);
        let newowner = if atc.newowner.is_null() {
            None
        } else {
            Some(protobuf::RoleSpec {
                roletype: (*atc.newowner).roletype as i32 + 1,
                rolename: convert_c_string((*atc.newowner).rolename),
                location: (*atc.newowner).location,
            })
        };
        protobuf::node::Node::AlterTableCmd(Box::new(protobuf::AlterTableCmd {
            subtype: atc.subtype as i32 + 1,
            name: convert_c_string(atc.name),
            num: atc.num as i32,
            newowner,
            def,
            behavior: atc.behavior as i32 + 1,
            missing_ok: atc.missing_ok,
            recurse: atc.recurse,
        }))
    }

    // ====================================================================
    // RenameStmt
    // ====================================================================
    unsafe fn queue_rename_stmt(&mut self, rs: &bindings_raw::RenameStmt) {
        if !rs.relation.is_null() {
            self.queue_node(rs.relation as *const bindings_raw::Node);
        }
        self.queue_node(rs.object);
    }
    unsafe fn collect_rename_stmt(&mut self, rs: &bindings_raw::RenameStmt) -> protobuf::node::Node {
        let relation = if rs.relation.is_null() { None } else { self.pop_range_var() };
        let object = self.single_result_box(rs.object);
        protobuf::node::Node::RenameStmt(Box::new(protobuf::RenameStmt {
            rename_type: rs.renameType as i32 + 1,
            relation_type: rs.relationType as i32 + 1,
            relation,
            object,
            subname: convert_c_string(rs.subname),
            newname: convert_c_string(rs.newname),
            behavior: rs.behavior as i32 + 1,
            missing_ok: rs.missing_ok,
        }))
    }

    // ====================================================================
    // ViewStmt
    // ====================================================================
    unsafe fn queue_view_stmt(&mut self, vs: &bindings_raw::ViewStmt) {
        if !vs.view.is_null() {
            self.queue_node(vs.view as *const bindings_raw::Node);
        }
        self.queue_list_nodes(vs.aliases);
        self.queue_node(vs.query);
        self.queue_list_nodes(vs.options);
    }
    unsafe fn collect_view_stmt(&mut self, vs: &bindings_raw::ViewStmt) -> protobuf::node::Node {
        let view = if vs.view.is_null() { None } else { self.pop_range_var() };
        let aliases = self.fetch_list_results(vs.aliases);
        let query = self.single_result_box(vs.query);
        let options = self.fetch_list_results(vs.options);
        protobuf::node::Node::ViewStmt(Box::new(protobuf::ViewStmt {
            view,
            aliases,
            query,
            replace: vs.replace,
            options,
            with_check_option: vs.withCheckOption as i32 + 1,
        }))
    }

    // ====================================================================
    // CreateTableAsStmt
    // ====================================================================
    unsafe fn queue_create_table_as_stmt(&mut self, ctas: &bindings_raw::CreateTableAsStmt) {
        self.queue_node(ctas.query);
        self.queue_into_clause(ctas.into);
    }
    unsafe fn collect_create_table_as_stmt(&mut self, ctas: &bindings_raw::CreateTableAsStmt) -> protobuf::node::Node {
        let query = self.single_result_box(ctas.query);
        let into = self.fetch_into_clause(ctas.into);
        protobuf::node::Node::CreateTableAsStmt(Box::new(protobuf::CreateTableAsStmt {
            query,
            into,
            objtype: ctas.objtype as i32 + 1,
            is_select_into: ctas.is_select_into,
            if_not_exists: ctas.if_not_exists,
        }))
    }

    // ====================================================================
    // TruncateStmt
    // ====================================================================
    unsafe fn queue_truncate_stmt(&mut self, ts: &bindings_raw::TruncateStmt) {
        self.queue_list_nodes(ts.relations);
    }
    unsafe fn collect_truncate_stmt(&mut self, ts: &bindings_raw::TruncateStmt) -> protobuf::node::Node {
        let relations = self.fetch_list_results(ts.relations);
        protobuf::node::Node::TruncateStmt(protobuf::TruncateStmt { relations, restart_seqs: ts.restart_seqs, behavior: ts.behavior as i32 + 1 })
    }

    // ====================================================================
    // AlterOwnerStmt
    // ====================================================================
    unsafe fn queue_alter_owner_stmt(&mut self, aos: &bindings_raw::AlterOwnerStmt) {
        if !aos.relation.is_null() {
            self.queue_node(aos.relation as *const bindings_raw::Node);
        }
        self.queue_node(aos.object);
    }
    unsafe fn collect_alter_owner_stmt(&mut self, aos: &bindings_raw::AlterOwnerStmt) -> protobuf::node::Node {
        let relation = if aos.relation.is_null() { None } else { self.pop_range_var() };
        let object = self.single_result_box(aos.object);
        let newowner = if aos.newowner.is_null() {
            None
        } else {
            Some(protobuf::RoleSpec {
                roletype: (*aos.newowner).roletype as i32 + 1,
                rolename: convert_c_string((*aos.newowner).rolename),
                location: (*aos.newowner).location,
            })
        };
        protobuf::node::Node::AlterOwnerStmt(Box::new(protobuf::AlterOwnerStmt {
            object_type: aos.objectType as i32 + 1,
            relation,
            object,
            newowner,
        }))
    }

    // ====================================================================
    // CreateSeqStmt
    // ====================================================================
    unsafe fn queue_create_seq_stmt(&mut self, css: &bindings_raw::CreateSeqStmt) {
        if !css.sequence.is_null() {
            self.queue_node(css.sequence as *const bindings_raw::Node);
        }
        self.queue_list_nodes(css.options);
    }
    unsafe fn collect_create_seq_stmt(&mut self, css: &bindings_raw::CreateSeqStmt) -> protobuf::node::Node {
        let sequence = if css.sequence.is_null() { None } else { self.pop_range_var() };
        let options = self.fetch_list_results(css.options);
        protobuf::node::Node::CreateSeqStmt(protobuf::CreateSeqStmt {
            sequence,
            options,
            owner_id: css.ownerId,
            for_identity: css.for_identity,
            if_not_exists: css.if_not_exists,
        })
    }

    // ====================================================================
    // AlterSeqStmt
    // ====================================================================
    unsafe fn queue_alter_seq_stmt(&mut self, ass_: &bindings_raw::AlterSeqStmt) {
        if !ass_.sequence.is_null() {
            self.queue_node(ass_.sequence as *const bindings_raw::Node);
        }
        self.queue_list_nodes(ass_.options);
    }
    unsafe fn collect_alter_seq_stmt(&mut self, ass_: &bindings_raw::AlterSeqStmt) -> protobuf::node::Node {
        let sequence = if ass_.sequence.is_null() { None } else { self.pop_range_var() };
        let options = self.fetch_list_results(ass_.options);
        protobuf::node::Node::AlterSeqStmt(protobuf::AlterSeqStmt { sequence, options, for_identity: ass_.for_identity, missing_ok: ass_.missing_ok })
    }

    // ====================================================================
    // CreateDomainStmt
    // ====================================================================
    unsafe fn queue_create_domain_stmt(&mut self, cds: &bindings_raw::CreateDomainStmt) {
        self.queue_list_nodes(cds.domainname);
        if !cds.typeName.is_null() {
            self.queue_node(cds.typeName as *const bindings_raw::Node);
        }
        if !cds.collClause.is_null() {
            self.queue_node(cds.collClause as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cds.constraints);
    }
    unsafe fn collect_create_domain_stmt(&mut self, cds: &bindings_raw::CreateDomainStmt) -> protobuf::node::Node {
        let domainname = self.fetch_list_results(cds.domainname);
        let type_name = if cds.typeName.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::TypeName(tn)) => Some(tn),
                _ => None,
            })
        };
        let coll_clause = if cds.collClause.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::CollateClause(cc)) => Some(cc),
                _ => None,
            })
        };
        let constraints = self.fetch_list_results(cds.constraints);
        protobuf::node::Node::CreateDomainStmt(Box::new(protobuf::CreateDomainStmt { domainname, type_name, coll_clause, constraints }))
    }

    // ====================================================================
    // CompositeTypeStmt
    // ====================================================================
    unsafe fn queue_composite_type_stmt(&mut self, cts: &bindings_raw::CompositeTypeStmt) {
        if !cts.typevar.is_null() {
            self.queue_node(cts.typevar as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cts.coldeflist);
    }
    unsafe fn collect_composite_type_stmt(&mut self, cts: &bindings_raw::CompositeTypeStmt) -> protobuf::node::Node {
        let typevar = if cts.typevar.is_null() { None } else { self.pop_range_var() };
        let coldeflist = self.fetch_list_results(cts.coldeflist);
        protobuf::node::Node::CompositeTypeStmt(protobuf::CompositeTypeStmt { typevar, coldeflist })
    }

    // ====================================================================
    // CreateEnumStmt
    // ====================================================================
    unsafe fn queue_create_enum_stmt(&mut self, ces: &bindings_raw::CreateEnumStmt) {
        self.queue_list_nodes(ces.typeName);
        self.queue_list_nodes(ces.vals);
    }
    unsafe fn collect_create_enum_stmt(&mut self, ces: &bindings_raw::CreateEnumStmt) -> protobuf::node::Node {
        let type_name = self.fetch_list_results(ces.typeName);
        let vals = self.fetch_list_results(ces.vals);
        protobuf::node::Node::CreateEnumStmt(protobuf::CreateEnumStmt { type_name, vals })
    }

    // ====================================================================
    // CreateExtensionStmt
    // ====================================================================
    unsafe fn queue_create_extension_stmt(&mut self, ces: &bindings_raw::CreateExtensionStmt) {
        self.queue_list_nodes(ces.options);
    }
    unsafe fn collect_create_extension_stmt(&mut self, ces: &bindings_raw::CreateExtensionStmt) -> protobuf::node::Node {
        let options = self.fetch_list_results(ces.options);
        protobuf::node::Node::CreateExtensionStmt(protobuf::CreateExtensionStmt {
            extname: convert_c_string(ces.extname),
            if_not_exists: ces.if_not_exists,
            options,
        })
    }

    // ====================================================================
    // Publication / Subscription / Trigger stmts
    // ====================================================================
    unsafe fn queue_create_publication_stmt(&mut self, cps: &bindings_raw::CreatePublicationStmt) {
        self.queue_list_nodes(cps.options);
        self.queue_list_nodes(cps.pubobjects);
    }
    unsafe fn collect_create_publication_stmt(&mut self, cps: &bindings_raw::CreatePublicationStmt) -> protobuf::node::Node {
        let options = self.fetch_list_results(cps.options);
        let pubobjects = self.fetch_list_results(cps.pubobjects);
        protobuf::node::Node::CreatePublicationStmt(protobuf::CreatePublicationStmt {
            pubname: convert_c_string(cps.pubname),
            options,
            pubobjects,
            for_all_tables: cps.for_all_tables,
        })
    }
    unsafe fn queue_alter_publication_stmt(&mut self, aps: &bindings_raw::AlterPublicationStmt) {
        self.queue_list_nodes(aps.options);
        self.queue_list_nodes(aps.pubobjects);
    }
    unsafe fn collect_alter_publication_stmt(&mut self, aps: &bindings_raw::AlterPublicationStmt) -> protobuf::node::Node {
        let options = self.fetch_list_results(aps.options);
        let pubobjects = self.fetch_list_results(aps.pubobjects);
        protobuf::node::Node::AlterPublicationStmt(protobuf::AlterPublicationStmt {
            pubname: convert_c_string(aps.pubname),
            options,
            pubobjects,
            for_all_tables: aps.for_all_tables,
            action: aps.action as i32 + 1,
        })
    }
    unsafe fn queue_create_subscription_stmt(&mut self, css: &bindings_raw::CreateSubscriptionStmt) {
        self.queue_list_nodes(css.publication);
        self.queue_list_nodes(css.options);
    }
    unsafe fn collect_create_subscription_stmt(&mut self, css: &bindings_raw::CreateSubscriptionStmt) -> protobuf::node::Node {
        let publication = self.fetch_list_results(css.publication);
        let options = self.fetch_list_results(css.options);
        protobuf::node::Node::CreateSubscriptionStmt(protobuf::CreateSubscriptionStmt {
            subname: convert_c_string(css.subname),
            conninfo: convert_c_string(css.conninfo),
            publication,
            options,
        })
    }
    unsafe fn queue_alter_subscription_stmt(&mut self, ass_: &bindings_raw::AlterSubscriptionStmt) {
        self.queue_list_nodes(ass_.publication);
        self.queue_list_nodes(ass_.options);
    }
    unsafe fn collect_alter_subscription_stmt(&mut self, ass_: &bindings_raw::AlterSubscriptionStmt) -> protobuf::node::Node {
        let publication = self.fetch_list_results(ass_.publication);
        let options = self.fetch_list_results(ass_.options);
        protobuf::node::Node::AlterSubscriptionStmt(protobuf::AlterSubscriptionStmt {
            kind: ass_.kind as i32 + 1,
            subname: convert_c_string(ass_.subname),
            conninfo: convert_c_string(ass_.conninfo),
            publication,
            options,
        })
    }
    unsafe fn queue_create_trig_stmt(&mut self, cts: &bindings_raw::CreateTrigStmt) {
        if !cts.relation.is_null() {
            self.queue_node(cts.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(cts.funcname);
        self.queue_list_nodes(cts.args);
        self.queue_list_nodes(cts.columns);
        self.queue_node(cts.whenClause);
        self.queue_list_nodes(cts.transitionRels);
        if !cts.constrrel.is_null() {
            self.queue_node(cts.constrrel as *const bindings_raw::Node);
        }
    }
    unsafe fn collect_create_trig_stmt(&mut self, cts: &bindings_raw::CreateTrigStmt) -> protobuf::node::Node {
        let relation = if cts.relation.is_null() { None } else { self.pop_range_var() };
        let funcname = self.fetch_list_results(cts.funcname);
        let args = self.fetch_list_results(cts.args);
        let columns = self.fetch_list_results(cts.columns);
        let when_clause = self.single_result_box(cts.whenClause);
        let transition_rels = self.fetch_list_results(cts.transitionRels);
        let constrrel = if cts.constrrel.is_null() { None } else { self.pop_range_var() };
        protobuf::node::Node::CreateTrigStmt(Box::new(protobuf::CreateTrigStmt {
            replace: cts.replace,
            isconstraint: cts.isconstraint,
            trigname: convert_c_string(cts.trigname),
            relation,
            funcname,
            args,
            row: cts.row,
            timing: cts.timing as i32,
            events: cts.events as i32,
            columns,
            when_clause,
            transition_rels,
            deferrable: cts.deferrable,
            initdeferred: cts.initdeferred,
            constrrel,
        }))
    }
    unsafe fn queue_publication_obj_spec(&mut self, pos: &bindings_raw::PublicationObjSpec) {
        if !pos.pubtable.is_null() {
            let pt = &*pos.pubtable;
            if !pt.relation.is_null() {
                self.queue_node(pt.relation as *const bindings_raw::Node);
            }
            self.queue_node(pt.whereClause as *const bindings_raw::Node);
            self.queue_list_nodes(pt.columns);
        }
    }
    unsafe fn collect_publication_obj_spec(&mut self, pos: &bindings_raw::PublicationObjSpec) -> protobuf::node::Node {
        let pubtable = if pos.pubtable.is_null() {
            None
        } else {
            let pt = &*pos.pubtable;
            let relation = if pt.relation.is_null() { None } else { self.pop_range_var() };
            let where_clause = self.single_result_box(pt.whereClause as *const bindings_raw::Node);
            let columns = self.fetch_list_results(pt.columns);
            Some(Box::new(protobuf::PublicationTable { relation, where_clause, columns }))
        };
        protobuf::node::Node::PublicationObjSpec(Box::new(protobuf::PublicationObjSpec {
            pubobjtype: pos.pubobjtype as i32 + 1,
            name: convert_c_string(pos.name),
            pubtable,
            location: pos.location,
        }))
    }

    // ====================================================================
    // Partition nodes
    // ====================================================================
    unsafe fn queue_partition_elem(&mut self, pe: &bindings_raw::PartitionElem) {
        self.queue_node(pe.expr);
        self.queue_list_nodes(pe.collation);
        self.queue_list_nodes(pe.opclass);
    }
    unsafe fn collect_partition_elem(&mut self, pe: &bindings_raw::PartitionElem) -> protobuf::node::Node {
        let expr = self.single_result_box(pe.expr);
        let collation = self.fetch_list_results(pe.collation);
        let opclass = self.fetch_list_results(pe.opclass);
        protobuf::node::Node::PartitionElem(Box::new(protobuf::PartitionElem {
            name: convert_c_string(pe.name),
            expr,
            collation,
            opclass,
            location: pe.location,
        }))
    }
    unsafe fn queue_partition_spec(&mut self, ps: &bindings_raw::PartitionSpec) {
        self.queue_list_nodes(ps.partParams);
    }
    unsafe fn collect_partition_spec(&mut self, ps: &bindings_raw::PartitionSpec) -> protobuf::node::Node {
        let part_params = self.fetch_list_results(ps.partParams);
        let strategy = match ps.strategy as u8 as char {
            'l' => 1,
            'r' => 2,
            'h' => 3,
            _ => 0,
        };
        protobuf::node::Node::PartitionSpec(protobuf::PartitionSpec { strategy, part_params, location: ps.location })
    }
    unsafe fn queue_partition_bound_spec(&mut self, pbs: &bindings_raw::PartitionBoundSpec) {
        self.queue_list_nodes(pbs.listdatums);
        self.queue_list_nodes(pbs.lowerdatums);
        self.queue_list_nodes(pbs.upperdatums);
    }
    unsafe fn collect_partition_bound_spec(&mut self, pbs: &bindings_raw::PartitionBoundSpec) -> protobuf::node::Node {
        let listdatums = self.fetch_list_results(pbs.listdatums);
        let lowerdatums = self.fetch_list_results(pbs.lowerdatums);
        let upperdatums = self.fetch_list_results(pbs.upperdatums);
        protobuf::node::Node::PartitionBoundSpec(protobuf::PartitionBoundSpec {
            strategy: if pbs.strategy == 0 { String::new() } else { String::from_utf8_lossy(&[pbs.strategy as u8]).to_string() },
            is_default: pbs.is_default,
            modulus: pbs.modulus,
            remainder: pbs.remainder,
            listdatums,
            lowerdatums,
            upperdatums,
            location: pbs.location,
        })
    }
    unsafe fn queue_partition_range_datum(&mut self, prd: &bindings_raw::PartitionRangeDatum) {
        self.queue_node(prd.value);
    }
    unsafe fn collect_partition_range_datum(&mut self, prd: &bindings_raw::PartitionRangeDatum) -> protobuf::node::Node {
        let value = self.single_result_box(prd.value);
        let kind = match prd.kind {
            bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_MINVALUE => 1,
            bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_VALUE => 2,
            bindings_raw::PartitionRangeDatumKind_PARTITION_RANGE_DATUM_MAXVALUE => 3,
            _ => 0,
        };
        protobuf::node::Node::PartitionRangeDatum(Box::new(protobuf::PartitionRangeDatum { kind, value, location: prd.location }))
    }

    // ====================================================================
    // Statement types (utility / session)
    // ====================================================================
    unsafe fn queue_explain_stmt(&mut self, es: &bindings_raw::ExplainStmt) {
        self.queue_node(es.query);
        self.queue_list_nodes(es.options);
    }
    unsafe fn collect_explain_stmt(&mut self, es: &bindings_raw::ExplainStmt) -> protobuf::node::Node {
        let query = self.single_result_box(es.query);
        let options = self.fetch_list_results(es.options);
        protobuf::node::Node::ExplainStmt(Box::new(protobuf::ExplainStmt { query, options }))
    }
    unsafe fn queue_copy_stmt(&mut self, cs: &bindings_raw::CopyStmt) {
        if !cs.relation.is_null() {
            self.queue_node(cs.relation as *const bindings_raw::Node);
        }
        self.queue_node(cs.query);
        self.queue_list_nodes(cs.attlist);
        self.queue_list_nodes(cs.options);
        self.queue_node(cs.whereClause);
    }
    unsafe fn collect_copy_stmt(&mut self, cs: &bindings_raw::CopyStmt) -> protobuf::node::Node {
        let relation = if cs.relation.is_null() { None } else { self.pop_range_var() };
        let query = self.single_result_box(cs.query);
        let attlist = self.fetch_list_results(cs.attlist);
        let options = self.fetch_list_results(cs.options);
        let where_clause = self.single_result_box(cs.whereClause);
        protobuf::node::Node::CopyStmt(Box::new(protobuf::CopyStmt {
            relation,
            query,
            attlist,
            is_from: cs.is_from,
            is_program: cs.is_program,
            filename: convert_c_string(cs.filename),
            options,
            where_clause,
        }))
    }
    unsafe fn queue_prepare_stmt(&mut self, ps: &bindings_raw::PrepareStmt) {
        self.queue_list_nodes(ps.argtypes);
        self.queue_node(ps.query);
    }
    unsafe fn collect_prepare_stmt(&mut self, ps: &bindings_raw::PrepareStmt) -> protobuf::node::Node {
        let argtypes = self.fetch_list_results(ps.argtypes);
        let query = self.single_result_box(ps.query);
        protobuf::node::Node::PrepareStmt(Box::new(protobuf::PrepareStmt { name: convert_c_string(ps.name), argtypes, query }))
    }
    unsafe fn queue_execute_stmt(&mut self, es: &bindings_raw::ExecuteStmt) {
        self.queue_list_nodes(es.params);
    }
    unsafe fn collect_execute_stmt(&mut self, es: &bindings_raw::ExecuteStmt) -> protobuf::node::Node {
        let params = self.fetch_list_results(es.params);
        protobuf::node::Node::ExecuteStmt(protobuf::ExecuteStmt { name: convert_c_string(es.name), params })
    }
    unsafe fn queue_transaction_stmt(&mut self, ts: &bindings_raw::TransactionStmt) {
        self.queue_list_nodes(ts.options);
    }
    unsafe fn collect_transaction_stmt(&mut self, ts: &bindings_raw::TransactionStmt) -> protobuf::node::Node {
        let options = self.fetch_list_results(ts.options);
        protobuf::node::Node::TransactionStmt(protobuf::TransactionStmt {
            kind: ts.kind as i32 + 1,
            options,
            savepoint_name: convert_c_string(ts.savepoint_name),
            gid: convert_c_string(ts.gid),
            chain: ts.chain,
            location: ts.location,
        })
    }
    unsafe fn queue_vacuum_stmt(&mut self, vs: &bindings_raw::VacuumStmt) {
        self.queue_list_nodes(vs.options);
        self.queue_list_nodes(vs.rels);
    }
    unsafe fn collect_vacuum_stmt(&mut self, vs: &bindings_raw::VacuumStmt) -> protobuf::node::Node {
        let options = self.fetch_list_results(vs.options);
        let rels = self.fetch_list_results(vs.rels);
        protobuf::node::Node::VacuumStmt(protobuf::VacuumStmt { options, rels, is_vacuumcmd: vs.is_vacuumcmd })
    }
    unsafe fn queue_vacuum_relation(&mut self, vr: &bindings_raw::VacuumRelation) {
        if !vr.relation.is_null() {
            self.queue_node(vr.relation as *const bindings_raw::Node);
        }
        self.queue_list_nodes(vr.va_cols);
    }
    unsafe fn collect_vacuum_relation(&mut self, vr: &bindings_raw::VacuumRelation) -> protobuf::node::Node {
        let relation = if vr.relation.is_null() { None } else { self.pop_range_var() };
        let va_cols = self.fetch_list_results(vr.va_cols);
        protobuf::node::Node::VacuumRelation(protobuf::VacuumRelation { relation, oid: vr.oid, va_cols })
    }
    unsafe fn queue_variable_set_stmt(&mut self, vss: &bindings_raw::VariableSetStmt) {
        self.queue_list_nodes(vss.args);
    }
    unsafe fn collect_variable_set_stmt(&mut self, vss: &bindings_raw::VariableSetStmt) -> protobuf::node::Node {
        let args = self.fetch_list_results(vss.args);
        protobuf::node::Node::VariableSetStmt(protobuf::VariableSetStmt {
            kind: vss.kind as i32 + 1,
            name: convert_c_string(vss.name),
            args,
            is_local: vss.is_local,
        })
    }
    unsafe fn queue_lock_stmt(&mut self, ls: &bindings_raw::LockStmt) {
        self.queue_list_nodes(ls.relations);
    }
    unsafe fn collect_lock_stmt(&mut self, ls: &bindings_raw::LockStmt) -> protobuf::node::Node {
        let relations = self.fetch_list_results(ls.relations);
        protobuf::node::Node::LockStmt(protobuf::LockStmt { relations, mode: ls.mode, nowait: ls.nowait })
    }
    unsafe fn queue_do_stmt(&mut self, ds: &bindings_raw::DoStmt) {
        self.queue_list_nodes(ds.args);
    }
    unsafe fn collect_do_stmt(&mut self, ds: &bindings_raw::DoStmt) -> protobuf::node::Node {
        let args = self.fetch_list_results(ds.args);
        protobuf::node::Node::DoStmt(protobuf::DoStmt { args })
    }
    unsafe fn queue_object_with_args(&mut self, owa: &bindings_raw::ObjectWithArgs) {
        self.queue_list_nodes(owa.objname);
        self.queue_list_nodes(owa.objargs);
        self.queue_list_nodes(owa.objfuncargs);
    }
    unsafe fn collect_object_with_args(&mut self, owa: &bindings_raw::ObjectWithArgs) -> protobuf::node::Node {
        let objname = self.fetch_list_results(owa.objname);
        let objargs = self.fetch_list_results(owa.objargs);
        let objfuncargs = self.fetch_list_results(owa.objfuncargs);
        protobuf::node::Node::ObjectWithArgs(protobuf::ObjectWithArgs { objname, objargs, objfuncargs, args_unspecified: owa.args_unspecified })
    }
    unsafe fn queue_coerce_to_domain(&mut self, ctd: &bindings_raw::CoerceToDomain) {
        self.queue_node(ctd.arg as *const bindings_raw::Node);
    }
    unsafe fn collect_coerce_to_domain(&mut self, ctd: &bindings_raw::CoerceToDomain) -> protobuf::node::Node {
        let arg = self.single_result_box(ctd.arg as *const bindings_raw::Node);
        protobuf::node::Node::CoerceToDomain(Box::new(protobuf::CoerceToDomain {
            xpr: None,
            arg,
            resulttype: ctd.resulttype,
            resulttypmod: ctd.resulttypmod,
            resultcollid: ctd.resultcollid,
            coercionformat: ctd.coercionformat as i32 + 1,
            location: ctd.location,
        }))
    }
    unsafe fn queue_function_parameter(&mut self, fp: &bindings_raw::FunctionParameter) {
        if !fp.argType.is_null() {
            self.queue_node(fp.argType as *const bindings_raw::Node);
        }
        self.queue_node(fp.defexpr);
    }
    unsafe fn collect_function_parameter(&mut self, fp: &bindings_raw::FunctionParameter) -> protobuf::node::Node {
        let arg_type = if fp.argType.is_null() {
            None
        } else {
            self.result_stack.pop().and_then(|n| match n.node {
                Some(protobuf::node::Node::TypeName(tn)) => Some(tn),
                _ => None,
            })
        };
        let defexpr = self.single_result_box(fp.defexpr);
        let mode = match fp.mode {
            bindings_raw::FunctionParameterMode_FUNC_PARAM_IN => protobuf::FunctionParameterMode::FuncParamIn as i32,
            bindings_raw::FunctionParameterMode_FUNC_PARAM_OUT => protobuf::FunctionParameterMode::FuncParamOut as i32,
            bindings_raw::FunctionParameterMode_FUNC_PARAM_INOUT => protobuf::FunctionParameterMode::FuncParamInout as i32,
            bindings_raw::FunctionParameterMode_FUNC_PARAM_VARIADIC => protobuf::FunctionParameterMode::FuncParamVariadic as i32,
            bindings_raw::FunctionParameterMode_FUNC_PARAM_TABLE => protobuf::FunctionParameterMode::FuncParamTable as i32,
            bindings_raw::FunctionParameterMode_FUNC_PARAM_DEFAULT => protobuf::FunctionParameterMode::FuncParamDefault as i32,
            _ => 0,
        };
        protobuf::node::Node::FunctionParameter(Box::new(protobuf::FunctionParameter { name: convert_c_string(fp.name), arg_type, mode, defexpr }))
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

/// Converts a C string pointer to a Rust String.
unsafe fn convert_c_string(ptr: *const c_char) -> std::string::String {
    if ptr.is_null() {
        std::string::String::new()
    } else {
        CStr::from_ptr(ptr).to_string_lossy().to_string()
    }
}

unsafe fn convert_string(s: &bindings_raw::String) -> protobuf::String {
    protobuf::String { sval: convert_c_string(s.sval) }
}

unsafe fn convert_bit_string(bs: &bindings_raw::BitString) -> protobuf::BitString {
    protobuf::BitString { bsval: convert_c_string(bs.bsval) }
}
