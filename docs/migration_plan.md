# Migration Plan: Iterative `raw_parse_iter.rs` — Test-Driven Approach

## Strategy

We migrate **test by test** — running a single test from simple to complex, adding
the missing node type to the iterative `Processor`, and moving on only after it passes.

All tests live in `tests/raw_parse_iter/` and mirror `tests/raw_parse/` exactly, except
they call `parse_raw_iter` instead of `parse_raw`. Every test compares `parse_raw_iter`
output against `parse` (protobuf) output for structural equality.

### How to run a single test

```sh
cargo test --test raw_parse_iter_tests raw_parse_iter::basic::it_parses_simple_select -- --nocapture
```

### Workflow for each test

1. Run the test
2. If it **passes** → move to the next test
3. If it **fails** → read the error, identify the missing/broken node conversion,
   fix it in `raw_parse_iter.rs` (or `Processor`), re-run, repeat
4. After the fix passes, run all previously-passing tests as regression:
   `cargo test --test raw_parse_iter_tests -- --nocapture`

---

## How the Iterative Processor Works

The `Processor` struct in `raw_parse_iter.rs` replaces recursive `convert_node()` calls
with an explicit stack. It has two stacks:

- **`stack`** — `Vec<ProcessingNode>` — work items (C node pointers + a `collect` flag)
- **`result_stack`** — `Vec<protobuf::Node>` — completed protobuf nodes

### Two-pass processing

Every node is visited **twice**:

1. **Queue pass** (`collect=false`): Push a collect marker for this node, then push
   its children onto the stack. Children go on top, so they are processed first.
2. **Collect pass** (`collect=true`): All children have been processed and their results
   sit on `result_stack`. Pop them, assemble the protobuf struct, push the result.

```
stack (LIFO):
  ┌─────────────────┐
  │ child C          │ ← processed first
  │ child B          │
  │ child A          │
  │ COLLECT(parent)  │ ← processed after all children
  └─────────────────┘
```

### Key helper methods

| Method | Purpose |
|--------|---------|
| `queue_collect(ptr)` | Push a collect marker (same pointer, `collect=true`) |
| `queue_node(ptr)` | Push a child node for processing (`collect=false`); null ptrs are skipped |
| `queue_list_nodes(list)` | Push every element of a C `List*` as individual nodes |
| `single_result(ptr)` | Pop one result from `result_stack`; returns `None` if ptr was null |
| `single_result_box(ptr)` | Same but returns `Option<Box<Node>>` |
| `fetch_list_results(list)` | Pop N results (where N = list length); returns `Vec<Node>` |
| `push_result(node)` | Push a completed protobuf node onto `result_stack` |

### Symmetry rule

**Every queued node produces exactly one result.** This means:

- `queue_node(ptr)` must be balanced by `single_result(ptr)` or `single_result_box(ptr)`
  using **the same pointer** so the null-check matches.
- `queue_list_nodes(list)` must be balanced by `fetch_list_results(list)` using
  **the same list pointer**.
- The order must be the same: queue A then B → collect fetches A then B.

---

## How to Migrate a Node Type

### Step 1: Identify the category

| Category | Description | Example |
|----------|-------------|---------|
| **Leaf** | No child nodes/lists that need conversion | `SQLValueFunction`, `SetToDefault` |
| **Simple** | Has child nodes and/or lists | `BoolExpr`, `NullTest`, `SubLink` |

### Step 2: Add the match arm in `process()`

In the `match node_tag { ... }` block, add an arm for the new `NodeTag`:

```rust
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
```

For **leaf nodes** (no children), skip queue/collect entirely:

```rust
bindings_raw::NodeTag_T_SQLValueFunction => {
    let svf = node_ptr as *const bindings_raw::SQLValueFunction;
    self.push_result(protobuf::node::Node::SqlvalueFunction(Box::new(
        protobuf::SqlValueFunction {
            xpr: None,
            op: (*svf).op as i32 + 1,
            r#type: (*svf).type_,
            typmod: (*svf).typmod,
            location: (*svf).location,
        },
    )));
}
```

### Step 3: Add `queue_*` and `collect_*` methods (non-leaf only)

The **queue method** pushes children onto the stack. The **collect method** pops
results and assembles the protobuf struct.

Look at the existing recursive `convert_*` function to see which fields need conversion.
Each call to `convert_node_boxed(field)` becomes a `queue_node(field)` / `single_result_box(field)` pair.
Each call to `convert_list_to_nodes(list)` becomes a `queue_list_nodes(list)` / `fetch_list_results(list)` pair.

**Example — BoolExpr** (one list child):

Recursive version (in `raw_parse.rs`):
```rust
unsafe fn convert_bool_expr(be: &bindings_raw::BoolExpr) -> protobuf::BoolExpr {
    protobuf::BoolExpr {
        xpr: None,
        boolop: be.boolop as i32 + 1,
        args: convert_list_to_nodes(be.args),       // ← list child
        location: be.location,
    }
}
```

Iterative version (queue + collect methods on `Processor`):
```rust
unsafe fn queue_bool_expr(&mut self, be: &bindings_raw::BoolExpr) {
    self.queue_list_nodes(be.args);                  // ← queue the list
}

unsafe fn collect_bool_expr(&mut self, be: &bindings_raw::BoolExpr) -> protobuf::node::Node {
    let args = self.fetch_list_results(be.args);     // ← fetch matching results
    protobuf::node::Node::BoolExpr(Box::new(protobuf::BoolExpr {
        xpr: None,
        boolop: be.boolop as i32 + 1,
        args,
        location: be.location,
    }))
}
```

**Example — SubLink** (two node children + one list child):

```rust
unsafe fn queue_sub_link(&mut self, sl: &bindings_raw::SubLink) {
    self.queue_node(sl.testexpr);                    // node child 1
    self.queue_list_nodes(sl.operName);              // list child
    self.queue_node(sl.subselect);                   // node child 2
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
```

### Step 4: Add to `node_tag_name()` (for debug logging)

```rust
bindings_raw::NodeTag_T_BoolExpr => "BoolExpr",
```

### Step 5: Handle non-Node helper structs

Some PostgreSQL structs (like `Alias`, `IntoClause`, `OnConflictClause`) are embedded
inside other nodes but are not themselves `Node` types in the processor's match. These
are handled with dedicated `queue_*` / `fetch_*` helper pairs on the Processor, called
from the parent's queue/collect methods. See `queue_into_clause` / `fetch_into_clause`
and `queue_on_conflict_clause` / `fetch_on_conflict_clause` as examples.

### Step 6: Run tests and verify

```sh
# Run the specific test that needs this node type
cargo test --test raw_parse_iter_tests "raw_parse_iter::expressions::it_parses_null_tests" -- --nocapture

# Run all tests as regression
cargo test --test raw_parse_iter_tests -- --nocapture
```

### Common pitfalls

- **Queue/collect order mismatch**: The collect method must pop results in the **same
  order** as the queue method pushed children. If queue does `A, B, C` then collect
  must do `A, B, C` (not `C, B, A`).
- **Forgetting null checks**: `queue_node` with null is harmless (skipped in process),
  but `single_result` must be called with **the same pointer** so it knows whether to
  pop. If the pointer was null, `single_result` returns `None` without popping.
- **Missing list symmetry**: If you call `queue_list_nodes(some_list)`, you MUST call
  `fetch_list_results(some_list)` with the **same list pointer** — not a different copy.
- **Wrong protobuf enum offset**: PostgreSQL C enums start at 0, protobuf enums reserve
  0 for `UNDEFINED`. Most fields need `as i32 + 1`.
- **Boxed vs unboxed**: Check the protobuf struct definition — some fields use
  `Option<Box<Node>>` (call `single_result_box`), others use `Vec<Node>` (call
  `fetch_list_results`), and some are scalar (just copy directly).

---

## Test Execution Order

Tests are ordered from simplest SQL (fewest node types involved) to most complex
(deeply nested, many node types combined). Within each file, tests are listed in
the order they should be attempted.

### Step 1 — `basic` (fundamentals)

These tests exercise the core path: `RawStmt` → `SelectStmt` → `ResTarget` → `ColumnRef` / `AConst`.

| # | Test name | What it exercises |
|---|-----------|-------------------|
| 1 | `basic::it_parses_simple_select` | `SELECT 1` — SelectStmt + ResTarget + AConst(int) |
| 2 | `basic::it_matches_parse_for_simple_select` | Same, with deparse check |
| 3 | `basic::it_handles_parse_errors` | Error path (no node conversion) |
| 4 | `basic::it_handles_empty_queries` | Empty parse tree |
| 5 | `basic::it_matches_parse_for_select_from_table` | `SELECT * FROM users` — adds RangeVar |
| 6 | `basic::it_deparses_parse_raw_iter_result` | Deparse round-trip |
| 7 | `basic::it_parses_multiple_statements` | Multiple RawStmt entries |
| 8 | `basic::it_returns_tables_like_parse` | JOIN + WHERE — adds JoinExpr, A_Expr, ColumnRef |
| 9 | `basic::it_returns_functions_like_parse` | FuncCall, `count(*)`, `sum()` |
| 10 | `basic::it_returns_statement_types_like_parse` | Mixed SELECT/INSERT/UPDATE/DELETE |
| 11 | `basic::it_deparse_raw_simple_select` | Deparse from parse_raw_iter result |
| 12 | `basic::it_deparse_raw_select_from_table` | Deparse with RangeVar |
| 13 | `basic::it_deparse_raw_complex_select` | Deparse with WHERE + ORDER BY |
| 14 | `basic::it_deparse_raw_insert` | InsertStmt deparse |
| 15 | `basic::it_deparse_raw_update` | UpdateStmt deparse |
| 16 | `basic::it_deparse_raw_delete` | DeleteStmt deparse |
| 17 | `basic::it_deparse_raw_multiple_statements` | Multi-statement deparse |
| 18 | `basic::it_deparse_raw_method_on_parse_result` | Method call variant |
| 19 | `basic::it_deparse_raw_method_on_protobuf_parse_result` | Method on protobuf struct |
| 20 | `basic::it_deparse_raw_method_on_node_ref` | NodeRef method |
| 21 | `basic::it_deparse_raw_matches_deparse` | Cross-check deparse vs deparse_raw |

### Step 2 — `expressions` (literals & operators)

Exercises leaf nodes and simple expression trees.

| # | Test name | What it exercises |
|---|-----------|-------------------|
| 22 | `expressions::it_extracts_integer_const` | AConst(Ival) |
| 23 | `expressions::it_extracts_string_const` | AConst(Sval) |
| 24 | `expressions::it_extracts_float_const` | AConst(Fval) |
| 25 | `expressions::it_extracts_boolean_true_const` | AConst(Boolval) |
| 26 | `expressions::it_extracts_boolean_false_const` | AConst(Boolval) |
| 27 | `expressions::it_extracts_null_const` | AConst(isnull) |
| 28 | `expressions::it_extracts_negative_integer_const` | Unary minus → A_Expr or AConst |
| 29 | `expressions::it_parses_floats_with_leading_dot` | AConst(Fval) edge case |
| 30 | `expressions::it_extracts_bit_string_const` | AConst(Bsval) |
| 31 | `expressions::it_extracts_hex_bit_string_const` | AConst(Bsval) |
| 32 | `expressions::it_parses_null_tests` | NullTest node |
| 33 | `expressions::it_parses_is_distinct_from` | A_Expr(DISTINCT) |
| 34 | `expressions::it_parses_between` | A_Expr(BETWEEN) |
| 35 | `expressions::it_parses_like_ilike` | A_Expr(LIKE/ILIKE) + BoolExpr(OR) |
| 36 | `expressions::it_parses_similar_to` | A_Expr(SIMILAR TO) |
| 37 | `expressions::it_parses_complex_boolean` | BoolExpr(AND/OR/NOT) nesting |
| 38 | `expressions::it_parses_coalesce` | CoalesceExpr |
| 39 | `expressions::it_parses_nullif` | NullIfExpr (maps to OpExpr or special) |
| 40 | `expressions::it_parses_greatest_least` | MinMaxExpr |
| 41 | `expressions::it_parses_pg_type_cast` | TypeCast + TypeName |
| 42 | `expressions::it_parses_sql_cast` | TypeCast via CAST() |
| 43 | `expressions::it_parses_array_cast` | TypeCast with array type |
| 44 | `expressions::it_parses_array_constructor` | ArrayExpr |
| 45 | `expressions::it_parses_array_subscript` | A_Indirection + A_Indices |
| 46 | `expressions::it_parses_array_slice` | A_Indirection + A_Indices (slice) |
| 47 | `expressions::it_parses_unnest` | FuncCall |
| 48 | `expressions::it_parses_json_operators` | A_Expr with JSON ops |
| 49 | `expressions::it_parses_jsonb_containment` | A_Expr with @> |
| 50 | `expressions::it_parses_positional_params` | ParamRef |
| 51 | `expressions::it_parses_params_in_insert` | ParamRef inside InsertStmt |
| 52 | `expressions::it_parses_current_timestamp` | SQLValueFunction |
| 53 | `expressions::it_parses_sql_value_functions` | All SQLValueFunction variants |
| 54 | `expressions::it_parses_real_world_query` | Combined: JOIN + BETWEEN + A_Expr + ORDER BY |
| 55 | `expressions::it_parses_bit_strings_hex` | Full query with X'...' literal |

### Step 3 — `select` (complex queries)

Progressively harder SELECT features. Each test tends to add one new node type.

| # | Test name | Key new node types |
|---|-----------|-------------------|
| 56 | `select::it_parses_join` | JoinExpr (INNER) |
| 57 | `select::it_parses_left_join` | JoinExpr (LEFT) |
| 58 | `select::it_parses_right_join` | JoinExpr (RIGHT) |
| 59 | `select::it_parses_full_outer_join` | JoinExpr (FULL) |
| 60 | `select::it_parses_cross_join` | JoinExpr (CROSS) |
| 61 | `select::it_parses_natural_join` | JoinExpr (NATURAL) |
| 62 | `select::it_parses_join_using` | JoinExpr + USING list |
| 63 | `select::it_parses_multiple_joins` | Nested JoinExpr |
| 64 | `select::it_parses_lateral_join` | RangeSubselect (LATERAL) |
| 65 | `select::it_parses_union` | SelectStmt with set_op |
| 66 | `select::it_parses_intersect` | INTERSECT |
| 67 | `select::it_parses_except` | EXCEPT |
| 68 | `select::it_parses_union_all` | UNION ALL |
| 69 | `select::it_parses_compound_set_operations` | Nested set ops |
| 70 | `select::it_parses_subquery` | SubLink (IN) |
| 71 | `select::it_parses_correlated_subquery` | SubLink (EXISTS) |
| 72 | `select::it_parses_not_exists_subquery` | BoolExpr(NOT) + SubLink |
| 73 | `select::it_parses_scalar_subquery` | SubLink (scalar) in target |
| 74 | `select::it_parses_derived_table` | RangeSubselect |
| 75 | `select::it_parses_any_subquery` | SubLink (ANY) |
| 76 | `select::it_parses_all_subquery` | SubLink (ALL) |
| 77 | `select::it_parses_case_expression` | CaseExpr + CaseWhen |
| 78 | `select::it_parses_aggregates` | FuncCall (aggregate) |
| 79 | `select::it_parses_window_function` | WindowFunc, WindowDef |
| 80 | `select::it_parses_window_function_partition` | PARTITION BY |
| 81 | `select::it_parses_window_function_frame` | Frame clause (ROWS) |
| 82 | `select::it_parses_named_window` | WINDOW clause |
| 83 | `select::it_parses_lag_lead` | LAG/LEAD window funcs |
| 84 | `select::it_parses_cte` | WithClause + CommonTableExpr |
| 85 | `select::it_parses_multiple_ctes` | Multiple CTEs |
| 86 | `select::it_parses_recursive_cte` | RECURSIVE + UNION ALL |
| 87 | `select::it_parses_cte_with_columns` | CTE column list |
| 88 | `select::it_parses_cte_materialized` | MATERIALIZED hint |
| 89 | `select::it_parses_cte_search_breadth_first` | SEARCH BREADTH FIRST |
| 90 | `select::it_parses_cte_search_depth_first` | SEARCH DEPTH FIRST |
| 91 | `select::it_parses_cte_cycle` | CYCLE detection |
| 92 | `select::it_parses_cte_search_and_cycle` | Combined SEARCH + CYCLE |
| 93 | `select::it_parses_group_by_rollup` | GroupingSet (ROLLUP) |
| 94 | `select::it_parses_group_by_cube` | GroupingSet (CUBE) |
| 95 | `select::it_parses_grouping_sets` | GroupingSet (SETS) |
| 96 | `select::it_parses_distinct_on` | DISTINCT ON |
| 97 | `select::it_parses_order_by_nulls` | SortBy (NULLS LAST) |
| 98 | `select::it_parses_fetch_first` | FETCH FIRST |
| 99 | `select::it_parses_offset_fetch` | OFFSET + FETCH |
| 100 | `select::it_parses_for_update` | LockingClause |
| 101 | `select::it_parses_for_share` | LockingClause (SHARE) |
| 102 | `select::it_parses_for_update_nowait` | LockingClause (NOWAIT) |
| 103 | `select::it_parses_for_update_skip_locked` | LockingClause (SKIP LOCKED) |
| 104 | `select::it_parses_complex_select` | All basic features combined |
| 105 | `select::it_parses_analytics_query` | Window + interval + aggregate |
| 106 | `select::it_parses_hierarchy_query` | Recursive CTE + ARRAY + path |
| 107 | `select::it_parses_complex_report_query` | CTE + JOIN + NULLIF + window |
| 108 | `select::it_parses_mixed_subqueries_and_ctes` | CTE + scalar sub + EXISTS + ORDER BY sub |
| 109 | `select::it_parses_column_with_collate` | CollateClause |
| 110 | `select::it_parses_partition_by_range` | PartitionSpec + PartitionElem |
| 111 | `select::it_parses_partition_by_list` | PARTITION BY LIST |
| 112 | `select::it_parses_partition_by_hash` | PARTITION BY HASH |
| 113 | `select::it_parses_partition_for_values_range` | PartitionBoundSpec (range) |
| 114 | `select::it_parses_partition_for_values_list` | PartitionBoundSpec (list) |
| 115 | `select::it_parses_partition_for_values_hash` | PartitionBoundSpec (hash) |
| 116 | `select::it_parses_partition_default` | PartitionBoundSpec (default) |

### Step 4 — `dml` (INSERT / UPDATE / DELETE)

| # | Test name | Key new node types |
|---|-----------|-------------------|
| 117 | `dml::it_parses_insert` | InsertStmt basic |
| 118 | `dml::it_parses_update` | UpdateStmt basic |
| 119 | `dml::it_parses_delete` | DeleteStmt basic |
| 120 | `dml::it_parses_insert_returning` | RETURNING clause |
| 121 | `dml::it_parses_insert_multiple_rows` | Multiple VALUES tuples |
| 122 | `dml::it_parses_insert_default_values` | SetToDefault node |
| 123 | `dml::it_parses_insert_select` | INSERT ... SELECT |
| 124 | `dml::it_parses_insert_select_complex` | INSERT ... SELECT with aggregates |
| 125 | `dml::it_parses_insert_on_conflict_do_nothing` | OnConflictClause (DO NOTHING) |
| 126 | `dml::it_parses_insert_on_conflict` | OnConflictClause (DO UPDATE) + InferClause |
| 127 | `dml::it_parses_insert_on_conflict_with_where` | ON CONFLICT with WHERE |
| 128 | `dml::it_parses_insert_on_conflict_multiple_columns` | Multi-column conflict |
| 129 | `dml::it_parses_insert_returning_multiple` | RETURNING multiple cols |
| 130 | `dml::it_parses_insert_with_subquery_value` | SubLink in VALUES |
| 131 | `dml::it_parses_insert_overriding` | OVERRIDING SYSTEM VALUE |
| 132 | `dml::it_parses_insert_with_cte` | INSERT with CTE |
| 133 | `dml::it_parses_update_multiple_columns` | Multiple SET clauses |
| 134 | `dml::it_parses_update_returning` | UPDATE RETURNING |
| 135 | `dml::it_parses_update_with_subquery_set` | SubLink in SET |
| 136 | `dml::it_parses_update_from` | FROM clause in UPDATE |
| 137 | `dml::it_parses_update_from_multiple_tables` | FROM + JOIN in UPDATE |
| 138 | `dml::it_parses_update_with_cte` | UPDATE with CTE |
| 139 | `dml::it_parses_update_complex_where` | Complex WHERE + NOT EXISTS |
| 140 | `dml::it_parses_update_row_comparison` | MultiAssignRef |
| 141 | `dml::it_parses_update_with_case` | CaseExpr in SET |
| 142 | `dml::it_parses_update_array` | FuncCall (array_append) |
| 143 | `dml::it_parses_delete_returning` | DELETE RETURNING |
| 144 | `dml::it_parses_delete_with_subquery` | SubLink in DELETE WHERE |
| 145 | `dml::it_parses_delete_using` | USING clause |
| 146 | `dml::it_parses_delete_using_multiple_tables` | USING + multiple tables |
| 147 | `dml::it_parses_delete_with_cte` | DELETE with CTE |
| 148 | `dml::it_parses_delete_with_exists` | NOT EXISTS in DELETE |
| 149 | `dml::it_parses_delete_complex_conditions` | Complex boolean WHERE |
| 150 | `dml::it_parses_delete_only` | DELETE FROM ONLY |
| 151 | `dml::it_parses_insert_cte_returning` | DML CTE (INSERT in CTE) |
| 152 | `dml::it_parses_update_cte_returning` | DML CTE (UPDATE in CTE) |
| 153 | `dml::it_parses_delete_cte_returning` | DML CTE (DELETE in CTE) |
| 154 | `dml::it_parses_chained_dml_ctes` | Multiple DML CTEs chained |

### Step 5 — `ddl` (CREATE / ALTER / DROP)

| # | Test name | Key new node types |
|---|-----------|-------------------|
| 155 | `ddl::it_parses_create_table` | CreateStmt, ColumnDef, TypeName |
| 156 | `ddl::it_parses_drop_table` | DropStmt |
| 157 | `ddl::it_parses_create_index` | IndexStmt |
| 158 | `ddl::it_parses_create_table_with_constraints` | Constraint nodes |
| 159 | `ddl::it_parses_create_table_as` | CreateTableAsStmt |
| 160 | `ddl::it_parses_create_view` | ViewStmt |
| 161 | `ddl::it_parses_create_materialized_view` | CreateTableAsStmt (matview) |
| 162 | `ddl::it_parses_alter_table_add_column` | AlterTableStmt + AlterTableCmd |
| 163 | `ddl::it_parses_alter_table_drop_column` | AlterTableCmd (DROP) |
| 164 | `ddl::it_parses_alter_table_add_constraint` | Constraint (FK) |
| 165 | `ddl::it_parses_alter_table_rename` | RenameStmt |
| 166 | `ddl::it_parses_alter_table_rename_column` | RenameStmt (column) |
| 167 | `ddl::it_parses_alter_owner` | AlterOwnerStmt |
| 168 | `ddl::it_parses_create_index_expression` | IndexElem with expr |
| 169 | `ddl::it_parses_partial_unique_index` | IndexStmt with WHERE |
| 170 | `ddl::it_parses_create_index_concurrently` | IndexStmt (CONCURRENTLY) |
| 171 | `ddl::it_parses_truncate` | TruncateStmt |
| 172 | `ddl::it_parses_create_sequence` | CreateSeqStmt |
| 173 | `ddl::it_parses_create_sequence_with_options` | DefElem options |
| 174 | `ddl::it_parses_create_sequence_if_not_exists` | IF NOT EXISTS |
| 175 | `ddl::it_parses_alter_sequence` | AlterSeqStmt |
| 176 | `ddl::it_parses_create_domain` | CreateDomainStmt |
| 177 | `ddl::it_parses_create_domain_not_null` | Domain + NOT NULL |
| 178 | `ddl::it_parses_create_domain_default` | Domain + DEFAULT |
| 179 | `ddl::it_parses_create_composite_type` | CompositeTypeStmt |
| 180 | `ddl::it_parses_create_enum_type` | CreateEnumStmt |
| 181 | `ddl::it_parses_create_extension` | CreateExtensionStmt |
| 182 | `ddl::it_parses_create_extension_with_schema` | WITH SCHEMA option |
| 183 | `ddl::it_parses_create_publication` | CreatePublicationStmt |
| 184 | `ddl::it_parses_create_publication_for_tables` | FOR TABLE |
| 185 | `ddl::it_parses_alter_publication` | AlterPublicationStmt |
| 186 | `ddl::it_parses_create_subscription` | CreateSubscriptionStmt |
| 187 | `ddl::it_parses_alter_subscription` | AlterSubscriptionStmt |
| 188 | `ddl::it_parses_create_trigger` | CreateTrigStmt |
| 189 | `ddl::it_parses_create_trigger_after_update` | Trigger with WHEN |
| 190 | `ddl::it_parses_create_constraint_trigger` | Constraint trigger |

### Step 6 — `statements` (utility / session)

| # | Test name | Key new node types |
|---|-----------|-------------------|
| 191 | `statements::it_parses_explain` | ExplainStmt |
| 192 | `statements::it_parses_explain_analyze` | ExplainStmt with options |
| 193 | `statements::it_parses_copy` | CopyStmt |
| 194 | `statements::it_parses_prepare` | PrepareStmt |
| 195 | `statements::it_parses_execute` | ExecuteStmt |
| 196 | `statements::it_parses_deallocate` | DeallocateStmt |
| 197 | `statements::it_parses_begin` | TransactionStmt |
| 198 | `statements::it_parses_begin_with_options` | Transaction options |
| 199 | `statements::it_parses_commit` | TransactionStmt (COMMIT) |
| 200 | `statements::it_parses_rollback` | TransactionStmt (ROLLBACK) |
| 201 | `statements::it_parses_start_transaction` | START TRANSACTION |
| 202 | `statements::it_parses_savepoint` | TransactionStmt (SAVEPOINT) |
| 203 | `statements::it_parses_rollback_to_savepoint` | ROLLBACK TO |
| 204 | `statements::it_parses_release_savepoint` | RELEASE |
| 205 | `statements::it_parses_vacuum` | VacuumStmt |
| 206 | `statements::it_parses_vacuum_table` | VacuumStmt + relation |
| 207 | `statements::it_parses_vacuum_analyze` | VACUUM ANALYZE |
| 208 | `statements::it_parses_vacuum_full` | VACUUM FULL |
| 209 | `statements::it_parses_analyze` | VacuumStmt (ANALYZE) |
| 210 | `statements::it_parses_analyze_table` | ANALYZE + relation |
| 211 | `statements::it_parses_analyze_columns` | ANALYZE + column list |
| 212 | `statements::it_parses_set` | VariableSetStmt |
| 213 | `statements::it_parses_set_equals` | SET with = |
| 214 | `statements::it_parses_set_local` | SET LOCAL |
| 215 | `statements::it_parses_set_session` | SET SESSION |
| 216 | `statements::it_parses_reset` | VariableSetStmt (RESET) |
| 217 | `statements::it_parses_reset_all` | RESET ALL |
| 218 | `statements::it_parses_show` | VariableShowStmt |
| 219 | `statements::it_parses_show_all` | SHOW ALL |
| 220 | `statements::it_parses_listen` | ListenStmt |
| 221 | `statements::it_parses_notify` | NotifyStmt |
| 222 | `statements::it_parses_notify_with_payload` | NotifyStmt + payload |
| 223 | `statements::it_parses_unlisten` | UnlistenStmt |
| 224 | `statements::it_parses_unlisten_all` | UNLISTEN * |
| 225 | `statements::it_parses_discard_all` | DiscardStmt |
| 226 | `statements::it_parses_discard_plans` | DISCARD PLANS |
| 227 | `statements::it_parses_discard_sequences` | DISCARD SEQUENCES |
| 228 | `statements::it_parses_discard_temp` | DISCARD TEMP |
| 229 | `statements::it_parses_lock_table` | LockStmt |
| 230 | `statements::it_parses_lock_multiple_tables` | LockStmt multi-table |
| 231 | `statements::it_parses_do_statement` | DoStmt |
| 232 | `statements::it_parses_do_with_language` | DoStmt + language |

---

## Progress Tracking

**232 / 232 tests passing** (100%) ✅ — last checked 2026-02-08

### Step 1 — `basic`: ✅ 21/21
### Step 2 — `expressions`: ✅ 34/34
### Step 3 — `select`: ✅ 61/61
### Step 4 — `dml`: ✅ 38/38
### Step 5 — `ddl`: ✅ 36/36
### Step 6 — `statements`: ✅ 42/42

All 232 tests pass. The iterative migration is functionally complete.

---

## After All Tests Pass

1. **Run full regression**: `cargo test --test raw_parse_iter_tests`
2. **Run acceptance suite** against `parse_raw_iter` (port `parse_raw_acceptance.rs`)
3. **Run benchmarks**: `cargo test --test raw_parse_iter_tests benchmark -- --nocapture` (if added)
4. **Clean up dead code**: remove unused standalone `convert_*` functions, the legacy
   `convert_node()` / `convert_node_boxed()` functions, `convert_list_to_nodes()`,
   and the `stacker` dependency
5. **Swap entrypoint**: make `parse_raw` call the iterative implementation internally

## General Rules

- **No recursive fallback**: every node type that appears in a parse tree must have its
  own match arm in `process()`. The `_ =>` catch-all panics to surface missing types early.
- **Queue and collect must be symmetric**: every node queued produces exactly one result.
  Every `fetch_list_results` call must match a corresponding `queue_list_nodes` call with
  the same list pointer. Every `single_result` must match a `queue_node`.
- **Null pointers produce no result**: `queue_node` with null is skipped in `process()`,
  `single_result` checks the original pointer to decide whether to pop.
- **Null lists produce no results**: `queue_list_nodes` and `fetch_list_results` both
  early-return on null.
- **Push order**: `queue_collect` first (bottom of stack), then children left-to-right.
  Children are processed right-to-left (LIFO). Results accumulate in reverse.
  `fetch_results` drains from the end and reverses.
- **One test at a time**: run, fix, verify, commit.
