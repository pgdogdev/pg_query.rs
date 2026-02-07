# Recursion Classification of `raw_parse.rs` Convert Functions

This document classifies every `convert_*` function in `raw_parse.rs` into two categories:

1. **Recursive** — the function calls `convert_node`, `convert_node_boxed`, `convert_list_to_nodes`,
   or transitively calls another function that does, meaning it re-enters the main `convert_node`
   dispatch and can contribute to unbounded recursion depth.

2. **Non-recursive (leaf)** — the function never re-enters `convert_node`. It only reads scalar
   fields, calls `convert_c_string`, or calls other leaf functions. These are safe from stack
   overflow regardless of input.

---

## Transitive helper classification

Before classifying the top-level converters, we need to know which "typed helper" converters
are themselves recursive. A helper is recursive if it calls `convert_node_boxed`,
`convert_list_to_nodes`, or another recursive helper.

| Helper | Recursive? | Reason |
|---|---|---|
| `convert_c_string` | No | Pure C string → Rust string |
| `convert_function_parameter_mode` | No | Pure enum mapping |
| `convert_string` | No | Only calls `convert_c_string` |
| `convert_role_spec` | No | Only calls `convert_c_string` |
| `convert_json_format` | No | Only scalar fields |
| `convert_json_returning` | No | Only calls `convert_json_format` (leaf) |
| `convert_json_table_path` | No | Only calls `convert_c_string` |
| `convert_alias` | **Yes** | Calls `convert_list_to_nodes(alias.colnames)` |
| `convert_range_var` | **Yes** | Calls `convert_alias` (recursive) |
| `convert_type_name` | **Yes** | Calls `convert_list_to_nodes` (×3: names, typmods, arrayBounds) |
| `convert_object_with_args` | **Yes** | Calls `convert_list_to_nodes` (×3) |
| `convert_window_def` | **Yes** | Calls `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_with_clause` | **Yes** | Calls `convert_list_to_nodes` |
| `convert_with_clause_opt` | **Yes** | Calls `convert_with_clause` |
| `convert_variable_set_stmt` | **Yes** | Calls `convert_list_to_nodes` |
| `convert_variable_set_stmt_opt` | **Yes** | Calls `convert_variable_set_stmt` |
| `convert_collate_clause` | **Yes** | Calls `convert_node_boxed`, `convert_list_to_nodes` |
| `convert_collate_clause_opt` | **Yes** | Calls `convert_collate_clause` |
| `convert_partition_spec` | **Yes** | Calls `convert_list_to_nodes` |
| `convert_partition_spec_opt` | **Yes** | Calls `convert_partition_spec` |
| `convert_partition_bound_spec` | **Yes** | Calls `convert_list_to_nodes` (×3) |
| `convert_partition_bound_spec_opt` | **Yes** | Calls `convert_partition_bound_spec` |
| `convert_cte_search_clause` | **Yes** | Calls `convert_list_to_nodes` |
| `convert_cte_search_clause_opt` | **Yes** | Calls `convert_cte_search_clause` |
| `convert_cte_cycle_clause` | **Yes** | Calls `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_cte_cycle_clause_opt` | **Yes** | Calls `convert_cte_cycle_clause` |
| `convert_infer_clause` | **Yes** | Calls `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_infer_clause_opt` | **Yes** | Calls `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_on_conflict_clause` | **Yes** | Calls `convert_infer_clause`, `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_into_clause` | **Yes** | Calls `convert_range_var`, `convert_list_to_nodes`, `convert_node_boxed` |
| `convert_json_output` | **Yes** | Calls `convert_type_name` (recursive) |
| `convert_json_value_expr` | **Yes** | Calls `convert_node_boxed` |
| `convert_json_behavior` | **Yes** | Calls `convert_node_boxed` |
| `convert_json_agg_constructor` | **Yes** | Calls `convert_node_boxed`, `convert_list_to_nodes`, `convert_window_def` |
| `convert_json_key_value` | **Yes** | Calls `convert_node_boxed`, `convert_json_value_expr` |
| `convert_func_call` | **Yes** | Calls `convert_list_to_nodes`, `convert_node_boxed`, `convert_window_def` |
| `convert_grant_stmt` | **Yes** | Calls `convert_list_to_nodes`, `convert_role_spec` |
| `convert_publication_table` | **Yes** | Calls `convert_range_var`, `convert_node_boxed`, `convert_list_to_nodes` |
| `convert_create_stmt` | **Yes** | Calls `convert_range_var`, `convert_list_to_nodes`, `convert_type_name`, etc. |

---

## Non-Recursive (Leaf) Functions

These functions **never** re-enter `convert_node` — not directly and not through any transitive
call chain. They are entirely safe from contributing to stack depth.

| # | Function | Lines | What it calls |
|---|---|---|---|
| 1 | `convert_c_string` | L2177–2183 | — (raw pointer to String) |
| 2 | `convert_function_parameter_mode` | L1933–1943 | — (pure enum match) |
| 3 | `convert_string` | L1516–1518 | `convert_c_string` |
| 4 | `convert_a_const` | L1179–1217 | Reads union fields directly; no node calls |
| 5 | `convert_bit_string` | L2189–2191 | `convert_c_string` |
| 6 | `convert_role_spec` | L1600–1602 | `convert_c_string` |
| 7 | `convert_replica_identity_stmt` | L1520–1522 | `convert_c_string` |
| 8 | `convert_notify_stmt` | L1945–1947 | `convert_c_string` |
| 9 | `convert_listen_stmt` | L1949–1951 | `convert_c_string` |
| 10 | `convert_unlisten_stmt` | L1953–1955 | `convert_c_string` |
| 11 | `convert_discard_stmt` | L1957–1961 | Scalar only |
| 12 | `convert_set_to_default` | L1658–1666 | Scalar only |
| 13 | `convert_trigger_transition` | L2765–2767 | `convert_c_string` |
| 14 | `convert_variable_show_stmt` | L1836–1838 | `convert_c_string` |
| 15 | `convert_deallocate_stmt` | L1654–1656 | `convert_c_string`, scalars |
| 16 | `convert_close_portal_stmt` | L2217–2219 | `convert_c_string` |
| 17 | `convert_fetch_stmt` | L2221–2228 | `convert_c_string`, scalars |
| 18 | `convert_load_stmt` | L2651–2653 | `convert_c_string` |
| 19 | `convert_alter_database_refresh_coll_stmt` | L2622–2624 | `convert_c_string` |
| 20 | `convert_alter_event_trig_stmt` | L2314–2319 | `convert_c_string`, scalars |
| 21 | `convert_drop_table_space_stmt` | L2464–2466 | `convert_c_string`, scalars |
| 22 | `convert_drop_subscription_stmt` | L2666–2668 | `convert_c_string`, scalars |
| 23 | `convert_drop_user_mapping_stmt` | L2436–2442 | `convert_role_spec` (leaf), `convert_c_string` |
| 24 | `convert_json_format` | L2837–2839 | Scalar only |
| 25 | `convert_json_returning` | L2841–2847 | `convert_json_format` (leaf) |
| 26 | `convert_json_table_path` | L2907–2910 | `convert_c_string` |
| 27 | `convert_sql_value_function` | L2793–2795 | Scalar only |

**Total: 27 non-recursive functions**

---

## Recursive Functions

These functions **do** re-enter `convert_node` (directly via `convert_node_boxed` /
`convert_list_to_nodes`, or transitively via a recursive helper like `convert_range_var`,
`convert_alias`, `convert_type_name`, etc.). They contribute to stack depth when processing
nested parse trees.

| # | Function | Lines | Recursive calls (direct) |
|---|---|---|---|
| 1 | `convert_node` | L102–988 | Central dispatch — calls all `convert_*` functions |
| 2 | `convert_node_boxed` | L97–99 | `convert_node` |
| 3 | `convert_list` | L991–994 | `convert_list_to_nodes` |
| 4 | `convert_list_to_nodes` | L999–1020 | `convert_node` (per element) |
| 5 | `convert_list_to_raw_stmts` | L66–89 | `convert_raw_stmt` |
| 6 | `convert_raw_stmt` | L92–94 | `convert_node_boxed` |
| 7 | `convert_select_stmt` | L1026–1049 | `convert_list_to_nodes` (×8), `convert_node_boxed` (×4), `convert_into_clause`, `convert_with_clause_opt`, **self-recursive via larg/rarg** |
| 8 | `convert_insert_stmt` | L1051–1061 | `convert_range_var`, `convert_list_to_nodes` (×2), `convert_node_boxed`, `convert_on_conflict_clause`, `convert_with_clause_opt` |
| 9 | `convert_update_stmt` | L1063–1072 | `convert_range_var`, `convert_list_to_nodes` (×3), `convert_node_boxed`, `convert_with_clause_opt` |
| 10 | `convert_delete_stmt` | L1074–1082 | `convert_range_var`, `convert_list_to_nodes` (×2), `convert_node_boxed`, `convert_with_clause_opt` |
| 11 | `convert_create_stmt` | L1084–1099 | `convert_range_var`, `convert_list_to_nodes` (×4), `convert_type_name`, `convert_partition_bound_spec_opt`, `convert_partition_spec_opt` |
| 12 | `convert_drop_stmt` | L1101–1109 | `convert_list_to_nodes` |
| 13 | `convert_index_stmt` | L1111–1138 | `convert_range_var`, `convert_list_to_nodes` (×4), `convert_node_boxed` |
| 14 | `convert_range_var` | L1144–1154 | `convert_alias` → `convert_list_to_nodes` |
| 15 | `convert_column_ref` | L1156–1158 | `convert_list_to_nodes` |
| 16 | `convert_res_target` | L1160–1167 | `convert_list_to_nodes`, `convert_node_boxed` |
| 17 | `convert_a_expr` | L1169–1177 | `convert_list_to_nodes`, `convert_node_boxed` (×2) |
| 18 | `convert_func_call` | L1219–1233 | `convert_list_to_nodes` (×3), `convert_node_boxed`, `convert_window_def` |
| 19 | `convert_type_cast` | L1235–1241 | `convert_node_boxed`, `convert_type_name` |
| 20 | `convert_type_name` | L1243–1254 | `convert_list_to_nodes` (×3) |
| 21 | `convert_alias` | L1256–1258 | `convert_list_to_nodes` |
| 22 | `convert_join_expr` | L1260–1272 | `convert_node_boxed` (×3), `convert_list_to_nodes`, `convert_alias` (×2) |
| 23 | `convert_sort_by` | L1274–1282 | `convert_node_boxed`, `convert_list_to_nodes` |
| 24 | `convert_bool_expr` | L1284–1291 | `convert_list_to_nodes` |
| 25 | `convert_sub_link` | L1293–1303 | `convert_node_boxed` (×2), `convert_list_to_nodes` |
| 26 | `convert_null_test` | L1305–1313 | `convert_node_boxed` |
| 27 | `convert_case_expr` | L1315–1325 | `convert_node_boxed` (×2), `convert_list_to_nodes` |
| 28 | `convert_case_when` | L1327–1334 | `convert_node_boxed` (×2) |
| 29 | `convert_coalesce_expr` | L1336–1344 | `convert_list_to_nodes` |
| 30 | `convert_with_clause` | L1346–1348 | `convert_list_to_nodes` |
| 31 | `convert_with_clause_opt` | L1350–1356 | `convert_with_clause` |
| 32 | `convert_common_table_expr` | L1358–1374 | `convert_list_to_nodes` (×5), `convert_node_boxed`, `convert_cte_search_clause_opt`, `convert_cte_cycle_clause_opt` |
| 33 | `convert_window_def` | L1376–1387 | `convert_list_to_nodes` (×2), `convert_node_boxed` (×2) |
| 34 | `convert_into_clause` | L1389–1404 | `convert_range_var`, `convert_list_to_nodes` (×2), `convert_node_boxed` |
| 35 | `convert_infer_clause` | L1406–1417 | `convert_list_to_nodes`, `convert_node_boxed` |
| 36 | `convert_on_conflict_clause` | L1419–1431 | `convert_infer_clause`, `convert_list_to_nodes`, `convert_node_boxed` |
| 37 | `convert_column_def` | L1433–1455 | `convert_type_name`, `convert_node_boxed` (×2), `convert_range_var`, `convert_collate_clause_opt`, `convert_list_to_nodes` (×2) |
| 38 | `convert_constraint` | L1457–1491 | `convert_node_boxed` (×2), `convert_list_to_nodes` (×7), `convert_range_var` |
| 39 | `convert_index_elem` | L1493–1504 | `convert_node_boxed`, `convert_list_to_nodes` (×3) |
| 40 | `convert_def_elem` | L1506–1514 | `convert_node_boxed` |
| 41 | `convert_grouping_func` | L1524–1532 | `convert_list_to_nodes` (×2) |
| 42 | `convert_locking_clause` | L1534–1540 | `convert_list_to_nodes` |
| 43 | `convert_min_max_expr` | L1542–1552 | `convert_list_to_nodes` |
| 44 | `convert_grouping_set` | L1554–1556 | `convert_list_to_nodes` |
| 45 | `convert_range_subselect` | L1558–1564 | `convert_node_boxed`, `convert_alias` |
| 46 | `convert_a_array_expr` | L1566–1568 | `convert_list_to_nodes` |
| 47 | `convert_a_indirection` | L1570–1572 | `convert_node_boxed`, `convert_list_to_nodes` |
| 48 | `convert_a_indices` | L1574–1576 | `convert_node_boxed` (×2) |
| 49 | `convert_alter_table_stmt` | L1578–1585 | `convert_range_var`, `convert_list_to_nodes` |
| 50 | `convert_alter_table_cmd` | L1587–1598 | `convert_role_spec`, `convert_node_boxed` |
| 51 | `convert_copy_stmt` | L1604–1615 | `convert_range_var`, `convert_node_boxed` (×2), `convert_list_to_nodes` (×2) |
| 52 | `convert_truncate_stmt` | L1617–1619 | `convert_list_to_nodes` |
| 53 | `convert_view_stmt` | L1621–1630 | `convert_range_var`, `convert_list_to_nodes` (×2), `convert_node_boxed` |
| 54 | `convert_explain_stmt` | L1632–1634 | `convert_node_boxed`, `convert_list_to_nodes` |
| 55 | `convert_create_table_as_stmt` | L1636–1644 | `convert_node_boxed`, `convert_into_clause` |
| 56 | `convert_prepare_stmt` | L1646–1648 | `convert_list_to_nodes`, `convert_node_boxed` |
| 57 | `convert_execute_stmt` | L1650–1652 | `convert_list_to_nodes` |
| 58 | `convert_multi_assign_ref` | L1668–1670 | `convert_node_boxed` |
| 59 | `convert_row_expr` | L1672–1681 | `convert_list_to_nodes` (×2) |
| 60 | `convert_collate_clause` | L1683–1685 | `convert_node_boxed`, `convert_list_to_nodes` |
| 61 | `convert_collate_clause_opt` | L1687–1693 | `convert_collate_clause` |
| 62 | `convert_partition_spec` | L1695–1706 | `convert_list_to_nodes` |
| 63 | `convert_partition_spec_opt` | L1708–1714 | `convert_partition_spec` |
| 64 | `convert_partition_bound_spec` | L1716–1727 | `convert_list_to_nodes` (×3) |
| 65 | `convert_partition_bound_spec_opt` | L1729–1735 | `convert_partition_bound_spec` |
| 66 | `convert_partition_elem` | L1737–1745 | `convert_node_boxed`, `convert_list_to_nodes` (×2) |
| 67 | `convert_partition_range_datum` | L1747–1758 | `convert_node_boxed` |
| 68 | `convert_cte_search_clause` | L1760–1767 | `convert_list_to_nodes` |
| 69 | `convert_cte_search_clause_opt` | L1769–1775 | `convert_cte_search_clause` |
| 70 | `convert_cte_cycle_clause` | L1777–1790 | `convert_list_to_nodes`, `convert_node_boxed` (×2) |
| 71 | `convert_cte_cycle_clause_opt` | L1792–1798 | `convert_cte_cycle_clause` |
| 72 | `convert_transaction_stmt` | L1804–1813 | `convert_list_to_nodes` |
| 73 | `convert_vacuum_stmt` | L1815–1817 | `convert_list_to_nodes` (×2) |
| 74 | `convert_vacuum_relation` | L1819–1825 | `convert_range_var`, `convert_list_to_nodes` |
| 75 | `convert_variable_set_stmt` | L1827–1834 | `convert_list_to_nodes` |
| 76 | `convert_create_seq_stmt` | L1840–1848 | `convert_range_var`, `convert_list_to_nodes` |
| 77 | `convert_do_stmt` | L1850–1852 | `convert_list_to_nodes` |
| 78 | `convert_lock_stmt` | L1854–1856 | `convert_list_to_nodes` |
| 79 | `convert_create_schema_stmt` | L1858–1865 | `convert_role_spec`, `convert_list_to_nodes` |
| 80 | `convert_rename_stmt` | L1867–1878 | `convert_range_var`, `convert_node_boxed` |
| 81 | `convert_create_function_stmt` | L1880–1890 | `convert_list_to_nodes` (×3), `convert_type_name`, `convert_node_boxed` |
| 82 | `convert_alter_owner_stmt` | L1892–1899 | `convert_range_var`, `convert_node_boxed`, `convert_role_spec` |
| 83 | `convert_alter_seq_stmt` | L1901–1908 | `convert_range_var`, `convert_list_to_nodes` |
| 84 | `convert_create_enum_stmt` | L1910–1912 | `convert_list_to_nodes` (×2) |
| 85 | `convert_object_with_args` | L1914–1921 | `convert_list_to_nodes` (×3) |
| 86 | `convert_function_parameter` | L1923–1930 | `convert_type_name`, `convert_node_boxed` |
| 87 | `convert_coerce_to_domain` | L1963–1973 | `convert_node_boxed` |
| 88 | `convert_composite_type_stmt` | L1975–1980 | `convert_range_var`, `convert_list_to_nodes` |
| 89 | `convert_create_domain_stmt` | L1982–1989 | `convert_list_to_nodes` (×2), `convert_type_name`, `convert_collate_clause_opt` |
| 90 | `convert_create_extension_stmt` | L1991–1997 | `convert_list_to_nodes` |
| 91 | `convert_create_publication_stmt` | L1999–2006 | `convert_list_to_nodes` (×2) |
| 92 | `convert_alter_publication_stmt` | L2008–2016 | `convert_list_to_nodes` (×2) |
| 93 | `convert_create_subscription_stmt` | L2018–2025 | `convert_list_to_nodes` (×2) |
| 94 | `convert_alter_subscription_stmt` | L2027–2035 | `convert_list_to_nodes` (×2) |
| 95 | `convert_publication_obj_spec` | L2037–2040 | `convert_publication_table` (recursive) |
| 96 | `convert_publication_table` | L2042–2049 | `convert_range_var`, `convert_node_boxed`, `convert_list_to_nodes` |
| 97 | `convert_create_trig_stmt` | L2051–2069 | `convert_range_var` (×2), `convert_list_to_nodes` (×4), `convert_node_boxed` |
| 98 | `convert_call_stmt` | L2071–2077 | `convert_func_call`, `convert_list_to_nodes` |
| 99 | `convert_rule_stmt` | L2079–2089 | `convert_range_var`, `convert_node_boxed`, `convert_list_to_nodes` |
| 100 | `convert_grant_stmt` | L2091–2103 | `convert_list_to_nodes` (×3), `convert_role_spec` |
| 101 | `convert_grant_role_stmt` | L2105–2114 | `convert_list_to_nodes` (×3), `convert_role_spec` |
| 102 | `convert_refresh_mat_view_stmt` | L2116–2122 | `convert_range_var` (recursive via alias) |
| 103 | `convert_merge_stmt` | L2124–2133 | `convert_range_var`, `convert_node_boxed` (×2), `convert_list_to_nodes` (×2), `convert_with_clause_opt` |
| 104 | `convert_merge_action` | L2135–2144 | `convert_node_boxed`, `convert_list_to_nodes` (×2) |
| 105 | `convert_merge_when_clause` | L2146–2155 | `convert_node_boxed`, `convert_list_to_nodes` (×2) |
| 106 | `convert_range_function` | L2157–2166 | `convert_list_to_nodes` (×2), `convert_alias` |
| 107 | `convert_access_priv` | L2168–2170 | `convert_list_to_nodes` |
| 108 | `convert_boolean_test` | L2193–2200 | `convert_node_boxed` |
| 109 | `convert_create_range_stmt` | L2202–2204 | `convert_list_to_nodes` (×2) |
| 110 | `convert_alter_enum_stmt` | L2206–2215 | `convert_list_to_nodes` |
| 111 | `convert_declare_cursor_stmt` | L2230–2232 | `convert_node_boxed` |
| 112 | `convert_define_stmt` | L2234–2244 | `convert_list_to_nodes` (×3) |
| 113 | `convert_comment_stmt` | L2246–2248 | `convert_node_boxed` |
| 114 | `convert_sec_label_stmt` | L2250–2257 | `convert_node_boxed` |
| 115 | `convert_create_role_stmt` | L2259–2261 | `convert_list_to_nodes` |
| 116 | `convert_alter_role_stmt` | L2263–2269 | `convert_role_spec`, `convert_list_to_nodes` |
| 117 | `convert_alter_role_set_stmt` | L2271–2277 | `convert_role_spec`, `convert_variable_set_stmt_opt` (recursive) |
| 118 | `convert_drop_role_stmt` | L2279–2281 | `convert_list_to_nodes` |
| 119 | `convert_create_policy_stmt` | L2283–2293 | `convert_range_var`, `convert_list_to_nodes`, `convert_node_boxed` (×2) |
| 120 | `convert_alter_policy_stmt` | L2295–2303 | `convert_range_var`, `convert_list_to_nodes`, `convert_node_boxed` (×2) |
| 121 | `convert_create_event_trig_stmt` | L2305–2312 | `convert_list_to_nodes` (×2) |
| 122 | `convert_create_plang_stmt` | L2321–2330 | `convert_list_to_nodes` (×3) |
| 123 | `convert_create_am_stmt` | L2332–2338 | `convert_list_to_nodes` |
| 124 | `convert_create_op_class_stmt` | L2340–2349 | `convert_list_to_nodes` (×3), `convert_type_name` |
| 125 | `convert_create_op_class_item` | L2351–2360 | `convert_object_with_args`, `convert_list_to_nodes` (×2), `convert_type_name` |
| 126 | `convert_create_op_family_stmt` | L2362–2364 | `convert_list_to_nodes` |
| 127 | `convert_alter_op_family_stmt` | L2366–2373 | `convert_list_to_nodes` (×2) |
| 128 | `convert_create_fdw_stmt` | L2375–2381 | `convert_list_to_nodes` (×2) |
| 129 | `convert_alter_fdw_stmt` | L2383–2389 | `convert_list_to_nodes` (×2) |
| 130 | `convert_create_foreign_server_stmt` | L2391–2400 | `convert_list_to_nodes` |
| 131 | `convert_alter_foreign_server_stmt` | L2402–2409 | `convert_list_to_nodes` |
| 132 | `convert_create_foreign_table_stmt` | L2411–2417 | `convert_create_stmt` (recursive), `convert_list_to_nodes` |
| 133 | `convert_create_user_mapping_stmt` | L2419–2426 | `convert_role_spec`, `convert_list_to_nodes` |
| 134 | `convert_alter_user_mapping_stmt` | L2428–2434 | `convert_role_spec`, `convert_list_to_nodes` |
| 135 | `convert_import_foreign_schema_stmt` | L2444–2453 | `convert_list_to_nodes` (×2) |
| 136 | `convert_create_table_space_stmt` | L2455–2462 | `convert_role_spec`, `convert_list_to_nodes` |
| 137 | `convert_alter_table_space_options_stmt` | L2468–2474 | `convert_list_to_nodes` |
| 138 | `convert_alter_table_move_all_stmt` | L2476–2484 | `convert_list_to_nodes` |
| 139 | `convert_alter_extension_stmt` | L2486–2488 | `convert_list_to_nodes` |
| 140 | `convert_alter_extension_contents_stmt` | L2490–2497 | `convert_node_boxed` |
| 141 | `convert_alter_domain_stmt` | L2499–2508 | `convert_list_to_nodes`, `convert_node_boxed` |
| 142 | `convert_alter_function_stmt` | L2510–2516 | `convert_object_with_args`, `convert_list_to_nodes` |
| 143 | `convert_alter_operator_stmt` | L2518–2523 | `convert_object_with_args`, `convert_list_to_nodes` |
| 144 | `convert_alter_type_stmt` | L2525–2527 | `convert_list_to_nodes` (×2) |
| 145 | `convert_alter_object_schema_stmt` | L2529–2537 | `convert_range_var`, `convert_node_boxed` |
| 146 | `convert_alter_object_depends_stmt` | L2539–2547 | `convert_range_var`, `convert_node_boxed`, `convert_string` |
| 147 | `convert_alter_collation_stmt` | L2549–2551 | `convert_list_to_nodes` |
| 148 | `convert_alter_default_privileges_stmt` | L2553–2558 | `convert_list_to_nodes`, `convert_grant_stmt` (recursive) |
| 149 | `convert_create_cast_stmt` | L2560–2568 | `convert_type_name` (×2), `convert_object_with_args` |
| 150 | `convert_create_transform_stmt` | L2570–2578 | `convert_type_name`, `convert_object_with_args` (×2) |
| 151 | `convert_create_conversion_stmt` | L2580–2588 | `convert_list_to_nodes` (×2) |
| 152 | `convert_alter_ts_dictionary_stmt` | L2590–2592 | `convert_list_to_nodes` (×2) |
| 153 | `convert_alter_ts_configuration_stmt` | L2594–2604 | `convert_list_to_nodes` (×3) |
| 154 | `convert_createdb_stmt` | L2606–2608 | `convert_list_to_nodes` |
| 155 | `convert_dropdb_stmt` | L2610–2612 | `convert_list_to_nodes` |
| 156 | `convert_alter_database_stmt` | L2614–2616 | `convert_list_to_nodes` |
| 157 | `convert_alter_database_set_stmt` | L2618–2620 | `convert_variable_set_stmt_opt` (recursive) |
| 158 | `convert_alter_system_stmt` | L2626–2628 | `convert_variable_set_stmt_opt` (recursive) |
| 159 | `convert_cluster_stmt` | L2630–2636 | `convert_range_var`, `convert_list_to_nodes` |
| 160 | `convert_reindex_stmt` | L2638–2645 | `convert_range_var`, `convert_list_to_nodes` |
| 161 | `convert_constraints_set_stmt` | L2647–2649 | `convert_list_to_nodes` |
| 162 | `convert_drop_owned_stmt` | L2655–2657 | `convert_list_to_nodes` |
| 163 | `convert_reassign_owned_stmt` | L2659–2664 | `convert_list_to_nodes`, `convert_role_spec` |
| 164 | `convert_table_func` | L2670–2690 | `convert_list_to_nodes` (×11), `convert_node_boxed` (×3) |
| 165 | `convert_into_clause_node` | L2692–2703 | `convert_range_var`, `convert_list_to_nodes` (×2), `convert_node_boxed` |
| 166 | `convert_table_like_clause` | L2705–2711 | `convert_range_var` |
| 167 | `convert_range_table_func` | L2713–2723 | `convert_node_boxed` (×2), `convert_list_to_nodes` (×2), `convert_alias` |
| 168 | `convert_range_table_func_col` | L2725–2735 | `convert_type_name`, `convert_node_boxed` (×2) |
| 169 | `convert_range_table_sample` | L2737–2745 | `convert_node_boxed` (×2), `convert_list_to_nodes` (×2) |
| 170 | `convert_partition_cmd` | L2747–2753 | `convert_range_var`, `convert_partition_bound_spec_opt` |
| 171 | `convert_on_conflict_clause_node` | L2755–2763 | `convert_infer_clause_opt`, `convert_list_to_nodes`, `convert_node_boxed` |
| 172 | `convert_create_stats_stmt` | L2769–2779 | `convert_list_to_nodes` (×4) |
| 173 | `convert_alter_stats_stmt` | L2781–2787 | `convert_list_to_nodes`, `convert_node_boxed` |
| 174 | `convert_stats_elem` | L2789–2791 | `convert_node_boxed` |
| 175 | `convert_xml_expr` | L2797–2811 | `convert_list_to_nodes` (×3) |
| 176 | `convert_xml_serialize` | L2813–2821 | `convert_node_boxed`, `convert_type_name` |
| 177 | `convert_named_arg_expr` | L2823–2831 | `convert_node_boxed` |
| 178 | `convert_json_value_expr` | L2849–2855 | `convert_node_boxed` (×2), `convert_json_format` |
| 179 | `convert_json_constructor_expr` | L2857–2869 | `convert_list_to_nodes`, `convert_node_boxed` (×2), `convert_json_returning` |
| 180 | `convert_json_is_predicate` | L2871–2879 | `convert_node_boxed`, `convert_json_format` |
| 181 | `convert_json_behavior` | L2881–2883 | `convert_node_boxed` |
| 182 | `convert_json_expr` | L2885–2905 | `convert_node_boxed` (×2), `convert_json_format`, `convert_json_returning`, `convert_list_to_nodes` (×2), `convert_json_behavior` (×2) |
| 183 | `convert_json_table_path_scan` | L2912–2921 | `convert_node_boxed` (×2), `convert_json_table_path` |
| 184 | `convert_json_table_sibling_join` | L2923–2929 | `convert_node_boxed` (×3) |
| 185 | `convert_json_output` | L2931–2936 | `convert_type_name` (recursive), `convert_json_returning` |
| 186 | `convert_json_argument` | L2938–2943 | `convert_json_value_expr` (recursive) |
| 187 | `convert_json_func_expr` | L2945–2959 | `convert_json_value_expr`, `convert_node_boxed`, `convert_list_to_nodes`, `convert_json_output`, `convert_json_behavior` (×2) |
| 188 | `convert_json_table_path_spec` | L2961–2968 | `convert_node_boxed` |
| 189 | `convert_json_table` | L2970–2981 | `convert_json_value_expr`, `convert_json_table_path_spec`, `convert_list_to_nodes` (×2), `convert_json_behavior`, `convert_alias` |
| 190 | `convert_json_table_column` | L2983–2997 | `convert_type_name`, `convert_json_table_path_spec`, `convert_json_format`, `convert_list_to_nodes`, `convert_json_behavior` (×2) |
| 191 | `convert_json_key_value` | L2999–3004 | `convert_node_boxed`, `convert_json_value_expr` |
| 192 | `convert_json_parse_expr` | L3006–3013 | `convert_json_value_expr`, `convert_json_output` |
| 193 | `convert_json_scalar_expr` | L3015–3021 | `convert_node_boxed`, `convert_json_output` |
| 194 | `convert_json_serialize_expr` | L3023–3029 | `convert_json_value_expr`, `convert_json_output` |
| 195 | `convert_json_object_constructor` | L3031–3039 | `convert_list_to_nodes`, `convert_json_output` |
| 196 | `convert_json_array_constructor` | L3041–3048 | `convert_list_to_nodes`, `convert_json_output` |
| 197 | `convert_json_array_query_constructor` | L3050–3058 | `convert_node_boxed`, `convert_json_output`, `convert_json_format` |
| 198 | `convert_json_agg_constructor` | L3060–3068 | `convert_json_output`, `convert_node_boxed`, `convert_list_to_nodes`, `convert_window_def` |
| 199 | `convert_json_object_agg` | L3070–3077 | `convert_json_agg_constructor` (recursive), `convert_json_key_value` (recursive) |
| 200 | `convert_json_array_agg` | L3079–3085 | `convert_json_agg_constructor` (recursive), `convert_json_value_expr` (recursive) |
| 201 | `convert_variable_set_stmt_opt` | L3091–3097 | `convert_variable_set_stmt` (recursive) |
| 202 | `convert_infer_clause_opt` | L3099–3111 | `convert_list_to_nodes`, `convert_node_boxed` |

**Total: 202 recursive functions** (including the 6 core dispatch/list functions)

---

## Summary

| Category | Count | Notes |
|---|---|---|
| **Non-recursive (leaf)** | 27 | Safe — never re-enter `convert_node` |
| **Recursive** | 202 | Re-enter `convert_node` directly or transitively |
| **Total** | 229 | All `convert_*` functions in `raw_parse.rs` |

The overwhelming majority of functions are recursive because nearly every PostgreSQL node type
contains at least one `List` field (converted via `convert_list_to_nodes`) or a child `Node*`
pointer (converted via `convert_node_boxed`). Even seemingly simple types like `RangeVar` are
recursive because they contain an optional `Alias`, which in turn has a `colnames` list that
goes through `convert_list_to_nodes`.

### Highest-risk recursion paths (unbounded depth)

| Path | Cause |
|---|---|
| `convert_select_stmt` → `convert_select_stmt` (larg/rarg) | Long `UNION`/`INTERSECT`/`EXCEPT` chains |
| `convert_bool_expr` → `convert_list_to_nodes` → `convert_node` → `convert_bool_expr` | Deeply nested `AND`/`OR` |
| `convert_join_expr` → `convert_node_boxed` (larg/rarg) → `convert_node` → `convert_join_expr` | Many-way explicit `JOIN` |
| `convert_sub_link` → `convert_node_boxed` (subselect) → `convert_node` → `convert_select_stmt` → ... | Nested subqueries |
| `convert_a_expr` → `convert_node_boxed` (lexpr/rexpr) → `convert_node` → `convert_a_expr` | Deeply nested expressions |
| `convert_case_expr` → `convert_list_to_nodes` → ... → `convert_case_expr` | Nested `CASE` expressions |
