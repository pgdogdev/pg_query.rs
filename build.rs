#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

use fs_extra::dir::CopyOptions;
use glob::glob;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let build_path = Path::new(".").join("libpg_query");
    let out_header_path = out_dir.join("pg_query").with_extension("h");
    let out_raw_header_path = out_dir.join("pg_query_raw").with_extension("h");
    let out_protobuf_path = out_dir.join("protobuf");
    let target = env::var("TARGET").unwrap();

    println!("cargo:rerun-if-changed=libpg_query");
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=pg_query");

    // Copy the relevant source files to the OUT_DIR
    let source_paths = vec![
        build_path.join("pg_query").with_extension("h"),
        build_path.join("pg_query_raw.h"),
        build_path.join("postgres_deparse").with_extension("h"),
        build_path.join("Makefile"),
        build_path.join("src"),
        build_path.join("protobuf"),
        build_path.join("vendor"),
    ];

    let copy_options = CopyOptions { overwrite: true, ..CopyOptions::default() };

    fs_extra::copy_items(&source_paths, &out_dir, &copy_options)?;

    // Compile the C library.
    let mut build = cc::Build::new();
    build
        .files(glob(out_dir.join("src/*.c").to_str().unwrap()).unwrap().map(|p| p.unwrap()))
        .files(glob(out_dir.join("src/postgres/*.c").to_str().unwrap()).unwrap().map(|p| p.unwrap()))
        .file(out_dir.join("vendor/protobuf-c/protobuf-c.c"))
        .file(out_dir.join("vendor/xxhash/xxhash.c"))
        .file(out_dir.join("protobuf/pg_query.pb-c.c"))
        .include(out_dir.join("."))
        .include(out_dir.join("./vendor"))
        .include(out_dir.join("./src/postgres/include"))
        .include(out_dir.join("./src/include"))
        .warnings(false); // Avoid unnecessary warnings, as they are already considered as part of libpg_query development
    if env::var("PROFILE").unwrap() == "debug" || env::var("DEBUG").unwrap() == "1" {
        build.define("USE_ASSERT_CHECKING", None);
    }
    if target.contains("windows") {
        build.include(out_dir.join("./src/postgres/include/port/win32"));
        if target.contains("msvc") {
            build.include(out_dir.join("./src/postgres/include/port/win32_msvc"));
        }
    }
    build.compile("pg_query");

    // Generate bindings for Rust (basic API)
    bindgen::Builder::default()
        .header(out_header_path.to_str().ok_or("Invalid header path")?)
        // Blocklist raw parse functions that are used via bindings_raw
        .blocklist_function("pg_query_parse_raw")
        .blocklist_function("pg_query_parse_raw_opts")
        .blocklist_function("pg_query_free_raw_parse_result")
        .blocklist_type("PgQueryRawParseResult")
        // Blocklist raw deparse functions that use types from bindings_raw
        .blocklist_function("pg_query_deparse_raw")
        .blocklist_function("pg_query_deparse_raw_opts")
        // Blocklist raw fingerprint function that uses types from bindings_raw
        .blocklist_function("pg_query_fingerprint_raw")
        .generate()
        .map_err(|_| "Unable to generate bindings")?
        .write_to_file(out_dir.join("bindings.rs"))?;

    // Generate bindings for raw parse tree access (includes PostgreSQL internal types)
    let mut raw_builder = bindgen::Builder::default()
        .header(out_raw_header_path.to_str().ok_or("Invalid raw header path")?)
        .clang_arg(format!("-I{}", out_dir.display()))
        .clang_arg(format!("-I{}", out_dir.join("src/postgres/include").display()))
        .clang_arg(format!("-I{}", out_dir.join("src/include").display()));

    if target.contains("windows") {
        raw_builder = raw_builder.clang_arg(format!("-I{}", out_dir.join("src/postgres/include/port/win32").display()));
        if target.contains("msvc") {
            raw_builder = raw_builder.clang_arg(format!("-I{}", out_dir.join("src/postgres/include/port/win32_msvc").display()));
        }
    }

    raw_builder
        // Allowlist only the types we need for parse tree traversal
        .allowlist_type("List")
        .allowlist_type("ListCell")
        .allowlist_type("Node")
        .allowlist_type("NodeTag")
        .allowlist_type("RawStmt")
        .allowlist_type("SelectStmt")
        .allowlist_type("InsertStmt")
        .allowlist_type("UpdateStmt")
        .allowlist_type("DeleteStmt")
        .allowlist_type("MergeStmt")
        .allowlist_type("CreateStmt")
        .allowlist_type("AlterTableStmt")
        .allowlist_type("DropStmt")
        .allowlist_type("TruncateStmt")
        .allowlist_type("IndexStmt")
        .allowlist_type("ViewStmt")
        .allowlist_type("RangeVar")
        .allowlist_type("ColumnRef")
        .allowlist_type("ResTarget")
        .allowlist_type("A_Expr")
        .allowlist_type("FuncCall")
        .allowlist_type("TypeCast")
        .allowlist_type("TypeName")
        .allowlist_type("ColumnDef")
        .allowlist_type("Constraint")
        .allowlist_type("JoinExpr")
        .allowlist_type("SortBy")
        .allowlist_type("WindowDef")
        .allowlist_type("WithClause")
        .allowlist_type("CommonTableExpr")
        .allowlist_type("IntoClause")
        .allowlist_type("OnConflictClause")
        .allowlist_type("InferClause")
        .allowlist_type("Alias")
        .allowlist_type("A_Const")
        .allowlist_type("A_Star")
        .allowlist_type("A_Indices")
        .allowlist_type("A_Indirection")
        .allowlist_type("A_ArrayExpr")
        .allowlist_type("SubLink")
        .allowlist_type("BoolExpr")
        .allowlist_type("NullTest")
        .allowlist_type("BooleanTest")
        .allowlist_type("CaseExpr")
        .allowlist_type("CaseWhen")
        .allowlist_type("CoalesceExpr")
        .allowlist_type("MinMaxExpr")
        .allowlist_type("RowExpr")
        .allowlist_type("SetToDefault")
        .allowlist_type("MultiAssignRef")
        .allowlist_type("ParamRef")
        .allowlist_type("CollateClause")
        .allowlist_type("PartitionSpec")
        .allowlist_type("PartitionBoundSpec")
        .allowlist_type("PartitionRangeDatum")
        .allowlist_type("PartitionElem")
        .allowlist_type("CTESearchClause")
        .allowlist_type("CTECycleClause")
        .allowlist_type("RangeSubselect")
        .allowlist_type("RangeFunction")
        .allowlist_type("DefElem")
        .allowlist_type("IndexElem")
        .allowlist_type("SortGroupClause")
        .allowlist_type("GroupingSet")
        .allowlist_type("LockingClause")
        .allowlist_type("MergeWhenClause")
        .allowlist_type("TransactionStmt")
        .allowlist_type("VariableSetStmt")
        .allowlist_type("VariableShowStmt")
        .allowlist_type("ExplainStmt")
        .allowlist_type("CopyStmt")
        .allowlist_type("GrantStmt")
        .allowlist_type("RoleSpec")
        .allowlist_type("FunctionParameter")
        .allowlist_type("AlterTableCmd")
        .allowlist_type("AccessPriv")
        .allowlist_type("ObjectWithArgs")
        .allowlist_type("CreateFunctionStmt")
        .allowlist_type("CreateSchemaStmt")
        .allowlist_type("CreateSeqStmt")
        .allowlist_type("CreateTrigStmt")
        .allowlist_type("RuleStmt")
        .allowlist_type("CallStmt")
        .allowlist_type("GrantRoleStmt")
        .allowlist_type("MergeAction")
        .allowlist_type("CreateDomainStmt")
        .allowlist_type("CreateTableAsStmt")
        .allowlist_type("RefreshMatViewStmt")
        .allowlist_type("VacuumStmt")
        .allowlist_type("VacuumRelation")
        .allowlist_type("LockStmt")
        .allowlist_type("AlterOwnerStmt")
        .allowlist_type("AlterSeqStmt")
        .allowlist_type("CreateEnumStmt")
        .allowlist_type("AlterEnumStmt")
        .allowlist_type("CreateRangeStmt")
        .allowlist_type("DoStmt")
        .allowlist_type("RenameStmt")
        .allowlist_type("NotifyStmt")
        .allowlist_type("ListenStmt")
        .allowlist_type("UnlistenStmt")
        .allowlist_type("DiscardStmt")
        .allowlist_type("CoerceToDomain")
        .allowlist_type("CompositeTypeStmt")
        .allowlist_type("CreateExtensionStmt")
        .allowlist_type("CreatePublicationStmt")
        .allowlist_type("AlterPublicationStmt")
        .allowlist_type("PublicationObjSpec")
        .allowlist_type("PublicationTable")
        .allowlist_type("PublicationObjSpecType")
        .allowlist_type("CreateSubscriptionStmt")
        .allowlist_type("AlterSubscriptionStmt")
        .allowlist_type("PrepareStmt")
        .allowlist_type("ExecuteStmt")
        .allowlist_type("DeallocateStmt")
        .allowlist_type("FetchStmt")
        .allowlist_type("ClosePortalStmt")
        .allowlist_type("String")
        .allowlist_type("Integer")
        .allowlist_type("Float")
        .allowlist_type("Boolean")
        .allowlist_type("BitString")
        // Additional statement types
        .allowlist_type("DeclareCursorStmt")
        .allowlist_type("DefineStmt")
        .allowlist_type("CommentStmt")
        .allowlist_type("SecLabelStmt")
        .allowlist_type("CreateStatsStmt")
        .allowlist_type("AlterStatsStmt")
        .allowlist_type("StatsElem")
        .allowlist_type("CreateRoleStmt")
        .allowlist_type("AlterRoleStmt")
        .allowlist_type("AlterRoleSetStmt")
        .allowlist_type("DropRoleStmt")
        .allowlist_type("CreatePolicyStmt")
        .allowlist_type("AlterPolicyStmt")
        .allowlist_type("CreateEventTrigStmt")
        .allowlist_type("AlterEventTrigStmt")
        .allowlist_type("CreatePLangStmt")
        .allowlist_type("CreateAmStmt")
        .allowlist_type("CreateOpClassStmt")
        .allowlist_type("CreateOpClassItem")
        .allowlist_type("CreateOpFamilyStmt")
        .allowlist_type("AlterOpFamilyStmt")
        .allowlist_type("CreateFdwStmt")
        .allowlist_type("AlterFdwStmt")
        .allowlist_type("CreateForeignServerStmt")
        .allowlist_type("AlterForeignServerStmt")
        .allowlist_type("CreateForeignTableStmt")
        .allowlist_type("CreateUserMappingStmt")
        .allowlist_type("AlterUserMappingStmt")
        .allowlist_type("DropUserMappingStmt")
        .allowlist_type("ImportForeignSchemaStmt")
        .allowlist_type("CreateTableSpaceStmt")
        .allowlist_type("DropTableSpaceStmt")
        .allowlist_type("AlterTableSpaceOptionsStmt")
        .allowlist_type("AlterTableMoveAllStmt")
        .allowlist_type("AlterExtensionStmt")
        .allowlist_type("AlterExtensionContentsStmt")
        .allowlist_type("AlterDomainStmt")
        .allowlist_type("AlterFunctionStmt")
        .allowlist_type("AlterOperatorStmt")
        .allowlist_type("AlterTypeStmt")
        .allowlist_type("AlterObjectSchemaStmt")
        .allowlist_type("AlterObjectDependsStmt")
        .allowlist_type("AlterCollationStmt")
        .allowlist_type("AlterDefaultPrivilegesStmt")
        .allowlist_type("CreateCastStmt")
        .allowlist_type("CreateTransformStmt")
        .allowlist_type("CreateConversionStmt")
        .allowlist_type("AlterTSDictionaryStmt")
        .allowlist_type("AlterTSConfigurationStmt")
        .allowlist_type("CreatedbStmt")
        .allowlist_type("DropdbStmt")
        .allowlist_type("AlterDatabaseStmt")
        .allowlist_type("AlterDatabaseSetStmt")
        .allowlist_type("AlterDatabaseRefreshCollStmt")
        .allowlist_type("AlterSystemStmt")
        .allowlist_type("ClusterStmt")
        .allowlist_type("ReindexStmt")
        .allowlist_type("ConstraintsSetStmt")
        .allowlist_type("LoadStmt")
        .allowlist_type("DropOwnedStmt")
        .allowlist_type("ReassignOwnedStmt")
        .allowlist_type("DropSubscriptionStmt")
        // Table-related nodes
        .allowlist_type("TableFunc")
        .allowlist_type("TableLikeClause")
        .allowlist_type("RangeTableFunc")
        .allowlist_type("RangeTableFuncCol")
        .allowlist_type("RangeTableSample")
        .allowlist_type("PartitionCmd")
        .allowlist_type("SinglePartitionSpec")
        // Expression nodes
        .allowlist_type("Aggref")
        .allowlist_type("Var")
        .allowlist_type("Param")
        .allowlist_type("WindowFunc")
        .allowlist_type("GroupingFunc")
        .allowlist_type("FuncExpr")
        .allowlist_type("NamedArgExpr")
        .allowlist_type("OpExpr")
        .allowlist_type("DistinctExpr")
        .allowlist_type("NullIfExpr")
        .allowlist_type("ScalarArrayOpExpr")
        .allowlist_type("FieldSelect")
        .allowlist_type("FieldStore")
        .allowlist_type("RelabelType")
        .allowlist_type("CoerceViaIO")
        .allowlist_type("ArrayCoerceExpr")
        .allowlist_type("ConvertRowtypeExpr")
        .allowlist_type("CollateExpr")
        .allowlist_type("CaseTestExpr")
        .allowlist_type("ArrayExpr")
        .allowlist_type("RowCompareExpr")
        .allowlist_type("CoerceToDomainValue")
        .allowlist_type("CurrentOfExpr")
        .allowlist_type("NextValueExpr")
        .allowlist_type("InferenceElem")
        .allowlist_type("SubscriptingRef")
        .allowlist_type("SQLValueFunction")
        .allowlist_type("XmlExpr")
        .allowlist_type("XmlSerialize")
        // Query/Plan nodes
        .allowlist_type("SubPlan")
        .allowlist_type("AlternativeSubPlan")
        .allowlist_type("TargetEntry")
        .allowlist_type("RangeTblRef")
        .allowlist_type("FromExpr")
        .allowlist_type("OnConflictExpr")
        .allowlist_type("Query")
        .allowlist_type("SetOperationStmt")
        .allowlist_type("ReturnStmt")
        .allowlist_type("PLAssignStmt")
        .allowlist_type("WindowClause")
        .allowlist_type("RowMarkClause")
        .allowlist_type("WithCheckOption")
        .allowlist_type("RangeTblEntry")
        .allowlist_type("RangeTblFunction")
        .allowlist_type("TableSampleClause")
        // JSON nodes
        .allowlist_type("JsonFormat")
        .allowlist_type("JsonReturning")
        .allowlist_type("JsonValueExpr")
        .allowlist_type("JsonConstructorExpr")
        .allowlist_type("JsonIsPredicate")
        .allowlist_type("JsonBehavior")
        .allowlist_type("JsonExpr")
        .allowlist_type("JsonTablePath")
        .allowlist_type("JsonTablePathScan")
        .allowlist_type("JsonTableSiblingJoin")
        .allowlist_type("JsonOutput")
        .allowlist_type("JsonArgument")
        .allowlist_type("JsonFuncExpr")
        .allowlist_type("JsonTablePathSpec")
        .allowlist_type("JsonTable")
        .allowlist_type("JsonTableColumn")
        .allowlist_type("JsonKeyValue")
        .allowlist_type("JsonParseExpr")
        .allowlist_type("JsonScalarExpr")
        .allowlist_type("JsonSerializeExpr")
        .allowlist_type("JsonObjectConstructor")
        .allowlist_type("JsonArrayConstructor")
        .allowlist_type("JsonArrayQueryConstructor")
        .allowlist_type("JsonAggConstructor")
        .allowlist_type("JsonObjectAgg")
        .allowlist_type("JsonArrayAgg")
        // Other nodes
        .allowlist_type("TriggerTransition")
        .allowlist_type("InlineCodeBlock")
        .allowlist_type("CallContext")
        .allowlist_type("ReplicaIdentityStmt")
        .allowlist_type("WindowFuncRunCondition")
        .allowlist_type("MergeSupportFunc")
        // Allowlist enums
        .allowlist_type("SetOperation")
        .allowlist_type("LimitOption")
        .allowlist_type("A_Expr_Kind")
        .allowlist_type("BoolExprType")
        .allowlist_type("SubLinkType")
        .allowlist_type("NullTestType")
        .allowlist_type("BoolTestType")
        .allowlist_type("MinMaxOp")
        .allowlist_type("JoinType")
        .allowlist_type("SortByDir")
        .allowlist_type("SortByNulls")
        .allowlist_type("CTEMaterialize")
        .allowlist_type("OnCommitAction")
        .allowlist_type("ObjectType")
        .allowlist_type("DropBehavior")
        .allowlist_type("OnConflictAction")
        .allowlist_type("GroupingSetKind")
        .allowlist_type("CmdType")
        .allowlist_type("TransactionStmtKind")
        .allowlist_type("ConstrType")
        .allowlist_type("DefElemAction")
        .allowlist_type("RoleSpecType")
        .allowlist_type("CoercionForm")
        .allowlist_type("VariableSetKind")
        .allowlist_type("LockClauseStrength")
        .allowlist_type("LockWaitPolicy")
        .allowlist_type("ViewCheckOption")
        .allowlist_type("DiscardMode")
        .allowlist_type("FetchDirection")
        .allowlist_type("FunctionParameterMode")
        .allowlist_type("AlterTableType")
        .allowlist_type("GrantTargetType")
        .allowlist_type("OverridingKind")
        .allowlist_type("PartitionStrategy")
        .allowlist_type("PartitionRangeDatumKind")
        .allowlist_type("ReindexObjectType")
        .allowlist_type("AlterSubscriptionType")
        .allowlist_type("AlterPublicationAction")
        .allowlist_type("ImportForeignSchemaType")
        .allowlist_type("RoleStmtType")
        .allowlist_type("RowCompareType")
        .allowlist_type("XmlExprOp")
        .allowlist_type("XmlOptionType")
        .allowlist_type("JsonFormatType")
        .allowlist_type("JsonConstructorType")
        .allowlist_type("JsonValueType")
        .allowlist_type("JsonTableColumnType")
        .allowlist_type("JsonQuotes")
        .allowlist_type("JsonExprOp")
        .allowlist_type("JsonEncoding")
        .allowlist_type("JsonWrapper")
        .allowlist_type("SQLValueFunctionOp")
        .allowlist_type("TableLikeOption")
        // Allowlist raw parse functions
        .allowlist_function("pg_query_parse_raw")
        .allowlist_function("pg_query_parse_raw_opts")
        .allowlist_function("pg_query_free_raw_parse_result")
        // Allowlist raw deparse functions
        .allowlist_function("pg_query_deparse_raw")
        .allowlist_function("pg_query_deparse_raw_opts")
        .allowlist_function("pg_query_free_deparse_result")
        // Node building helpers for deparse_raw
        .allowlist_function("pg_query_deparse_enter_context")
        .allowlist_function("pg_query_deparse_exit_context")
        .allowlist_function("pg_query_alloc_node")
        .allowlist_function("pg_query_pstrdup")
        .allowlist_function("pg_query_list_make1")
        .allowlist_function("pg_query_list_append")
        .allowlist_function("pg_query_deparse_nodes")
        // Raw scan functions (bypasses protobuf)
        .allowlist_type("PgQueryRawScanToken")
        .allowlist_type("PgQueryRawScanResult")
        .allowlist_function("pg_query_scan_raw")
        .allowlist_function("pg_query_free_raw_scan_result")
        // Raw fingerprint (works with raw parse result)
        .allowlist_type("PgQueryFingerprintResult")
        .allowlist_function("pg_query_fingerprint_raw")
        .allowlist_function("pg_query_free_fingerprint_result")
        .generate()
        .map_err(|_| "Unable to generate raw bindings")?
        .write_to_file(out_dir.join("bindings_raw.rs"))?;

    // Only generate protobuf bindings if protoc is available
    let protoc_exists = Command::new("protoc").arg("--version").status().is_ok();
    // If the package is being built by docs.rs, we don't want to regenerate the protobuf bindings
    let is_built_by_docs_rs = env::var("DOCS_RS").is_ok();

    if !is_built_by_docs_rs && (env::var("REGENERATE_PROTOBUF").is_ok() || protoc_exists) {
        println!("generating protobuf bindings");
        // HACK: Set OUT_DIR to src/ so that the generated protobuf file is copied to src/protobuf.rs
        let src_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("src");
        env::set_var("OUT_DIR", &src_dir);

        let mut prost_build = prost_build::Config::new();
        prost_build.type_attribute(".", "#[derive(serde::Serialize)]");
        prost_build.compile_protos(&[&out_protobuf_path.join("pg_query").with_extension("proto")], &[&out_protobuf_path])?;

        std::fs::rename(src_dir.join("pg_query.rs"), src_dir.join("protobuf.rs"))?;

        // Reset OUT_DIR to the original value
        env::set_var("OUT_DIR", &out_dir);
    } else {
        println!("skipping protobuf generation");
    }

    Ok(())
}
