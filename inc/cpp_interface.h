#pragma once

#include <fstream>

#include "duckdb_plan_to_ir.h"
#include "ir_to_duckdb_plan.h"
#include "ir_to_nodestr.h"
#include "ir_to_sql.h"
#include "nodestr_to_ir.h"
#include "parsetree_to_ir.h"
#include "schema_parser.h"
#include "simplest_ir.h"

namespace ir_sql_converter {

// Global schema parser (can be initialized once and reused)
extern SchemaParser *g_schema_parser;

// Initialize global schema parser from file
bool InitSchemaParser(const std::string &schema_path);

// Clean up global schema parser
void CleanupSchemaParser();

// Get the global schema parser
SchemaParser *GetSchemaParser();

std::vector<std::unique_ptr<SimplestStmt>>
ConvertNodeStrToIRFromFile(const std::string &nodestr_file_name);

std::unique_ptr<SimplestStmt> ConvertNodeStrToIR(const std::string &nodestr,
                                                 size_t query_id);

// Convert SimplestIR to PostgreSQL nodestring format
std::string
ConvertIRToNodeStr(const std::unique_ptr<SimplestStmt> &simplest_ir);

// Convert parse tree to IR (without schema - column indices will be 0)
std::unique_ptr<SimplestStmt> ConvertParseTreeToIR(const json &parse_tree,
                                                   unsigned int sub_plan_id);

// Convert parse tree to IR with schema parser for correct column indices
std::unique_ptr<SimplestStmt> ConvertParseTreeToIR(const json &parse_tree,
                                                   unsigned int sub_plan_id,
                                                   const SchemaParser *schema);

// Convert parse tree to IR using global schema parser
std::unique_ptr<SimplestStmt>
ConvertParseTreeToIRWithSchema(const json &parse_tree,
                               unsigned int sub_plan_id);

std::unique_ptr<SimplestStmt> ConvertDuckDBPlanToIR(
    duckdb::Binder &binder, duckdb::ClientContext &context,
    duckdb::LogicalOperator *duckdb_plan_pointer,
    const std::unordered_map<unsigned int, std::string> &intermediate_table_map,
    bool embed_intermediate_data = false,
    const std::unordered_map<unsigned int, std::vector<std::string>>
        *chunk_col_names = nullptr);

duckdb::unique_ptr<duckdb::LogicalOperator> ConvertIRToDuckDBPlan(
    duckdb::Binder &binder, duckdb::ClientContext &context,
    const std::unique_ptr<SimplestStmt> &simplest_ir,
    std::unordered_map<duckdb::idx_t,
                       duckdb::unique_ptr<duckdb::ColumnDataCollection>>
        *intermediate_results = nullptr);

std::string ConvertIRToSQL(SimplestStmt &simplest_stmt, size_t query_id,
                           bool save_file = false,
                           const std::string &sql_path = "");

// SimplestIR serialization/deserialization functions
void SaveSimplestIRToFile(const std::unique_ptr<SimplestStmt> &ir,
                          const std::string &filename);

std::unique_ptr<SimplestStmt>
LoadSimplestIRFromFile(const std::string &filename);

} // namespace ir_sql_converter