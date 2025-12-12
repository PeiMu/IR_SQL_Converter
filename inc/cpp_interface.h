#pragma once

#include <fstream>

#include "duckdb_plan_to_ir.h"
#include "ir_to_sql.h"
#include "nodestr_to_ir.h"
#include "simplest_ir.h"

namespace ir_sql_converter {
std::vector<std::unique_ptr<SimplestStmt>>
ConvertNodeStrToIRFromFile(const std::string &nodestr_file_name);

std::unique_ptr<SimplestStmt> ConvertNodeStrToIR(const std::string &nodestr,
                                                 size_t query_id);

std::unique_ptr<SimplestStmt>
ConvertDuckDBPlanToIR(duckdb::Binder &binder, duckdb::ClientContext &context,
                      duckdb::LogicalOperator *duckdb_plan_pointer,
                      const std::unordered_map<unsigned int, std::string>
                          &intermediate_table_map);

std::string ConvertIRToSQL(SimplestStmt &simplest_stmt, int query_id,
                           bool save_file = false,
                           const std::string &sql_path = "");
} // namespace ir_sql_converter