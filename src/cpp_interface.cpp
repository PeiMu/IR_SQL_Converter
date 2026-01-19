#include <iostream>

#include "cpp_interface.h"

namespace ir_sql_converter {

std::vector<std::unique_ptr<SimplestStmt>>
ConvertNodeStrToIRFromFile(const std::string &nodestr_file_name) {
  std::vector<std::unique_ptr<SimplestStmt>> simplest_irs;

  // get the postgres node string
  std::ifstream input_stream(nodestr_file_name, std::ios_base::binary);
  if (input_stream.fail()) {
    std::cout << "Error! Failed to open file!!!" << std::endl;
    exit(-1);
  }
  std::string str_line;

  std::vector<std::string> query_string_vec;
  while (std::getline(input_stream, str_line)) {
    query_string_vec.emplace_back(str_line);
  }
  size_t subqueries_num = query_string_vec.size();

  NodestrToIR nodestr_to_ir_converter;
  for (size_t i = 0; i < subqueries_num; i++) {
    std::unique_ptr<SimplestStmt> postgres_stmt =
        ConvertNodeStrToIR(query_string_vec[i], i);

    simplest_irs.emplace_back(std::move(postgres_stmt));
  }

  return simplest_irs;
}

std::unique_ptr<SimplestStmt> ConvertNodeStrToIR(const std::string &nodestr,
                                                 size_t query_id) {
  NodestrToIR nodestr_to_ir_converter;
  nodestr_to_ir_converter.Clear();
  std::unique_ptr<SimplestNode> postgres_plan =
      nodestr_to_ir_converter.StringToNode(nodestr.c_str());
  std::unique_ptr<SimplestStmt> postgres_stmt =
      unique_ptr_cast<SimplestNode, SimplestStmt>(std::move(postgres_plan));
  postgres_stmt = nodestr_to_ir_converter.GenerateProjHead(
      std::move(postgres_stmt), query_id);
  return std::move(postgres_stmt);
}

std::unique_ptr<SimplestStmt> ConvertParseTreeToIR(const json &parse_tree,
                                                   unsigned int sub_plan_id) {
  ParseTreeToIR converter;
  return converter.Convert(parse_tree, sub_plan_id);
}

std::unique_ptr<SimplestStmt> ConvertDuckDBPlanToIR(
    duckdb::Binder &binder, duckdb::ClientContext &context,
    duckdb::LogicalOperator *duckdb_plan_pointer,
    const std::unordered_map<unsigned int, std::string> &intermediate_table_map,
    bool embed_intermediate_data) {
  ir_sql_converter::DuckToIR converter(binder, context);
  auto ir = converter.ConstructSimplestStmt(
      duckdb_plan_pointer, intermediate_table_map, embed_intermediate_data);
  return std::move(ir);
}

duckdb::unique_ptr<duckdb::LogicalOperator> ConvertIRToDuckDBPlan(
    duckdb::Binder &binder, duckdb::ClientContext &context,
    const std::unique_ptr<SimplestStmt> &simplest_ir,
    std::unordered_map<duckdb::idx_t,
                       duckdb::unique_ptr<duckdb::ColumnDataCollection>>
        *intermediate_results,
    bool run_post_optimize) {
  ir_sql_converter::IRToDuck converter(binder, context, intermediate_results);
  auto duckdb_plan = converter.ConstructDuckdbPlan(simplest_ir);
  // optionally run post optimization
  if (run_post_optimize) {
    duckdb::Optimizer optimizer(binder, context);
    duckdb_plan = optimizer.PostOptimize(std::move(duckdb_plan));
  }
  return std::move(duckdb_plan);
}

std::string ConvertIRToSQL(SimplestStmt &simplest_stmt, size_t query_id,
                           bool save_file, const std::string &sql_path) {
  IRToSQLConverter ir_to_sql_converter;
  std::string sql_code =
      ir_to_sql_converter.ConvertSimplestIRToSQL(simplest_stmt);
  if (save_file) {
    std::string sql_file_name = sql_path + std::to_string(query_id) + ".sql";
    std::ofstream sql_file(sql_file_name);
    sql_file << sql_code;
    sql_file.close();
  }
  return sql_code;
}

} // namespace ir_sql_converter
