#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

#include "cpp_interface.h"
#include "pg_query.h"

void TestConvertIRToSQL(ir_sql_converter::SimplestStmt &stmt, size_t query_id) {
  std::string sql =
      ir_sql_converter::ConvertIRToSQL(stmt, query_id, true, "../query_");
  if (!sql.empty()) {
    std::cout << "Generated SQL code for query " << query_id
              << " is: " << std::endl;
    std::cout << sql << std::endl;
  } else {
    throw std::runtime_error("Failed to convert IR to SQL for query " +
                             std::to_string(query_id));
  }
}

int TestConvertNodeStrToIRFromFile() {
  std::vector<std::unique_ptr<ir_sql_converter::SimplestStmt>> stmt_list =
      ir_sql_converter::ConvertNodeStrToIRFromFile("../postgres_plan");
  if (stmt_list.empty()) {
    throw std::runtime_error("Failed to convert nodestr to IR");
  }

  size_t query_id = 1;
  for (const auto &stmt : stmt_list) {
    if (!stmt) {
      throw std::runtime_error("Failed to get stmt " +
                               std::to_string(query_id));
    }

    TestConvertIRToSQL(*stmt, query_id);
    query_id++;
  }

  return 0;
}

int TestConvertNodeStrToIR() {
  std::ifstream file("../postgres_plan");
  std::string node_str;

  size_t query_id = 1;
  while (std::getline(file, node_str)) {
    std::unique_ptr<ir_sql_converter::SimplestStmt> stmt =
        ir_sql_converter::ConvertNodeStrToIR(node_str, query_id);
    if (!stmt) {
      throw std::runtime_error("Failed to get stmt " +
                               std::to_string(query_id));
    }

    TestConvertIRToSQL(*stmt, query_id);
    query_id++;
  }

  return 0;
}

using json = nlohmann::json;

int TestConvertPGParseTreeToIR() {
  std::ifstream file(
      "/home/pei/Project/benchmarks/imdb_job-postgres/queries/6d.sql");
  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string sql = buffer.str();

  std::cout << "Testing ParseTree to IR conversion for SQL query:" << sql
            << std::endl;

  // Parse SQL using libpg_query
  PgQueryParseResult result = pg_query_parse(sql.c_str());

  if (result.error) {
    std::runtime_error("Parse error: " + std::string(result.error->message));
    pg_query_free_parse_result(result);
    exit(-1);
  }

  // Parse JSON
  json parse_tree = json::parse(result.parse_tree);
  pg_query_free_parse_result(result);

  size_t query_id = 1;

  std::unique_ptr<ir_sql_converter::SimplestStmt> stmt =
      ir_sql_converter::ConvertParseTreeToIR(parse_tree, query_id);
  if (!stmt) {
    throw std::runtime_error("Failed to get stmt " + std::to_string(query_id));
  }

  TestConvertIRToSQL(*stmt, query_id);
  return 0;
}

int main(void) {
  TestConvertNodeStrToIRFromFile();
  TestConvertNodeStrToIR();
  TestConvertPGParseTreeToIR();

  return 0;
}
