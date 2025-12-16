//===----------------------------------------------------------------------===//
//                         ParseTree_to_IR
//
// parsetree_to_ir.h
// Converts libpg_query parse tree (JSON format) to SimplestIR
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "simplest_ir.h"

// Forward declare libpg_query types
extern "C" {
#include "pg_query.h"
}

#include <nlohmann/json.hpp>

namespace ir_sql_converter {

using json = nlohmann::json;

class ParseTreeToIR {
public:
  ParseTreeToIR() = default;
  ~ParseTreeToIR() = default;

  // Main conversion function
  std::unique_ptr<SimplestStmt> Convert(const std::string &sql,
                                        unsigned int sub_plan_id);

  void Clear() {
    table_index_map.clear();
    alias_to_table_map.clear();
    table_names.clear();
    next_table_index = 0;
    agg_functions.clear();
  }

private:
  // Parse tree processing
  std::unique_ptr<SimplestStmt> ConvertSelectStmt(const json &select_node,
                                                  unsigned int sub_plan_id);

  // FROM clause processing
  std::unique_ptr<SimplestStmt> ConvertFromClause(const json &from_list);
  std::unique_ptr<SimplestStmt> ConvertJoinExpr(const json &join_node);
  std::unique_ptr<SimplestStmt> ConvertRangeVar(const json &range_var);

  // WHERE clause processing
  std::vector<std::unique_ptr<SimplestExpr>>
  ConvertWhereClause(const json &where_node);
  std::unique_ptr<SimplestExpr> ConvertAExpr(const json &expr_node);
  std::unique_ptr<SimplestExpr> ConvertBoolExpr(const json &bool_expr);
  std::unique_ptr<SimplestExpr> ConvertNullTest(const json &null_test);

  // Target list processing
  std::vector<std::unique_ptr<SimplestAttr>>
  ConvertTargetList(const json &target_list);
  std::unique_ptr<SimplestAttr> ConvertResTarget(const json &res_target);

  // Helper to detect and extract aggregate functions
  SimplestAggFnType GetAggFnType(const std::string &func_name);

  // Column reference processing
  std::unique_ptr<SimplestAttr> ConvertColumnRef(const json &col_ref);

  // Constant/literal processing
  std::unique_ptr<SimplestConstVar> ConvertAConst(const json &a_const);

  // Helper functions
  SimplestVarType GetVarTypeFromColumn(const std::string &table_name,
                                       const std::string &column_name);
  SimplestExprType ConvertToSimplestExprType(const std::string &op_name);
  SimplestJoinType ConvertToSimplestJoinType(const std::string &join_type);

  unsigned int GetOrCreateTableIndex(const std::string &table_name);
  std::string ResolveTableName(const std::string &table_or_alias);

  // Helper to extract join conditions from expression trees
  void ExtractJoinAndFilterConditions(
      std::unique_ptr<SimplestExpr> expr,
      std::vector<std::unique_ptr<SimplestVarComparison>> &join_conditions,
      std::vector<std::unique_ptr<SimplestExpr>> &filter_conditions);

  // State
  std::unordered_map<std::string, unsigned int> table_index_map;
  std::unordered_map<std::string, std::string> alias_to_table_map;
  std::vector<std::string> table_names;
  unsigned int next_table_index = 0;

  // Aggregate function tracking
  std::vector<std::pair<std::unique_ptr<SimplestAttr>, SimplestAggFnType>>
      agg_functions;
};
} // namespace ir_sql_converter