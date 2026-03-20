//===----------------------------------------------------------------------===//
//                         ParseTree_to_IR
//
// parsetree_to_ir.h
// Converts libpg_query parse tree (JSON format) to SimplestIR
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <limits.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "schema_parser.h"
#include "simplest_ir.h"

#include <nlohmann/json.hpp>

namespace ir_sql_converter {

using json = nlohmann::json;

// Callback type for looking up column ordinal position from database schema
// Parameters: table_name, column_name
// Returns: column index (0-based), or -1 if not found
using ColumnIndexLookup =
    std::function<int(const std::string &, const std::string &)>;

class ParseTreeToIR {
public:
  ParseTreeToIR() = default;

  // Constructor with schema parser for column index lookup
  explicit ParseTreeToIR(const SchemaParser *schema) : schema_parser_(schema) {}

  // Constructor with custom callback (for flexibility)
  explicit ParseTreeToIR(ColumnIndexLookup column_lookup)
      : column_index_lookup_(std::move(column_lookup)) {}

  ~ParseTreeToIR() = default;

  // Main conversion function
  std::unique_ptr<AQPStmt> Convert(const json &parse_tree,
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
  std::unique_ptr<AQPStmt> ConvertSelectStmt(const json &select_node,
                                                  unsigned int sub_plan_id);

  // FROM clause processing
  std::unique_ptr<AQPStmt> ConvertFromClause(const json &from_list);
  std::unique_ptr<AQPStmt> ConvertJoinExpr(const json &join_node);
  std::unique_ptr<AQPStmt> ConvertRangeVar(const json &range_var);

  // WHERE clause processing
  std::vector<std::unique_ptr<AQPExpr>>
  ConvertWhereClause(const json &where_node);
  std::unique_ptr<AQPExpr> ConvertAExpr(const json &expr_node);
  std::unique_ptr<AQPExpr> ConvertBoolExpr(const json &bool_expr);
  std::unique_ptr<AQPExpr> ConvertNullTest(const json &null_test);

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
      std::unique_ptr<AQPExpr> expr,
      std::vector<std::unique_ptr<SimplestVarComparison>> &join_conditions,
      std::vector<std::unique_ptr<AQPExpr>> &filter_conditions);

  // State
  std::unordered_map<std::string, unsigned int> table_index_map;
  std::unordered_map<std::string, std::string> alias_to_table_map;
  std::vector<std::string> table_names;
  unsigned int next_table_index = 0;

  // Aggregate function tracking
  std::vector<std::pair<std::unique_ptr<SimplestAttr>, SimplestAggFnType>>
      agg_functions;

  // Schema lookup options (use one or the other)
  const SchemaParser *schema_parser_ = nullptr;
  ColumnIndexLookup column_index_lookup_;
};
} // namespace ir_sql_converter