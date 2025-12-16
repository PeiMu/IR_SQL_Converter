#include "parsetree_to_ir.h"
#include <iostream>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace ir_sql_converter {
std::unique_ptr<SimplestStmt> ParseTreeToIR::Convert(const std::string &sql) {
  Clear();

  // Parse SQL using libpg_query
  PgQueryParseResult result = pg_query_parse(sql.c_str());

  if (result.error) {
    std::string error_msg =
        "Parse error: " + std::string(result.error->message);
    pg_query_free_parse_result(result);
    throw std::runtime_error(error_msg);
  }

  // Parse JSON
  json parse_tree = json::parse(result.parse_tree);
  pg_query_free_parse_result(result);

  // Extract SELECT statement
  if (!parse_tree.contains("stmts") || parse_tree["stmts"].empty()) {
    throw std::runtime_error("No statements found in parse tree");
  }

  json first_stmt = parse_tree["stmts"][0];
  if (!first_stmt.contains("stmt") ||
      !first_stmt["stmt"].contains("SelectStmt")) {
    throw std::runtime_error("Only SELECT statements are supported");
  }

  json select_stmt = first_stmt["stmt"]["SelectStmt"];

  return ConvertSelectStmt(select_stmt, 0);
}

std::unique_ptr<SimplestStmt>
ParseTreeToIR::ConvertSelectStmt(const json &select_node,
                                 unsigned int sub_plan_id) {
  std::cout << select_node.dump(2) << std::endl;

  // Convert FROM clause first (builds table index map)
  std::unique_ptr<SimplestStmt> from_tree = nullptr;
  if (select_node.contains("fromClause") &&
      !select_node["fromClause"].empty()) {
    from_tree = ConvertFromClause(select_node["fromClause"]);
  }

  // Convert target list (SELECT columns)
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  if (select_node.contains("targetList")) {
    target_list = ConvertTargetList(select_node["targetList"]);
  }

  // Convert WHERE clause and separate join conditions from filter conditions
  std::vector<std::unique_ptr<SimplestExpr>> filter_conditions;
  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;

  if (select_node.contains("whereClause")) {
    auto all_conditions = ConvertWhereClause(select_node["whereClause"]);

    // Recursively extract VarComparison (join conditions) from nested AND
    // expressions
    for (auto &cond : all_conditions) {
      ExtractJoinAndFilterConditions(std::move(cond), join_conditions,
                                     filter_conditions);
    }
  }

  // Build IR tree
  std::unique_ptr<SimplestStmt> result_tree;

  if (from_tree) {
    result_tree = std::move(from_tree);
    // Check if we have join conditions that need to be applied
    if (!join_conditions.empty()) {
      // SimplestJoin requires exactly 2 children (binary join)
      // We need to extract the children from the CrossProduct tree
      // For a CrossProduct tree, we can split it into left and right subtrees

      if (result_tree->GetNodeType() == SimplestNodeType::CrossProductNode) {
        //        ConstructCrossProduct();
        // CrossProduct has children - split into left and right for Join
        auto cross_product =
            unique_ptr_cast<SimplestStmt, SimplestCrossProduct>(
                std::move(result_tree));

        if (cross_product->children.size() >= 2) {
          // Extract children from CrossProduct
          auto left_child = std::move(cross_product->children[0]);
          auto right_child = std::move(cross_product->children[1]);

          // If there are more than 2 children, rebuild the right side as nested
          // CrossProducts
          for (size_t i = 2; i < cross_product->children.size(); i++) {
            std::vector<std::unique_ptr<SimplestStmt>> cp_children;
            cp_children.push_back(std::move(right_child));
            cp_children.push_back(std::move(cross_product->children[i]));

            auto base = std::make_unique<SimplestStmt>(
                std::move(cp_children), SimplestNodeType::StmtNode);
            right_child =
                std::make_unique<SimplestCrossProduct>(std::move(base));
          }

          // Create Join with left and right children
          std::vector<std::unique_ptr<SimplestStmt>> join_children;
          join_children.push_back(std::move(left_child));
          join_children.push_back(std::move(right_child));

          auto base_stmt = std::make_unique<SimplestStmt>(
              std::move(join_children),
              std::vector<std::unique_ptr<SimplestAttr>>(),
              SimplestNodeType::StmtNode);

          result_tree = std::make_unique<SimplestJoin>(
              std::move(base_stmt), std::move(join_conditions),
              SimplestJoinType::Inner);
        }
      } else {
        // Not a CrossProduct
        // Put join conditions back into filter conditions since we can't create
        // a proper Join
        for (auto &jc : join_conditions) {
          filter_conditions.push_back(std::move(jc));
        }
        join_conditions.clear();
      }
    }

    // Add filter conditions (non-join conditions) as Filter node if present
    if (!filter_conditions.empty()) {
      // Rebuild filter conditions with AND if we have multiple conditions
      std::vector<std::unique_ptr<SimplestExpr>> combined_filters;

      if (filter_conditions.size() == 1) {
        combined_filters.push_back(std::move(filter_conditions[0]));
      } else {
        // Chain all filter conditions with AND
        std::unique_ptr<SimplestExpr> combined =
            std::move(filter_conditions[0]);
        for (size_t i = 1; i < filter_conditions.size(); i++) {
          combined = std::make_unique<SimplestLogicalExpr>(
              SimplestLogicalOp::LogicalAnd, std::move(combined),
              std::move(filter_conditions[i]));
        }
        combined_filters.push_back(std::move(combined));
      }

      std::vector<std::unique_ptr<SimplestStmt>> filter_children;
      filter_children.push_back(std::move(result_tree));

      auto filter_base = std::make_unique<SimplestStmt>(
          std::move(filter_children),
          std::vector<std::unique_ptr<SimplestAttr>>(),
          SimplestNodeType::StmtNode);
      filter_base->qual_vec = std::move(combined_filters);

      result_tree = std::make_unique<SimplestFilter>(std::move(filter_base));
    }

    // Create Projection node on top with target_list
    std::vector<std::unique_ptr<SimplestStmt>> proj_children;
    proj_children.push_back(std::move(result_tree));

    auto proj_base = std::make_unique<SimplestStmt>(std::move(proj_children),
                                                    std::move(target_list),
                                                    SimplestNodeType::StmtNode);
    auto table_index = UINT_MAX - sub_plan_id;
    result_tree =
        std::make_unique<SimplestProjection>(std::move(proj_base), table_index);
  } else {
    // No FROM clause - just a projection
    std::cout << "Warning: No FROM clause - just a projection" << std::endl;
    std::vector<std::unique_ptr<SimplestStmt>> empty_children;
    auto proj_base = std::make_unique<SimplestStmt>(std::move(empty_children),
                                                    std::move(target_list),
                                                    SimplestNodeType::StmtNode);
    auto table_index = UINT_MAX - sub_plan_id;
    result_tree =
        std::make_unique<SimplestProjection>(std::move(proj_base), table_index);
  }

  // Wrap in SimplestAggregate node if aggregate functions were found
  if (!agg_functions.empty()) {
    // The target list should be on the Aggregate node, not the child
    std::vector<std::unique_ptr<SimplestAttr>> saved_target_list =
        std::move(result_tree->target_list);

    std::vector<std::unique_ptr<SimplestStmt>> agg_children;
    agg_children.push_back(std::move(result_tree));

    auto agg_base = std::make_unique<SimplestStmt>(std::move(agg_children),
                                                   std::move(saved_target_list),
                                                   SimplestNodeType::StmtNode);

    result_tree = std::make_unique<SimplestAggregate>(std::move(agg_base),
                                                      std::move(agg_functions));
  }

  return result_tree;
}

std::unique_ptr<SimplestStmt>
ParseTreeToIR::ConvertFromClause(const json &from_list) {
  if (from_list.empty()) {
    return nullptr;
  }

  // Handle first table/join
  std::unique_ptr<SimplestStmt> current_tree = nullptr;

  for (const auto &from_item : from_list) {
    if (from_item.contains("JoinExpr")) {
      current_tree = ConvertJoinExpr(from_item["JoinExpr"]);
    } else if (from_item.contains("RangeVar")) {
      auto scan = ConvertRangeVar(from_item["RangeVar"]);
      if (!current_tree) {
        current_tree = std::move(scan);
      } else {
        // Implicit cross product - create base stmt with both children
        std::vector<std::unique_ptr<SimplestStmt>> children;
        children.push_back(std::move(current_tree));
        children.push_back(std::move(scan));

        auto base_stmt = std::make_unique<SimplestStmt>(
            std::move(children), SimplestNodeType::StmtNode);

        current_tree =
            std::make_unique<SimplestCrossProduct>(std::move(base_stmt));
      }
    }
  }

  return current_tree;
}

std::unique_ptr<SimplestStmt>
ParseTreeToIR::ConvertJoinExpr(const json &join_node) {
  // Convert left and right sides
  auto left_tree = ConvertFromClause({join_node["larg"]});
  auto right_tree = ConvertFromClause({join_node["rarg"]});

  // Get join type
  SimplestJoinType join_type = SimplestJoinType::Inner;
  if (join_node.contains("jointype")) {
    int join_type_val = join_node["jointype"];
    // 0 = INNER, 1 = LEFT, 2 = FULL, 3 = RIGHT
    switch (join_type_val) {
    case 0:
      join_type = SimplestJoinType::Inner;
      break;
    case 1:
      join_type = SimplestJoinType::Left;
      break;
    case 2:
      join_type = SimplestJoinType::Full;
      break;
    case 3:
      join_type = SimplestJoinType::Right;
      break;
    }
  }

  // Convert join quals (ON clause)
  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;
  if (join_node.contains("quals")) {
    auto qual_exprs = ConvertWhereClause(join_node["quals"]);
    // Convert SimplestExpr to SimplestVarComparison
    for (auto &expr : qual_exprs) {
      if (expr->GetNodeType() == SimplestNodeType::VarComparisonNode) {
        auto var_comp = unique_ptr_cast<SimplestExpr, SimplestVarComparison>(
            std::move(expr));
        join_conditions.emplace_back(std::move(var_comp));
      }
    }
  }

  // Build join node
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.push_back(std::move(left_tree));
  children.push_back(std::move(right_tree));

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), std::vector<std::unique_ptr<SimplestAttr>>(),
      SimplestNodeType::StmtNode);

  return std::make_unique<SimplestJoin>(std::move(base_stmt),
                                        std::move(join_conditions), join_type);
}

std::unique_ptr<SimplestStmt>
ParseTreeToIR::ConvertRangeVar(const json &range_var) {
  std::string table_name = range_var["relname"];
  unsigned int table_index = GetOrCreateTableIndex(table_name);

  // Store alias if present
  if (range_var.contains("alias") && range_var["alias"].contains("aliasname")) {
    std::string alias = range_var["alias"]["aliasname"];
    alias_to_table_map[alias] = table_name;
  }

  std::vector<std::unique_ptr<SimplestStmt>> empty_children;
  std::vector<std::unique_ptr<SimplestAttr>> empty_attrs;
  auto base_stmt = std::make_unique<SimplestStmt>(std::move(empty_children),
                                                  std::move(empty_attrs),
                                                  SimplestNodeType::StmtNode);

  return std::make_unique<SimplestScan>(std::move(base_stmt), table_index,
                                        table_name);
}

std::vector<std::unique_ptr<SimplestExpr>>
ParseTreeToIR::ConvertWhereClause(const json &where_node) {
  std::vector<std::unique_ptr<SimplestExpr>> exprs;

  if (where_node.contains("A_Expr")) {
    exprs.push_back(ConvertAExpr(where_node["A_Expr"]));
  } else if (where_node.contains("BoolExpr")) {
    exprs.push_back(ConvertBoolExpr(where_node["BoolExpr"]));
  } else if (where_node.contains("NullTest")) {
    exprs.push_back(ConvertNullTest(where_node["NullTest"]));
  }

  return exprs;
}

std::unique_ptr<SimplestExpr>
ParseTreeToIR::ConvertAExpr(const json &expr_node) {
  // Check the expression kind
  std::string kind;
  if (expr_node.contains("kind")) {
    kind = expr_node["kind"].is_string() ? expr_node["kind"].get<std::string>()
                                         : "";
  }

  std::unique_ptr<SimplestExpr> result = nullptr;

  // Handle IN expressions
  if ("AEXPR_IN" == kind) {
    // For IN expressions, convert to OR of multiple equality conditions
    json left_node = expr_node["lexpr"];
    json right_node = expr_node["rexpr"];

    if (!left_node.contains("ColumnRef") || !right_node.contains("List")) {
      throw std::runtime_error("Unsupported IN expression format");
    }

    auto list_items = right_node["List"]["items"];
    if (list_items.empty()) {
      throw std::runtime_error("Empty IN list");
    }

    // Create first comparison
    auto attr = ConvertColumnRef(left_node["ColumnRef"]);
    auto const_var = ConvertAConst(list_items[0]["A_Const"]);
    result = std::make_unique<SimplestVarConstComparison>(
        SimplestExprType::Equal, std::move(attr), std::move(const_var));

    // Chain remaining comparisons with OR
    for (size_t i = 1; i < list_items.size(); i++) {
      auto attr_i = ConvertColumnRef(left_node["ColumnRef"]);
      auto const_var_i = ConvertAConst(list_items[i]["A_Const"]);
      auto comparison = std::make_unique<SimplestVarConstComparison>(
          SimplestExprType::Equal, std::move(attr_i), std::move(const_var_i));

      result = std::make_unique<SimplestLogicalExpr>(
          SimplestLogicalOp::LogicalOr, std::move(result),
          std::move(comparison));
    }
  } else {
    // For the others expressions, e.g., "Like", "=", ">", "<", etc.
    // Get operator
    std::string op_name;
    if (expr_node.contains("name") && !expr_node["name"].empty()) {
      op_name = expr_node["name"][0]["String"]["sval"];
    }

    SimplestExprType expr_type = ConvertToSimplestExprType(op_name);

    // Get left and right operands
    json left_node = expr_node["lexpr"];
    json right_node = expr_node["rexpr"];

    // Check if it's column-column comparison (VarComparison) or column-const
    // (VarConstComparison)
    bool left_is_column = left_node.contains("ColumnRef");
    bool right_is_column = right_node.contains("ColumnRef");
    bool right_is_const = right_node.contains("A_Const");

    if (left_is_column && right_is_column) {
      // VarComparison (join condition)
      auto left_attr = ConvertColumnRef(left_node["ColumnRef"]);
      auto right_attr = ConvertColumnRef(right_node["ColumnRef"]);

      result = std::make_unique<SimplestVarComparison>(
          expr_type, std::move(left_attr), std::move(right_attr));
    } else if (left_is_column && right_is_const) {
      // VarConstComparison (filter condition)
      auto attr = ConvertColumnRef(left_node["ColumnRef"]);
      auto const_var = ConvertAConst(right_node["A_Const"]);

      result = std::make_unique<SimplestVarConstComparison>(
          expr_type, std::move(attr), std::move(const_var));
    } else {
      std::cout << "Warning: unsupported in ConvertAExpr. left_is_column: "
                << left_is_column << ", right_is_column: " << right_is_column
                << ", right_is_const" << right_is_const << "." << std::endl;
    }
  }

  return result;
}

std::unique_ptr<SimplestExpr>
ParseTreeToIR::ConvertBoolExpr(const json &bool_expr) {
  SimplestLogicalOp logical_op;

  // Handle both string and integer formats
  if (bool_expr["boolop"].is_string()) {
    std::string bool_op_str = bool_expr["boolop"];
    if (bool_op_str == "AND_EXPR") {
      logical_op = SimplestLogicalOp::LogicalAnd;
    } else if (bool_op_str == "OR_EXPR") {
      logical_op = SimplestLogicalOp::LogicalOr;
    } else if (bool_op_str == "NOT_EXPR") {
      logical_op = SimplestLogicalOp::LogicalNot;
    } else {
      throw std::runtime_error("Unknown boolean operator: " + bool_op_str);
    }
  } else {
    // Handle integer format: 0 = AND, 1 = OR, 2 = NOT
    int bool_op = bool_expr["boolop"];
    switch (bool_op) {
    case 0:
      logical_op = SimplestLogicalOp::LogicalAnd;
      break;
    case 1:
      logical_op = SimplestLogicalOp::LogicalOr;
      break;
    case 2:
      logical_op = SimplestLogicalOp::LogicalNot;
      break;
    default:
      throw std::runtime_error("Unknown boolean operator");
    }
  }

  std::unique_ptr<SimplestExpr> result = nullptr;
  json args = bool_expr["args"];
  if (logical_op == SimplestLogicalOp::LogicalNot) {
    // NOT has only one argument
    auto right_exprs = ConvertWhereClause(args[0]);
    result = std::make_unique<SimplestLogicalExpr>(logical_op, nullptr,
                                                   std::move(right_exprs[0]));
  } else {
    // AND/OR can have two or more arguments - chain them together
    if (args.empty()) {
      throw std::runtime_error("BoolExpr with no arguments");
    }

    // Start with the first argument
    auto first_exprs = ConvertWhereClause(args[0]);
    result = std::move(first_exprs[0]);

    // Chain all remaining arguments with the same logical operator
    for (size_t i = 1; i < args.size(); i++) {
      auto next_exprs = ConvertWhereClause(args[i]);
      result = std::make_unique<SimplestLogicalExpr>(
          logical_op, std::move(result), std::move(next_exprs[0]));
    }
  }

  return result;
}

std::unique_ptr<SimplestExpr>
ParseTreeToIR::ConvertNullTest(const json &null_test) {
  auto attr = ConvertColumnRef(null_test["arg"]["ColumnRef"]);

  int nulltesttype = null_test["nulltesttype"];
  SimplestExprType expr_type = (nulltesttype == 0)
                                   ? SimplestExprType::NullType
                                   : SimplestExprType::NonNullType;

  return std::make_unique<SimplestIsNullExpr>(expr_type, std::move(attr));
}

std::vector<std::unique_ptr<SimplestAttr>>
ParseTreeToIR::ConvertTargetList(const json &target_list) {
  std::vector<std::unique_ptr<SimplestAttr>> attrs;

  for (const auto &target : target_list) {
    if (target.contains("ResTarget")) {
      auto attr = ConvertResTarget(target["ResTarget"]);
      if (attr) {
        attrs.push_back(std::move(attr));
      }
    }
  }

  return attrs;
}

std::unique_ptr<SimplestAttr>
ParseTreeToIR::ConvertResTarget(const json &res_target) {
  std::unique_ptr<SimplestAttr> result;

  if (res_target.contains("val")) {
    json val = res_target["val"];
    if (val.contains("ColumnRef")) {
      result = ConvertColumnRef(val["ColumnRef"]);
    } else if (val.contains("FuncCall")) {
      // Handle aggregate functions like MIN, MAX, COUNT, etc.
      json func_call = val["FuncCall"];

      // Extract function name
      std::string func_name;
      if (func_call.contains("funcname") && !func_call["funcname"].empty()) {
        func_name = func_call["funcname"][0]["String"]["sval"];
        // Convert to uppercase for comparison
        for (auto &c : func_name)
          c = std::toupper(c);
      }

      SimplestAggFnType agg_type = GetAggFnType(func_name);

      if (agg_type != SimplestAggFnType::InvalidAggType &&
          func_call.contains("args") && !func_call["args"].empty()) {
        json first_arg = func_call["args"][0];
        if (first_arg.contains("ColumnRef")) {
          result = ConvertColumnRef(first_arg["ColumnRef"]);
          // Store aggregate function for later processing
          auto attr_copy = std::make_unique<SimplestAttr>(*result);
          agg_functions.emplace_back(std::move(attr_copy), agg_type);
        }
      } else {
        // If no args or unsupported format, skip this target
        std::cout << "Warning: FuncCall without column argument, skipping"
                  << std::endl;
        return nullptr;
      }
    }
  }
  return result;
}

std::unique_ptr<SimplestAttr>
ParseTreeToIR::ConvertColumnRef(const json &col_ref) {
  json fields = col_ref["fields"];

  std::string table_name;
  std::string column_name;

  if (fields.size() == 1) {
    // Just column name (e.g., "id")
    column_name = fields[0]["String"]["sval"];
    // Infer table from context (use first table for now)
    if (!table_names.empty()) {
      table_name = table_names[0];
    }
  } else if (fields.size() == 2) {
    // Table.column or Alias.column (e.g., "t1.id" or "t.id")
    std::string table_or_alias = fields[0]["String"]["sval"];
    column_name = fields[1]["String"]["sval"];
    // Resolve alias to actual table name
    table_name = ResolveTableName(table_or_alias);
  }

  unsigned int table_index = table_index_map[table_name];
  SimplestVarType var_type = GetVarTypeFromColumn(table_name, column_name);

  // Column index is unknown without schema, use 0 for now
  unsigned int column_index = 0;

  return std::make_unique<SimplestAttr>(var_type, table_index, column_index,
                                        column_name);
}

std::unique_ptr<SimplestConstVar>
ParseTreeToIR::ConvertAConst(const json &a_const) {
  if (a_const.contains("ival")) {
    return std::make_unique<SimplestConstVar>(
        a_const["ival"]["ival"].get<int>());
  } else if (a_const.contains("fval")) {
    return std::make_unique<SimplestConstVar>(
        std::stof(a_const["fval"]["fval"].get<std::string>()));
  } else if (a_const.contains("sval")) {
    return std::make_unique<SimplestConstVar>(
        a_const["sval"]["sval"].get<std::string>());
  } else if (a_const.contains("boolval")) {
    return std::make_unique<SimplestConstVar>(
        a_const["boolval"]["boolval"].get<bool>());
  }

  throw std::runtime_error("Unknown constant type");
}

SimplestVarType
ParseTreeToIR::GetVarTypeFromColumn(const std::string &table_name,
                                    const std::string &column_name) {
  // Without schema information, we can't determine the exact type
  // Default to StringVar (you can enhance this with a schema lookup)
  // TODO: Query database metadata to get actual column types
  return SimplestVarType::StringVar;
}

SimplestExprType
ParseTreeToIR::ConvertToSimplestExprType(const std::string &op_name) {
  if (op_name == "=")
    return SimplestExprType::Equal;
  if (op_name == "<>" || op_name == "!=")
    return SimplestExprType::NotEqual;
  if (op_name == "<")
    return SimplestExprType::LessThan;
  if (op_name == ">")
    return SimplestExprType::GreaterThan;
  if (op_name == "<=")
    return SimplestExprType::LessEqual;
  if (op_name == ">=")
    return SimplestExprType::GreaterEqual;
  if (op_name == "~~" || op_name == "LIKE")
    return SimplestExprType::TextLike;
  return SimplestExprType::Equal; // default
}

SimplestJoinType
ParseTreeToIR::ConvertToSimplestJoinType(const std::string &join_type) {
  if (join_type == "INNER")
    return SimplestJoinType::Inner;
  if (join_type == "LEFT")
    return SimplestJoinType::Left;
  if (join_type == "RIGHT")
    return SimplestJoinType::Right;
  if (join_type == "FULL")
    return SimplestJoinType::Full;
  return SimplestJoinType::Inner; // default
}

unsigned int
ParseTreeToIR::GetOrCreateTableIndex(const std::string &table_name) {
  auto it = table_index_map.find(table_name);
  if (it != table_index_map.end()) {
    return it->second;
  }

  unsigned int index = next_table_index++;
  table_index_map[table_name] = index;
  table_names.push_back(table_name);
  return index;
}

std::string ParseTreeToIR::ResolveTableName(const std::string &table_or_alias) {
  // Check if it's an alias first
  auto it = alias_to_table_map.find(table_or_alias);
  if (it != alias_to_table_map.end()) {
    return it->second;
  }
  // Otherwise, assume it's the actual table name
  return table_or_alias;
}

SimplestAggFnType ParseTreeToIR::GetAggFnType(const std::string &func_name) {
  if (func_name == "MIN")
    return SimplestAggFnType::Min;
  if (func_name == "MAX")
    return SimplestAggFnType::Max;
  if (func_name == "SUM")
    return SimplestAggFnType::Sum;
  if (func_name == "AVG" || func_name == "AVERAGE")
    return SimplestAggFnType::Average;
  return SimplestAggFnType::InvalidAggType;
}

void ParseTreeToIR::ExtractJoinAndFilterConditions(
    std::unique_ptr<SimplestExpr> expr,
    std::vector<std::unique_ptr<SimplestVarComparison>> &join_conditions,
    std::vector<std::unique_ptr<SimplestExpr>> &filter_conditions) {

  if (!expr) {
    return;
  }

  SimplestNodeType node_type = expr->GetNodeType();

  if (node_type == SimplestNodeType::VarComparisonNode) {
    // This is a join condition (column = column)
    auto var_comp_expr =
        unique_ptr_cast<SimplestExpr, SimplestVarComparison>(std::move(expr));
    join_conditions.emplace_back(std::move(var_comp_expr));
  } else if (node_type == SimplestNodeType::LogicalExprNode) {
    // Recursively process logical expressions (AND/OR)
    auto logical_expr =
        unique_ptr_cast<SimplestExpr, SimplestLogicalExpr>(std::move(expr));

    // For AND expressions, we can separate join and filter conditions
    // For OR expressions, we need to keep them together
    if (logical_expr->GetLogicalOp() == SimplestLogicalOp::LogicalAnd) {
      // Extract left side
      if (logical_expr->left_expr) {
        auto left = std::move(logical_expr->left_expr);
        ExtractJoinAndFilterConditions(std::move(left), join_conditions,
                                       filter_conditions);
      }
      // Extract right side
      if (logical_expr->right_expr) {
        auto right = std::move(logical_expr->right_expr);
        ExtractJoinAndFilterConditions(std::move(right), join_conditions,
                                       filter_conditions);
      }
      // Don't add the LogicalExpr itself since we've extracted its children
    } else {
      // For OR/NOT, keep the entire expression as a filter condition
      filter_conditions.push_back(
          unique_ptr_cast<SimplestLogicalExpr, SimplestExpr>(
              std::move(logical_expr)));
    }
  } else {
    // This is a filter condition (VarConstComparison, IsNull, etc.)
    filter_conditions.push_back(std::move(expr));
  }
}
} // namespace ir_sql_converter