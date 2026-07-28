#include "parsetree_to_ir.h"
#include <algorithm>
#include <iostream>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace ir_sql_converter {
std::unique_ptr<AQPStmt> ParseTreeToIR::Convert(const json &parse_tree,
                                                     unsigned int sub_plan_id) {
  Clear();

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

  return ConvertSelectStmt(select_stmt, sub_plan_id);
}

std::unique_ptr<AQPStmt>
ParseTreeToIR::ConvertSelectStmt(const json &select_node,
                                 unsigned int sub_plan_id) {
  // Convert FROM clause first (builds table index map)
  std::unique_ptr<AQPStmt> from_tree = nullptr;
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
  std::vector<std::unique_ptr<AQPExpr>> filter_conditions;
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
  std::unique_ptr<AQPStmt> result_tree;

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
            unique_ptr_cast<AQPStmt, SimplestCrossProduct>(
                std::move(result_tree));

        if (cross_product->children.size() >= 2) {
          // Extract children from CrossProduct
          auto left_child = std::move(cross_product->children[0]);
          auto right_child = std::move(cross_product->children[1]);

          // If there are more than 2 children, rebuild the right side as nested
          // CrossProducts
          for (size_t i = 2; i < cross_product->children.size(); i++) {
            std::vector<std::unique_ptr<AQPStmt>> cp_children;
            cp_children.push_back(std::move(right_child));
            cp_children.push_back(std::move(cross_product->children[i]));

            auto base = std::make_unique<AQPStmt>(
                std::move(cp_children), SimplestNodeType::StmtNode);
            right_child =
                std::make_unique<SimplestCrossProduct>(std::move(base));
          }

          // Create Join with left and right children
          std::vector<std::unique_ptr<AQPStmt>> join_children;
          join_children.push_back(std::move(left_child));
          join_children.push_back(std::move(right_child));

          auto base_stmt = std::make_unique<AQPStmt>(
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
      std::vector<std::unique_ptr<AQPExpr>> combined_filters;

      if (filter_conditions.size() == 1) {
        combined_filters.push_back(std::move(filter_conditions[0]));
      } else {
        // Chain all filter conditions with AND
        std::unique_ptr<AQPExpr> combined =
            std::move(filter_conditions[0]);
        for (size_t i = 1; i < filter_conditions.size(); i++) {
          combined = std::make_unique<SimplestLogicalExpr>(
              SimplestLogicalOp::LogicalAnd, std::move(combined),
              std::move(filter_conditions[i]));
        }
        combined_filters.push_back(std::move(combined));
      }

      std::vector<std::unique_ptr<AQPStmt>> filter_children;
      filter_children.push_back(std::move(result_tree));

      auto filter_base = std::make_unique<AQPStmt>(
          std::move(filter_children),
          std::vector<std::unique_ptr<SimplestAttr>>(),
          SimplestNodeType::StmtNode);
      filter_base->qual_vec = std::move(combined_filters);

      result_tree = std::make_unique<SimplestFilter>(std::move(filter_base));
    }

    // Create Projection node on top with target_list
    std::vector<std::unique_ptr<AQPStmt>> proj_children;
    proj_children.push_back(std::move(result_tree));

    auto proj_base = std::make_unique<AQPStmt>(std::move(proj_children),
                                                    std::move(target_list),
                                                    SimplestNodeType::StmtNode);
    auto table_index = UINT_MAX - sub_plan_id;
    result_tree =
        std::make_unique<SimplestProjection>(std::move(proj_base), table_index);
    if (!expr_target_list_scratch_.empty())
      result_tree->expr_target_list = std::move(expr_target_list_scratch_);
  } else {
    // No FROM clause - just a projection
#ifndef NDEBUG
    std::cerr << "Warning: No FROM clause - just a projection" << std::endl;
#endif
    std::vector<std::unique_ptr<AQPStmt>> empty_children;
    auto proj_base = std::make_unique<AQPStmt>(std::move(empty_children),
                                                    std::move(target_list),
                                                    SimplestNodeType::StmtNode);
    auto table_index = UINT_MAX - sub_plan_id;
    result_tree =
        std::make_unique<SimplestProjection>(std::move(proj_base), table_index);
  }

  bool has_group = select_node.contains("groupClause") &&
                    !select_node["groupClause"].empty();
  if (!agg_functions.empty() || has_group) {
    std::vector<std::unique_ptr<SimplestAttr>> saved_target_list =
        std::move(result_tree->target_list);
    std::vector<std::unique_ptr<AQPExpr>> saved_expr_target_list =
        std::move(result_tree->expr_target_list);

    std::vector<std::unique_ptr<AQPStmt>> agg_children;
    agg_children.push_back(std::move(result_tree));

    auto agg_base = std::make_unique<AQPStmt>(std::move(agg_children),
                                                   std::move(saved_target_list),
                                                   SimplestNodeType::StmtNode);
    if (!saved_expr_target_list.empty())
      agg_base->expr_target_list = std::move(saved_expr_target_list);

    result_tree = std::make_unique<SimplestAggregate>(std::move(agg_base),
                                                      std::move(agg_functions));
    if (!agg_fn_exprs_scratch_.empty()) {
      auto *agg_ptr = static_cast<SimplestAggregate *>(result_tree.get());
      agg_ptr->agg_fn_exprs = std::move(agg_fn_exprs_scratch_);
    }

    if (has_group) {
      auto *agg = static_cast<SimplestAggregate *>(result_tree.get());
      for (const auto &gc : select_node["groupClause"]) {
        if (gc.contains("ColumnRef")) {
          agg->groups.push_back(ConvertColumnRef(gc["ColumnRef"]));
          agg->group_exprs.push_back(nullptr);
        } else if (gc.contains("FuncCall") || gc.contains("A_Expr") ||
                   gc.contains("TypeCast")) {
          agg->groups.push_back(std::make_unique<SimplestAttr>(
              SimplestVarType::StringVar, 0, 0, "grp_expr"));
          agg->group_exprs.push_back(ConvertExpr(gc));
        } else if (gc.contains("GroupingSet")) {
          auto &gs = gc["GroupingSet"];
          if (gs.contains("content")) {
            for (const auto &item : gs["content"]) {
              if (item.contains("ColumnRef"))
                agg->groups.push_back(ConvertColumnRef(item["ColumnRef"]));
            }
          }
          std::string kind = gs.contains("kind") && gs["kind"].is_string()
                                 ? gs["kind"].get<std::string>()
                                 : "";
          if (kind == "GROUPING_SET_ROLLUP") {
            size_t n = agg->groups.size();
            for (size_t i = n;; --i) {
              std::set<idx_t> s;
              for (size_t j = 0; j < i; j++)
                s.insert(j);
              agg->grouping_sets.push_back(std::move(s));
              if (i == 0)
                break;
            }
          }
        }
      }
    }
  }

  if (select_node.contains("sortClause") &&
      !select_node["sortClause"].empty()) {
    std::vector<OrderStruct> orders;
    for (const auto &sc : select_node["sortClause"]) {
      if (!sc.contains("SortBy"))
        continue;
      auto &sb = sc["SortBy"];
      SimplestOrderType ot = SimplestOrderType::ORDER_DEFAULT;
      if (sb.contains("sortby_dir") && sb["sortby_dir"].is_string()) {
        std::string dir = sb["sortby_dir"].get<std::string>();
        if (dir == "SORTBY_ASC")
          ot = SimplestOrderType::Ascending;
        else if (dir == "SORTBY_DESC")
          ot = SimplestOrderType::Descending;
      }
      std::unique_ptr<SimplestAttr> attr;
      if (sb.contains("node") && sb["node"].contains("ColumnRef")) {
        auto resolved = ConvertColumnRef(sb["node"]["ColumnRef"]);
        unsigned int target_pos = 0;
        for (size_t ti = 0; ti < target_list.size(); ti++) {
          if (target_list[ti]->GetColumnName() ==
              resolved->GetColumnName()) {
            target_pos = static_cast<unsigned int>(ti);
            break;
          }
        }
        attr = std::make_unique<SimplestAttr>(
            resolved->GetType(), resolved->GetTableIndex(), target_pos,
            resolved->GetColumnName());
      } else if (sb.contains("node") && sb["node"].contains("A_Const")) {
        int pos = ConvertAConst(sb["node"]["A_Const"])->GetIntValue() - 1;
        attr = std::make_unique<SimplestAttr>(SimplestVarType::IntVar, 0,
                                              pos, "");
      } else {
        attr = std::make_unique<SimplestAttr>(SimplestVarType::IntVar, 0,
                                              0, "");
      }
      orders.push_back({ot, std::move(attr)});
    }
    if (!orders.empty()) {
      std::vector<std::unique_ptr<AQPStmt>> order_children;
      order_children.push_back(std::move(result_tree));
      auto order_base = std::make_unique<AQPStmt>(
          std::move(order_children), SimplestNodeType::StmtNode);
      result_tree = std::make_unique<SimplestOrderBy>(
          std::move(order_base), std::move(orders));
    }
  }

  if (select_node.contains("limitCount")) {
    LimitVal lv{SimplestLimitType::CONSTANT_VALUE, 0};
    if (select_node["limitCount"].contains("A_Const")) {
      auto lc = ConvertAConst(select_node["limitCount"]["A_Const"]);
      lv.val = static_cast<idx_t>(lc->GetIntValue());
    }
    LimitVal ov{SimplestLimitType::UNSET, 0};
    if (select_node.contains("limitOffset") &&
        select_node["limitOffset"].contains("A_Const")) {
      auto lo = ConvertAConst(select_node["limitOffset"]["A_Const"]);
      ov = {SimplestLimitType::CONSTANT_VALUE,
            static_cast<idx_t>(lo->GetIntValue())};
    }
    std::vector<std::unique_ptr<AQPStmt>> limit_children;
    limit_children.push_back(std::move(result_tree));
    auto limit_base = std::make_unique<AQPStmt>(
        std::move(limit_children), SimplestNodeType::StmtNode);
    result_tree =
        std::make_unique<SimplestLimit>(std::move(limit_base), lv, ov);
  }

  return result_tree;
}

std::unique_ptr<AQPStmt>
ParseTreeToIR::ConvertFromClause(const json &from_list) {
  if (from_list.empty()) {
    return nullptr;
  }

  // Handle first table/join
  std::unique_ptr<AQPStmt> current_tree = nullptr;

  for (const auto &from_item : from_list) {
    if (from_item.contains("JoinExpr")) {
      current_tree = ConvertJoinExpr(from_item["JoinExpr"]);
    } else if (from_item.contains("RangeVar")) {
      auto scan = ConvertRangeVar(from_item["RangeVar"]);
      if (!current_tree) {
        current_tree = std::move(scan);
      } else {
        // Implicit cross product - create base stmt with both children
        std::vector<std::unique_ptr<AQPStmt>> children;
        children.push_back(std::move(current_tree));
        children.push_back(std::move(scan));

        auto base_stmt = std::make_unique<AQPStmt>(
            std::move(children), SimplestNodeType::StmtNode);

        current_tree =
            std::make_unique<SimplestCrossProduct>(std::move(base_stmt));
      }
    }
  }

  return current_tree;
}

std::unique_ptr<AQPStmt>
ParseTreeToIR::ConvertJoinExpr(const json &join_node) {
  // Convert left and right sides
  auto left_tree = ConvertFromClause(json::array({join_node["larg"]}));
  auto right_tree = ConvertFromClause(json::array({join_node["rarg"]}));

  // Get join type
  SimplestJoinType join_type = SimplestJoinType::Inner;
  if (join_node.contains("jointype")) {
    if (join_node["jointype"].is_string()) {
      std::string jt = join_node["jointype"];
      if (jt == "JOIN_LEFT")
        join_type = SimplestJoinType::Left;
      else if (jt == "JOIN_FULL")
        join_type = SimplestJoinType::Full;
      else if (jt == "JOIN_RIGHT")
        join_type = SimplestJoinType::Right;
      else if (jt == "JOIN_INNER")
        join_type = SimplestJoinType::Inner;
    } else {
      int join_type_val = join_node["jointype"];
      switch (join_type_val) {
      case 0: join_type = SimplestJoinType::Inner; break;
      case 1: join_type = SimplestJoinType::Left; break;
      case 2: join_type = SimplestJoinType::Full; break;
      case 3: join_type = SimplestJoinType::Right; break;
      }
    }
  }

  // Convert join quals (ON clause)
  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;
  if (join_node.contains("quals")) {
    auto qual_exprs = ConvertWhereClause(join_node["quals"]);
    std::function<void(std::unique_ptr<AQPExpr>)> extract =
        [&](std::unique_ptr<AQPExpr> expr) {
          if (!expr) return;
          if (expr->GetNodeType() == SimplestNodeType::VarComparisonNode) {
            join_conditions.emplace_back(
                unique_ptr_cast<AQPExpr, SimplestVarComparison>(
                    std::move(expr)));
          } else if (expr->GetNodeType() == SimplestNodeType::LogicalExprNode) {
            auto &le = static_cast<SimplestLogicalExpr &>(*expr);
            if (le.GetLogicalOp() == SimplestLogicalOp::LogicalAnd) {
              extract(std::move(le.left_expr));
              extract(std::move(le.right_expr));
            }
          }
        };
    for (auto &expr : qual_exprs)
      extract(std::move(expr));
  }

  // Build join node
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.push_back(std::move(left_tree));
  children.push_back(std::move(right_tree));

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(children), std::vector<std::unique_ptr<SimplestAttr>>(),
      SimplestNodeType::StmtNode);

  return std::make_unique<SimplestJoin>(std::move(base_stmt),
                                        std::move(join_conditions), join_type);
}

std::unique_ptr<AQPStmt>
ParseTreeToIR::ConvertRangeVar(const json &range_var) {
  std::string table_name = range_var["relname"];

  // Use alias as the table identifier if present, otherwise use table name
  std::string table_identifier = table_name;
  if (range_var.contains("alias") && range_var["alias"].contains("aliasname")) {
    table_identifier = range_var["alias"]["aliasname"];
    alias_to_table_map[table_identifier] = table_name;
  }

  // Create index based on the identifier (alias or table name)
  unsigned int table_index = GetOrCreateTableIndex(table_identifier);

  std::vector<std::unique_ptr<AQPStmt>> empty_children;
  std::vector<std::unique_ptr<SimplestAttr>> empty_attrs;
  auto base_stmt = std::make_unique<AQPStmt>(std::move(empty_children),
                                                  std::move(empty_attrs),
                                                  SimplestNodeType::StmtNode);

  return std::make_unique<SimplestScan>(std::move(base_stmt), table_index,
                                        table_name);
}

std::vector<std::unique_ptr<AQPExpr>>
ParseTreeToIR::ConvertWhereClause(const json &where_node) {
  std::vector<std::unique_ptr<AQPExpr>> exprs;

  if (where_node.contains("A_Expr")) {
    exprs.push_back(ConvertAExpr(where_node["A_Expr"]));
  } else if (where_node.contains("BoolExpr")) {
    exprs.push_back(ConvertBoolExpr(where_node["BoolExpr"]));
  } else if (where_node.contains("NullTest")) {
    exprs.push_back(ConvertNullTest(where_node["NullTest"]));
  } else if (where_node.contains("TypeCast")) {
    exprs.push_back(ConvertTypeCast(where_node["TypeCast"]));
  } else if (where_node.contains("FuncCall")) {
    exprs.push_back(ConvertFuncCallExpr(where_node["FuncCall"]));
  }

  return exprs;
}

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertAExpr(const json &expr_node) {
  // Check the expression kind
  std::string kind;
  if (expr_node.contains("kind")) {
    kind = expr_node["kind"].is_string() ? expr_node["kind"].get<std::string>()
                                         : "";
  }

  std::unique_ptr<AQPExpr> result = nullptr;

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
  } else if ("AEXPR_BETWEEN" == kind) {
    json left_node = expr_node["lexpr"];
    json right_node = expr_node["rexpr"];

    if (!right_node.contains("List") ||
        right_node["List"]["items"].size() != 2) {
      throw std::runtime_error("BETWEEN requires List with exactly 2 values");
    }
    auto list_items = right_node["List"]["items"];

    bool left_simple = left_node.contains("ColumnRef");
    bool lower_simple = list_items[0].contains("A_Const");
    bool upper_simple = list_items[1].contains("A_Const");

    if (left_simple && lower_simple && upper_simple) {
      auto attr1 = ConvertColumnRef(left_node["ColumnRef"]);
      auto lower_const = ConvertAConst(list_items[0]["A_Const"]);
      auto lower_cmp = std::make_unique<SimplestVarConstComparison>(
          SimplestExprType::GreaterEqual, std::move(attr1),
          std::move(lower_const));

      auto attr2 = ConvertColumnRef(left_node["ColumnRef"]);
      auto upper_const = ConvertAConst(list_items[1]["A_Const"]);
      auto upper_cmp = std::make_unique<SimplestVarConstComparison>(
          SimplestExprType::LessEqual, std::move(attr2),
          std::move(upper_const));

      result = std::make_unique<SimplestLogicalExpr>(
          SimplestLogicalOp::LogicalAnd, std::move(lower_cmp),
          std::move(upper_cmp));
    } else {
      auto left_expr1 = ConvertExpr(left_node);
      auto left_expr2 = ConvertExpr(left_node);
      auto lower_expr = ConvertExpr(list_items[0]);
      auto upper_expr = ConvertExpr(list_items[1]);

      std::unique_ptr<AQPExpr> lower_cmp, upper_cmp;
      if (left_expr1 && lower_expr)
        lower_cmp = std::make_unique<SimplestGeneralComparison>(
            SimplestExprType::GreaterEqual, std::move(left_expr1),
            std::move(lower_expr));
      if (left_expr2 && upper_expr)
        upper_cmp = std::make_unique<SimplestGeneralComparison>(
            SimplestExprType::LessEqual, std::move(left_expr2),
            std::move(upper_expr));

      if (lower_cmp && upper_cmp)
        result = std::make_unique<SimplestLogicalExpr>(
            SimplestLogicalOp::LogicalAnd, std::move(lower_cmp),
            std::move(upper_cmp));
      else if (lower_cmp)
        result = std::move(lower_cmp);
      else if (upper_cmp)
        result = std::move(upper_cmp);
    }
  } else {
    std::string op_name;
    if (expr_node.contains("name") && !expr_node["name"].empty()) {
      op_name = expr_node["name"][0]["String"]["sval"];
    }

    SimplestArithOp arith_op = SimplestArithOp::ArithInvalid;
    if (op_name == "+") arith_op = SimplestArithOp::ArithAdd;
    else if (op_name == "-") arith_op = SimplestArithOp::ArithSub;
    else if (op_name == "*") arith_op = SimplestArithOp::ArithMul;
    else if (op_name == "/") arith_op = SimplestArithOp::ArithDiv;
    else if (op_name == "%") arith_op = SimplestArithOp::ArithMod;

    if (arith_op != SimplestArithOp::ArithInvalid) {
      auto left_expr = ConvertExpr(expr_node["lexpr"]);
      auto right_expr = ConvertExpr(expr_node["rexpr"]);
      if (left_expr && right_expr)
        result = std::make_unique<SimplestArithExpr>(
            arith_op, std::move(left_expr), std::move(right_expr));
      return result;
    }

    SimplestExprType expr_type = ConvertToSimplestExprType(op_name);

    json left_node = expr_node["lexpr"];
    json right_node = expr_node["rexpr"];

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
      auto left_expr = ConvertExpr(left_node);
      auto right_expr = ConvertExpr(right_node);
      if (left_expr && right_expr)
        result = std::make_unique<SimplestGeneralComparison>(
            expr_type, std::move(left_expr), std::move(right_expr));
    }
  }

  return result;
}

std::unique_ptr<AQPExpr>
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

  std::unique_ptr<AQPExpr> result = nullptr;
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

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertNullTest(const json &null_test) {
  auto attr = ConvertColumnRef(null_test["arg"]["ColumnRef"]);

  // Handle both string and integer formats
  SimplestExprType expr_type;
  if (null_test["nulltesttype"].is_string()) {
    std::string nulltesttype_str = null_test["nulltesttype"];
    expr_type = (nulltesttype_str == "IS_NULL") ? SimplestExprType::NullType
                                                : SimplestExprType::NonNullType;
  } else {
    int nulltesttype = null_test["nulltesttype"];
    expr_type = (nulltesttype == 0) ? SimplestExprType::NullType
                                    : SimplestExprType::NonNullType;
  }

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
        expr_target_list_scratch_.push_back(nullptr);
      } else if (target["ResTarget"].contains("val") &&
                 target["ResTarget"]["val"].contains("FuncCall")) {
        auto &fc = target["ResTarget"]["val"]["FuncCall"];
        std::string fn;
        if (fc.contains("funcname") && !fc["funcname"].empty()) {
          fn = fc["funcname"][0]["String"]["sval"];
          for (auto &c : fn) c = std::toupper(c);
        }
        if (GetAggFnType(fn) == SimplestAggFnType::InvalidAggType) {
          auto expr = ConvertResTargetExpr(target["ResTarget"]);
          if (expr) {
            attrs.push_back(std::make_unique<SimplestAttr>(
                SimplestVarType::StringVar, 0, 0, "expr"));
            expr_target_list_scratch_.push_back(std::move(expr));
          }
        }
      } else {
        auto expr = ConvertResTargetExpr(target["ResTarget"]);
        if (expr) {
          attrs.push_back(std::make_unique<SimplestAttr>(
              SimplestVarType::StringVar, 0, 0, "expr"));
          expr_target_list_scratch_.push_back(std::move(expr));
        }
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
          func_call.contains("agg_star") && func_call["agg_star"] == true) {
        if (!table_names.empty()) {
          std::string tname = table_names[0];
          unsigned int tidx = table_index_map[tname];
          auto attr_star = std::make_unique<SimplestAttr>(
              SimplestVarType::IntVar, tidx, 0, "*");
          agg_functions.emplace_back(std::move(attr_star),
                                     SimplestAggFnType::CountStar);
          agg_fn_exprs_scratch_.push_back(nullptr);
        }
      } else if (agg_type != SimplestAggFnType::InvalidAggType &&
                 func_call.contains("args") && !func_call["args"].empty()) {
        json first_arg = func_call["args"][0];
        if (first_arg.contains("ColumnRef")) {
          result = ConvertColumnRef(first_arg["ColumnRef"]);
          auto attr_copy = std::make_unique<SimplestAttr>(*result);
          agg_functions.emplace_back(std::move(attr_copy), agg_type);
          agg_fn_exprs_scratch_.push_back(nullptr);
        } else if (first_arg.contains("TypeCast")) {
          json cast_arg = first_arg["TypeCast"]["arg"];
          if (cast_arg.contains("ColumnRef")) {
            result = ConvertColumnRef(cast_arg["ColumnRef"]);
            auto attr_copy = std::make_unique<SimplestAttr>(*result);
            agg_functions.emplace_back(std::move(attr_copy), agg_type);
            agg_fn_exprs_scratch_.push_back(nullptr);
          }
        } else {
          auto arg_expr = ConvertExpr(first_arg);
          if (arg_expr) {
            agg_functions.emplace_back(nullptr, agg_type);
            agg_fn_exprs_scratch_.push_back(std::move(arg_expr));
          }
        }
      }
    }
  }
  return result;
}

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertResTargetExpr(const json &res_target) {
  if (!res_target.contains("val"))
    return nullptr;
  return ConvertExpr(res_target["val"]);
}

std::unique_ptr<SimplestAttr>
ParseTreeToIR::ConvertColumnRef(const json &col_ref) {
  json fields = col_ref["fields"];

  std::string table_identifier;  // The key to look up in table_index_map
  std::string actual_table_name; // The actual table name for schema lookup
  std::string column_name;

  if (fields.size() == 1) {
    column_name = fields[0]["String"]["sval"];
    bool found = false;
    for (const auto &tname : table_names) {
      std::string resolved = ResolveTableName(tname);
      if (schema_parser_ &&
          schema_parser_->GetColumnIndex(resolved, column_name) >= 0) {
        table_identifier = tname;
        actual_table_name = resolved;
        found = true;
        break;
      } else if (column_index_lookup_ &&
                 column_index_lookup_(resolved, column_name) >= 0) {
        table_identifier = tname;
        actual_table_name = resolved;
        found = true;
        break;
      }
    }
    if (!found && !table_names.empty()) {
      table_identifier = table_names[0];
      actual_table_name = ResolveTableName(table_identifier);
    }
  } else if (fields.size() == 2) {
    // Table.column or Alias.column (e.g., "t1.id" or "t.id")
    table_identifier = fields[0]["String"]["sval"];
    column_name = fields[1]["String"]["sval"];
    // Resolve alias to actual table name for schema lookup
    actual_table_name = ResolveTableName(table_identifier);
  }

  // Look up index using the identifier (alias or table name), NOT the resolved
  // name
  unsigned int table_index = table_index_map[table_identifier];
  SimplestVarType var_type =
      GetVarTypeFromColumn(actual_table_name, column_name);

  // Look up column index from schema
  unsigned int column_index = 0;
  if (schema_parser_) {
    int idx = schema_parser_->GetColumnIndex(actual_table_name, column_name);
    if (idx >= 0) {
      column_index = static_cast<unsigned int>(idx);
    }
  } else if (column_index_lookup_) {
    int idx = column_index_lookup_(actual_table_name, column_name);
    if (idx >= 0) {
      column_index = static_cast<unsigned int>(idx);
    }
  }

  return std::make_unique<SimplestAttr>(var_type, table_index, column_index,
                                        column_name);
}

std::unique_ptr<SimplestConstVar>
ParseTreeToIR::ConvertAConst(const json &a_const) {
  if (a_const.contains("ival")) {
    int val = 0;
    if (a_const["ival"].contains("ival"))
      val = a_const["ival"]["ival"].get<int>();
    return std::make_unique<SimplestConstVar>(val);
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

  return std::make_unique<SimplestConstVar>(std::string("NULL"));
}

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertExpr(const json &node) {
  if (node.contains("A_Const")) {
    auto cv = ConvertAConst(node["A_Const"]);
    return std::make_unique<SimplestConstExpr>(std::move(cv));
  }
  if (node.contains("ColumnRef")) {
    auto attr = ConvertColumnRef(node["ColumnRef"]);
    return std::make_unique<SimplestSingleAttrExpr>(std::move(attr));
  }
  if (node.contains("A_Expr")) {
    return ConvertAExpr(node["A_Expr"]);
  }
  if (node.contains("TypeCast")) {
    return ConvertTypeCast(node["TypeCast"]);
  }
  if (node.contains("FuncCall")) {
    return ConvertFuncCallExpr(node["FuncCall"]);
  }
  if (node.contains("BoolExpr")) {
    return ConvertBoolExpr(node["BoolExpr"]);
  }
  if (node.contains("NullTest")) {
    return ConvertNullTest(node["NullTest"]);
  }
  if (node.contains("CaseExpr")) {
    auto &ce = node["CaseExpr"];
    std::vector<CaseWhenClause> checks;
    if (ce.contains("args")) {
      for (const auto &arg : ce["args"]) {
        if (!arg.contains("CaseWhen"))
          continue;
        auto &cw = arg["CaseWhen"];
        CaseWhenClause clause;
        if (cw.contains("expr"))
          clause.when_expr = ConvertExpr(cw["expr"]);
        if (cw.contains("result"))
          clause.then_expr = ConvertExpr(cw["result"]);
        checks.push_back(std::move(clause));
      }
    }
    std::unique_ptr<AQPExpr> else_expr;
    if (ce.contains("defresult"))
      else_expr = ConvertExpr(ce["defresult"]);
    return std::make_unique<SimplestCaseExpr>(std::move(checks),
                                              std::move(else_expr));
  }
  if (node.contains("CoalesceExpr")) {
    auto &ce = node["CoalesceExpr"];
    std::vector<std::unique_ptr<AQPExpr>> args;
    if (ce.contains("args")) {
      for (const auto &arg : ce["args"])
        args.push_back(ConvertExpr(arg));
    }
    return std::make_unique<SimplestFunctionExpr>("coalesce", std::move(args));
  }
  return nullptr;
}

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertTypeCast(const json &typecast_node) {
  auto child = ConvertExpr(typecast_node["arg"]);
  if (!child)
    return nullptr;

  SimplestVarType target_type = SimplestVarType::StringVar;
  if (typecast_node.contains("typeName") &&
      typecast_node["typeName"].contains("names") &&
      !typecast_node["typeName"]["names"].empty()) {
    auto &names = typecast_node["typeName"]["names"];
    std::string type_name = names.back()["String"]["sval"];
    if (type_name == "date")
      target_type = SimplestVarType::Date;
    else if (type_name == "interval")
      target_type = SimplestVarType::IntervalVar;
    else if (type_name == "timestamp")
      target_type = SimplestVarType::TimestampVar;
    else if (type_name == "int4" || type_name == "int8" ||
             type_name == "int2" || type_name == "integer")
      target_type = SimplestVarType::IntVar;
    else if (type_name == "float8" || type_name == "float4" ||
             type_name == "numeric" || type_name == "decimal")
      target_type = SimplestVarType::FloatVar;
    else if (type_name == "bool")
      target_type = SimplestVarType::BoolVar;
  }
  return std::make_unique<SimplestCastExpr>(std::move(child), target_type);
}

std::unique_ptr<AQPExpr>
ParseTreeToIR::ConvertFuncCallExpr(const json &func_call) {
  std::string func_name;
  if (func_call.contains("funcname") && !func_call["funcname"].empty()) {
    func_name = func_call["funcname"].back()["String"]["sval"];
  }

  std::vector<std::unique_ptr<AQPExpr>> args;
  if (func_call.contains("args")) {
    for (const auto &arg : func_call["args"]) {
      auto expr = ConvertExpr(arg);
      if (expr)
        args.push_back(std::move(expr));
    }
  }
  return std::make_unique<SimplestFunctionExpr>(std::move(func_name),
                                                std::move(args));
}

SimplestVarType
ParseTreeToIR::GetVarTypeFromColumn(const std::string &table_name,
                                    const std::string &column_name) {
  if (!schema_parser_)
    return SimplestVarType::StringVar;
  const auto *schema = schema_parser_->GetTableSchema(table_name);
  if (!schema)
    return SimplestVarType::StringVar;
  std::string col_lower = column_name;
  std::transform(col_lower.begin(), col_lower.end(), col_lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  auto it = schema->column_name_to_index.find(col_lower);
  if (it == schema->column_name_to_index.end())
    return SimplestVarType::StringVar;
  const std::string &type_str = schema->columns[it->second].type;
  std::string upper = type_str;
  std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
  auto paren = upper.find('(');
  if (paren != std::string::npos)
    upper = upper.substr(0, paren);
  while (!upper.empty() && upper.back() == ' ')
    upper.pop_back();
  if (upper == "INTEGER" || upper == "INT" || upper == "BIGINT" ||
      upper == "SMALLINT" || upper == "TINYINT" || upper == "INT4" ||
      upper == "INT8" || upper == "INT2" || upper == "SERIAL" ||
      upper == "DATE")
    return SimplestVarType::IntVar;
  if (upper == "FLOAT" || upper == "DOUBLE" || upper == "REAL" ||
      upper == "DOUBLE PRECISION" || upper == "NUMERIC" || upper == "DECIMAL")
    return SimplestVarType::FloatVar;
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
  if (op_name == "!~~" || op_name == "NOT LIKE")
    return SimplestExprType::Text_Not_Like;
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
  if (func_name == "COUNT")
    return SimplestAggFnType::Count;
  if (func_name == "STDDEV_SAMP")
    return SimplestAggFnType::StddevSamp;
  return SimplestAggFnType::InvalidAggType;
}

void ParseTreeToIR::ExtractJoinAndFilterConditions(
    std::unique_ptr<AQPExpr> expr,
    std::vector<std::unique_ptr<SimplestVarComparison>> &join_conditions,
    std::vector<std::unique_ptr<AQPExpr>> &filter_conditions) {

  if (!expr) {
    return;
  }

  SimplestNodeType node_type = expr->GetNodeType();

  if (node_type == SimplestNodeType::VarComparisonNode) {
    // This is a join condition (column = column)
    auto var_comp_expr =
        unique_ptr_cast<AQPExpr, SimplestVarComparison>(std::move(expr));
    join_conditions.emplace_back(std::move(var_comp_expr));
  } else if (node_type == SimplestNodeType::LogicalExprNode) {
    // Recursively process logical expressions (AND/OR)
    auto logical_expr =
        unique_ptr_cast<AQPExpr, SimplestLogicalExpr>(std::move(expr));

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
          unique_ptr_cast<SimplestLogicalExpr, AQPExpr>(
              std::move(logical_expr)));
    }
  } else {
    // This is a filter condition (VarConstComparison, IsNull, etc.)
    filter_conditions.push_back(std::move(expr));
  }
}
} // namespace ir_sql_converter