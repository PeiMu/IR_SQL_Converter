#include "ir_to_sql.h"

namespace ir_sql_converter {
std::string IRToSQLConverter::ConvertSimplestIRToSQL(SimplestStmt &plan) {
  std::string sql_code;
#ifdef DEBUG
  assert(nullptr != plan);
#endif

  GenerateSQL(plan);

  //	sql_code = "PRAGMA disable_dbshaker_query_split;\n";
  //	sql_code += "PRAGMA disable_convert_duckdb_to_ir;\n";
  //	sql_code += "PRAGMA disable_convert_ir_to_sql;\n";

  sql_code += "SELECT ";
  for (const auto &select : select_field) {
    sql_code += select;
    sql_code += ", ";
  }
  if (!select_field.empty())
    sql_code.erase(sql_code.size() - 2);

  sql_code += "\nFROM ";
  for (const auto &table_name : table_names) {
    auto table_name_idx =
        table_name.second + "_" + std::to_string(table_name.first);
    auto alias = table_name.second + " AS " + table_name_idx + ", ";
    sql_code += alias;
  }
  if (!table_names.empty())
    sql_code.erase(sql_code.size() - 2);

  sql_code += "\nWHERE ";
  for (const auto &filter : filter_field) {
    sql_code += filter;
    sql_code += " AND ";
  }
  if (!filter_field.empty() && join_field.empty())
    sql_code.erase(sql_code.size() - 5);

  for (const auto &join : join_field) {
    sql_code += join;
    sql_code += " AND ";
  }
  if (!join_field.empty())
    sql_code.erase(sql_code.size() - 5);

  if (!group_by_field.empty()) {
    sql_code += "\nGROUP BY\n";
    for (const auto &group : group_by_field) {
      sql_code += group;
      sql_code += ",\n";
    }
    sql_code.erase(sql_code.size() - 2);
  }

  if (!order_by_field.empty()) {
    sql_code += "\nORDER BY\n";
    for (const auto &order : order_by_field) {
      sql_code += order;
      sql_code += ",\n";
    }
    sql_code.erase(sql_code.size() - 2);
  }

  if (!limit_field.empty()) {
    sql_code += "\n";
    sql_code += limit_field;
  }

  sql_code += ";";
#ifdef DEBUG
  std::cout << "current SQL code is:\n" + sql_code << std::endl;
#endif
  return sql_code;
}

void IRToSQLConverter::GenerateSQL(SimplestStmt &op) {
  std::string sql_code;
  if (!op.children.empty()) {
    GenerateSQL(*op.children[0]);
    if (op.children.size() == 2)
      GenerateSQL(*op.children[1]);
  }

  switch (op.GetNodeType()) {
  case SimplestNodeType::ProjectionNode: {
    auto &proj_op = op.Cast<SimplestProjection>();
#ifdef DEBUG
    assert(!proj_op.target_list.empty());
#endif
    // Generate unique alias for this column
    std::string alias_name;
    std::string orig_col_name;

    // `SELECT table_name.$target_list`
    for (size_t idx = 0; idx < proj_op.target_list.size(); idx++) {
      auto &target = proj_op.target_list[idx];
      auto target_table_index = target->GetTableIndex();
      // for DuckDB with agg
      if (table_names.find(target_table_index) == table_names.end()) {
        size_t agg_fn_index = idx;
        auto &child_op = proj_op.children[0];
        if (SimplestNodeType::AggregateNode == child_op->GetNodeType()) {
          auto &agg_op = child_op->Cast<SimplestAggregate>();
          if (target_table_index == agg_op.GetAggIndex()) {
            if (agg_op.agg_fns.size() - 1 < agg_fn_index) {
              // fixme: might have a bug here.
              //  This hack for JOB 17a/b/c.sql, where proj [9.0] and [9.0].
              //  but only one agg_fn
              agg_fn_index = 0;
            }
            std::string agg_fn_type =
                TranslateSimplestAggFnType(agg_op.agg_fns[agg_fn_index].second);
            unsigned int table_idx =
                agg_op.agg_fns[agg_fn_index].first->GetTableIndex();
            proj_table_to_real_table.emplace(
                std::make_pair(target->GetTableIndex(),
                               target->GetColumnIndex()),
                table_idx);
            auto table_name =
                table_names[table_idx] + "_" + std::to_string(table_idx);
            orig_col_name = agg_op.agg_fns[agg_fn_index].first->GetColumnName();
            unsigned int col_idx =
                agg_op.agg_fns[agg_fn_index].first->GetColumnIndex();
            std::string actual_col_name =
                GetActualColumnName(table_idx, col_idx, orig_col_name);
            std::string select_str = table_name + "." + actual_col_name;
            select_str = agg_fn_type + "(" + select_str + ")";
            // Generate unique alias: table_name + "_" + column_name
            alias_name = table_names[table_idx] + "_" + std::to_string(table_idx) + "_" + orig_col_name;
            select_str += " AS " + alias_name;
            select_field.emplace_back(select_str);
          } else if (target_table_index == agg_op.GetGroupIndex()) {
            unsigned int col_idx = target->GetColumnIndex();
            unsigned int table_idx = group_by_vec[col_idx]->GetTableIndex();
            proj_table_to_real_table.emplace(
                std::make_pair(target->GetTableIndex(),
                               target->GetColumnIndex()),
                table_idx);
            auto table_name =
                table_names[table_idx] + "_" + std::to_string(table_idx);
            orig_col_name = group_by_vec[col_idx]->GetColumnName();
            std::string actual_col_name =
                GetActualColumnName(table_idx, col_idx, orig_col_name);
            std::string select_str = table_name + "." + actual_col_name;
            // Generate unique alias: table_name + "_" + column_name
            alias_name = table_names[table_idx] + "_" + std::to_string(table_idx) + "_" + orig_col_name;
            select_str += " AS " + alias_name;
            group_by_field.emplace_back(select_str);
          } else {
            // todo
            std::cout << "TODO!" << std::endl;
            assert(false);
          }
        } else {
          std::cout << "Do not support yet, child_op node type: " +
                           std::to_string(child_op->GetNodeType())
                    << std::endl;
          assert(false);
        }
      } else {
        // for the others
        auto table_name = table_names[target_table_index] + "_" +
                          std::to_string(target_table_index);
        orig_col_name = target->GetColumnName();
        unsigned int col_idx = target->GetColumnIndex();
        unsigned int table_idx = target->GetTableIndex();
        std::string actual_col_name =
            GetActualColumnName(table_idx, col_idx, orig_col_name);
        std::string select_str = table_name + "." + actual_col_name;
        auto find_select_str = agg_field.find(
            agg_field_key(target_table_index, target->GetColumnIndex()));
        if (find_select_str != agg_field.end()) {
          select_str = find_select_str->second + "(" + select_str + ")";
        }
        if (!group_by_vec.empty()) {
          // todo
          std::cout << "TODO!" << std::endl;
          assert(false);
        }
        // Generate unique alias: table_name + "_" + column_name
        alias_name = table_names[target_table_index] + "_" + std::to_string(target_table_index) + "_" + orig_col_name;
        select_str += " AS " + alias_name;
        select_field.emplace_back(select_str);
      }
    }
    break;
  }
  case SimplestNodeType::AggregateNode: {
    auto &agg_op = op.Cast<SimplestAggregate>();
    for (const auto &agg_fn : agg_op.agg_fns) {
      agg_field.emplace(agg_field_key(agg_fn.first->GetTableIndex(),
                                      agg_fn.first->GetColumnIndex()),
                        TranslateSimplestAggFnType(agg_fn.second));
    }
    if (!agg_op.groups.empty()) {
      group_by_vec = std::move(agg_op.groups);
    }

    // Process target_list if present (for SELECT clause with aggregates)
    if (!agg_op.target_list.empty()) {
      for (size_t idx = 0; idx < agg_op.target_list.size(); idx++) {
        auto &target = agg_op.target_list[idx];
        auto target_table_index = target->GetTableIndex();

        if (table_names.find(target_table_index) == table_names.end()) {
          continue;
        }

        std::string table_name = table_names[target_table_index];
        std::string actual_col_name = target->GetColumnName();
        unsigned int col_index = target->GetColumnIndex();

        std::string select_str = table_name + "_" + std::to_string(target_table_index) + "." + actual_col_name;

        // Check if this column has an aggregate function
        auto agg_key = agg_field_key(target_table_index, col_index);
        auto agg_it = agg_field.find(agg_key);
        if (agg_it != agg_field.end()) {
          // Wrap with aggregate function
          select_str = agg_it->second + "(" + select_str + ")";
        }

        select_field.emplace_back(select_str);
      }
    }

    break;
  }
  case SimplestNodeType::OrderNode: {
    auto &order_by_op = op.Cast<SimplestOrderBy>();
    auto &child_op = order_by_op.children[0];
    idx_t proj_id;
    std::vector<std::pair<idx_t, idx_t>> proj_target_list_ids;
    if (SimplestNodeType::ProjectionNode == child_op->GetNodeType()) {
      auto &proj_op = child_op->Cast<SimplestProjection>();
      proj_id = proj_op.GetIndex();
      for (const auto &target : proj_op.target_list) {
        proj_target_list_ids.emplace_back(target->GetTableIndex(),
                                          target->GetColumnIndex());
      }
    } else {
      std::cout << "Do not support yet, child_op node type: " +
                       std::to_string(child_op->GetNodeType())
                << std::endl;
      assert(false);
    }
    for (const auto &order : order_by_op.orders) {
      std::string order_by_str = order.attr->GetColumnName();
      auto table_id = order.attr->GetTableIndex();
#ifdef DEBUG
      assert(table_id == proj_id);
#endif
      auto column_id = order.attr->GetColumnIndex();
      auto real_table_column_pair = proj_target_list_ids[column_id];
      auto find = proj_table_to_real_table.find(real_table_column_pair);
#ifdef DEBUG
      assert(find != proj_table_to_real_table.end());
#endif
      auto real_table_idx = find->second;
      size_t pos = order_by_str.find('.');
#ifdef DEBUG
      assert(pos != order_by_str.npos);
#endif
      std::string table_id_str = "_" + std::to_string(real_table_idx);
      order_by_str.insert(pos, table_id_str);
      switch (order.order_type) {
      case SimplestOrderType::INVALID:
        std::cout << "Invalid order Type!!!" << std::endl;
        assert(false);
        exit(-1);
      case SimplestOrderType::ORDER_DEFAULT:
        break;
      case SimplestOrderType::Ascending:
        order_by_str += " ASC";
        break;
      case SimplestOrderType::Descending:
        order_by_str += " DESC";
        break;
      }
      order_by_field.emplace_back(order_by_str);
    }
    break;
  }
  case SimplestNodeType::LimitNode: {
    auto &limit_op = op.Cast<SimplestLimit>();
    auto limit_val = limit_op.limit_val;
    auto offset_val = limit_op.offset_val;
    switch (limit_val.type) {
    case SimplestLimitType::UNSET:
      std::cout << "Unset limit node type!" << std::endl;
      assert(false);
      exit(-1);
    case SimplestLimitType::CONSTANT_VALUE: {
      limit_field += "limit ";
      limit_field += std::to_string(limit_val.val);
      break;
    }
    default:
      std::cout << "Unsupport limit type!!!" << std::endl;
      assert(false);
    }
    if (SimplestLimitType::UNSET != offset_val.type) {
      switch (offset_val.type) {
      case SimplestLimitType::CONSTANT_VALUE: {
        limit_field += "offset ";
        limit_field += std::to_string(offset_val.val);
        break;
      }
      default:
        std::cout << "Unsupport limit type!!!" << std::endl;
        assert(false);
      }
    }

    break;
  }
  case SimplestNodeType::FilterNode: {
    auto &filter_op = op.Cast<SimplestFilter>();
#ifdef DEBUG
    assert(!filter_op.qual_vec.empty());
#endif

    // Process target_list if present (for SELECT clause)
    if (!filter_op.target_list.empty()) {
      for (size_t idx = 0; idx < filter_op.target_list.size(); idx++) {
        auto &target = filter_op.target_list[idx];
        auto target_table_index = target->GetTableIndex();

        if (table_names.find(target_table_index) == table_names.end()) {
          continue;
        }

        std::string table_name = table_names[target_table_index];
        std::string actual_col_name = target->GetColumnName();

        std::string select_str = table_name + "_" + std::to_string(target_table_index) + "." + actual_col_name;
        select_field.emplace_back(select_str);
      }
    }

    // `WHERE `
    for (const auto &qual : filter_op.qual_vec) {
      if (SingleAttr == qual->GetSimplestExprType()) {
        // the `filter_field` should be collected in the child MARK/SEMI join node
#ifdef DEBUG
        assert(SimplestNodeType::JoinNode ==
               filter_op.children[0]->GetNodeType());
        auto &join_child_op = filter_op.children[0]->Cast<SimplestJoin>();
        assert(SimplestJoinType::Mark == join_child_op.GetSimplestJoinType() ||
               SimplestJoinType::Semi == join_child_op.GetSimplestJoinType());
#endif
      } else {
        std::string filter_str = CollectFilter(qual);
        filter_field.emplace_back(filter_str);
      }
    }
    break;
  }
  case SimplestNodeType::JoinNode: {
    auto &join_op = op.Cast<SimplestJoin>();
    auto &left_child = join_op.children[0];
    auto &right_child = join_op.children[1];
    auto &conditions = join_op.join_conditions;
    auto join_type = join_op.GetSimplestJoinType();
    switch (join_type) {
    case Inner: {
      for (const auto &cond : conditions) {
        auto &var_comp = cond->Cast<SimplestVarComparison>();
        auto &left_var_attr = var_comp.left_attr;
        auto left_table_name = table_names[left_var_attr->GetTableIndex()] +
                               "_" +
                               std::to_string(left_var_attr->GetTableIndex());
        std::string left_orig_col = left_var_attr->GetColumnName();
        unsigned int left_col_idx = left_var_attr->GetColumnIndex();
        unsigned int left_table_idx = left_var_attr->GetTableIndex();
        std::string left_actual_col =
            GetActualColumnName(left_table_idx, left_col_idx, left_orig_col);
        auto join_str = left_table_name + "." + left_actual_col;
        join_str += " = ";
        auto &right_var_attr = var_comp.right_attr;
        auto right_table_name = table_names[right_var_attr->GetTableIndex()] +
                                "_" +
                                std::to_string(right_var_attr->GetTableIndex());
        std::string right_orig_col = right_var_attr->GetColumnName();
        unsigned int right_col_idx = right_var_attr->GetColumnIndex();
        unsigned int right_table_idx = right_var_attr->GetTableIndex();
        std::string right_actual_col =
            GetActualColumnName(right_table_idx, right_col_idx, right_orig_col);
        join_str += right_table_name + "." + right_actual_col;
        join_field.emplace_back(join_str);
      }
      break;
    }
    case Mark:
    case Semi: {
      // todo: check if it is always be a `IN` claude
      for (const auto &cond : conditions) {
        auto &var_comp = cond->Cast<SimplestVarComparison>();
        auto &left_var_attr = var_comp.left_attr;
        auto table_name = table_names[left_var_attr->GetTableIndex()] + "_" +
                          std::to_string(left_var_attr->GetTableIndex());
        std::string orig_col = left_var_attr->GetColumnName();
        unsigned int col_idx = left_var_attr->GetColumnIndex();
        unsigned int table_idx = left_var_attr->GetColumnIndex();
        std::string actual_col =
            GetActualColumnName(table_idx, col_idx, orig_col);
        auto filter_str = table_name + "." + actual_col;

        auto &right_var_attr = var_comp.right_attr;
        auto chunk_contents_str =
            chunk_contents[right_var_attr->GetTableIndex()];
        if (chunk_contents_str.size() > 1) {
          filter_str += " IN ";
          filter_str += "(";
          for (const auto &content : chunk_contents_str) {
            std::string content_str = "'" + content + "', ";
            filter_str += content_str;
          }
          filter_str.erase(filter_str.size() - 2);
          filter_str += ")";
        } else {
          filter_str += " = '";
          filter_str += chunk_contents_str[0];
          filter_str += "'";
        }

        filter_field.emplace_back(filter_str);
      }
      break;
    }
    default:
      std::cout << "Do not support yet, join_type: " + std::to_string(join_type)
                << std::endl;
      assert(false);
    }
    break;
  }
  case SimplestNodeType::ScanNode: {
    auto &scan_op = op.Cast<SimplestScan>();
    table_names.emplace(scan_op.GetTableIndex(), scan_op.GetTableName());
    std::string filter_str;
    for (const auto &qual : scan_op.qual_vec) {
      filter_str = CollectFilter(qual);
      filter_field.emplace_back(filter_str);
    }
    break;
  }
  case SimplestNodeType::ChunkNode: {
    auto &chunk_op = op.Cast<SimplestChunk>();
    chunk_contents[chunk_op.GetTableIndex()] = chunk_op.GetContents();
    break;
  }
  case SimplestNodeType::HashNode:
  case SimplestNodeType::CrossProductNode:
  // SortNode in PostgreSQL is a physical node, and no information for
  // generating SQL fixme: need to check if PostgreSQL's order by use Sort or
  // Order
  case SimplestNodeType::SortNode:
    break;
  default:
    std::cout << "Do not support yet, op->type: " +
                     std::to_string(op.GetNodeType())
              << std::endl;
    assert(false);
  }
}

std::string
IRToSQLConverter::TranslateSimplestAggFnType(SimplestAggFnType agg_fn_type) {
  std::string agg_fn_type_str;
  switch (agg_fn_type) {
  case SimplestAggFnType::InvalidAggType:
    std::cout << "Invalid expression type!" << std::endl;
    assert(false);
    break;
  case SimplestAggFnType::Min:
    agg_fn_type_str = "min";
    break;
  case SimplestAggFnType::Max:
    agg_fn_type_str = "max";
    break;
  case SimplestAggFnType::Sum:
    agg_fn_type_str = "sum";
    break;
  case SimplestAggFnType::Average:
    agg_fn_type_str = "avg";
    break;
  }

  return agg_fn_type_str;
}

std::string IRToSQLConverter::CollectFilter(
    const std::unique_ptr<SimplestExpr> &qual_expr) {
  std::string ret_str;
  switch (qual_expr->GetNodeType()) {
  case SimplestNodeType::VarConstComparisonNode: {
    auto &var_const_comp = qual_expr->Cast<SimplestVarConstComparison>();
    auto &var_attr = var_const_comp.attr;
    auto table_name = table_names[var_attr->GetTableIndex()] + "_" +
                      std::to_string(var_attr->GetTableIndex());
    std::string orig_col_name = var_attr->GetColumnName();
    unsigned int col_idx = var_attr->GetColumnIndex();
    unsigned int table_idx = var_attr->GetTableIndex();
    std::string actual_col_name =
        GetActualColumnName(table_idx, col_idx, orig_col_name);
    ret_str = table_name + "." + actual_col_name;
    auto &const_attr = var_const_comp.const_var;

    switch (var_const_comp.GetSimplestExprType()) {
    case SimplestExprType::Equal:
      ret_str += SimplestVarType::StringVarArr == const_attr->GetType() ? " IN "
                                                                        : " = ";
      break;
    case SimplestExprType::NotEqual:
      ret_str += " != ";
      break;
    case SimplestExprType::LessThan:
      ret_str += " < ";
      break;
    case SimplestExprType::LessEqual:
      ret_str += " <= ";
      break;
    case SimplestExprType::GreaterThan:
      ret_str += " > ";
      break;
    case SimplestExprType::GreaterEqual:
      ret_str += " >= ";
      break;
    case SimplestExprType::TextLike:
      ret_str += " LIKE ";
      break;
    case SimplestExprType::Text_Not_Like:
      ret_str += " NOT LIKE ";
      break;
    default:
      std::cout << "Do not support yet, var_const_comp->type: " +
                       std::to_string(var_const_comp.GetSimplestExprType())
                << std::endl;
      assert(false);
    }

    std::string const_attr_str;
    switch (const_attr->GetType()) {
    // InvalidVarType = 0, BoolVar, IntVar, FloatVar, StringVar, StringVarArr
    case SimplestVarType::InvalidVarType:
      std::cout << "Invalid const_attr type!" << std::endl;
      assert(false);
      break;
    case SimplestVarType::BoolVar:
      const_attr_str = std::to_string(const_attr->GetBoolValue());
      break;
    case SimplestVarType::IntVar:
      const_attr_str = std::to_string(const_attr->GetIntValue());
      break;
    case SimplestVarType::FloatVar:
      const_attr_str = std::to_string(const_attr->GetFloatValue());
      break;
    case SimplestVarType::StringVar:
      const_attr_str = "'";
      const_attr_str += const_attr->GetStringValue();
      const_attr_str += "'";
      break;
    case SimplestVarType::StringVarArr: {
      const_attr_str = "(";
      auto string_var_arr = const_attr->GetStringVecValue();
      for (const auto &str_var : string_var_arr) {
        const_attr_str += "'";
        const_attr_str += str_var;
        const_attr_str += "', ";
      }
      const_attr_str.erase(const_attr_str.size() - 2);
      const_attr_str += ")";
      break;
    }
    case SimplestVarType::Date:
      const_attr_str = std::to_string(const_attr->GetIntValue());
      break;
    }
    ret_str += const_attr_str;
    return ret_str;
  }
  case SimplestNodeType::LogicalExprNode: {
    auto &logical_expr = qual_expr->Cast<SimplestLogicalExpr>();
    std::string left_expr_str, right_expr_str;
    if (SimplestLogicalOp::LogicalNot != logical_expr.GetLogicalOp()) {
      auto &left_expr = logical_expr.left_expr;
      left_expr_str = CollectFilter(left_expr);
    }
    auto &right_expr = logical_expr.right_expr;
    right_expr_str = CollectFilter(right_expr);
    switch (logical_expr.GetLogicalOp()) {
    case SimplestLogicalOp::InvalidLogicalOp:
      std::cout << "Invalid logical expr!" << std::endl;
      assert(false);
      break;
    case SimplestLogicalOp::LogicalAnd:
      ret_str = "(";
      ret_str += left_expr_str;
      ret_str += " AND ";
      ret_str += right_expr_str;
      ret_str += ")";
      return ret_str;
    case SimplestLogicalOp::LogicalOr:
      ret_str = "(";
      ret_str += left_expr_str;
      ret_str += " OR ";
      ret_str += right_expr_str;
      ret_str += ")";
      return ret_str;
    case SimplestLogicalOp::LogicalNot: {
      auto found = right_expr_str.find(' ');
#ifdef DEBUG
      assert(found != std::string::npos);
#endif
      ret_str = right_expr_str;
      ret_str.insert(found + 1, "NOT ");
      return ret_str;
    }
    default:
      std::cout << "Do not support yet, logical_expr->type: " +
                       std::to_string(logical_expr.GetLogicalOp())
                << std::endl;
      assert(false);
      break;
    }
    break;
  }
  case SimplestNodeType::IsNullExprNode: {
    auto &is_null_expr = qual_expr->Cast<SimplestIsNullExpr>();
    auto &var_attr = is_null_expr.attr;
    auto table_name = table_names[var_attr->GetTableIndex()] + "_" +
                      std::to_string(var_attr->GetTableIndex());
    std::string orig_col_name = var_attr->GetColumnName();
    unsigned int col_idx = var_attr->GetColumnIndex();
    unsigned int table_idx = var_attr->GetColumnIndex();
    std::string actual_col_name =
        GetActualColumnName(table_idx, col_idx, orig_col_name);
    ret_str = table_name + "." + actual_col_name;
    switch (is_null_expr.GetSimplestExprType()) {
    case SimplestExprType::InvalidExprType:
      std::cout << "Invalid logical expr!" << std::endl;
      assert(false);
      break;
    case SimplestExprType::NullType:
      ret_str += " IS NULL";
      return ret_str;
    case SimplestExprType::NonNullType:
      ret_str += " IS NOT NULL";
      return ret_str;
    default:
      std::cout << "Do not support yet, is_null_expr->type: " +
                       std::to_string(is_null_expr.GetSimplestExprType())
                << std::endl;
      assert(false);
      break;
    }
    break;
  }
  default:
    std::cout << "Do not support yet, qual_expr->type: " +
                     std::to_string(qual_expr->GetNodeType())
              << std::endl;
    assert(false);
  }
  return ret_str;
}

std::string
IRToSQLConverter::GetActualColumnName(idx_t table_index, idx_t column_index,
                                      const std::string &original_col_name) {
  // Check if this is an intermediate table with renamed columns
  auto match_key = std::make_pair(table_index, column_index);
  if (table_column_mappings.count(match_key) > 0) {
    auto actual_columns = table_column_mappings[match_key];
    return actual_columns;
  }

  // Not an intermediate table or position out of bounds, use original name
  return original_col_name;
}
} // namespace ir_sql_converter
