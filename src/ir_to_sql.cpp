#include "ir_to_sql.h"

#include <cstdint>
#include <cstdio>
#include <stdexcept>

namespace ir_sql_converter {

static std::string TruncateIdentifier(const std::string &name) {
  constexpr size_t kMaxLen = 63;
  if (name.size() <= kMaxLen)
    return name;
  uint64_t h = 14695981039346656037ULL;
  for (unsigned char c : name) {
    h ^= c;
    h *= 1099511628211ULL;
  }
  char buf[18];
  snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)h);
  return std::string("c_") + buf;
}
std::string IRToSQLConverter::ConvertSimplestIRToSQL(AQPStmt &plan) {
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
  bool first_table = true;
  for (const auto &table_name : table_names) {
    if (outer_join_tables.count(table_name.first))
      continue;
    if (!first_table)
      sql_code += ", ";
    auto table_name_idx =
        table_name.second + "_" + std::to_string(table_name.first);
    sql_code += table_name.second + " AS " + table_name_idx;
    first_table = false;
  }
  for (const auto &oj : outer_join_clauses)
    sql_code += oj;

  if (!filter_field.empty() || !join_field.empty()) {
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
  }

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

void IRToSQLConverter::GenerateSQL(AQPStmt &op) {
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
    // When a Projection sits above an Aggregate, the Aggregate's depth-first
    // processing already populated select_field from its groups/agg_fns.
    // The Projection's target_list is the authoritative SELECT list, so clear
    // the premature select_field entries.  Keep group_by_field intact — the
    // Aggregate's groups are the correct GROUP BY source.
    if (!proj_op.children.empty() &&
        proj_op.children[0]->GetNodeType() == SimplestNodeType::AggregateNode) {
      select_field.clear();
    }
    // Generate unique alias for this column
    std::string alias_name;
    std::string orig_col_name;

    // `SELECT table_name.$target_list`
    for (size_t idx = 0; idx < proj_op.target_list.size(); idx++) {
      // If this position has a function expression, render it directly.
      if (idx < proj_op.expr_target_list.size() &&
          proj_op.expr_target_list[idx]) {
        std::string select_str = CollectFilter(proj_op.expr_target_list[idx]);
        select_field.emplace_back(select_str);
        continue;
      }
      auto &target = proj_op.target_list[idx];
      auto target_table_index = target->GetTableIndex();
      // for DuckDB with agg
      if (table_names.find(target_table_index) == table_names.end()) {
        // The aggregate binding's column index selects the agg fn; the
        // positional target-list index is wrong when DuckDB dedupes
        // identical aggregates or interleaves group/agg outputs.
        size_t agg_fn_index = target->GetColumnIndex();
        auto &child_op = proj_op.children[0];
        if (SimplestNodeType::AggregateNode == child_op->GetNodeType()) {
          auto &agg_op = child_op->Cast<SimplestAggregate>();
          if (target_table_index == agg_op.GetAggIndex()) {
            if (agg_fn_index >= agg_op.agg_fns.size()) {
              throw std::runtime_error(
                  "IRToSQL unsupported: projection references aggregate fn #" +
                  std::to_string(agg_fn_index) + " but aggregate has only " +
                  std::to_string(agg_op.agg_fns.size()) + " fns");
            }
            auto agg_fn_type_enum = agg_op.agg_fns[agg_fn_index].second;
            std::string agg_fn_type =
                TranslateSimplestAggFnType(agg_fn_type_enum);
            if (agg_fn_type_enum == SimplestAggFnType::CountStar) {
              std::string select_str = "count(*)";
              alias_name = "count_star";
              select_str += " AS " + alias_name;
              select_field.emplace_back(select_str);
            } else {
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
              alias_name = TruncateIdentifier(table_names[table_idx] + "_" +
                           std::to_string(table_idx) + "_" + orig_col_name);
              select_str += " AS " + alias_name;
              select_field.emplace_back(select_str);
            }
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
            alias_name = TruncateIdentifier(table_names[table_idx] + "_" +
                         std::to_string(table_idx) + "_" + orig_col_name);
            select_str += " AS " + alias_name;
            select_field.emplace_back(select_str);
          } else {
            throw std::runtime_error(
                "IRToSQL unsupported: projection binding table " +
                std::to_string(target_table_index) +
                " matches neither agg nor group index");
          }
        } else {
          throw std::runtime_error(
              "IRToSQL unsupported: projection over child node type " +
              std::to_string(child_op->GetNodeType()));
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
          throw std::runtime_error(
              "IRToSQL unsupported: plain projection target with non-empty "
              "GROUP BY");
        }
        alias_name = TruncateIdentifier(table_names[target_table_index] + "_" +
                     std::to_string(target_table_index) + "_" + orig_col_name);
        select_str += " AS " + alias_name;
        select_field.emplace_back(select_str);
      }
    }
    break;
  }
  case SimplestNodeType::AggregateNode: {
    auto &agg_op = op.Cast<SimplestAggregate>();
    for (const auto &agg_fn : agg_op.agg_fns) {
      if (agg_fn.second == SimplestAggFnType::CountStar) {
        continue;
      }
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

        std::string select_str = table_name + "_" +
                                 std::to_string(target_table_index) + "." +
                                 actual_col_name;

        // Check if this column has an aggregate function
        auto agg_key = agg_field_key(target_table_index, col_index);
        auto agg_it = agg_field.find(agg_key);
        if (agg_it != agg_field.end()) {
          select_str = agg_it->second + "(" + select_str + ")";
        }

        select_field.emplace_back(select_str);
      }
    }

    // Build group_by_field from groups when no Projection above
    if (group_by_field.empty() && !group_by_vec.empty()) {
      bool target_list_covered = !agg_op.target_list.empty();
      for (const auto &grp : group_by_vec) {
        auto tbl = grp->GetTableIndex();
        if (table_names.find(tbl) == table_names.end())
          continue;
        std::string col_str = table_names[tbl] + "_" +
                              std::to_string(tbl) + "." +
                              grp->GetColumnName();
        group_by_field.emplace_back(col_str);
        if (!target_list_covered) {
          select_field.emplace_back(col_str);
        }
      }
      for (const auto &agg_fn : agg_op.agg_fns) {
        if (agg_fn.second == SimplestAggFnType::CountStar) {
          select_field.emplace_back("count(*)");
        }
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
    } else if (SimplestNodeType::AggregateNode == child_op->GetNodeType()) {
      auto &agg_op = child_op->Cast<SimplestAggregate>();
      proj_id = agg_op.GetAggIndex();
      for (const auto &grp : agg_op.groups) {
        auto key = std::make_pair((idx_t)grp->GetTableIndex(),
                                  (idx_t)grp->GetColumnIndex());
        proj_target_list_ids.push_back(key);
        proj_table_to_real_table.emplace(key, grp->GetTableIndex());
      }
      for (const auto &fn_pair : agg_op.agg_fns) {
        if (fn_pair.first) {
          auto key = std::make_pair((idx_t)fn_pair.first->GetTableIndex(),
                                    (idx_t)fn_pair.first->GetColumnIndex());
          proj_target_list_ids.push_back(key);
          proj_table_to_real_table.emplace(key, fn_pair.first->GetTableIndex());
        } else {
          proj_target_list_ids.emplace_back(
              (idx_t)-1, (idx_t)-1);
        }
      }
    } else {
      throw std::runtime_error(
          "ORDER BY with unsupported child node type " +
          std::to_string(child_op->GetNodeType()));
    }
    for (const auto &order : order_by_op.orders) {
      std::string order_by_str;
      auto column_id = order.attr->GetColumnIndex();
      order_by_str = std::to_string(column_id + 1);
      switch (order.order_type) {
      case SimplestOrderType::INVALID:
        throw std::runtime_error("IRToSQL unsupported: invalid order type");
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
      throw std::runtime_error("IRToSQL unsupported: unset limit type");
    case SimplestLimitType::CONSTANT_VALUE: {
      limit_field += "limit ";
      limit_field += std::to_string(limit_val.val);
      break;
    }
    default:
      throw std::runtime_error("IRToSQL unsupported: limit type " +
                               std::to_string(limit_val.type));
    }
    if (SimplestLimitType::UNSET != offset_val.type) {
      switch (offset_val.type) {
      case SimplestLimitType::CONSTANT_VALUE: {
        limit_field += "offset ";
        limit_field += std::to_string(offset_val.val);
        break;
      }
      default:
        throw std::runtime_error("IRToSQL unsupported: offset type " +
                                 std::to_string(offset_val.type));
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

        std::string select_str = table_name + "_" +
                                 std::to_string(target_table_index) + "." +
                                 actual_col_name;
        select_field.emplace_back(select_str);
      }
    }

    // `WHERE `
    for (const auto &qual : filter_op.qual_vec) {
      if (SingleAttr == qual->GetSimplestExprType()) {
        // the `filter_field` should be collected in the child MARK/SEMI join
        // node
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
        switch (var_comp.GetSimplestExprType()) {
        case SimplestExprType::Equal:        join_str += " = "; break;
        case SimplestExprType::NotEqual:     join_str += " != "; break;
        case SimplestExprType::LessThan:     join_str += " < "; break;
        case SimplestExprType::LessEqual:    join_str += " <= "; break;
        case SimplestExprType::GreaterThan:  join_str += " > "; break;
        case SimplestExprType::GreaterEqual: join_str += " >= "; break;
        default:
          throw std::runtime_error(
              "IRToSQL unsupported: join comparison expr type " +
              std::to_string(var_comp.GetSimplestExprType()));
        }
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
    case Left:
    case Right: {
      std::string on_clause;
      std::unordered_set<unsigned int> right_tables;
      for (const auto &cond : conditions) {
        auto &var_comp = cond->Cast<SimplestVarComparison>();
        auto &la = var_comp.left_attr;
        auto &ra = var_comp.right_attr;
        auto lt = table_names[la->GetTableIndex()] + "_" +
                  std::to_string(la->GetTableIndex());
        auto rt = table_names[ra->GetTableIndex()] + "_" +
                  std::to_string(ra->GetTableIndex());
        auto lc = GetActualColumnName(la->GetTableIndex(),
                                      la->GetColumnIndex(), la->GetColumnName());
        auto rc = GetActualColumnName(ra->GetTableIndex(),
                                      ra->GetColumnIndex(), ra->GetColumnName());
        if (!on_clause.empty())
          on_clause += " AND ";
        on_clause += lt + "." + lc;
        switch (var_comp.GetSimplestExprType()) {
        case SimplestExprType::Equal:        on_clause += " = "; break;
        case SimplestExprType::NotEqual:     on_clause += " != "; break;
        case SimplestExprType::LessThan:     on_clause += " < "; break;
        case SimplestExprType::LessEqual:    on_clause += " <= "; break;
        case SimplestExprType::GreaterThan:  on_clause += " > "; break;
        case SimplestExprType::GreaterEqual: on_clause += " >= "; break;
        default:
          throw std::runtime_error(
              "IRToSQL unsupported: outer join comparison expr type " +
              std::to_string(var_comp.GetSimplestExprType()));
        }
        on_clause += rt + "." + rc;
        right_tables.insert(ra->GetTableIndex());
      }
      std::string join_keyword =
          (join_type == Left) ? " LEFT JOIN " : " RIGHT JOIN ";
      for (auto rt_idx : right_tables) {
        auto rt_name = table_names[rt_idx];
        auto rt_alias = rt_name + "_" + std::to_string(rt_idx);
        outer_join_clauses.push_back(join_keyword + rt_name + " AS " +
                                     rt_alias + " ON " + on_clause);
        outer_join_tables.insert(rt_idx);
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
        unsigned int table_idx = left_var_attr->GetTableIndex();
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
      throw std::runtime_error("IRToSQL unsupported: join_type " +
                               std::to_string(join_type));
    }
    // Non-equi join conditions stored in qual_vec (e.g., BETWEEN with
    // interval arithmetic) — emit as WHERE predicates.
    for (const auto &qual : join_op.qual_vec) {
      filter_field.emplace_back(CollectFilter(qual));
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
    auto chunk_name = chunk_op.GetChunkName();
    if (!chunk_name.empty()) {
      table_names.emplace(chunk_op.GetTableIndex(), chunk_name);
    }
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
    throw std::runtime_error("IRToSQL unsupported: node type " +
                             std::to_string(op.GetNodeType()));
  }
}

std::string
IRToSQLConverter::TranslateSimplestAggFnType(SimplestAggFnType agg_fn_type) {
  std::string agg_fn_type_str;
  switch (agg_fn_type) {
  case SimplestAggFnType::InvalidAggType:
    throw std::runtime_error("IRToSQL unsupported: invalid aggregate fn type");
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
  case SimplestAggFnType::Count:
    agg_fn_type_str = "count";
    break;
  case SimplestAggFnType::CountStar:
    agg_fn_type_str = "count";
    break;
  }

  return agg_fn_type_str;
}

std::string IRToSQLConverter::CollectFilter(
    const std::unique_ptr<AQPExpr> &qual_expr) {
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
      throw std::runtime_error(
          "IRToSQL unsupported: var-const comparison expr type " +
          std::to_string(var_const_comp.GetSimplestExprType()));
    }

    std::string const_attr_str;
    switch (const_attr->GetType()) {
    // InvalidVarType = 0, BoolVar, IntVar, FloatVar, StringVar, StringVarArr
    case SimplestVarType::InvalidVarType:
      throw std::runtime_error("IRToSQL unsupported: invalid const var type");
    case SimplestVarType::BoolVar:
      const_attr_str = std::to_string(const_attr->GetBoolValue());
      break;
    case SimplestVarType::IntVar: {
      // Emit negative literals as (0 - N): lingodb's SQL frontend
      // rejects negative float/int literals (e.g. -7.0).
      auto iv = const_attr->GetIntValue();
      const_attr_str = iv < 0 ? "(0 - " + std::to_string(-iv) + ")"
                               : std::to_string(iv);
      break;
    }
    case SimplestVarType::FloatVar: {
      auto fv = const_attr->GetFloatValue();
      const_attr_str = fv < 0 ? "(0 - " + std::to_string(-fv) + ")"
                               : std::to_string(fv);
      break;
    }
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
    case SimplestVarType::TimestampVar:
      const_attr_str = "TIMESTAMP '";
      const_attr_str += const_attr->GetStringValue();
      const_attr_str += "'";
      break;
    case SimplestVarType::IntervalVar:
      const_attr_str = "INTERVAL '";
      const_attr_str += const_attr->GetStringValue();
      const_attr_str += "'";
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
      throw std::runtime_error("IRToSQL unsupported: logical expr op " +
                               std::to_string(logical_expr.GetLogicalOp()));
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
    unsigned int table_idx = var_attr->GetTableIndex();
    std::string actual_col_name =
        GetActualColumnName(table_idx, col_idx, orig_col_name);
    ret_str = table_name + "." + actual_col_name;
    switch (is_null_expr.GetSimplestExprType()) {
    case SimplestExprType::InvalidExprType:
      throw std::runtime_error("IRToSQL unsupported: invalid is-null expr");
    case SimplestExprType::NullType:
      ret_str += " IS NULL";
      return ret_str;
    case SimplestExprType::NonNullType:
      ret_str += " IS NOT NULL";
      return ret_str;
    default:
      throw std::runtime_error(
          "IRToSQL unsupported: is-null expr type " +
          std::to_string(is_null_expr.GetSimplestExprType()));
    }
    break;
  }
  case SimplestNodeType::VarComparisonNode: {
    auto &var_comp = qual_expr->Cast<SimplestVarComparison>();
    auto &left = var_comp.left_attr;
    auto &right = var_comp.right_attr;
    auto left_table = table_names[left->GetTableIndex()] + "_" +
                      std::to_string(left->GetTableIndex());
    auto right_table = table_names[right->GetTableIndex()] + "_" +
                       std::to_string(right->GetTableIndex());
    std::string left_col = GetActualColumnName(
        left->GetTableIndex(), left->GetColumnIndex(), left->GetColumnName());
    std::string right_col = GetActualColumnName(
        right->GetTableIndex(), right->GetColumnIndex(),
        right->GetColumnName());
    ret_str = left_table + "." + left_col;
    switch (var_comp.GetSimplestExprType()) {
    case SimplestExprType::Equal:        ret_str += " = "; break;
    case SimplestExprType::NotEqual:     ret_str += " != "; break;
    case SimplestExprType::LessThan:     ret_str += " < "; break;
    case SimplestExprType::LessEqual:    ret_str += " <= "; break;
    case SimplestExprType::GreaterThan:  ret_str += " > "; break;
    case SimplestExprType::GreaterEqual: ret_str += " >= "; break;
    default:
      throw std::runtime_error(
          "IRToSQL unsupported: var comparison expr type " +
          std::to_string(var_comp.GetSimplestExprType()));
    }
    ret_str += right_table + "." + right_col;
    return ret_str;
  }
  case SimplestNodeType::InExprNode: {
    auto &in_expr = qual_expr->Cast<SimplestInExpr>();
    auto &attr = in_expr.attr;
    auto tbl = table_names[attr->GetTableIndex()] + "_" +
               std::to_string(attr->GetTableIndex());
    std::string col = GetActualColumnName(
        attr->GetTableIndex(), attr->GetColumnIndex(), attr->GetColumnName());
    ret_str = tbl + "." + col;
    ret_str += in_expr.negated ? " NOT IN (" : " IN (";
    for (size_t i = 0; i < in_expr.values.size(); i++) {
      if (i > 0)
        ret_str += ", ";
      auto &v = in_expr.values[i];
      switch (v->GetType()) {
      case SimplestVarType::IntVar: {
        auto iv = v->GetIntValue();
        ret_str += iv < 0 ? "(0 - " + std::to_string(-iv) + ")"
                          : std::to_string(iv);
        break;
      }
      case SimplestVarType::FloatVar: {
        auto fv = v->GetFloatValue();
        ret_str += fv < 0 ? "(0 - " + std::to_string(-fv) + ")"
                          : std::to_string(fv);
        break;
      }
      case SimplestVarType::StringVar:
        ret_str += "'" + v->GetStringValue() + "'";
        break;
      default:
        throw std::runtime_error("IRToSQL unsupported: IN-list value type " +
                                 std::to_string(v->GetType()));
      }
    }
    ret_str += ")";
    return ret_str;
  }
  case SimplestNodeType::SingleAttrExprNode: {
    auto &single = qual_expr->Cast<SimplestSingleAttrExpr>();
    auto &attr = single.attr;
    auto tbl_it = table_names.find(attr->GetTableIndex());
    if (tbl_it != table_names.end()) {
      auto tbl = tbl_it->second + "_" + std::to_string(attr->GetTableIndex());
      std::string col = GetActualColumnName(
          attr->GetTableIndex(), attr->GetColumnIndex(), attr->GetColumnName());
      ret_str = tbl + "." + col;
    } else {
      ret_str = attr->GetColumnName();
    }
    return ret_str;
  }
  case SimplestNodeType::ArithExprNode: {
    auto &arith = qual_expr->Cast<SimplestArithExpr>();
    ret_str = "(";
    if (arith.left)
      ret_str += CollectFilter(arith.left);
    switch (arith.arith_op) {
    case SimplestArithOp::ArithAdd: ret_str += " + "; break;
    case SimplestArithOp::ArithSub: ret_str += " - "; break;
    case SimplestArithOp::ArithMul: ret_str += " * "; break;
    case SimplestArithOp::ArithDiv: ret_str += " / "; break;
    case SimplestArithOp::ArithMod: ret_str += " % "; break;
    default:
      throw std::runtime_error("IRToSQL unsupported: arith op " +
                               std::to_string(arith.arith_op));
    }
    if (arith.right)
      ret_str += CollectFilter(arith.right);
    ret_str += ")";
    return ret_str;
  }
  case SimplestNodeType::CastExprNode: {
    auto &cast = qual_expr->Cast<SimplestCastExpr>();
    ret_str = "CAST(";
    if (cast.child)
      ret_str += CollectFilter(cast.child);
    ret_str += " AS ";
    switch (cast.target_type) {
    case SimplestVarType::BoolVar:   ret_str += "BOOLEAN"; break;
    case SimplestVarType::IntVar:    ret_str += "INTEGER"; break;
    case SimplestVarType::FloatVar:  ret_str += "DOUBLE"; break;
    case SimplestVarType::StringVar: ret_str += "VARCHAR"; break;
    case SimplestVarType::Date:         ret_str += "DATE"; break;
    case SimplestVarType::IntervalVar: ret_str += "INTERVAL"; break;
    case SimplestVarType::TimestampVar:ret_str += "TIMESTAMP"; break;
    default:
      throw std::runtime_error("IRToSQL unsupported: cast target type " +
                               std::to_string(cast.target_type));
    }
    ret_str += ")";
    return ret_str;
  }
  case SimplestNodeType::ExprNode: {
    auto &gen = qual_expr->Cast<SimplestGeneralComparison>();
    if (gen.left_expr)
      ret_str += CollectFilter(gen.left_expr);
    switch (gen.GetSimplestExprType()) {
    case SimplestExprType::Equal:        ret_str += " = "; break;
    case SimplestExprType::NotEqual:     ret_str += " != "; break;
    case SimplestExprType::LessThan:     ret_str += " < "; break;
    case SimplestExprType::LessEqual:    ret_str += " <= "; break;
    case SimplestExprType::GreaterThan:  ret_str += " > "; break;
    case SimplestExprType::GreaterEqual: ret_str += " >= "; break;
    default:
      throw std::runtime_error(
          "IRToSQL unsupported: general comparison expr type " +
          std::to_string(gen.GetSimplestExprType()));
    }
    if (gen.right_expr)
      ret_str += CollectFilter(gen.right_expr);
    return ret_str;
  }
  case SimplestNodeType::ConstVarNode: {
    auto &ce = qual_expr->Cast<SimplestConstExpr>();
    if (ce.value) {
      switch (ce.value->GetType()) {
      case SimplestVarType::IntVar: {
        auto iv = ce.value->GetIntValue();
        ret_str = iv < 0 ? "(0 - " + std::to_string(-iv) + ")"
                         : std::to_string(iv);
        break;
      }
      case SimplestVarType::FloatVar: {
        auto fv = ce.value->GetFloatValue();
        ret_str = fv < 0 ? "(0 - " + std::to_string(-fv) + ")"
                         : std::to_string(fv);
        break;
      }
      case SimplestVarType::StringVar:
        ret_str = "'" + ce.value->GetStringValue() + "'";
        break;
      case SimplestVarType::IntervalVar:
        ret_str = "INTERVAL '" + ce.value->GetStringValue() + "'";
        break;
      case SimplestVarType::TimestampVar:
        ret_str = "TIMESTAMP '" + ce.value->GetStringValue() + "'";
        break;
      case SimplestVarType::BoolVar:
        ret_str = ce.value->GetBoolValue() ? "TRUE" : "FALSE";
        break;
      default:
        throw std::runtime_error("IRToSQL unsupported: const expr value type " +
                                 std::to_string(ce.value->GetType()));
      }
    }
    return ret_str;
  }
  case SimplestNodeType::FunctionExprNodeType: {
    auto &fn = qual_expr->Cast<SimplestFunctionExpr>();
    ret_str = fn.fn_name + "(";
    for (size_t i = 0; i < fn.args.size(); i++) {
      if (i > 0)
        ret_str += ", ";
      if (fn.args[i])
        ret_str += CollectFilter(fn.args[i]);
    }
    ret_str += ")";
    return ret_str;
  }
  default:
    throw std::runtime_error("IRToSQL unsupported: qual expr node type " +
                             std::to_string(qual_expr->GetNodeType()));
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
