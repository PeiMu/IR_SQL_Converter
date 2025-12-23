#include "duckdb_plan_to_ir.h"

#include <utility>

namespace ir_sql_converter {
std::unique_ptr<SimplestStmt> DuckToIR::ConstructSimplestStmt(
    duckdb::LogicalOperator *duckdb_plan_pointer,
    const std::unordered_map<unsigned int, std::string>
        &intermediate_table_map) {
  std::function<std::unique_ptr<SimplestStmt>(duckdb::LogicalOperator *
                                              duckdb_plan_pointer)>
      iterate_plan;
  iterate_plan = [&iterate_plan, intermediate_table_map,
                  this](duckdb::LogicalOperator *duckdb_plan_pointer)
      -> std::unique_ptr<SimplestStmt> {
    std::unique_ptr<SimplestStmt> left_child, right_child;
    if (!duckdb_plan_pointer->children.empty()) {
      left_child = iterate_plan(duckdb_plan_pointer->children[0].get());
      if (duckdb_plan_pointer->children.size() == 2)
        right_child = iterate_plan(duckdb_plan_pointer->children[1].get());
    }
    switch (duckdb_plan_pointer->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
      auto &proj_op = duckdb_plan_pointer->Cast<duckdb::LogicalProjection>();
      auto simplest_proj =
          ConstructSimplestProj(proj_op, std::move(left_child));
      return unique_ptr_cast<SimplestProjection, SimplestStmt>(
          std::move(simplest_proj));
    }
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      auto &agg_group_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalAggregate>();
      auto simplest_agg_group =
          ConstructSimplestAggGroup(agg_group_op, std::move(left_child));
      return unique_ptr_cast<SimplestAggregate, SimplestStmt>(
          std::move(simplest_agg_group));
    }
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
      auto &order_by_op = duckdb_plan_pointer->Cast<duckdb::LogicalOrder>();
      auto simplest_order_by =
          ConstructSimplestOrderBy(order_by_op, std::move(left_child));
      return unique_ptr_cast<SimplestOrderBy, SimplestStmt>(
          std::move(simplest_order_by));
    }
    case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
      auto &limit_op = duckdb_plan_pointer->Cast<duckdb::LogicalLimit>();
      auto simplest_limit =
          ConstructSimplestLimit(limit_op, std::move(left_child));
      return unique_ptr_cast<SimplestLimit, SimplestStmt>(
          std::move(simplest_limit));
    }
    case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
      auto &filter_op = duckdb_plan_pointer->Cast<duckdb::LogicalFilter>();
      auto simplest_filter =
          ConstructSimplestFilter(filter_op, std::move(left_child));
      return unique_ptr_cast<SimplestFilter, SimplestStmt>(
          std::move(simplest_filter));
    }
    case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
      auto &cross_product_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalCrossProduct>();
      auto simplest_cross_product = ConstructSimplestCrossProduct(
          cross_product_op, std::move(left_child), std::move(right_child));
      return unique_ptr_cast<SimplestCrossProduct, SimplestStmt>(
          std::move(simplest_cross_product));
    }
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      auto &join_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalComparisonJoin>();
      auto simplest_join = ConstructSimplestJoin(join_op, std::move(left_child),
                                                 std::move(right_child));
      return unique_ptr_cast<SimplestJoin, SimplestStmt>(
          std::move(simplest_join));
    }
    case duckdb::LogicalOperatorType::LOGICAL_GET: {
      auto &get_op = duckdb_plan_pointer->Cast<duckdb::LogicalGet>();
      auto simplest_scan = ConstructSimplestScan(get_op);
      return unique_ptr_cast<SimplestScan, SimplestStmt>(
          std::move(simplest_scan));
    }
    case duckdb::LogicalOperatorType::LOGICAL_CHUNK_GET: {
      auto &column_data_get_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalColumnDataGet>();
      auto find_intermediate_table =
          intermediate_table_map.find(column_data_get_op.table_index);
      if (find_intermediate_table != intermediate_table_map.end()) {
        auto simplest_scan = ConstructSimplestScan(
            column_data_get_op, find_intermediate_table->second);
        return unique_ptr_cast<SimplestScan, SimplestStmt>(
            std::move(simplest_scan));
      } else {
        // it might be an `IN` clause
        auto simplest_chunk = ConstructSimplestChunk(column_data_get_op);
        return unique_ptr_cast<SimplestChunk, SimplestStmt>(
            std::move(simplest_chunk));
      }
    }
    default:
      duckdb::Printer::Print(duckdb::StringUtil::Format(
          "Do not support yet, op->type:  %s",
          LogicalOperatorToString(duckdb_plan_pointer->type)));
      D_ASSERT(false);
      return nullptr;
    }
  };

  auto simplest_stmt = iterate_plan(duckdb_plan_pointer);

#ifdef DEBUG
  Printer::Print("constructed simplest stmt");
  simplest_stmt->Print();
#endif

  return simplest_stmt;
}

std::unique_ptr<SimplestProjection>
DuckToIR::ConstructSimplestProj(duckdb::LogicalProjection &proj_op,
                                std::unique_ptr<SimplestStmt> child) {
  auto table_index = proj_op.table_index;

  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(child));

  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  for (const auto &expr : proj_op.expressions) {
    auto table_expr = GetConstTableExpr(expr);
    // Resolve binding index to actual column index
    auto actual_column_idx = ResolveColumnIndex(table_expr.table_idx, table_expr.column_idx);
    // Get the correct column name from the base table schema
    std::string actual_column_name = table_expr.column_name;
    auto names_it = table_column_names_map.find(table_expr.table_idx);
    if (names_it != table_column_names_map.end() && actual_column_idx < names_it->second.size()) {
      actual_column_name = names_it->second[actual_column_idx];
    }
    auto simplest_target = std::make_unique<SimplestAttr>(
        ConvertVarType(table_expr.return_type), table_expr.table_idx,
        actual_column_idx, actual_column_name);
    target_list.emplace_back(std::move(simplest_target));
  }
  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), std::move(target_list),
      SimplestNodeType::ProjectionNode);

  auto simplest_projection =
      std::make_unique<SimplestProjection>(std::move(base_stmt), table_index);

  return simplest_projection;
}

std::unique_ptr<SimplestAggregate>
DuckToIR::ConstructSimplestAggGroup(duckdb::LogicalAggregate &agg_group_op,
                                    std::unique_ptr<SimplestStmt> child) {
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  // add table_expr of group by
  std::unique_ptr<SimplestAttr> simplest_attr;
  std::vector<std::unique_ptr<SimplestAttr>> groups;
  for (const auto &group_expr : agg_group_op.groups) {
#ifdef DEBUG
    D_ASSERT(ExpressionType::BOUND_COLUMN_REF == group_expr->type);
#endif
    auto &column_ref_expr =
        group_expr->Cast<duckdb::BoundColumnRefExpression>();
    simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(column_ref_expr.return_type),
        column_ref_expr.binding.table_index,
        column_ref_expr.binding.column_index, column_ref_expr.alias);
    groups.emplace_back(std::move(simplest_attr));
  }

  // set agg_index and group_index
  unsigned int agg_index = agg_group_op.aggregate_index;
  unsigned int group_index = agg_group_op.group_index;

  agg_fn_pair agg_fns;

  // add table_expr of aggregate op expression
  for (const auto &agg_expr : agg_group_op.expressions) {
#ifdef DEBUG
    D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
    auto &aggregate_expr = agg_expr->Cast<duckdb::BoundAggregateExpression>();
    std::string agg_fn_type = aggregate_expr.function.name;
    for (const auto &expr : aggregate_expr.children) {
#ifdef DEBUG
      D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
      auto &column_ref_expr = expr->Cast<duckdb::BoundColumnRefExpression>();
      simplest_attr = std::make_unique<SimplestAttr>(
          ConvertVarType(column_ref_expr.return_type),
          column_ref_expr.binding.table_index,
          column_ref_expr.binding.column_index, column_ref_expr.alias);
      agg_fns.emplace_back(std::make_pair(std::move(simplest_attr),
                                          ConvertAggFnType(agg_fn_type)));
    }
  }

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), std::move(target_list),
      SimplestNodeType::AggregateNode);

  std::unique_ptr<SimplestAggregate> simplest_aggregate;
  if (groups.empty()) {
    simplest_aggregate = std::make_unique<SimplestAggregate>(
        std::move(base_stmt), std::move(agg_fns), agg_index, group_index);
  } else {
    simplest_aggregate = std::make_unique<SimplestAggregate>(
        std::move(base_stmt), std::move(agg_fns), std::move(groups), agg_index,
        group_index);
  }
  return simplest_aggregate;
}

std::unique_ptr<SimplestOrderBy>
DuckToIR::ConstructSimplestOrderBy(duckdb::LogicalOrder &order_op,
                                   std::unique_ptr<SimplestStmt> child) {
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  std::vector<OrderStruct> orders;
  OrderStruct order_struct;
  std::unique_ptr<SimplestAttr> simplest_attr;
  for (auto &order : order_op.orders) {
    order_struct.order_type = ConvertOrderType(order.type);
#ifdef DEBUG
    D_ASSERT(order.expression->type == ExpressionType::BOUND_COLUMN_REF);
#endif
    auto &column_ref_expr =
        order.expression->Cast<duckdb::BoundColumnRefExpression>();
    simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(column_ref_expr.return_type),
        column_ref_expr.binding.table_index,
        column_ref_expr.binding.column_index, column_ref_expr.alias);
    order_struct.attr = std::move(simplest_attr);
    orders.emplace_back(std::move(order_struct));
  }
  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), std::move(target_list), SimplestNodeType::OrderNode);

  auto simplest_order_by = std::make_unique<SimplestOrderBy>(
      std::move(base_stmt), std::move(orders));
  return simplest_order_by;
}

std::unique_ptr<SimplestLimit>
DuckToIR::ConstructSimplestLimit(duckdb::LogicalLimit &limit_op,
                                 std::unique_ptr<SimplestStmt> child) {
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  std::vector<OrderStruct> orders;
  OrderStruct order_struct;
  std::unique_ptr<SimplestAttr> simplest_attr;
  LimitVal limit_val{}, offset_val{};
  switch (limit_op.limit_val.Type()) {
  case duckdb::LimitNodeType::UNSET:
    duckdb::Printer::Print("Unset limit node type!");
    D_ASSERT(false);
    exit(-1);
  case duckdb::LimitNodeType::CONSTANT_VALUE: {
    limit_val.type = SimplestLimitType::CONSTANT_VALUE;
    limit_val.val = limit_op.limit_val.GetConstantValue();
    break;
  }
  default:
    duckdb::Printer::Print("Unsupport limit type!!!");
    D_ASSERT(false);
  }
  switch (limit_op.offset_val.Type()) {
  case duckdb::LimitNodeType::UNSET: {
    offset_val.type = SimplestLimitType::UNSET;
    offset_val.val = 0;
    break;
  }
  case duckdb::LimitNodeType::CONSTANT_VALUE: {
    offset_val.type = SimplestLimitType::CONSTANT_VALUE;
    offset_val.val = limit_op.offset_val.GetConstantValue();
    break;
  }
  default:
    duckdb::Printer::Print("Unsupport limit type!!!");
    D_ASSERT(false);
  }

  auto base_stmt = std::make_unique<SimplestStmt>(std::move(children),
                                                  SimplestNodeType::LimitNode);

  auto simplest_limit = std::make_unique<SimplestLimit>(std::move(base_stmt),
                                                        limit_val, offset_val);
  return simplest_limit;
}

std::unique_ptr<SimplestCrossProduct> DuckToIR::ConstructSimplestCrossProduct(
    duckdb::LogicalCrossProduct &cross_product_op,
    std::unique_ptr<SimplestStmt> left_child,
    std::unique_ptr<SimplestStmt> right_child) {
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(left_child));
  children.emplace_back(std::move(right_child));
  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), SimplestNodeType::CrossProductNode);
  auto simplest_cross_product =
      std::make_unique<SimplestCrossProduct>(std::move(base_stmt));

  return simplest_cross_product;
}

std::unique_ptr<SimplestJoin>
DuckToIR::ConstructSimplestJoin(duckdb::LogicalComparisonJoin &join_op,
                                std::unique_ptr<SimplestStmt> left_child,
                                std::unique_ptr<SimplestStmt> right_child) {
  SimplestJoinType join_type;
  switch (join_op.join_type) {
  case duckdb::JoinType::INVALID:
    duckdb::Printer::Print("Invalid join type!");
    join_type = SimplestJoinType::InvalidJoinType;
    D_ASSERT(false);
    break;
  case duckdb::JoinType::LEFT:
    join_type = SimplestJoinType::Left;
    break;
  case duckdb::JoinType::RIGHT:
    join_type = SimplestJoinType::Right;
    break;
  case duckdb::JoinType::INNER:
    join_type = SimplestJoinType::Inner;
    break;
  case duckdb::JoinType::MARK:
    join_type = SimplestJoinType::Mark;
    break;
  default:
    duckdb::Printer::Print(duckdb::StringUtil::Format(
        "Do not support yet, join_type:  %s", join_op.join_type));
    join_type = SimplestJoinType::InvalidJoinType;
    D_ASSERT(false);
  }

  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(left_child));
  children.emplace_back(std::move(right_child));
  auto base_stmt = std::make_unique<SimplestStmt>(std::move(children),
                                                  SimplestNodeType::JoinNode);

  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;
  for (const auto &cond : join_op.conditions) {
    auto comp_type = ConvertCompType(cond.comparison);
    const auto &left_cond = cond.left;
    auto left_type = ConvertVarType(left_cond->return_type);
    auto left_expr_info = GetConstTableExpr(left_cond);
    // Resolve binding index to actual column index
    auto left_actual_col_idx = ResolveColumnIndex(left_expr_info.table_idx, left_expr_info.column_idx);
    // Get the correct column name from the base table schema
    std::string left_actual_col_name = left_expr_info.column_name;
    auto left_names_it = table_column_names_map.find(left_expr_info.table_idx);
    if (left_names_it != table_column_names_map.end() && left_actual_col_idx < left_names_it->second.size()) {
      left_actual_col_name = left_names_it->second[left_actual_col_idx];
    }
    auto left_simplest_cond = std::make_unique<SimplestAttr>(
        left_type, left_expr_info.table_idx, left_actual_col_idx,
        left_actual_col_name);
    const auto &right_cond = cond.right;
    auto right_type = ConvertVarType(right_cond->return_type);
    auto right_expr_info = GetConstTableExpr(right_cond);
    // Resolve binding index to actual column index
    auto right_actual_col_idx = ResolveColumnIndex(right_expr_info.table_idx, right_expr_info.column_idx);
    // Get the correct column name from the base table schema
    std::string right_actual_col_name = right_expr_info.column_name;
    auto right_names_it = table_column_names_map.find(right_expr_info.table_idx);
    if (right_names_it != table_column_names_map.end() && right_actual_col_idx < right_names_it->second.size()) {
      right_actual_col_name = right_names_it->second[right_actual_col_idx];
    }
    auto right_simplest_cond = std::make_unique<SimplestAttr>(
        right_type, right_expr_info.table_idx, right_actual_col_idx,
        right_actual_col_name);
#ifdef DEBUG
    D_ASSERT(left_type == right_type);
#endif
    auto simplest_cond = std::make_unique<SimplestVarComparison>(
        comp_type, std::move(left_simplest_cond),
        std::move(right_simplest_cond));
    join_conditions.emplace_back(std::move(simplest_cond));
  }

  auto simplest_join = std::make_unique<SimplestJoin>(
      std::move(base_stmt), std::move(join_conditions), join_type);

  return simplest_join;
}

std::unique_ptr<SimplestFilter>
DuckToIR::ConstructSimplestFilter(duckdb::LogicalFilter &filter_op,
                                  std::unique_ptr<SimplestStmt> child) {
  std::vector<std::unique_ptr<SimplestStmt>> children;
  children.emplace_back(std::move(child));
  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  // add qual vec
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec =
      CollectQualVecExprs(filter_op.expressions);

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(children), std::move(target_list), std::move(qual_vec),
      SimplestNodeType::FilterNode);

  auto simplest_filter = std::make_unique<SimplestFilter>(std::move(base_stmt));

  return simplest_filter;
}

std::unique_ptr<SimplestScan>
DuckToIR::ConstructSimplestScan(duckdb::LogicalGet &get_op) {
  auto table_index = get_op.table_index;

  // Store column_ids and column names mapping for this table
  table_column_ids_map[table_index] = get_op.column_ids;
  table_column_names_map[table_index] = get_op.names;

  // add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
#ifdef DEBUG
  get_op.names.size() == get_op.returned_types.size();
#endif
  // Build target list using actual column indices from the base table
  // column_ids maps: column_ids[binding_idx] = base_table_column_idx
  for (size_t binding_idx = 0; binding_idx < get_op.column_ids.size(); binding_idx++) {
    auto column_id = get_op.column_ids[binding_idx];
    if (column_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
      continue; // Skip row ID column
    }
    // Store the base table column index (column_id) in SimplestAttr
    // This ensures the IR uses base table schema indices consistently
    std::unique_ptr<SimplestAttr> simplest_attr =
        std::make_unique<SimplestAttr>(
            ConvertVarType(get_op.returned_types[column_id]), table_index,
            column_id, get_op.names[column_id]);
    target_list.emplace_back(std::move(simplest_attr));
  }

  // add qual vec
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec;
  for (const auto &filter : get_op.table_filters.filters) {
    auto column_index = filter.first;
    auto &filter_cond = filter.second;
    auto simplest_var_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(get_op.returned_types[column_index]), table_index,
        column_index, get_op.names[column_index]);
    auto simplest_scan_filter_expr =
        CollectScanFilter(filter_cond, std::move(simplest_var_attr));
    qual_vec.emplace_back(std::move(simplest_scan_filter_expr));
  }

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);
  auto table_name = get_op.function.to_string(get_op.bind_data.get());
  auto simplest_scan = std::make_unique<SimplestScan>(std::move(base_stmt),
                                                      table_index, table_name);
  return simplest_scan;
}

std::unique_ptr<SimplestScan>
DuckToIR::ConstructSimplestScan(duckdb::LogicalColumnDataGet &get_op,
                                const std::string &intermediate_table_name) {
  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  // todo: add qual vec
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec;

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);

  auto table_index = get_op.table_index;
  auto simplest_scan = std::make_unique<SimplestScan>(
      std::move(base_stmt), table_index, intermediate_table_name);
  return simplest_scan;
}

std::unique_ptr<SimplestChunk> DuckToIR::ConstructSimplestChunk(
    duckdb::LogicalColumnDataGet &column_data_get_op) {
  // fixme: might have other types
  std::vector<std::string> chunk_contents;
  duckdb::DataChunk chunk;

  column_data_get_op.collection->InitializeScanChunk(chunk);
  duckdb::ColumnDataScanState scan_state;
  column_data_get_op.collection->InitializeScan(scan_state);
  while (column_data_get_op.collection->Scan(scan_state, chunk)) {
    for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
      for (idx_t j = 0; j < chunk.size(); j++) {
        chunk_contents.emplace_back(chunk.data[i].GetValue(j).ToString());
      }
    }
  }

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  // todo: add qual vec
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec;

  auto base_stmt = std::make_unique<SimplestStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);
  auto simplest_chunk = std::make_unique<SimplestChunk>(
      std::move(base_stmt), column_data_get_op.table_index, chunk_contents);
  return simplest_chunk;
}

SimplestExprType DuckToIR::ConvertCompType(duckdb::ExpressionType type) {
  switch (type) {
  case duckdb::ExpressionType::COMPARE_EQUAL:
    return SimplestExprType::Equal;
  case duckdb::ExpressionType::COMPARE_LESSTHAN:
    return SimplestExprType::LessThan;
  case duckdb::ExpressionType::COMPARE_GREATERTHAN:
    return SimplestExprType::GreaterThan;
  case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
    return SimplestExprType::LessEqual;
  case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    return SimplestExprType::GreaterEqual;
  case duckdb::ExpressionType::COMPARE_NOTEQUAL:
    return SimplestExprType::NotEqual;
  case duckdb::ExpressionType::COMPARE_IN:
    return SimplestExprType::TextLike;
  case duckdb::ExpressionType::COMPARE_NOT_IN:
    return SimplestExprType::Text_Not_Like;
  case duckdb::ExpressionType::CONJUNCTION_AND:
  case duckdb::ExpressionType::CONJUNCTION_OR:
    return SimplestExprType::LogicalOp;
  default:
    duckdb::Printer::Print("Invalid comparison type!");
    return SimplestExprType::InvalidExprType;
  }
}

SimplestLogicalOp DuckToIR::ConvertLogicalType(duckdb::ExpressionType type) {
  switch (type) {
  case duckdb::ExpressionType::CONJUNCTION_AND:
    return SimplestLogicalOp::LogicalAnd;
  case duckdb::ExpressionType::CONJUNCTION_OR:
    return SimplestLogicalOp::LogicalOr;
  case duckdb::ExpressionType::OPERATOR_NOT:
    return SimplestLogicalOp::LogicalNot;
  default:
    duckdb::Printer::Print("Invalid logical op!");
    return SimplestLogicalOp::InvalidLogicalOp;
  }
}

SimplestVarType DuckToIR::ConvertVarType(const duckdb::LogicalType &type) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::BOOLEAN:
    return SimplestVarType::BoolVar;
  case duckdb::LogicalTypeId::INTEGER:
    return SimplestVarType::IntVar;
  case duckdb::LogicalTypeId::FLOAT:
  case duckdb::LogicalTypeId::DECIMAL:
  case duckdb::LogicalTypeId::DOUBLE:
    return SimplestVarType::FloatVar;
  case duckdb::LogicalTypeId::VARCHAR:
    return SimplestVarType::StringVar;
  case duckdb::LogicalTypeId::DATE:
    return SimplestVarType::Date;
  default:
    duckdb::Printer::Print("Invalid postgres var type!");
    return SimplestVarType::InvalidVarType;
  }
}

SimplestAggFnType DuckToIR::ConvertAggFnType(const std::string &agg_fn_type) {
  if (agg_fn_type == "min")
    return SimplestAggFnType::Min;
  else if (agg_fn_type == "max")
    return SimplestAggFnType::Max;
  else if (agg_fn_type == "sum")
    return SimplestAggFnType::Sum;
  else if (agg_fn_type == "avg")
    return SimplestAggFnType::Average;
  else
    return SimplestAggFnType::InvalidAggType;
}

SimplestOrderType DuckToIR::ConvertOrderType(duckdb::OrderType type) {
  switch (type) {
  case duckdb::OrderType::INVALID:
    return SimplestOrderType::INVALID;
  case duckdb::OrderType::ORDER_DEFAULT:
    return SimplestOrderType::ORDER_DEFAULT;
  case duckdb::OrderType::ASCENDING:
    return SimplestOrderType::Ascending;
  case duckdb::OrderType::DESCENDING:
    return SimplestOrderType::Descending;
  }
}

duckdb::column_t DuckToIR::ResolveColumnIndex(duckdb::idx_t table_idx, duckdb::idx_t binding_idx) {
  auto it = table_column_ids_map.find(table_idx);
  if (it == table_column_ids_map.end() || binding_idx >= it->second.size()) {
    // If mapping not found or binding_idx out of range, return binding_idx as-is
    // This handles cases where the table might not be a LogicalGet (e.g., intermediate results)
    return binding_idx;
  }
  auto column_id = it->second[binding_idx];
  if (column_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    return binding_idx; // Return binding index for row ID
  }
  return column_id;
}

std::unique_ptr<SimplestAttr>
DuckToIR::ConvertAttr(const duckdb::unique_ptr<duckdb::Expression> &expr) {
  duckdb::TableExpr table_expr = GetConstTableExpr(expr);
  // Resolve binding index to actual column index using column_ids mapping
  auto actual_column_idx = ResolveColumnIndex(table_expr.table_idx, table_expr.column_idx);

  // Get the correct column name from the base table schema
  std::string actual_column_name = table_expr.column_name;
  auto names_it = table_column_names_map.find(table_expr.table_idx);
  if (names_it != table_column_names_map.end() && actual_column_idx < names_it->second.size()) {
    actual_column_name = names_it->second[actual_column_idx];
  }

  auto simplest_attr = std::make_unique<SimplestAttr>(
      ConvertVarType(table_expr.return_type), table_expr.table_idx,
      actual_column_idx, actual_column_name);
  return simplest_attr;
}

std::unique_ptr<SimplestConstVar>
DuckToIR::ConvertConstVar(const duckdb::BoundConstantExpression &expr,
                          std::string prefix, const std::string &appendix) {
  std::unique_ptr<SimplestConstVar> simplest_attr;
  switch (expr.value.type().id()) {
  case duckdb::LogicalTypeId::VARCHAR:
  case duckdb::LogicalTypeId::DECIMAL:
  case duckdb::LogicalTypeId::FLOAT:
  case duckdb::LogicalTypeId::DOUBLE:
  case duckdb::LogicalTypeId::TINYINT:
  case duckdb::LogicalTypeId::SMALLINT:
  case duckdb::LogicalTypeId::INTEGER:
  case duckdb::LogicalTypeId::BIGINT: {
    std::string str = std::move(prefix);
    str += expr.value.ToString();
    str += appendix;
    simplest_attr = std::make_unique<SimplestConstVar>(str);
    break;
  }
  default:
    duckdb::Printer::Print(duckdb::StringUtil::Format(
        "Do not support yet, right_expr.value.type:  %s",
        LogicalTypeIdToString(expr.value.type().id())));
    D_ASSERT(false);
    break;
  }
  return simplest_attr;
}

std::unique_ptr<SimplestExpr>
DuckToIR::ConvertExpr(const duckdb::unique_ptr<duckdb::Expression> &expr) {
  switch (expr->type) {
  case duckdb::ExpressionType::BOUND_FUNCTION: {
    auto &bound_func = expr->Cast<duckdb::BoundFunctionExpression>();
    if (bound_func.function.name == "~~" ||
        bound_func.function.name == "contains" ||
        bound_func.function.name == "prefix" ||
        bound_func.function.name == "suffix") {
      auto simplest_expr_type = SimplestExprType::TextLike;
      auto &left_expr = bound_func.children[0];
      auto left_simplest_attr = ConvertAttr(left_expr);
#ifdef DEBUG
      D_ASSERT(bound_func.children[1]->expression_class ==
               ExpressionClass::BOUND_CONSTANT);
#endif
      auto &right_expr =
          bound_func.children[1]->Cast<duckdb::BoundConstantExpression>();
      std::string prefix_str, appendix_str;
      if ("contains" == bound_func.function.name) {
        prefix_str = appendix_str = "%";
      } else if ("prefix" == bound_func.function.name) {
        appendix_str = "%";
      } else if ("suffix" == bound_func.function.name) {
        prefix_str = "%";
      }
      auto right_simplest_attr =
          ConvertConstVar(right_expr, prefix_str, appendix_str);

      auto simplest_var_const_comp =
          std::make_unique<SimplestVarConstComparison>(
              simplest_expr_type, std::move(left_simplest_attr),
              std::move(right_simplest_attr));
      return unique_ptr_cast<SimplestVarConstComparison, SimplestExpr>(
          std::move(simplest_var_const_comp));
    } else {
      duckdb::Printer::Print(
          "Unknown bound function type in CollectQualVecExprs");
      D_ASSERT(false);
    }

    break;
  }
  case duckdb::ExpressionType::CASE_EXPR: {
    auto &case_expr = expr->Cast<duckdb::CaseExpression>();
    for (auto &case_check : case_expr.case_checks) {
      auto &when_expr = case_check.when_expr;
      auto &then_expr = case_check.then_expr;
    }
    auto &else_expr = case_expr.else_expr;
    duckdb::Printer::Print(
        "TODO: Need to implement CASE_EXPR in CollectQualVecExprs");
    D_ASSERT(false);
    break;
  }
  case duckdb::ExpressionType::COMPARE_NOTEQUAL:
  case duckdb::ExpressionType::COMPARE_EQUAL:
  case duckdb::ExpressionType::COMPARE_GREATERTHAN:
  case duckdb::ExpressionType::COMPARE_LESSTHAN:
  case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
  case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO: {
    auto &comparison_expr = expr->Cast<duckdb::BoundComparisonExpression>();
    auto &left_expr = comparison_expr.left;
    auto left_simplest_attr = ConvertAttr(left_expr);
#ifdef DEBUG
    D_ASSERT(comparison_expr.right->expression_class ==
             ExpressionClass::BOUND_CONSTANT);
#endif
    auto &right_expr =
        comparison_expr.right->Cast<duckdb::BoundConstantExpression>();
    auto right_simplest_attr = ConvertConstVar(right_expr);

    auto simplest_var_const_comp = std::make_unique<SimplestVarConstComparison>(
        ConvertCompType(expr->type), std::move(left_simplest_attr),
        std::move(right_simplest_attr));
    return unique_ptr_cast<SimplestVarConstComparison, SimplestExpr>(
        std::move(simplest_var_const_comp));
  }
  case duckdb::ExpressionType::CONJUNCTION_AND:
  case duckdb::ExpressionType::CONJUNCTION_OR: {
    auto &conjunction_expr = expr->Cast<duckdb::BoundConjunctionExpression>();
#ifdef DEBUG
    D_ASSERT(2 <= conjunction_expr.children.size());
#endif
    auto left_simplest_expr = ConvertExpr(conjunction_expr.children[0]);
    for (size_t idx = 1; idx < conjunction_expr.children.size(); idx++) {
      auto right_simplest_expr = ConvertExpr(conjunction_expr.children[idx]);
      left_simplest_expr = std::make_unique<SimplestLogicalExpr>(
          ConvertLogicalType(expr->type), std::move(left_simplest_expr),
          std::move(right_simplest_expr));
    }
    return left_simplest_expr;
  }
  case duckdb::ExpressionType::OPERATOR_IS_NULL:
  case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
    auto &operator_expr = expr->Cast<duckdb::BoundOperatorExpression>();
#ifdef DEBUG
    D_ASSERT(1 == operator_expr.children.size());
    D_ASSERT(ExpressionType::BOUND_COLUMN_REF ==
             operator_expr.children[0]->GetExpressionType());
#endif
    auto simplest_expr = ConvertAttr(operator_expr.children[0]);
    auto is_null = duckdb::ExpressionType::OPERATOR_IS_NULL == expr->type
                       ? SimplestExprType::NullType
                       : SimplestExprType::NonNullType;
    auto simplest_is_null =
        std::make_unique<SimplestIsNullExpr>(is_null, std::move(simplest_expr));
    return unique_ptr_cast<SimplestIsNullExpr, SimplestExpr>(
        std::move(simplest_is_null));
  }
  case duckdb::ExpressionType::OPERATOR_NOT: {
    auto &operator_expr = expr->Cast<duckdb::BoundOperatorExpression>();
#ifdef DEBUG
    D_ASSERT(1 == operator_expr.children.size());
#endif
    auto simplest_expr = ConvertExpr(operator_expr.children[0]);
    auto simplest_not_expr = std::make_unique<SimplestLogicalExpr>(
        SimplestLogicalOp::LogicalNot, nullptr, std::move(simplest_expr));
    return unique_ptr_cast<SimplestLogicalExpr, SimplestExpr>(
        std::move(simplest_not_expr));
  }
  case duckdb::ExpressionType::OPERATOR_COALESCE: {
    auto &operator_expr = expr->Cast<duckdb::BoundOperatorExpression>();
    for (auto &child_expr : operator_expr.children) {
    }
    duckdb::Printer::Print("TODO: Need to implement BoundOperatorExpression in "
                           "CollectQualVecExprs");
    D_ASSERT(false);
    break;
  }
  case duckdb::ExpressionType::BOUND_COLUMN_REF: {
    duckdb::TableExpr table_expr = GetConstTableExpr(expr);
    // Resolve binding index to actual column index
    auto actual_column_idx = ResolveColumnIndex(table_expr.table_idx, table_expr.column_idx);
    // Get the correct column name from the base table schema
    std::string actual_column_name = table_expr.column_name;
    auto names_it = table_column_names_map.find(table_expr.table_idx);
    if (names_it != table_column_names_map.end() && actual_column_idx < names_it->second.size()) {
      actual_column_name = names_it->second[actual_column_idx];
    }
    auto simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(table_expr.return_type), table_expr.table_idx,
        actual_column_idx, actual_column_name);
    auto simplest_attr_expr =
        std::make_unique<SimplestSingleAttrExpr>(std::move(simplest_attr));
    return unique_ptr_cast<SimplestSingleAttrExpr, SimplestExpr>(
        std::move(simplest_attr_expr));
  }
  default:
    duckdb::Printer::Print(
        duckdb::StringUtil::Format("Do not support yet, expr->type:  %s",
                                   ExpressionTypeToString(expr->type)));
    D_ASSERT(false);
  }

  return nullptr;
}

std::vector<std::unique_ptr<SimplestExpr>> DuckToIR::CollectQualVecExprs(
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &exprs) {
  std::vector<std::unique_ptr<SimplestExpr>> qual_vec;
  for (const auto &expr : exprs) {
    auto simplest_expr = ConvertExpr(expr);
    qual_vec.emplace_back(std::move(simplest_expr));
  }

  return qual_vec;
}

std::unique_ptr<SimplestExpr> DuckToIR::CollectScanFilter(
    const std::unique_ptr<duckdb::TableFilter> &filter_cond,
    std::unique_ptr<SimplestAttr> var_attr) {
  switch (filter_cond->filter_type) {
  case duckdb::TableFilterType::CONJUNCTION_AND: {
    auto &conjunction_and = filter_cond->Cast<duckdb::ConjunctionAndFilter>();
#ifdef DEBUG
    D_ASSERT(2 <= conjunction_and.child_filters.size());
#endif
    auto left_filter =
        CollectScanFilter(conjunction_and.child_filters[0],
                          std::make_unique<SimplestAttr>(*var_attr));
    for (size_t idx = 1; idx < conjunction_and.child_filters.size(); idx++) {
      auto right_filter =
          CollectScanFilter(conjunction_and.child_filters[idx],
                            std::make_unique<SimplestAttr>(*var_attr));
      left_filter = std::make_unique<SimplestLogicalExpr>(
          SimplestLogicalOp::LogicalAnd, std::move(left_filter),
          std::move(right_filter));
    }
    return left_filter;
  }
  case duckdb::TableFilterType::CONJUNCTION_OR: {
    auto &conjunction_or = filter_cond->Cast<duckdb::ConjunctionOrFilter>();
#ifdef DEBUG
    D_ASSERT(2 <= conjunction_or.child_filters.size());
#endif
    auto left_filter =
        CollectScanFilter(conjunction_or.child_filters[0],
                          std::make_unique<SimplestAttr>(*var_attr));
    for (size_t idx = 1; idx < conjunction_or.child_filters.size(); idx++) {
      auto right_filter =
          CollectScanFilter(conjunction_or.child_filters[idx],
                            std::make_unique<SimplestAttr>(*var_attr));
      left_filter = std::make_unique<SimplestLogicalExpr>(
          SimplestLogicalOp::LogicalOr, std::move(left_filter),
          std::move(right_filter));
    }
    return left_filter;
  }
  case duckdb::TableFilterType::CONSTANT_COMPARISON: {
    auto &constant_filter = filter_cond->Cast<duckdb::ConstantFilter>();
    std::string constant_str = constant_filter.constant.ToString();
    auto simplest_comp_type = ConvertCompType(constant_filter.comparison_type);
    auto simplest_const_var = std::make_unique<SimplestConstVar>(constant_str);
    auto simplest_constant_comp = std::make_unique<SimplestVarConstComparison>(
        simplest_comp_type, std::make_unique<SimplestAttr>(*var_attr),
        std::move(simplest_const_var));
    return unique_ptr_cast<SimplestVarConstComparison, SimplestExpr>(
        std::move(simplest_constant_comp));
  }
  case duckdb::TableFilterType::IS_NOT_NULL: {
    auto simplest_is_not_null = std::make_unique<SimplestIsNullExpr>(
        SimplestExprType::NonNullType,
        std::make_unique<SimplestAttr>(*var_attr));
    return unique_ptr_cast<SimplestIsNullExpr, SimplestExpr>(
        std::move(simplest_is_not_null));
  }
  case duckdb::TableFilterType::IS_NULL: {
    auto simplest_is_null = std::make_unique<SimplestIsNullExpr>(
        SimplestExprType::NullType, std::make_unique<SimplestAttr>(*var_attr));
    return unique_ptr_cast<SimplestIsNullExpr, SimplestExpr>(
        std::move(simplest_is_null));
  }
  case duckdb::TableFilterType::STRUCT_EXTRACT: {
    auto &struct_filter = filter_cond->Cast<duckdb::StructFilter>();
    duckdb::Printer::Print(
        "Do not support yet: TableFilterType::STRUCT_EXTRACT");
    D_ASSERT(false);
  }
  }
  return nullptr;
}
} // namespace ir_sql_converter
