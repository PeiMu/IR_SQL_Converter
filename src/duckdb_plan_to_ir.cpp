#include "duckdb_plan_to_ir.h"

#include <limits>
#include <stdexcept>
#include <utility>

namespace ir_sql_converter {

// Physical bit-width that FlatTable uses for a given DuckDB LogicalType.
// DECIMAL(p<=9) and small ints → 32; BIGINT/DECIMAL(9<p<=18) → 64; others → 0.
static uint8_t GetPhysicalBitWidth(const duckdb::LogicalType &type) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::TINYINT:
  case duckdb::LogicalTypeId::UTINYINT:
  case duckdb::LogicalTypeId::SMALLINT:
  case duckdb::LogicalTypeId::USMALLINT:
  case duckdb::LogicalTypeId::INTEGER:
  case duckdb::LogicalTypeId::UINTEGER:
  case duckdb::LogicalTypeId::DATE:
    return 32;
  case duckdb::LogicalTypeId::BIGINT:
  case duckdb::LogicalTypeId::UBIGINT:
  case duckdb::LogicalTypeId::HUGEINT:
  case duckdb::LogicalTypeId::TIMESTAMP:
  case duckdb::LogicalTypeId::TIMESTAMP_TZ:
  case duckdb::LogicalTypeId::TIMESTAMP_NS:
  case duckdb::LogicalTypeId::TIMESTAMP_MS:
  case duckdb::LogicalTypeId::TIMESTAMP_SEC:
    return 64;
  case duckdb::LogicalTypeId::DECIMAL: {
    auto width = duckdb::DecimalType::GetWidth(type);
    return width <= 9 ? 32 : 64;
  }
  case duckdb::LogicalTypeId::FLOAT:
    return 32;
  case duckdb::LogicalTypeId::DOUBLE:
    return 64;
  default:
    return 0;
  }
}

// Unwrap casts and DuckDB Compressed-Materialization wrappers around a column
// reference; nullptr when the expression is anything else.
// __internal_compress_* functions are storage-layer wrappers inserted by
// DuckDB's CompressedMaterialization optimizer — the first child is always the
// original column reference and unwrapping is semantically safe.
static const duckdb::Expression *
TryUnwrapCastToColumnRef(const duckdb::Expression *e) {
  for (;;) {
    if (e->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
      e = e->Cast<duckdb::BoundCastExpression>().child.get();
      continue;
    }
    if (e->expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
      auto &fn = e->Cast<duckdb::BoundFunctionExpression>();
      if (fn.function.name.rfind("__internal_compress_", 0) == 0 &&
          !fn.children.empty()) {
        e = fn.children[0].get();
        continue;
      }
    }
    break;
  }
  return e->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF
             ? e
             : nullptr;
}

static const duckdb::Expression *
UnwrapToColumnRef(const duckdb::Expression *e) {
  auto *col = TryUnwrapCastToColumnRef(e);
  if (!col) {
    throw std::runtime_error(
        "DuckToIR unsupported: expression is not a (possibly cast) column "
        "reference: " + e->ToString());
  }
  return col;
}
std::unique_ptr<AQPStmt> DuckToIR::ConstructSimplestStmt(
    duckdb::LogicalOperator *duckdb_plan_pointer,
    const std::unordered_map<unsigned int, std::string> &intermediate_table_map,
    bool embed_intermediate_data,
    const std::unordered_map<unsigned int, std::vector<std::string>>
        *chunk_col_names) {
  chunk_col_names_ = chunk_col_names;
  std::function<std::unique_ptr<AQPStmt>(duckdb::LogicalOperator *
                                              duckdb_plan_pointer)>
      iterate_plan;
  iterate_plan = [&iterate_plan, &intermediate_table_map,
                  embed_intermediate_data,
                  this](duckdb::LogicalOperator *duckdb_plan_pointer)
      -> std::unique_ptr<AQPStmt> {
    if (!duckdb_plan_pointer)
      return nullptr;
    std::unique_ptr<AQPStmt> left_child, right_child;
    if (!duckdb_plan_pointer->children.empty()) {
      left_child = iterate_plan(duckdb_plan_pointer->children[0].get());
      if (duckdb_plan_pointer->children.size() == 2)
        right_child = iterate_plan(duckdb_plan_pointer->children[1].get());
    }
    std::unique_ptr<AQPStmt> result;
    switch (duckdb_plan_pointer->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
      auto &proj_op = duckdb_plan_pointer->Cast<duckdb::LogicalProjection>();
      auto simplest_proj =
          ConstructSimplestProj(proj_op, std::move(left_child));
      result = unique_ptr_cast<SimplestProjection, AQPStmt>(
          std::move(simplest_proj));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
      auto &agg_group_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalAggregate>();
      auto simplest_agg_group =
          ConstructSimplestAggGroup(agg_group_op, std::move(left_child));
      result = unique_ptr_cast<SimplestAggregate, AQPStmt>(
          std::move(simplest_agg_group));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
      auto &order_by_op = duckdb_plan_pointer->Cast<duckdb::LogicalOrder>();
      auto simplest_order_by =
          ConstructSimplestOrderBy(order_by_op, std::move(left_child));
      result = unique_ptr_cast<SimplestOrderBy, AQPStmt>(
          std::move(simplest_order_by));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
      auto &limit_op = duckdb_plan_pointer->Cast<duckdb::LogicalLimit>();
      auto simplest_limit =
          ConstructSimplestLimit(limit_op, std::move(left_child));
      result = unique_ptr_cast<SimplestLimit, AQPStmt>(
          std::move(simplest_limit));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
      auto &filter_op = duckdb_plan_pointer->Cast<duckdb::LogicalFilter>();
      auto simplest_filter =
          ConstructSimplestFilter(filter_op, std::move(left_child));
      result = unique_ptr_cast<SimplestFilter, AQPStmt>(
          std::move(simplest_filter));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
      auto &cross_product_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalCrossProduct>();
      auto simplest_cross_product = ConstructSimplestCrossProduct(
          cross_product_op, std::move(left_child), std::move(right_child));
      result = unique_ptr_cast<SimplestCrossProduct, AQPStmt>(
          std::move(simplest_cross_product));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
      auto &join_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalComparisonJoin>();
      auto simplest_join = ConstructSimplestJoin(join_op, std::move(left_child),
                                                 std::move(right_child));
      result =
          unique_ptr_cast<SimplestJoin, AQPStmt>(std::move(simplest_join));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_GET: {
      auto &get_op = duckdb_plan_pointer->Cast<duckdb::LogicalGet>();
      if (intermediate_table_map.count(get_op.table_index)) {
        auto simplest_chunk =
            ConstructSimplestChunkFromGet(get_op, intermediate_table_map);
        result = unique_ptr_cast<SimplestChunk, AQPStmt>(
            std::move(simplest_chunk));
      } else {
        auto simplest_scan = ConstructSimplestScan(get_op);
        result =
            unique_ptr_cast<SimplestScan, AQPStmt>(std::move(simplest_scan));
      }
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_CHUNK_GET: {
      auto &column_data_get_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalColumnDataGet>();
      auto find_intermediate_table =
          intermediate_table_map.find(column_data_get_op.table_index);
      if (find_intermediate_table != intermediate_table_map.end()) {
        // Intermediate result from previous subplan
        if (embed_intermediate_data) {
#ifndef NDEBUG
          std::cout << "cross engine\n";
#endif
          // Cross-engine: embed actual data in SimplestChunk
          auto simplest_chunk = ConstructSimplestChunk(column_data_get_op,
                                                       intermediate_table_map);
          result = unique_ptr_cast<SimplestChunk, AQPStmt>(
              std::move(simplest_chunk));
        } else {
#ifndef NDEBUG
          std::cout << "same engine\n";
#endif
          // Same-engine: create SimplestChunk with empty contents
          // The engine will provide the data at execution time
          auto simplest_chunk = ConstructSimplestChunkPlaceholder(
              column_data_get_op, intermediate_table_map);
          result = unique_ptr_cast<SimplestChunk, AQPStmt>(
              std::move(simplest_chunk));
        }
      } else {
        // IN-clause constant list - always embed data
        auto simplest_chunk =
            ConstructSimplestChunk(column_data_get_op, intermediate_table_map);
        result = unique_ptr_cast<SimplestChunk, AQPStmt>(
            std::move(simplest_chunk));
      }
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_JOIN: {
      auto &join_op =
          duckdb_plan_pointer->Cast<duckdb::LogicalComparisonJoin>();
      auto simplest_join = ConstructSimplestJoin(join_op, std::move(left_child),
                                                 std::move(right_child));
      result =
          unique_ptr_cast<SimplestJoin, AQPStmt>(std::move(simplest_join));
      break;
    }
    case duckdb::LogicalOperatorType::LOGICAL_DELIM_GET: {
      auto &delim_get =
          duckdb_plan_pointer->Cast<duckdb::LogicalDelimGet>();
      std::vector<std::unique_ptr<AQPStmt>> children;
      std::vector<std::unique_ptr<SimplestAttr>> target_list;
      compat::column_ids_vector_t col_ids;
      std::string table_name =
          "delim_" + std::to_string(delim_get.table_index);
      for (duckdb::idx_t i = 0; i < delim_get.chunk_types.size(); i++) {
        col_ids.push_back(compat::MakeColumnIndex(i));
        auto col_name = "col" + std::to_string(i);
        target_list.emplace_back(std::make_unique<SimplestAttr>(
            ConvertVarType(delim_get.chunk_types[i]),
            delim_get.table_index, static_cast<duckdb::column_t>(i),
            col_name, GetPhysicalBitWidth(delim_get.chunk_types[i])));
      }
      table_column_ids_map[delim_get.table_index] = std::move(col_ids);
      auto base_stmt = std::make_unique<AQPStmt>(
          std::move(children), std::move(target_list),
          SimplestNodeType::ScanNode);
      result = std::make_unique<SimplestScan>(std::move(base_stmt),
                                              delim_get.table_index, table_name);
      break;
    }
    default:
      throw std::runtime_error(
          "DuckToIR unsupported: logical operator type " +
          LogicalOperatorToString(duckdb_plan_pointer->type));
    }

    // Set estimated cardinality for all operators
    if (result) {
      if (duckdb_plan_pointer->has_estimated_cardinality) {
        result->SetEstimatedCardinality(
            duckdb_plan_pointer->EstimateCardinality(context));
      } else {
        result->SetEstimatedCardinality(0);
      }
    }

    return result;
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
                                std::unique_ptr<AQPStmt> child) {
  auto table_index = proj_op.table_index;

  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(child));

  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  std::vector<std::unique_ptr<AQPExpr>> expr_target_list;
  bool has_expr_targets = false;
  for (const auto &expr : proj_op.expressions) {
    auto *col_expr = TryUnwrapCastToColumnRef(expr.get());
    if (col_expr) {
      auto &column_ref_expr = col_expr->Cast<duckdb::BoundColumnRefExpression>();
      auto actual_column_idx =
          ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                                   column_ref_expr.binding.column_index);
      std::string actual_column_name =
          ResolveColumnName(column_ref_expr.binding.table_index,
                            actual_column_idx, column_ref_expr.alias);
      auto simplest_target = std::make_unique<SimplestAttr>(
          ConvertVarType(column_ref_expr.return_type),
          column_ref_expr.binding.table_index, actual_column_idx,
          actual_column_name,
          GetPhysicalBitWidth(column_ref_expr.return_type));
      target_list.emplace_back(std::move(simplest_target));
      expr_target_list.emplace_back(nullptr);
    } else {
      auto aqp_expr = ConvertExprToAQPExpr(expr.get());
      // Placeholder attr with table_index=0, col_index=0
      auto placeholder = std::make_unique<SimplestAttr>(
          ConvertVarType(expr->return_type), 0, 0, expr->GetName());
      target_list.emplace_back(std::move(placeholder));
      expr_target_list.emplace_back(std::move(aqp_expr));
      has_expr_targets = true;
    }
  }
  // Register the projection's output bindings so parent joins can resolve
  // column references through this projection's table_index.
  compat::column_ids_vector_t proj_ids;
  for (duckdb::idx_t i = 0; i < proj_op.expressions.size(); i++) {
    proj_ids.push_back(compat::MakeColumnIndex(i));
  }
  table_column_ids_map[table_index] = std::move(proj_ids);

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(children), std::move(target_list),
      SimplestNodeType::ProjectionNode);
  if (has_expr_targets)
    base_stmt->expr_target_list = std::move(expr_target_list);

  auto simplest_projection =
      std::make_unique<SimplestProjection>(std::move(base_stmt), table_index);

  return simplest_projection;
}

std::unique_ptr<SimplestAggregate>
DuckToIR::ConstructSimplestAggGroup(duckdb::LogicalAggregate &agg_group_op,
                                    std::unique_ptr<AQPStmt> child) {
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  // add table_expr of group by
  std::unique_ptr<SimplestAttr> simplest_attr;
  std::vector<std::unique_ptr<SimplestAttr>> groups;
  if (!agg_group_op.groups.empty()) {
    compat::column_ids_vector_t group_ids;
    for (duckdb::idx_t i = 0; i < agg_group_op.groups.size(); i++) {
      group_ids.push_back(compat::MakeColumnIndex(i));
    }
    table_column_ids_map[agg_group_op.group_index] = group_ids;
  }
  compat::column_ids_vector_t agg_ids;
  for (duckdb::idx_t i = 0; i < agg_group_op.expressions.size(); i++) {
    agg_ids.push_back(compat::MakeColumnIndex(i));
  }
  table_column_ids_map[agg_group_op.aggregate_index] = agg_ids;

  std::vector<std::unique_ptr<AQPExpr>> group_exprs_vec;
  for (duckdb::idx_t gi = 0; gi < agg_group_op.groups.size(); gi++) {
    auto &group_expr = agg_group_op.groups[gi];
    auto *grp_col = TryUnwrapCastToColumnRef(group_expr.get());
    if (grp_col) {
      auto &column_ref_expr =
          grp_col->Cast<duckdb::BoundColumnRefExpression>();
      auto actual_col_idx =
          ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                                   column_ref_expr.binding.column_index);
      simplest_attr = std::make_unique<SimplestAttr>(
          ConvertVarType(column_ref_expr.return_type),
          column_ref_expr.binding.table_index, actual_col_idx,
          ResolveColumnName(column_ref_expr.binding.table_index, actual_col_idx,
                            column_ref_expr.alias),
          GetPhysicalBitWidth(column_ref_expr.return_type));
      groups.emplace_back(std::move(simplest_attr));
      group_exprs_vec.push_back(nullptr);
    } else {
      auto aqp_expr = ConvertExprToAQPExpr(group_expr.get());
      simplest_attr = std::make_unique<SimplestAttr>(
          SimplestVarType::IntVar, 0, static_cast<duckdb::column_t>(gi),
          "grp_expr_" + std::to_string(gi), 32);
      groups.emplace_back(std::move(simplest_attr));
      group_exprs_vec.push_back(std::move(aqp_expr));
    }
  }

  // set agg_index and group_index
  unsigned int agg_index = agg_group_op.aggregate_index;
  unsigned int group_index = agg_group_op.group_index;

  agg_fn_pair agg_fns;
  std::vector<std::unique_ptr<AQPExpr>> agg_fn_exprs;

  // add table_expr of aggregate op expression
  std::vector<bool> distinct_flags;
  for (const auto &agg_expr : agg_group_op.expressions) {
#ifdef DEBUG
    D_ASSERT(ExpressionType::BOUND_AGGREGATE == agg_expr->type);
#endif
    auto &aggregate_expr = agg_expr->Cast<duckdb::BoundAggregateExpression>();
    std::string agg_fn_type = aggregate_expr.function.name;
    bool is_distinct = aggregate_expr.IsDistinct();
    if (aggregate_expr.filter) {
      throw std::runtime_error(
          "DuckToIR unsupported: FILTER clause on aggregate " + agg_fn_type);
    }
    if (aggregate_expr.children.empty() && agg_fn_type == "count_star") {
      agg_fns.emplace_back(nullptr, SimplestAggFnType::CountStar);
      agg_fn_exprs.push_back(nullptr);
      distinct_flags.push_back(is_distinct);
    } else {
      for (const auto &expr : aggregate_expr.children) {
        auto *agg_col = TryUnwrapCastToColumnRef(expr.get());
        if (agg_col) {
          auto &column_ref_expr = agg_col->Cast<duckdb::BoundColumnRefExpression>();
          auto actual_col_idx =
              ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                                       column_ref_expr.binding.column_index);
          simplest_attr = std::make_unique<SimplestAttr>(
              ConvertVarType(column_ref_expr.return_type),
              column_ref_expr.binding.table_index, actual_col_idx,
              ResolveColumnName(column_ref_expr.binding.table_index, actual_col_idx,
                                column_ref_expr.alias),
              GetPhysicalBitWidth(column_ref_expr.return_type));
          agg_fns.emplace_back(std::make_pair(std::move(simplest_attr),
                                              ConvertAggFnType(agg_fn_type)));
          agg_fn_exprs.push_back(nullptr);
        } else {
          auto aqp_expr = ConvertExprToAQPExpr(expr.get());
          agg_fns.emplace_back(nullptr, ConvertAggFnType(agg_fn_type));
          agg_fn_exprs.push_back(std::move(aqp_expr));
        }
        distinct_flags.push_back(is_distinct);
      }
    }
  }

  auto base_stmt = std::make_unique<AQPStmt>(
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
  simplest_aggregate->agg_fn_exprs = std::move(agg_fn_exprs);
  simplest_aggregate->agg_distinct = std::move(distinct_flags);
  simplest_aggregate->group_exprs = std::move(group_exprs_vec);

  if (agg_group_op.grouping_sets.size() > 1 ||
      (agg_group_op.grouping_sets.size() == 1 &&
       agg_group_op.grouping_sets[0].size() !=
           simplest_aggregate->groups.size())) {
    for (const auto &gs : agg_group_op.grouping_sets) {
      std::set<ir_sql_converter::idx_t> converted;
      for (auto v : gs)
        converted.insert(static_cast<ir_sql_converter::idx_t>(v));
      simplest_aggregate->grouping_sets.push_back(std::move(converted));
    }
  }

  return simplest_aggregate;
}

std::unique_ptr<SimplestOrderBy>
DuckToIR::ConstructSimplestOrderBy(duckdb::LogicalOrder &order_op,
                                   std::unique_ptr<AQPStmt> child) {
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  std::vector<OrderStruct> orders;
  OrderStruct order_struct;
  std::unique_ptr<SimplestAttr> simplest_attr;
  for (duckdb::idx_t i = 0; i < order_op.orders.size(); i++) {
    auto &order = order_op.orders[i];
    order_struct.order_type = ConvertOrderType(order.type);
    auto *ord_col = TryUnwrapCastToColumnRef(order.expression.get());
    if (ord_col) {
      auto &column_ref_expr =
          ord_col->Cast<duckdb::BoundColumnRefExpression>();
      auto actual_col_idx =
          ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                                   column_ref_expr.binding.column_index);
      simplest_attr = std::make_unique<SimplestAttr>(
          ConvertVarType(column_ref_expr.return_type),
          column_ref_expr.binding.table_index, actual_col_idx,
          ResolveColumnName(column_ref_expr.binding.table_index, actual_col_idx,
                            column_ref_expr.alias),
          GetPhysicalBitWidth(column_ref_expr.return_type));
    } else {
      simplest_attr = std::make_unique<SimplestAttr>(
          SimplestVarType::IntVar, 0, static_cast<duckdb::column_t>(i),
          "ord_expr_" + std::to_string(i), 32);
    }
    order_struct.attr = std::move(simplest_attr);
    orders.emplace_back(std::move(order_struct));
  }
  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(children), std::move(target_list), SimplestNodeType::OrderNode);

  auto simplest_order_by = std::make_unique<SimplestOrderBy>(
      std::move(base_stmt), std::move(orders));
  return simplest_order_by;
}

std::unique_ptr<SimplestLimit>
DuckToIR::ConstructSimplestLimit(duckdb::LogicalLimit &limit_op,
                                 std::unique_ptr<AQPStmt> child) {
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(child));

  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;

  std::vector<OrderStruct> orders;
  OrderStruct order_struct;
  std::unique_ptr<SimplestAttr> simplest_attr;
  LimitVal limit_val{}, offset_val{};
  switch (limit_op.limit_val.Type()) {
  case duckdb::LimitNodeType::UNSET:
    throw std::runtime_error("DuckToIR unsupported: unset limit type");
  case duckdb::LimitNodeType::CONSTANT_VALUE: {
    limit_val.type = SimplestLimitType::CONSTANT_VALUE;
    limit_val.val = limit_op.limit_val.GetConstantValue();
    break;
  }
  default:
    throw std::runtime_error(
        "DuckToIR unsupported: limit type " +
        std::to_string(static_cast<int>(limit_op.limit_val.Type())));
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
    throw std::runtime_error(
        "DuckToIR unsupported: offset type " +
        std::to_string(static_cast<int>(limit_op.offset_val.Type())));
  }

  auto base_stmt = std::make_unique<AQPStmt>(std::move(children),
                                                  SimplestNodeType::LimitNode);

  auto simplest_limit = std::make_unique<SimplestLimit>(std::move(base_stmt),
                                                        limit_val, offset_val);
  return simplest_limit;
}

std::unique_ptr<SimplestCrossProduct> DuckToIR::ConstructSimplestCrossProduct(
    duckdb::LogicalCrossProduct &cross_product_op,
    std::unique_ptr<AQPStmt> left_child,
    std::unique_ptr<AQPStmt> right_child) {
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(left_child));
  children.emplace_back(std::move(right_child));
  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(children), SimplestNodeType::CrossProductNode);
  auto simplest_cross_product =
      std::make_unique<SimplestCrossProduct>(std::move(base_stmt));

  return simplest_cross_product;
}

std::unique_ptr<SimplestJoin>
DuckToIR::ConstructSimplestJoin(duckdb::LogicalComparisonJoin &join_op,
                                std::unique_ptr<AQPStmt> left_child,
                                std::unique_ptr<AQPStmt> right_child) {
  SimplestJoinType join_type;
  switch (join_op.join_type) {
  case duckdb::JoinType::INVALID:
    throw std::runtime_error("DuckToIR unsupported: invalid join type");
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
  case duckdb::JoinType::SEMI:
    join_type = SimplestJoinType::Semi;
    break;
  case duckdb::JoinType::ANTI:
    join_type = SimplestJoinType::Anti;
    break;
  case duckdb::JoinType::OUTER:
    join_type = SimplestJoinType::Full;
    break;
  case duckdb::JoinType::SINGLE:
    join_type = SimplestJoinType::Left;
    break;
  default:
    throw std::runtime_error(
        "DuckToIR unsupported: join type " +
        std::to_string(static_cast<int>(join_op.join_type)));
  }

  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(left_child));
  children.emplace_back(std::move(right_child));
  auto base_stmt = std::make_unique<AQPStmt>(std::move(children),
                                                  SimplestNodeType::JoinNode);

  std::vector<std::unique_ptr<SimplestVarComparison>> join_conditions;
  std::vector<std::unique_ptr<AQPExpr>> general_join_quals;
  for (const auto &cond : join_op.conditions) {
    auto comp_type = ConvertCompType(cond.comparison);
    const auto &left_cond = cond.left;
    const auto &right_cond = cond.right;

    auto *left_col_expr = TryUnwrapCastToColumnRef(left_cond.get());
    auto *right_col_expr = TryUnwrapCastToColumnRef(right_cond.get());

    if (left_col_expr && right_col_expr) {
      auto &lr = left_col_expr->Cast<duckdb::BoundColumnRefExpression>();
      auto left_idx = ResolveDuckDBColumnIndex(lr.binding.table_index,
                                                lr.binding.column_index);
      auto left_name = ResolveColumnName(lr.binding.table_index, left_idx,
                                          lr.alias);
      auto left_attr = std::make_unique<SimplestAttr>(
          ConvertVarType(left_cond->return_type), lr.binding.table_index,
          left_idx, left_name, GetPhysicalBitWidth(left_cond->return_type));

      auto &rr = right_col_expr->Cast<duckdb::BoundColumnRefExpression>();
      auto right_idx = ResolveDuckDBColumnIndex(rr.binding.table_index,
                                                 rr.binding.column_index);
      auto right_name = ResolveColumnName(rr.binding.table_index, right_idx,
                                           rr.alias);
      auto right_attr = std::make_unique<SimplestAttr>(
          ConvertVarType(right_cond->return_type), rr.binding.table_index,
          right_idx, right_name, GetPhysicalBitWidth(right_cond->return_type));

      join_conditions.emplace_back(std::make_unique<SimplestVarComparison>(
          comp_type, std::move(left_attr), std::move(right_attr)));
    } else {
      auto left_aqp = ConvertExprToAQPExpr(left_cond.get());
      auto right_aqp = ConvertExprToAQPExpr(right_cond.get());
      if (!left_aqp || !right_aqp) {
        throw std::runtime_error(
            "DuckToIR unsupported: join condition expression " +
            (left_aqp ? right_cond->ToString() : left_cond->ToString()));
      }
      general_join_quals.emplace_back(
          std::make_unique<SimplestGeneralComparison>(
              comp_type, std::move(left_aqp), std::move(right_aqp)));
    }
  }

  auto simplest_join = std::make_unique<SimplestJoin>(
      std::move(base_stmt), std::move(join_conditions), join_type);
  for (auto &q : general_join_quals)
    simplest_join->qual_vec.push_back(std::move(q));

  if (join_op.join_type == duckdb::JoinType::MARK) {
    simplest_join->SetMarkIndex(join_op.mark_index);

    // Register mark_index output binding (single boolean column)
    compat::column_ids_vector_t mark_ids;
    mark_ids.push_back(compat::MakeColumnIndex(0));
    table_column_ids_map[join_op.mark_index] = mark_ids;
  }

  return simplest_join;
}

std::unique_ptr<SimplestFilter>
DuckToIR::ConstructSimplestFilter(duckdb::LogicalFilter &filter_op,
                                  std::unique_ptr<AQPStmt> child) {
  std::vector<std::unique_ptr<AQPStmt>> children;
  children.emplace_back(std::move(child));
  // todo: add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  // add qual vec
  std::vector<std::unique_ptr<AQPExpr>> qual_vec =
      CollectQualVecExprs(filter_op.expressions);

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(children), std::move(target_list), std::move(qual_vec),
      SimplestNodeType::FilterNode);

  auto simplest_filter = std::make_unique<SimplestFilter>(std::move(base_stmt));

  return simplest_filter;
}

std::unique_ptr<SimplestScan>
DuckToIR::ConstructSimplestScan(duckdb::LogicalGet &get_op) {
  auto table_index = get_op.table_index;

  // Store column_ids and column names mapping for this table
  table_column_ids_map[table_index] = compat::GetLogicalGetColumnIds(get_op);
  table_column_names_map_[table_index] = get_op.names;

  // add target list
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
#ifdef DEBUG
  get_op.names.size() == get_op.returned_types.size();
#endif
  // Build target list using actual column indices from the base table
  // column_ids maps: column_ids[binding_idx] = base_table_column_idx
  auto &column_ids = compat::GetLogicalGetColumnIds(get_op);
  for (size_t binding_idx = 0; binding_idx < column_ids.size(); binding_idx++) {
    auto actual_column_id = ResolveDuckDBColumnIndex(table_index, binding_idx);
    // Store the base table column index (column_id) in SimplestAttr
    // This ensures the IR uses base table schema indices consistently
    std::unique_ptr<SimplestAttr> simplest_attr =
        std::make_unique<SimplestAttr>(
            ConvertVarType(get_op.returned_types[actual_column_id]),
            table_index, actual_column_id, get_op.names[actual_column_id],
            GetPhysicalBitWidth(get_op.returned_types[actual_column_id]));
    target_list.emplace_back(std::move(simplest_attr));
  }

  // add qual vec
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;
  for (const auto &filter : get_op.table_filters.filters) {
    auto actual_column_index = filter.first;
    auto &filter_cond = filter.second;
    auto simplest_var_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(get_op.returned_types[actual_column_index]), table_index,
        actual_column_index, get_op.names[actual_column_index],
        GetPhysicalBitWidth(get_op.returned_types[actual_column_index]));
    auto simplest_scan_filter_expr =
        CollectScanFilter(filter_cond, std::move(simplest_var_attr));
    qual_vec.emplace_back(std::move(simplest_scan_filter_expr));
  }

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ScanNode);
  auto table_name = compat::GetTableNameFromLogicalGet(get_op);
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
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ChunkNode);

  auto table_index = get_op.table_index;
  auto simplest_scan = std::make_unique<SimplestScan>(
      std::move(base_stmt), table_index, intermediate_table_name);
  return simplest_scan;
}

std::unique_ptr<SimplestChunk> DuckToIR::ConstructSimplestChunk(
    duckdb::LogicalColumnDataGet &column_data_get_op,
    const std::unordered_map<unsigned int, std::string>
        &intermediate_table_map) {
  std::vector<std::string> chunk_contents;
  duckdb::DataChunk chunk;

  auto table_index = column_data_get_op.table_index;
  std::string chunk_name;
  if (intermediate_table_map.find(table_index) !=
      intermediate_table_map.end()) {
    chunk_name = intermediate_table_map.at(table_index);
  }

  column_data_get_op.collection->InitializeScanChunk(chunk);
  duckdb::ColumnDataScanState scan_state;
  column_data_get_op.collection->InitializeScan(scan_state);
  compat::column_ids_vector_t chunk_column_idx;
  auto column_bindings = column_data_get_op.GetColumnBindings();
  for (const auto &bind_pair : column_bindings) {
    chunk_column_idx.emplace_back(
        compat::MakeColumnIndex(bind_pair.column_index));
  }
  table_column_ids_map[table_index] = chunk_column_idx;

  while (column_data_get_op.collection->Scan(scan_state, chunk)) {
    for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
      for (idx_t j = 0; j < chunk.size(); j++) {
        chunk_contents.emplace_back(chunk.data[i].GetValue(j).ToString());
      }
    }
  }

  // Build target_list with column type info (needed to reconstruct schema)
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  auto &types = column_data_get_op.types;
  for (size_t i = 0; i < types.size(); i++) {
    auto simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(types[i]), table_index, i, "col" + std::to_string(i),
        GetPhysicalBitWidth(types[i]));
    target_list.emplace_back(std::move(simplest_attr));
  }

  // todo: add qual vec
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ChunkNode);
  auto simplest_chunk = std::make_unique<SimplestChunk>(
      std::move(base_stmt), column_data_get_op.table_index, chunk_contents);
  if (!chunk_name.empty()) {
    simplest_chunk->SetChunkName(chunk_name);
  }
  return simplest_chunk;
}

std::unique_ptr<SimplestChunk> DuckToIR::ConstructSimplestChunkPlaceholder(
    duckdb::LogicalColumnDataGet &column_data_get_op,
    const std::unordered_map<unsigned int, std::string>
        &intermediate_table_map) {
  // Create a placeholder SimplestChunk for intermediate results (same-engine)
  // Contents are empty, but target_list has column type info for reconstruction
  auto table_index = column_data_get_op.table_index;
  std::string chunk_name;
  if (intermediate_table_map.find(table_index) !=
      intermediate_table_map.end()) {
    chunk_name = intermediate_table_map.at(table_index);
  }
  uint64_t estimated_card = column_data_get_op.EstimateCardinality(context);

  // Register column mapping
  compat::column_ids_vector_t chunk_column_idx;
  auto column_bindings = column_data_get_op.GetColumnBindings();
  for (const auto &bind_pair : column_bindings) {
    chunk_column_idx.emplace_back(
        compat::MakeColumnIndex(bind_pair.column_index));
  }
  table_column_ids_map[table_index] = chunk_column_idx;

  // Build target_list with column type info (needed to reconstruct schema)
  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  auto &types = column_data_get_op.types;
  for (size_t i = 0; i < types.size(); i++) {
    auto simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(types[i]), table_index, i, "col" + std::to_string(i),
        GetPhysicalBitWidth(types[i]));
    target_list.emplace_back(std::move(simplest_attr));
  }

  // Empty contents - data will be provided at execution time
  std::vector<std::string> empty_contents;
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ChunkNode);
  auto simplest_chunk = std::make_unique<SimplestChunk>(
      std::move(base_stmt), table_index, empty_contents);
  if (!chunk_name.empty()) {
    simplest_chunk->SetChunkName(chunk_name);
  }
  simplest_chunk->SetEstimatedCardinality(estimated_card);
  return simplest_chunk;
}

std::unique_ptr<SimplestChunk> DuckToIR::ConstructSimplestChunkFromGet(
    duckdb::LogicalGet &get_op,
    const std::unordered_map<unsigned int, std::string>
        &intermediate_table_map) {
  auto table_index = get_op.table_index;
  std::string chunk_name;
  auto it = intermediate_table_map.find(table_index);
  if (it != intermediate_table_map.end()) {
    chunk_name = it->second;
  }
  uint64_t estimated_card = get_op.EstimateCardinality(context);

  auto &column_ids = compat::GetLogicalGetColumnIds(get_op);
  table_column_ids_map[table_index] = column_ids;

  std::vector<std::unique_ptr<SimplestAttr>> target_list;
  for (size_t binding_idx = 0; binding_idx < column_ids.size(); binding_idx++) {
    auto actual_col_id = ResolveDuckDBColumnIndex(table_index, binding_idx);
    auto col_name =
        ResolveColumnName(table_index, actual_col_id,
                          actual_col_id < get_op.names.size()
                              ? get_op.names[actual_col_id]
                              : "col" + std::to_string(actual_col_id));
    auto simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(get_op.returned_types[actual_col_id]), table_index,
        actual_col_id, col_name,
        GetPhysicalBitWidth(get_op.returned_types[actual_col_id]));
    target_list.emplace_back(std::move(simplest_attr));
  }

  std::vector<std::string> empty_contents;
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;

  auto base_stmt = std::make_unique<AQPStmt>(
      std::move(target_list), std::move(qual_vec), SimplestNodeType::ChunkNode);
  auto simplest_chunk = std::make_unique<SimplestChunk>(
      std::move(base_stmt), table_index, empty_contents);
  if (!chunk_name.empty()) {
    simplest_chunk->SetChunkName(chunk_name);
  }
  simplest_chunk->SetEstimatedCardinality(estimated_card);
  return simplest_chunk;
}

SimplestExprType DuckToIR::ConvertCompType(duckdb::ExpressionType type) {
  switch (type) {
  case duckdb::ExpressionType::COMPARE_EQUAL:
  case duckdb::ExpressionType::COMPARE_NOT_DISTINCT_FROM:
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
    return SimplestExprType::InType;
  case duckdb::ExpressionType::COMPARE_NOT_IN:
    return SimplestExprType::NotInType;
  case duckdb::ExpressionType::CONJUNCTION_AND:
  case duckdb::ExpressionType::CONJUNCTION_OR:
    return SimplestExprType::LogicalOp;
  default:
    throw std::runtime_error(
        "DuckToIR unsupported: comparison expression type " +
        std::to_string(static_cast<int>(type)));
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
    throw std::runtime_error("DuckToIR unsupported: logical op type " +
                             std::to_string(static_cast<int>(type)));
  }
}

SimplestVarType DuckToIR::ConvertVarType(const duckdb::LogicalType &type) {
  switch (type.id()) {
  case duckdb::LogicalTypeId::BOOLEAN:
    return SimplestVarType::BoolVar;
  case duckdb::LogicalTypeId::TINYINT:
  case duckdb::LogicalTypeId::SMALLINT:
  case duckdb::LogicalTypeId::INTEGER:
  case duckdb::LogicalTypeId::BIGINT:
  case duckdb::LogicalTypeId::HUGEINT:
  case duckdb::LogicalTypeId::UTINYINT:
  case duckdb::LogicalTypeId::USMALLINT:
  case duckdb::LogicalTypeId::UINTEGER:
  case duckdb::LogicalTypeId::UBIGINT:
    return SimplestVarType::IntVar;
  case duckdb::LogicalTypeId::FLOAT:
  case duckdb::LogicalTypeId::DECIMAL:
  case duckdb::LogicalTypeId::DOUBLE:
    return SimplestVarType::FloatVar;
  case duckdb::LogicalTypeId::VARCHAR:
    return SimplestVarType::StringVar;
  case duckdb::LogicalTypeId::DATE:
    return SimplestVarType::Date;
  case duckdb::LogicalTypeId::TIMESTAMP:
  case duckdb::LogicalTypeId::TIMESTAMP_TZ:
  case duckdb::LogicalTypeId::TIMESTAMP_NS:
  case duckdb::LogicalTypeId::TIMESTAMP_MS:
  case duckdb::LogicalTypeId::TIMESTAMP_SEC:
    return SimplestVarType::TimestampVar;
  case duckdb::LogicalTypeId::TIME:
  case duckdb::LogicalTypeId::TIME_TZ:
    return SimplestVarType::StringVar;
  case duckdb::LogicalTypeId::INTERVAL:
    return SimplestVarType::IntervalVar;
  default:
    throw std::runtime_error("DuckToIR unsupported: var type " +
                             type.ToString());
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
  else if (agg_fn_type == "count")
    return SimplestAggFnType::Count;
  else if (agg_fn_type == "count_star")
    return SimplestAggFnType::CountStar;
  else if (agg_fn_type == "stddev_samp")
    return SimplestAggFnType::StddevSamp;
  else
    throw std::runtime_error("DuckToIR unsupported: aggregate function " +
                             agg_fn_type);
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

duckdb::column_t DuckToIR::ResolveDuckDBColumnIndex(duckdb::idx_t table_idx,
                                                    duckdb::idx_t binding_idx) {
  auto it = table_column_ids_map.find(table_idx);
  if (it == table_column_ids_map.end() || binding_idx >= it->second.size()) {
    throw std::runtime_error("Didn't find the column index mapping of (" +
                             std::to_string(table_idx) + ", " +
                             std::to_string(binding_idx) + ")");
  }
  auto column_id = compat::GetColumnId(it->second[binding_idx]);
  if (column_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    std::cout << "warning: return COLUMN_IDENTIFIER_ROW_ID\n";
    return binding_idx; // Return binding index for row ID
  }
  return column_id;
}

std::unique_ptr<SimplestAttr>
DuckToIR::ConvertAttr(const duckdb::unique_ptr<duckdb::Expression> &expr) {
  auto *col_expr = UnwrapToColumnRef(expr.get());
  D_ASSERT(col_expr->expression_class ==
           duckdb::ExpressionClass::BOUND_COLUMN_REF);
  auto &column_ref_expr = col_expr->Cast<duckdb::BoundColumnRefExpression>();
  // Resolve binding index to actual column index using column_ids mapping
  auto actual_column_idx =
      ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                               column_ref_expr.binding.column_index);

  // Get the correct column name (use chunk_col_names_ for temp tables)
  std::string actual_column_name =
      ResolveColumnName(column_ref_expr.binding.table_index, actual_column_idx,
                        column_ref_expr.alias);

  auto simplest_attr = std::make_unique<SimplestAttr>(
      ConvertVarType(column_ref_expr.return_type),
      column_ref_expr.binding.table_index, actual_column_idx,
      actual_column_name, GetPhysicalBitWidth(column_ref_expr.return_type));
  return simplest_attr;
}

std::unique_ptr<SimplestConstVar>
DuckToIR::ConvertConstVar(const duckdb::Value &value, const std::string &prefix,
                          const std::string &appendix) {
  std::unique_ptr<SimplestConstVar> simplest_attr;

  switch (value.type().id()) {
  case duckdb::LogicalTypeId::TINYINT:
  case duckdb::LogicalTypeId::SMALLINT:
  case duckdb::LogicalTypeId::INTEGER:
  case duckdb::LogicalTypeId::BIGINT: {
    int64_t int_val = value.GetValue<int64_t>();
    if (int_val < std::numeric_limits<int>::min() ||
        int_val > std::numeric_limits<int>::max()) {
      throw std::runtime_error(
          "DuckToIR unsupported: integer constant out of 32-bit range: " +
          std::to_string(int_val));
    }
    simplest_attr =
        std::make_unique<SimplestConstVar>(static_cast<int>(int_val));
    break;
  }
  case duckdb::LogicalTypeId::FLOAT:
  case duckdb::LogicalTypeId::DOUBLE:
  case duckdb::LogicalTypeId::DECIMAL: {
    double float_val = value.GetValue<double>();
    simplest_attr =
        std::make_unique<SimplestConstVar>(static_cast<float>(float_val));
    break;
  }
  case duckdb::LogicalTypeId::VARCHAR: {
    std::string str = prefix + value.ToString() + appendix;
    simplest_attr = std::make_unique<SimplestConstVar>(str);
    break;
  }
  case duckdb::LogicalTypeId::BOOLEAN: {
    bool bool_val = value.GetValue<bool>();
    simplest_attr = std::make_unique<SimplestConstVar>(bool_val);
    break;
  }
  case duckdb::LogicalTypeId::DATE:
  case duckdb::LogicalTypeId::TIME:
  case duckdb::LogicalTypeId::TIME_TZ: {
    std::string str = prefix + value.ToString() + appendix;
    simplest_attr = std::make_unique<SimplestConstVar>(str);
    break;
  }
  case duckdb::LogicalTypeId::TIMESTAMP:
  case duckdb::LogicalTypeId::TIMESTAMP_TZ:
  case duckdb::LogicalTypeId::TIMESTAMP_NS:
  case duckdb::LogicalTypeId::TIMESTAMP_MS:
  case duckdb::LogicalTypeId::TIMESTAMP_SEC: {
    std::string str = prefix + value.ToString() + appendix;
    simplest_attr = std::make_unique<SimplestConstVar>(str);
    simplest_attr->ChangeVarType(TimestampVar);
    break;
  }
  case duckdb::LogicalTypeId::INTERVAL: {
    std::string str = value.ToString();
    simplest_attr = std::make_unique<SimplestConstVar>(str);
    simplest_attr->ChangeVarType(IntervalVar);
    break;
  }
  default:
    throw std::runtime_error(
        "DuckToIR unsupported: constant value type " +
        LogicalTypeIdToString(value.type().id()));
  }
  return simplest_attr;
}

std::unique_ptr<AQPExpr>
DuckToIR::ConvertExprToAQPExpr(const duckdb::Expression *expr) {
  if (!expr)
    throw std::runtime_error("DuckToIR: null expression in "
                             "ConvertExprToAQPExpr");

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto &col = expr->Cast<duckdb::BoundColumnRefExpression>();
    auto actual_col = ResolveDuckDBColumnIndex(col.binding.table_index,
                                               col.binding.column_index);
    auto name = ResolveColumnName(col.binding.table_index, actual_col, col.alias);
    auto attr = std::make_unique<SimplestAttr>(
        ConvertVarType(col.return_type), col.binding.table_index, actual_col, name,
        GetPhysicalBitWidth(col.return_type));
    return std::make_unique<SimplestSingleAttrExpr>(std::move(attr));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    auto &c = expr->Cast<duckdb::BoundConstantExpression>();
    auto cv = ConvertConstVar(c.value);
    return std::make_unique<SimplestConstExpr>(std::move(cv));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    auto &cast = expr->Cast<duckdb::BoundCastExpression>();
    auto child_expr = ConvertExprToAQPExpr(cast.child.get());
    return std::make_unique<SimplestCastExpr>(
        std::move(child_expr), ConvertVarType(cast.return_type));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto &fn = expr->Cast<duckdb::BoundFunctionExpression>();
    if (fn.children.size() == 2) {
      SimplestArithOp arith_op = ArithInvalid;
      const auto &name = fn.function.name;
      if (name == "+" || name == "date_add" || name == "add")
        arith_op = ArithAdd;
      else if (name == "-" || name == "date_sub" || name == "subtract" ||
               name == "date_diff")
        arith_op = ArithSub;
      else if (name == "*" || name == "multiply")
        arith_op = ArithMul;
      else if (name == "/" || name == "divide")
        arith_op = ArithDiv;
      else if (name == "%" || name == "mod")
        arith_op = ArithMod;

      if (arith_op != ArithInvalid) {
        auto left = ConvertExprToAQPExpr(fn.children[0].get());
        auto right = ConvertExprToAQPExpr(fn.children[1].get());
        return std::make_unique<SimplestArithExpr>(
            arith_op, std::move(left), std::move(right),
            ConvertVarType(fn.return_type));
      }
    }
    // Generic function expression (substring, etc.)
    std::vector<std::unique_ptr<AQPExpr>> args;
    for (auto &child : fn.children) {
      args.push_back(ConvertExprToAQPExpr(child.get()));
    }
    return std::make_unique<SimplestFunctionExpr>(
        fn.function.name, std::move(args));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_OPERATOR) {
    auto &op = expr->Cast<duckdb::BoundOperatorExpression>();
    if (expr->type == duckdb::ExpressionType::OPERATOR_COALESCE) {
      std::vector<std::unique_ptr<AQPExpr>> args;
      for (auto &child : op.children) {
        args.push_back(ConvertExprToAQPExpr(child.get()));
      }
      return std::make_unique<SimplestFunctionExpr>(
          "COALESCE", std::move(args));
    }
    if (expr->type == duckdb::ExpressionType::OPERATOR_IS_NULL ||
        expr->type == duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
      auto *col = TryUnwrapCastToColumnRef(op.children[0].get());
      if (col) {
        auto &cr = col->Cast<duckdb::BoundColumnRefExpression>();
        auto actual_col = ResolveDuckDBColumnIndex(
            cr.binding.table_index, cr.binding.column_index);
        auto attr = std::make_unique<SimplestAttr>(
            ConvertVarType(cr.return_type), cr.binding.table_index,
            actual_col,
            ResolveColumnName(cr.binding.table_index, actual_col, cr.alias),
            GetPhysicalBitWidth(cr.return_type));
        auto null_type =
            expr->type == duckdb::ExpressionType::OPERATOR_IS_NULL
                ? SimplestExprType::NullType
                : SimplestExprType::NonNullType;
        return std::make_unique<SimplestIsNullExpr>(null_type,
                                                    std::move(attr));
      }
    }
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CASE) {
    auto &case_expr = expr->Cast<duckdb::BoundCaseExpression>();
    std::vector<CaseWhenClause> when_clauses;
    for (auto &check : case_expr.case_checks) {
      auto when_expr = ConvertExprToAQPExpr(check.when_expr.get());
      auto then_expr = ConvertExprToAQPExpr(check.then_expr.get());
      when_clauses.push_back({std::move(when_expr), std::move(then_expr)});
    }
    auto else_expr = ConvertExprToAQPExpr(case_expr.else_expr.get());
    return std::make_unique<SimplestCaseExpr>(
        std::move(when_clauses), std::move(else_expr));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_COMPARISON) {
    auto &cmp = expr->Cast<duckdb::BoundComparisonExpression>();
    auto *left_col = TryUnwrapCastToColumnRef(cmp.left.get());
    if (left_col &&
        cmp.right->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
      auto left_attr = ConvertAttr(cmp.left);
      auto &right_const = cmp.right->Cast<duckdb::BoundConstantExpression>();
      auto right_cv = ConvertConstVar(right_const.value);
      return std::make_unique<SimplestVarConstComparison>(
          ConvertCompType(cmp.type), std::move(left_attr),
          std::move(right_cv));
    }
    auto *right_col = TryUnwrapCastToColumnRef(cmp.right.get());
    if (left_col && right_col) {
      auto left_attr = ConvertAttr(cmp.left);
      auto right_attr = ConvertAttr(cmp.right);
      return std::make_unique<SimplestVarComparison>(
          ConvertCompType(cmp.type), std::move(left_attr),
          std::move(right_attr));
    }
    std::string op_name;
    switch (cmp.type) {
    case duckdb::ExpressionType::COMPARE_EQUAL: op_name = "="; break;
    case duckdb::ExpressionType::COMPARE_NOTEQUAL: op_name = "!="; break;
    case duckdb::ExpressionType::COMPARE_LESSTHAN: op_name = "<"; break;
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO: op_name = "<="; break;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN: op_name = ">"; break;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: op_name = ">="; break;
    default: op_name = "??"; break;
    }
    auto left = ConvertExprToAQPExpr(cmp.left.get());
    auto right = ConvertExprToAQPExpr(cmp.right.get());
    std::vector<std::unique_ptr<AQPExpr>> args;
    args.push_back(std::move(left));
    args.push_back(std::move(right));
    return std::make_unique<SimplestFunctionExpr>(
        "__infix_" + op_name, std::move(args));
  }

  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    auto &conj = expr->Cast<duckdb::BoundConjunctionExpression>();
    std::string conj_name =
        conj.type == duckdb::ExpressionType::CONJUNCTION_AND
            ? "__infix_AND"
            : "__infix_OR";
    std::vector<std::unique_ptr<AQPExpr>> args;
    for (auto &child : conj.children) {
      args.push_back(ConvertExprToAQPExpr(child.get()));
    }
    return std::make_unique<SimplestFunctionExpr>(conj_name, std::move(args));
  }

  throw std::runtime_error("DuckToIR unsupported: expression class " +
                           std::to_string(static_cast<int>(
                               expr->expression_class)) +
                           " in " + expr->ToString());
}

std::unique_ptr<AQPExpr>
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
          ConvertConstVar(right_expr.value, prefix_str, appendix_str);

      auto simplest_var_const_comp =
          std::make_unique<SimplestVarConstComparison>(
              simplest_expr_type, std::move(left_simplest_attr),
              std::move(right_simplest_attr));
      return unique_ptr_cast<SimplestVarConstComparison, AQPExpr>(
          std::move(simplest_var_const_comp));
    } else {
      // Generic function in filter context → use ConvertExprToAQPExpr
      return ConvertExprToAQPExpr(expr.get());
    }
  }
  case duckdb::ExpressionType::CASE_EXPR: {
    auto &case_expr = expr->Cast<duckdb::BoundCaseExpression>();
    std::vector<CaseWhenClause> checks;
    for (auto &check : case_expr.case_checks) {
      CaseWhenClause clause;
      clause.when_expr = ConvertExpr(check.when_expr);
      clause.then_expr = ConvertExpr(check.then_expr);
      checks.push_back(std::move(clause));
    }
    std::unique_ptr<AQPExpr> else_result =
        case_expr.else_expr ? ConvertExpr(case_expr.else_expr) : nullptr;
    return std::make_unique<SimplestCaseExpr>(std::move(checks),
                                             std::move(else_result));
  }
  case duckdb::ExpressionType::COMPARE_NOTEQUAL:
  case duckdb::ExpressionType::COMPARE_EQUAL:
  case duckdb::ExpressionType::COMPARE_GREATERTHAN:
  case duckdb::ExpressionType::COMPARE_LESSTHAN:
  case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
  case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO: {
    auto &comparison_expr = expr->Cast<duckdb::BoundComparisonExpression>();
    auto &left_expr_duck = comparison_expr.left;
    auto &right_expr_duck = comparison_expr.right;

    // Fast path: left is column ref (possibly cast-wrapped)
    bool left_is_col = TryUnwrapCastToColumnRef(left_expr_duck.get()) != nullptr;

    if (left_is_col && right_expr_duck->expression_class ==
                           duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      // col vs col → VarComparison
      auto left_attr = ConvertAttr(left_expr_duck);
      auto right_attr = ConvertAttr(right_expr_duck);
      return std::make_unique<SimplestVarComparison>(
          ConvertCompType(expr->type), std::move(left_attr),
          std::move(right_attr));
    }

    // col vs constant (possibly cast-wrapped)
    duckdb::BoundConstantExpression *const_ptr = nullptr;
    if (right_expr_duck->expression_class ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      const_ptr =
          &right_expr_duck->Cast<duckdb::BoundConstantExpression>();
    } else if (right_expr_duck->expression_class ==
               duckdb::ExpressionClass::BOUND_CAST) {
      auto &cast = right_expr_duck->Cast<duckdb::BoundCastExpression>();
      if (cast.child->expression_class ==
          duckdb::ExpressionClass::BOUND_CONSTANT)
        const_ptr =
            &cast.child->Cast<duckdb::BoundConstantExpression>();
    }

    if (left_is_col && const_ptr) {
      auto left_attr = ConvertAttr(left_expr_duck);
      auto right_const = ConvertConstVar(const_ptr->value);
      return std::make_unique<SimplestVarConstComparison>(
          ConvertCompType(expr->type), std::move(left_attr),
          std::move(right_const));
    }

    // General path: one or both sides are complex expressions
    {
      auto left_aqp = ConvertExprToAQPExpr(left_expr_duck.get());
      auto right_aqp = ConvertExprToAQPExpr(right_expr_duck.get());
      return std::make_unique<SimplestGeneralComparison>(
          ConvertCompType(expr->type), std::move(left_aqp),
          std::move(right_aqp));
    }
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
  case duckdb::ExpressionType::COMPARE_BETWEEN: {
    auto &between_expr = expr->Cast<duckdb::BoundBetweenExpression>();

    auto make_bound = [&](const duckdb::unique_ptr<duckdb::Expression> &input,
                          const duckdb::unique_ptr<duckdb::Expression> &bound,
                          SimplestExprType cmp) -> std::unique_ptr<AQPExpr> {
      if (TryUnwrapCastToColumnRef(input.get())) {
        if (bound->expression_class ==
            duckdb::ExpressionClass::BOUND_CONSTANT) {
          auto &ce = bound->Cast<duckdb::BoundConstantExpression>();
          auto cv = ConvertConstVar(ce.value);
          return std::make_unique<SimplestVarConstComparison>(
              cmp, ConvertAttr(input), std::move(cv));
        }
        if (bound->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
          auto &cast = bound->Cast<duckdb::BoundCastExpression>();
          if (cast.child->expression_class ==
              duckdb::ExpressionClass::BOUND_CONSTANT) {
            auto &ce = cast.child->Cast<duckdb::BoundConstantExpression>();
            auto cv = ConvertConstVar(ce.value);
            return std::make_unique<SimplestVarConstComparison>(
                cmp, ConvertAttr(input), std::move(cv));
          }
        }
      }
      auto left_aqp = ConvertExprToAQPExpr(input.get());
      auto right_aqp = ConvertExprToAQPExpr(bound.get());
      return std::make_unique<SimplestGeneralComparison>(
          cmp, std::move(left_aqp), std::move(right_aqp));
    };

    auto lower_comp = make_bound(between_expr.input, between_expr.lower,
                                 SimplestExprType::GreaterEqual);
    auto upper_comp = make_bound(between_expr.input, between_expr.upper,
                                 SimplestExprType::LessEqual);
    return std::make_unique<SimplestLogicalExpr>(
        LogicalAnd, std::move(lower_comp), std::move(upper_comp));

    //    // Convert expressions - need to call ConvertExpr twice for input
    //    since we use it twice auto input_expr1 =
    //    ConvertExpr(between_expr.input); auto lower_expr =
    //    ConvertExpr(between_expr.lower); auto input_expr2 =
    //    ConvertExpr(between_expr.input);  // Second copy for upper comparison
    //    auto upper_expr = ConvertExpr(between_expr.upper);
    //
    //    // Downcast for first comparison: input >= lower
    //    auto input_attr1 = std::unique_ptr<SimplestAttr>(
    //        static_cast<SimplestAttr*>(input_expr1.release())
    //    );
    //    auto lower_const = std::unique_ptr<SimplestConstVar>(
    //        static_cast<SimplestConstVar*>(lower_expr.release())
    //    );
    //
    //    auto lower_comp = std::make_unique<SimplestVarConstComparison>(
    //        SimplestExprType::GreaterEqual,
    //        std::move(input_attr1),
    //        std::move(lower_const)
    //    );
    //
    //    // Downcast for second comparison: input <= upper
    //    auto input_attr2 = std::unique_ptr<SimplestAttr>(
    //        static_cast<SimplestAttr*>(input_expr2.release())
    //    );
    //    auto upper_const = std::unique_ptr<SimplestConstVar>(
    //        static_cast<SimplestConstVar*>(upper_expr.release())
    //    );
    //
    //    auto upper_comp = std::make_unique<SimplestVarConstComparison>(
    //        SimplestExprType::LessEqual,
    //        std::move(input_attr2),
    //        std::move(upper_const)
    //    );
    //
    //    // Combine with AND
    //    return std::make_unique<SimplestLogicalExpr>(
    //        LogicalAnd,
    //        std::move(lower_comp),
    //        std::move(upper_comp)
    //    );
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
    return unique_ptr_cast<SimplestIsNullExpr, AQPExpr>(
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
    return unique_ptr_cast<SimplestLogicalExpr, AQPExpr>(
        std::move(simplest_not_expr));
  }
  case duckdb::ExpressionType::OPERATOR_COALESCE: {
    auto &op_expr = expr->Cast<duckdb::BoundOperatorExpression>();
    std::vector<std::unique_ptr<AQPExpr>> args;
    for (const auto &child : op_expr.children) {
      args.push_back(ConvertExpr(child));
    }
    return std::make_unique<SimplestFunctionExpr>("COALESCE", std::move(args));
  }
  case duckdb::ExpressionType::BOUND_COLUMN_REF: {
#ifdef DEBUG
    D_ASSERT(ExpressionType::BOUND_COLUMN_REF == expr->type);
#endif
    auto &column_ref_expr = expr->Cast<duckdb::BoundColumnRefExpression>();
    // Resolve binding index to actual column index
    auto actual_column_idx =
        ResolveDuckDBColumnIndex(column_ref_expr.binding.table_index,
                                 column_ref_expr.binding.column_index);
    // Get the correct column name (use chunk_col_names_ for temp tables)
    std::string actual_column_name =
        ResolveColumnName(column_ref_expr.binding.table_index,
                          actual_column_idx, column_ref_expr.alias);
    auto simplest_attr = std::make_unique<SimplestAttr>(
        ConvertVarType(column_ref_expr.return_type),
        column_ref_expr.binding.table_index, actual_column_idx,
        actual_column_name,
        GetPhysicalBitWidth(column_ref_expr.return_type));
    auto simplest_attr_expr =
        std::make_unique<SimplestSingleAttrExpr>(std::move(simplest_attr));
    return unique_ptr_cast<SimplestSingleAttrExpr, AQPExpr>(
        std::move(simplest_attr_expr));
  }
  default:
    throw std::runtime_error("DuckToIR unsupported: expression type " +
                             ExpressionTypeToString(expr->type));
  }
}

std::vector<std::unique_ptr<AQPExpr>> DuckToIR::CollectQualVecExprs(
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &exprs) {
  std::vector<std::unique_ptr<AQPExpr>> qual_vec;
  for (const auto &expr : exprs) {
    auto simplest_expr = ConvertExpr(expr);
    qual_vec.emplace_back(std::move(simplest_expr));
  }

  return qual_vec;
}

std::unique_ptr<AQPExpr> DuckToIR::CollectScanFilter(
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
    auto simplest_comp_type = ConvertCompType(constant_filter.comparison_type);
    auto simplest_const_var = ConvertConstVar(constant_filter.constant);
    auto simplest_constant_comp = std::make_unique<SimplestVarConstComparison>(
        simplest_comp_type, std::make_unique<SimplestAttr>(*var_attr),
        std::move(simplest_const_var));
    return unique_ptr_cast<SimplestVarConstComparison, AQPExpr>(
        std::move(simplest_constant_comp));
  }
  case duckdb::TableFilterType::IS_NOT_NULL: {
    auto simplest_is_not_null = std::make_unique<SimplestIsNullExpr>(
        SimplestExprType::NonNullType,
        std::make_unique<SimplestAttr>(*var_attr));
    return unique_ptr_cast<SimplestIsNullExpr, AQPExpr>(
        std::move(simplest_is_not_null));
  }
  case duckdb::TableFilterType::IS_NULL: {
    auto simplest_is_null = std::make_unique<SimplestIsNullExpr>(
        SimplestExprType::NullType, std::make_unique<SimplestAttr>(*var_attr));
    return unique_ptr_cast<SimplestIsNullExpr, AQPExpr>(
        std::move(simplest_is_null));
  }
  default:
    throw std::runtime_error(
        "DuckToIR unsupported: table filter type " +
        std::to_string(static_cast<int>(filter_cond->filter_type)));
  }
}
} // namespace ir_sql_converter
