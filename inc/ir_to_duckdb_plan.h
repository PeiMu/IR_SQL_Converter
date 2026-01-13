//===----------------------------------------------------------------------===//
//                         DuckDB
//
// ir_to_duckdb_plan.h
// from DuckDB source code:
// src/include/duckdb/optimizer/converter/duckdb_to_ir.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/optimizer/query_split/split_algorithm.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "simplest_ir.h"

namespace ir_sql_converter {

class IRToDuck {
public:
  IRToDuck(duckdb::Binder &binder, duckdb::ClientContext &context)
      : binder(binder), context(context) {};
  ~IRToDuck() = default;

  duckdb::unique_ptr<duckdb::LogicalOperator>
  ConstructDuckdbPlan(const std::unique_ptr<SimplestStmt> &simplest_ir);

private:
  duckdb::unique_ptr<duckdb::LogicalComparisonJoin>
  ConstructDuckdbJoin(const SimplestJoin &ir_join,
                      duckdb::unique_ptr<duckdb::LogicalOperator> left_child,
                      duckdb::unique_ptr<duckdb::LogicalOperator> right_child);
  duckdb::unique_ptr<duckdb::LogicalGet>
  ConstructDuckdbScan(const SimplestScan &simplest_scan);

  duckdb::unique_ptr<duckdb::LogicalFilter>
  ConstructDuckdbFilter(const SimplestFilter &simplest_filter,
                        duckdb::unique_ptr<duckdb::LogicalOperator> child);

  duckdb::unique_ptr<duckdb::LogicalOrder>
  ConstructDuckdbSort(const SimplestSort &simplest_sort,
                      duckdb::unique_ptr<duckdb::LogicalOperator> child);

  duckdb::unique_ptr<duckdb::LogicalColumnDataGet>
  ConstructDuckdbChunk(const SimplestChunk &simplest_chunk);

  duckdb::unique_ptr<duckdb::LogicalProjection>
  ConstructDuckdbProjection(const SimplestProjection &simplest_projection,
                            duckdb::unique_ptr<duckdb::LogicalOperator> child);

  duckdb::unique_ptr<duckdb::LogicalAggregate>
  ConstructDuckdbAggregate(const SimplestAggregate &simplest_agg,
                           duckdb::unique_ptr<duckdb::LogicalOperator> child);

  // Expression construction
  duckdb::unique_ptr<duckdb::Expression>
  ConstructDuckdbExpression(const std::unique_ptr<SimplestExpr> &simplest_expr);

  duckdb::unique_ptr<duckdb::BoundColumnRefExpression>
  ConstructDuckdbColumnRef(const std::unique_ptr<SimplestAttr> &simplest_attr);

  duckdb::unique_ptr<duckdb::BoundConstantExpression> ConstructDuckdbConstant(
      const std::unique_ptr<SimplestConstVar> &simplest_const);

  duckdb::ExpressionType ConvertCompType(SimplestExprType type);
  duckdb::LogicalType ConvertVarType(SimplestVarType type);
  duckdb::OrderType ConvertOrderType(SimplestExprType type);
  std::string ConvertAggFnType(SimplestAggFnType type);

  duckdb::Binder &binder;
  duckdb::ClientContext &context;
};

} // namespace ir_sql_converter