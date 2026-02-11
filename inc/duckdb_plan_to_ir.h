//===----------------------------------------------------------------------===//
//                         DuckDB_to_IR
//
// duckdb_plan_to_ir.h
// from DuckDB source code:
// src/include/duckdb/optimizer/converter/duckdb_to_ir.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb_version_compat.h"
#include "simplest_ir.h"

#include <memory>

namespace ir_sql_converter {
class DuckToIR {
public:
  DuckToIR(duckdb::Binder &binder, duckdb::ClientContext &context)
      : binder(binder), context(context) {};
  ~DuckToIR() = default;

  std::unique_ptr<SimplestStmt>
  ConstructSimplestStmt(duckdb::LogicalOperator *duckdb_plan_pointer,
                        const std::unordered_map<unsigned int, std::string>
                            &intermediate_table_map,
                        bool embed_intermediate_data = false);

private:
  std::unique_ptr<SimplestProjection>
  ConstructSimplestProj(duckdb::LogicalProjection &proj_op,
                        std::unique_ptr<SimplestStmt> child);
  std::unique_ptr<SimplestJoin>
  ConstructSimplestJoin(duckdb::LogicalComparisonJoin &join_op,
                        std::unique_ptr<SimplestStmt> left_child,
                        std::unique_ptr<SimplestStmt> right_child);
  std::unique_ptr<SimplestFilter>
  ConstructSimplestFilter(duckdb::LogicalFilter &filter_op,
                          std::unique_ptr<SimplestStmt> child);
  std::unique_ptr<SimplestScan>
  ConstructSimplestScan(duckdb::LogicalGet &get_op);
  std::unique_ptr<SimplestScan>
  ConstructSimplestScan(duckdb::LogicalColumnDataGet &get_op,
                        const std::string &intermediate_table_name);
  std::unique_ptr<SimplestChunk>
  ConstructSimplestChunk(duckdb::LogicalColumnDataGet &column_data_get_op);
  // Create a placeholder SimplestChunk for intermediate results (same-engine)
  // Contents are empty, but target_list has column type info
  std::unique_ptr<SimplestChunk> ConstructSimplestChunkPlaceholder(
      duckdb::LogicalColumnDataGet &column_data_get_op);
  std::unique_ptr<SimplestCrossProduct>
  ConstructSimplestCrossProduct(duckdb::LogicalCrossProduct &cross_product_op,
                                std::unique_ptr<SimplestStmt> left_child,
                                std::unique_ptr<SimplestStmt> right_child);
  std::unique_ptr<SimplestAggregate>
  ConstructSimplestAggGroup(duckdb::LogicalAggregate &agg_group_op,
                            std::unique_ptr<SimplestStmt> child);
  std::unique_ptr<SimplestOrderBy>
  ConstructSimplestOrderBy(duckdb::LogicalOrder &order_op,
                           std::unique_ptr<SimplestStmt> child);
  std::unique_ptr<SimplestLimit>
  ConstructSimplestLimit(duckdb::LogicalLimit &limit_op,
                         std::unique_ptr<SimplestStmt> child);

  SimplestExprType ConvertCompType(duckdb::ExpressionType type);
  SimplestVarType ConvertVarType(const duckdb::LogicalType &type);
  SimplestAggFnType ConvertAggFnType(const std::string &agg_fn_type);
  SimplestLogicalOp ConvertLogicalType(duckdb::ExpressionType type);
  SimplestOrderType ConvertOrderType(duckdb::OrderType type);
  std::unique_ptr<SimplestAttr>
  ConvertAttr(const duckdb::unique_ptr<duckdb::Expression> &expr);
  // map duckdb's specific column_ids and column names to the actual column
  // index and column names
  // we need to resolve the column idx mapping when cross different engines
  duckdb::column_t ResolveDuckDBColumnIndex(duckdb::idx_t table_idx,
                                            duckdb::idx_t binding_idx);
  std::unique_ptr<SimplestConstVar>
  ConvertConstVar(const duckdb::Value &value, const std::string &prefix = "",
                  const std::string &appendix = "");
  std::unique_ptr<SimplestExpr>
  ConvertExpr(const duckdb::unique_ptr<duckdb::Expression> &expr);

  std::vector<std::unique_ptr<SimplestExpr>> CollectQualVecExprs(
      const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &exprs);
  std::unique_ptr<SimplestExpr>
  CollectScanFilter(const std::unique_ptr<duckdb::TableFilter> &filter_cond,
                    std::unique_ptr<SimplestAttr> var_attr);

  duckdb::Binder &binder;
  duckdb::ClientContext &context;

  // <table id, column binding mapping: binding id -> actual id>
  std::unordered_map<duckdb::idx_t, compat::column_ids_vector_t>
      table_column_ids_map;
};
} // namespace ir_sql_converter