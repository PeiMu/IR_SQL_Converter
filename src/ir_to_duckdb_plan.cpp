#include "ir_to_duckdb_plan.h"

namespace ir_sql_converter {

duckdb::unique_ptr<duckdb::LogicalComparisonJoin> IRToDuck::ConstructDuckdbJoin(
    const SimplestJoin &ir_join,
    duckdb::unique_ptr<duckdb::LogicalOperator> left_child,
    duckdb::unique_ptr<duckdb::LogicalOperator> right_child) {
  auto duckdb_join =
      duckdb::make_uniq<duckdb::LogicalComparisonJoin>(duckdb::JoinType::INNER);
  duckdb_join->children.push_back(std::move(left_child));
  duckdb_join->children.push_back(std::move(right_child));

  // Build duckdb_join conditions from IR
  for (const auto &ir_cond : ir_join.join_conditions) {
    duckdb::JoinCondition cond;
    cond.comparison = ConvertCompType(ir_cond->GetSimplestExprType());

    // Use IR's table/column indices directly
    cond.left = ConstructDuckdbColumnRef(ir_cond->left_attr);
    cond.right = ConstructDuckdbColumnRef(ir_cond->right_attr);

    duckdb_join->conditions.push_back(std::move(cond));
  }

  return duckdb_join;
}

duckdb::unique_ptr<duckdb::LogicalGet>
IRToDuck::ConstructDuckdbScan(const SimplestScan &simplest_scan) {
  std::string table_name = simplest_scan.GetTableName();
  duckdb::idx_t ir_table_idx = simplest_scan.GetTableIndex();

  // Get table from catalog
  auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
  auto table_entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
      context, "", "main", table_name, duckdb::OnEntryNotFound::RETURN_NULL);

  if (!table_entry) {
    throw std::runtime_error("Table not found: " + table_name);
  }

  // Get scan function and bind data
  duckdb::unique_ptr<duckdb::FunctionData> bind_data;
  auto scan_function = table_entry->GetScanFunction(context, bind_data);

  // Get all columns from table
  duckdb::vector<duckdb::LogicalType> return_types;
  duckdb::vector<std::string> return_names;
  for (auto &col : table_entry->GetColumns().Logical()) {
    return_types.push_back(col.Type());
    return_names.push_back(col.Name());
  }

  // Create LogicalGet using IR's table index
  return duckdb::make_uniq<duckdb::LogicalGet>(
      ir_table_idx, // Use IR's index directly!
      scan_function, std::move(bind_data), std::move(return_types),
      std::move(return_names), duckdb::virtual_column_map_t());
}

duckdb::unique_ptr<duckdb::LogicalFilter> IRToDuck::ConstructDuckdbFilter(
    const SimplestFilter &simplest_filter,
    duckdb::unique_ptr<duckdb::LogicalOperator> child) {

  auto filter = duckdb::make_uniq<duckdb::LogicalFilter>();

  // Build filter expressions from IR
  for (const auto &ir_expr : simplest_filter.qual_vec) {
    auto duckdb_expr = ConstructDuckdbExpression(ir_expr);
    filter->expressions.push_back(std::move(duckdb_expr));
  }

  filter->AddChild(std::move(child));
  return filter;
}

duckdb::unique_ptr<duckdb::LogicalOrder> IRToDuck::ConstructDuckdbSort(
    const SimplestSort &simplest_sort,
    duckdb::unique_ptr<duckdb::LogicalOperator> child) {

  duckdb::vector<duckdb::BoundOrderByNode> orders;

  for (const auto &order_struct : simplest_sort.GetOrderStructVec()) {
    // Get the column being sorted
    auto sort_col_idx = order_struct.sort_col_idx - 1; // IR is 1-indexed
    if (sort_col_idx >= simplest_sort.target_list.size()) {
      throw std::runtime_error("Invalid sort column index");
    }

    auto &sort_attr = simplest_sort.target_list[sort_col_idx];
    auto sort_expr = ConstructDuckdbColumnRef(sort_attr);

    auto order_type = ConvertOrderType(order_struct.order_type);
    auto nulls_order = order_struct.nulls_first
                           ? duckdb::OrderByNullType::NULLS_FIRST
                           : duckdb::OrderByNullType::NULLS_LAST;

    orders.emplace_back(order_type, nulls_order, std::move(sort_expr));
  }

  auto order = duckdb::make_uniq<duckdb::LogicalOrder>(std::move(orders));
  order->AddChild(std::move(child));
  return order;
}

duckdb::unique_ptr<duckdb::LogicalColumnDataGet>
IRToDuck::ConstructDuckdbChunk(const SimplestChunk &simplest_chunk) {
  auto contents = simplest_chunk.GetContents();
  duckdb::idx_t table_idx = simplest_chunk.GetTableIndex();

  // Create types for single VARCHAR column
  duckdb::vector<duckdb::LogicalType> chunk_types = {
      duckdb::LogicalType::VARCHAR};

  // Create ColumnDataCollection
  auto collection =
      duckdb::make_uniq<duckdb::ColumnDataCollection>(context, chunk_types);
  duckdb::ColumnDataAppendState append_state;
  collection->InitializeAppend(append_state);

  // Create DataChunk to hold values
  duckdb::DataChunk output;
  output.Initialize(duckdb::Allocator::Get(context), chunk_types);

  // Add each string value as a row
  for (duckdb::idx_t i = 0; i < contents.size(); i++) {
    output.SetValue(0, output.size(), duckdb::Value(contents[i]));
    output.SetCardinality(output.size() + 1);

    // Flush chunk when full
    if (output.size() == STANDARD_VECTOR_SIZE) {
      collection->Append(append_state, output);
      output.Reset();
    }
  }

  // Append any remaining values
  if (output.size() > 0) {
    collection->Append(append_state, output);
  }

  // Create LogicalColumnDataGet
  return duckdb::make_uniq<duckdb::LogicalColumnDataGet>(table_idx, chunk_types,
                                                         std::move(collection));
}

duckdb::unique_ptr<duckdb::LogicalProjection>
IRToDuck::ConstructDuckdbProjection(
    const SimplestProjection &simplest_projection,
    duckdb::unique_ptr<duckdb::LogicalOperator> child) {
  duckdb::idx_t table_idx = simplest_projection.GetIndex();

  // Build projection expressions from target_list
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> select_list;
  for (const auto &attr : simplest_projection.target_list) {
    auto col_expr = ConstructDuckdbColumnRef(attr);
    select_list.push_back(std::move(col_expr));
  }

  // Create LogicalProjection
  auto projection = duckdb::make_uniq<duckdb::LogicalProjection>(
      table_idx, std::move(select_list));

  // Add child
  projection->AddChild(std::move(child));

  return projection;
}

duckdb::unique_ptr<duckdb::Expression> IRToDuck::ConstructDuckdbExpression(
    const std::unique_ptr<SimplestExpr> &simplest_expr) {
  switch (simplest_expr->GetNodeType()) {
  case VarConstComparisonNode: {
    auto &comp = simplest_expr->Cast<SimplestVarConstComparison>();
    auto left = ConstructDuckdbColumnRef(comp.attr);
    auto right = ConstructDuckdbConstant(comp.const_var);
    return duckdb::make_uniq<duckdb::BoundComparisonExpression>(
        ConvertCompType(comp.GetSimplestExprType()), std::move(left),
        std::move(right));
  }

  case VarComparisonNode: {
    auto &comp = simplest_expr->Cast<SimplestVarComparison>();
    auto left = ConstructDuckdbColumnRef(comp.left_attr);
    auto right = ConstructDuckdbColumnRef(comp.right_attr);
    return duckdb::make_uniq<duckdb::BoundComparisonExpression>(
        ConvertCompType(comp.GetSimplestExprType()), std::move(left),
        std::move(right));
  }

  case LogicalExprNode: {
    auto &logical = simplest_expr->Cast<SimplestLogicalExpr>();
    auto left = ConstructDuckdbExpression(logical.left_expr);
    auto right = ConstructDuckdbExpression(logical.right_expr);

    auto expr_type = (logical.GetLogicalOp() == LogicalAnd)
                         ? duckdb::ExpressionType::CONJUNCTION_AND
                         : duckdb::ExpressionType::CONJUNCTION_OR;

    auto conj =
        duckdb::make_uniq<duckdb::BoundConjunctionExpression>(expr_type);
    conj->children.push_back(std::move(left));
    conj->children.push_back(std::move(right));
    return std::move(conj);
  }

  case IsNullExprNode: {
    auto &is_null = simplest_expr->Cast<SimplestIsNullExpr>();
    auto operand = ConstructDuckdbColumnRef(is_null.attr);

    // Check the SimplestExprType: NullType or NonNullType
    auto expr_type = (is_null.GetSimplestExprType() == NullType)
                         ? duckdb::ExpressionType::OPERATOR_IS_NULL
                         : duckdb::ExpressionType::OPERATOR_IS_NOT_NULL;

    auto op = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
        expr_type, duckdb::LogicalType::BOOLEAN);
    op->children.push_back(std::move(operand));
    return std::move(op);
  }

  case SingleAttrExprNode: {
    // Single attribute expression (e.g., column in IN clause)
    auto &single_attr = simplest_expr->Cast<SimplestSingleAttrExpr>();
    return ConstructDuckdbColumnRef(single_attr.attr);
  }

  default:
    throw std::runtime_error("Unsupported expression type: " +
                             std::to_string(simplest_expr->GetNodeType()) +
                             " in IR");
  }
}

duckdb::unique_ptr<duckdb::BoundColumnRefExpression>
IRToDuck::ConstructDuckdbColumnRef(
    const std::unique_ptr<SimplestAttr> &simplest_attr) {

  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      simplest_attr->GetColumnName(), ConvertVarType(simplest_attr->GetType()),
      duckdb::ColumnBinding(
          simplest_attr->GetTableIndex(), // Use IR's table index directly
          simplest_attr->GetColumnIndex() // Use IR's column index directly
          ));
}

duckdb::unique_ptr<duckdb::BoundConstantExpression>
IRToDuck::ConstructDuckdbConstant(
    const std::unique_ptr<SimplestConstVar> &simplest_const) {

  duckdb::Value value;

  switch (simplest_const->GetType()) {
  case IntVar:
    value = duckdb::Value::INTEGER(simplest_const->GetIntValue());
    break;
  case StringVar:
    value = duckdb::Value(simplest_const->GetStringValue());
    break;
  case FloatVar:
    value = duckdb::Value::FLOAT(simplest_const->GetFloatValue());
    break;
  case BoolVar:
    value = duckdb::Value::BOOLEAN(simplest_const->GetBoolValue());
    break;
  default:
    throw std::runtime_error("Unsupported constant type");
  }

  return duckdb::make_uniq<duckdb::BoundConstantExpression>(value);
}

duckdb::unique_ptr<duckdb::LogicalOperator> IRToDuck::ConstructDuckdbPlan(
    const std::unique_ptr<SimplestStmt> &simplest_ir) {
  // Recursively build children first (bottom-up)
  duckdb::unique_ptr<duckdb::LogicalOperator> left_child, right_child;
  if (!simplest_ir->children.empty()) {
    left_child = ConstructDuckdbPlan(simplest_ir->children[0]);
    if (simplest_ir->children.size() == 2) {
      right_child = ConstructDuckdbPlan(simplest_ir->children[1]);
    }
  }

  switch (simplest_ir->GetNodeType()) {
  case ScanNode: {
    auto &simplest_scan = simplest_ir->Cast<SimplestScan>();
    return ConstructDuckdbScan(simplest_scan);
  }

  case JoinNode: {
    auto &simplest_join = simplest_ir->Cast<SimplestJoin>();
    return ConstructDuckdbJoin(simplest_join, std::move(left_child),
                               std::move(right_child));
  }

  case FilterNode: {
    auto &simplest_filter = simplest_ir->Cast<SimplestFilter>();
    return ConstructDuckdbFilter(simplest_filter, std::move(left_child));
  }

  case SortNode: {
    auto &simplest_sort = simplest_ir->Cast<SimplestSort>();
    return ConstructDuckdbSort(simplest_sort, std::move(left_child));
  }

  case HashNode:
    // Hash node doesn't add anything, just pass through child
    return left_child;

  case ChunkNode: {
    auto &simplest_chunk = simplest_ir->Cast<SimplestChunk>();
    return ConstructDuckdbChunk(simplest_chunk);
  }

  case ProjectionNode: {
    auto &simplest_projection = simplest_ir->Cast<SimplestProjection>();
    return ConstructDuckdbProjection(simplest_projection,
                                     std::move(left_child));
  }

  default:
    throw std::runtime_error("Unsupported IR node type: " +
                             std::to_string(simplest_ir->GetNodeType()));
  }
}

duckdb::ExpressionType IRToDuck::ConvertCompType(SimplestExprType type) {
  switch (type) {
  case Equal:
    return duckdb::ExpressionType::COMPARE_EQUAL;
  case LessThan:
    return duckdb::ExpressionType::COMPARE_LESSTHAN;
  case GreaterThan:
    return duckdb::ExpressionType::COMPARE_GREATERTHAN;
  case LessEqual:
    return duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
  case GreaterEqual:
    return duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
  case NotEqual:
    return duckdb::ExpressionType::COMPARE_NOTEQUAL;
  case TextLike:
    return duckdb::ExpressionType::COMPARE_IN;
  case Text_Not_Like:
    return duckdb::ExpressionType::COMPARE_NOT_IN;
  default:
    std::cout << "Invalid postgres comparison type!" << std::endl;
    return duckdb::ExpressionType::INVALID;
  }
}

duckdb::LogicalType IRToDuck::ConvertVarType(SimplestVarType type) {
  switch (type) {
  case BoolVar:
    return duckdb::LogicalType(duckdb::LogicalTypeId::BOOLEAN);
  case IntVar:
    return duckdb::LogicalType(duckdb::LogicalTypeId::INTEGER);
  case FloatVar:
    return duckdb::LogicalType(duckdb::LogicalTypeId::FLOAT);
  case StringVar:
    return duckdb::LogicalType(duckdb::LogicalTypeId::VARCHAR);
  default:
    std::cout << "Invalid postgres var type!" << std::endl;
    return duckdb::LogicalType(duckdb::LogicalTypeId::INVALID);
  }
}

duckdb::OrderType IRToDuck::ConvertOrderType(SimplestExprType type) {
  switch (type) {
  case InvalidExprType:
    std::cout << "Invalid Order Type!!!" << std::endl;
    return duckdb::OrderType::INVALID;
  case LessThan:
    return duckdb::OrderType::ASCENDING;
  case GreaterThan:
    return duckdb::OrderType::DESCENDING;
  default:
    throw std::runtime_error("Unsupported order type: " + std::to_string(type));
  }
}
} // namespace ir_sql_converter