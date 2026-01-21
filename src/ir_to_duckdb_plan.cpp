#include "ir_to_duckdb_plan.h"

namespace ir_sql_converter {

duckdb::unique_ptr<duckdb::LogicalComparisonJoin> IRToDuck::ConstructDuckdbJoin(
    const SimplestJoin &ir_join,
    duckdb::unique_ptr<duckdb::LogicalOperator> left_child,
    duckdb::unique_ptr<duckdb::LogicalOperator> right_child) {
  auto duckdb_join = duckdb::make_uniq<duckdb::LogicalComparisonJoin>(
      ConvertJoinType(ir_join.GetSimplestJoinType()));
  duckdb_join->estimated_cardinality = ir_join.GetEstimatedCardinality();
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

  if (ir_join.GetSimplestJoinType() == SimplestJoinType::Mark) {
    duckdb_join->mark_index = ir_join.GetMarkIndex();

    // Register mark_index binding
    compat::column_ids_vector_t mark_ids;
    mark_ids.push_back(compat::MakeColumnIndex(0));
    RegisterTableMapping(ir_join.GetMarkIndex(), mark_ids);
  }

  return duckdb_join;
}

duckdb::unique_ptr<duckdb::LogicalOperator>
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
  auto scan_op =
      compat::MakeLogicalGet(ir_table_idx, scan_function, std::move(bind_data),
                             std::move(return_types), std::move(return_names));

  // Set estimated cardinality
  scan_op->estimated_cardinality = simplest_scan.GetEstimatedCardinality();

  compat::column_ids_vector_t column_ids;
  for (duckdb::idx_t i = 0; i < table_entry->GetColumns().LogicalColumnCount();
       i++) {
    column_ids.push_back(compat::MakeColumnIndex(i));
  }
  compat::SetLogicalGetColumnIds(*scan_op, std::move(column_ids));
  RegisterTableMapping(ir_table_idx, column_ids);

  // fixme: check how duckdb has filter condition in scan node, now we wrap it
  //  with a filter node

  //  for (const auto &qual : simplest_scan.qual_vec) {
  //    scan_op->expressions.push_back(ConstructDuckdbExpression(qual));
  //  }
  if (!simplest_scan.qual_vec.empty()) {
    auto filter_op = duckdb::make_uniq<duckdb::LogicalFilter>();
    for (const auto &qual : simplest_scan.qual_vec) {
      filter_op->expressions.push_back(ConstructDuckdbExpression(qual));
    }
    filter_op->AddChild(std::move(scan_op));
    return unique_ptr_cast<duckdb::LogicalFilter, duckdb::LogicalOperator>(
        std::move(filter_op));
  } else {
    return unique_ptr_cast<duckdb::LogicalGet, duckdb::LogicalOperator>(
        std::move(scan_op));
  }
}

duckdb::unique_ptr<duckdb::LogicalFilter> IRToDuck::ConstructDuckdbFilter(
    const SimplestFilter &simplest_filter,
    duckdb::unique_ptr<duckdb::LogicalOperator> child) {

  auto filter = duckdb::make_uniq<duckdb::LogicalFilter>();
  filter->estimated_cardinality = simplest_filter.GetEstimatedCardinality();

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
  order->estimated_cardinality = simplest_sort.GetEstimatedCardinality();
  order->AddChild(std::move(child));
  return order;
}

duckdb::unique_ptr<duckdb::LogicalColumnDataGet>
IRToDuck::ConstructDuckdbChunk(const SimplestChunk &simplest_chunk) {
  auto contents = simplest_chunk.GetContents();
  duckdb::idx_t table_idx = simplest_chunk.GetTableIndex();

  // fixme: didn't get the correct target_list
  // Get column types from target_list
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::cout << "simplest_chunk.target_list.size()="
            << simplest_chunk.target_list.size() << "\n";
  for (const auto &attr : simplest_chunk.target_list) {
    chunk_types.push_back(ConvertVarType(attr->GetType()));
  }

  // Fallback to single VARCHAR if no target_list (legacy IN-clause case)
  if (chunk_types.empty()) {
    chunk_types.push_back(duckdb::LogicalType::VARCHAR);
  }

  // Create ColumnDataCollection
  auto collection =
      duckdb::make_uniq<duckdb::ColumnDataCollection>(context, chunk_types);
  duckdb::ColumnDataAppendState append_state;
  collection->InitializeAppend(append_state);

  // Create DataChunk to hold values
  duckdb::DataChunk output;
  output.Initialize(duckdb::Allocator::Get(context), chunk_types);

  // Calculate row count (contents stored column-by-column)
  duckdb::idx_t num_columns = chunk_types.size();
  duckdb::idx_t num_rows = num_columns > 0 ? contents.size() / num_columns : 0;

  // fixme: should be column-by-column?
  // Add values row by row
  for (duckdb::idx_t row = 0; row < num_rows; row++) {
    for (duckdb::idx_t col = 0; col < num_columns; col++) {
      duckdb::idx_t content_idx = col * num_rows + row;
      std::string &val_str = contents[content_idx];

      // Convert string to appropriate type
      duckdb::Value val;
      switch (chunk_types[col].id()) {
      case duckdb::LogicalTypeId::INTEGER:
        val = duckdb::Value::INTEGER(std::stoi(val_str));
        break;
      case duckdb::LogicalTypeId::FLOAT:
      case duckdb::LogicalTypeId::DOUBLE:
        val = duckdb::Value::DOUBLE(std::stod(val_str));
        break;
      case duckdb::LogicalTypeId::BOOLEAN:
        val = duckdb::Value::BOOLEAN(val_str == "true" || val_str == "1");
        break;
      case duckdb::LogicalTypeId::VARCHAR:
      default:
        val = duckdb::Value(val_str);
        break;
      }
      output.SetValue(col, output.size(), val);
    }
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

  // Create LogicalColumnDataGet with cardinality from IR
  auto chunk_get = duckdb::make_uniq<duckdb::LogicalColumnDataGet>(
      table_idx, chunk_types, std::move(collection));
  chunk_get->estimated_cardinality = simplest_chunk.GetEstimatedCardinality();

  // Register column mapping
  compat::column_ids_vector_t column_ids;
  for (duckdb::idx_t i = 0; i < chunk_types.size(); i++) {
    column_ids.push_back(compat::MakeColumnIndex(i));
  }
  RegisterTableMapping(table_idx, column_ids);

  return chunk_get;
}

duckdb::unique_ptr<duckdb::LogicalColumnDataGet>
IRToDuck::ConstructDuckdbColumnDataGet(const SimplestChunk &simplest_chunk) {
  auto contents = simplest_chunk.GetContents();
  duckdb::idx_t table_idx = simplest_chunk.GetTableIndex();

  if (contents.empty()) {
    // Intermediate result placeholder - get data from intermediate_results map
    if (!intermediate_results ||
        intermediate_results->find(table_idx) == intermediate_results->end()) {
      throw std::runtime_error(
          "Missing intermediate result for table_index " +
          std::to_string(table_idx) +
          ". Ensure previous subplan results are provided.");
    }

    auto &collection = (*intermediate_results)[table_idx];
    auto types = collection->Types();

    // Move the collection out of the map (each intermediate result is used
    // once)
    auto chunk_get = duckdb::make_uniq<duckdb::LogicalColumnDataGet>(
        table_idx, types, std::move(collection));
    chunk_get->estimated_cardinality = simplest_chunk.GetEstimatedCardinality();

    // Register column mapping (identity for intermediate results)
    compat::column_ids_vector_t column_ids;
    for (duckdb::idx_t i = 0; i < types.size(); i++) {
      column_ids.push_back(compat::MakeColumnIndex(i));
    }
    RegisterTableMapping(table_idx, column_ids);

    return chunk_get;
  } else {
    // Embedded data (IN-clause or cross-engine intermediate result)
    // Use existing ConstructDuckdbChunk logic
    return ConstructDuckdbChunk(simplest_chunk);
  }
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
  projection->estimated_cardinality = simplest_projection.GetEstimatedCardinality();

  // Add child
  projection->AddChild(std::move(child));

  return projection;
}

duckdb::unique_ptr<duckdb::Expression> IRToDuck::ConstructDuckdbExpression(
    const std::unique_ptr<SimplestExpr> &simplest_expr) {
  switch (simplest_expr->GetNodeType()) {
  case VarConstComparisonNode: {
    auto &comp = simplest_expr->Cast<SimplestVarConstComparison>();
    auto left_expr = ConstructDuckdbColumnRef(comp.attr);
    auto right_expr = ConstructDuckdbConstant(comp.const_var);
    if (comp.GetSimplestExprType() == SimplestExprType::TextLike ||
        comp.GetSimplestExprType() == SimplestExprType::Text_Not_Like) {

      // Get the LIKE function from catalog
      auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
      std::string func_name =
          (comp.GetSimplestExprType() == SimplestExprType::TextLike) ? "~~"
                                                                     : "!~~";

      auto &func_entry = catalog.GetEntry<duckdb::ScalarFunctionCatalogEntry>(
          context, "", "", func_name);

      // Create BoundFunctionExpression for LIKE
      duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
      children.push_back(std::move(left_expr));
      children.push_back(std::move(right_expr));

      return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
          duckdb::LogicalType::BOOLEAN,
          func_entry.functions.GetFunctionByOffset(0), std::move(children),
          nullptr);
    } else {
      return duckdb::make_uniq<duckdb::BoundComparisonExpression>(
          ConvertCompType(comp.GetSimplestExprType()), std::move(left_expr),
          std::move(right_expr));
    }
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

  duckdb::idx_t table_idx = simplest_attr->GetTableIndex();
  duckdb::idx_t actual_col_id = simplest_attr->GetColumnIndex();
  duckdb::idx_t binding_idx = GetBindingIdx(table_idx, actual_col_id);

  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      simplest_attr->GetColumnName(), ConvertVarType(simplest_attr->GetType()),
      duckdb::ColumnBinding(table_idx, binding_idx));
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

duckdb::unique_ptr<duckdb::LogicalAggregate> IRToDuck::ConstructDuckdbAggregate(
    const SimplestAggregate &simplest_agg,
    duckdb::unique_ptr<duckdb::LogicalOperator> child) {
  if (!simplest_agg.groups.empty()) {
    compat::column_ids_vector_t group_ids;
    for (duckdb::idx_t i = 0; i < simplest_agg.groups.size(); i++) {
      group_ids.push_back(compat::MakeColumnIndex(i));
    }
    RegisterTableMapping(simplest_agg.GetGroupIndex(), group_ids);
  }
  compat::column_ids_vector_t agg_ids;
  for (duckdb::idx_t i = 0; i < simplest_agg.agg_fns.size(); i++) {
    agg_ids.push_back(compat::MakeColumnIndex(i));
  }
  RegisterTableMapping(simplest_agg.GetAggIndex(), agg_ids);

  // Convert agg_fns to DuckDB aggregate expressions
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> agg_expressions;
  for (const auto &agg_fn : simplest_agg.agg_fns) {
    std::string agg_fn_name = ConvertAggFnType(agg_fn.second);

    // First create column ref to get the argument type
    auto col_ref = ConstructDuckdbColumnRef(agg_fn.first);
    duckdb::vector<duckdb::LogicalType> arg_types;
    arg_types.push_back(col_ref->return_type);

    // Get aggregate function from catalog
    auto &catalog = duckdb::Catalog::GetSystemCatalog(context);
    auto &func_catalog =
        catalog.GetEntry<duckdb::AggregateFunctionCatalogEntry>(context, "", "",
                                                                agg_fn_name);

    // Find the function overload that matches the argument types
    auto matched_func =
        func_catalog.functions.GetFunctionByArguments(context, arg_types);

    // Create children vector with the column ref
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
    children.push_back(std::move(col_ref));

    // Use FunctionBinder to properly bind the aggregate function
    // This handles setting return_type correctly for polymorphic functions
    duckdb::FunctionBinder function_binder(context);
    auto agg_expr = function_binder.BindAggregateFunction(
        matched_func, std::move(children), nullptr,
        duckdb::AggregateType::NON_DISTINCT);

    agg_expressions.push_back(std::move(agg_expr));
  }

  // Create LogicalAggregate
  auto logical_agg = duckdb::make_uniq<duckdb::LogicalAggregate>(
      simplest_agg.GetGroupIndex(), simplest_agg.GetAggIndex(),
      std::move(agg_expressions));
  logical_agg->estimated_cardinality = simplest_agg.GetEstimatedCardinality();

  // Convert groups to DuckDB group expressions
  for (const auto &group : simplest_agg.groups) {
    logical_agg->groups.push_back(ConstructDuckdbColumnRef(group));
  }

  // Add child
  logical_agg->children.push_back(std::move(child));

  return logical_agg;
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
    // Use ConstructDuckdbColumnDataGet which handles both:
    // - Embedded data (IN-clause or cross-engine intermediate result)
    // - Placeholder (same-engine intermediate result, data from map)
    return ConstructDuckdbColumnDataGet(simplest_chunk);
  }

  case ProjectionNode: {
    auto &simplest_projection = simplest_ir->Cast<SimplestProjection>();
    return ConstructDuckdbProjection(simplest_projection,
                                     std::move(left_child));
  }

  case AggregateNode: {
    auto &simplest_agg = simplest_ir->Cast<SimplestAggregate>();
    return ConstructDuckdbAggregate(simplest_agg, std::move(left_child));
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

std::string IRToDuck::ConvertAggFnType(SimplestAggFnType type) {
  switch (type) {
  case SimplestAggFnType::Min:
    return "min";
  case SimplestAggFnType::Max:
    return "max";
  case SimplestAggFnType::Sum:
    return "sum";
  case SimplestAggFnType::Average:
    return "avg";
  default:
    throw std::runtime_error("Unsupported SimplestAggFnType");
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

duckdb::JoinType IRToDuck::ConvertJoinType(SimplestJoinType type) {
  switch (type) {
  case SimplestJoinType::Inner:
    return duckdb::JoinType::INNER;
  case SimplestJoinType::Left:
    return duckdb::JoinType::LEFT;
  case SimplestJoinType::Right:
    return duckdb::JoinType::RIGHT;
  case SimplestJoinType::Full:
    return duckdb::JoinType::OUTER;
  case SimplestJoinType::Mark:
    return duckdb::JoinType::MARK;
  case SimplestJoinType::Semi:
    return duckdb::JoinType::SEMI;
  case SimplestJoinType::Anti:
    return duckdb::JoinType::ANTI;
  default:
    throw std::runtime_error("Unsupported SimplestJoinType");
  }
}

void IRToDuck::RegisterTableMapping(
    duckdb::idx_t table_idx, const compat::column_ids_vector_t &column_ids) {
  std::unordered_map<duckdb::idx_t, duckdb::idx_t> mapping;
  for (duckdb::idx_t binding_idx = 0; binding_idx < column_ids.size();
       binding_idx++) {
    duckdb::idx_t actual_col_id = compat::GetColumnId(column_ids[binding_idx]);
    mapping[actual_col_id] = binding_idx;
  }
  actual_to_binding_map[table_idx] = mapping;
}

duckdb::idx_t IRToDuck::GetBindingIdx(duckdb::idx_t table_idx,
                                      duckdb::idx_t actual_column_id) {
  auto table_it = actual_to_binding_map.find(table_idx);
  if (table_it == actual_to_binding_map.end()) {
    return actual_column_id; // No mapping, assume identity
  }
  auto col_it = table_it->second.find(actual_column_id);
  if (col_it == table_it->second.end()) {
    return actual_column_id; // Not found, assume identity
  }
  return col_it->second;
}

} // namespace ir_sql_converter