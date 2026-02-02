//===----------------------------------------------------------------------===//
//                         IR_to_NodeStr
//
// ir_to_nodestr.cpp
// Convert SimplestIR to PostgreSQL's nodestring format
// (inverse of nodestr_to_ir.cpp)
//
//===----------------------------------------------------------------------===//

#include "ir_to_nodestr.h"

#include <cassert>
#include <cstring>
#include <iomanip>

namespace ir_sql_converter {

//===----------------------------------------------------------------------===//
// Main Entry Point
//===----------------------------------------------------------------------===//

std::string IRToNodestr::NodeToString(const std::unique_ptr<SimplestStmt> &stmt) {
  output.str("");
  output.clear();

  WritePlannedStmt(*stmt);

  return output.str();
}

//===----------------------------------------------------------------------===//
// Write Helper Functions
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteToken(const char *token) { output << token; }

void IRToNodestr::WriteInt(const char *field, int value) {
  output << " :" << field << " " << value;
}

void IRToNodestr::WriteUint(const char *field, unsigned int value) {
  output << " :" << field << " " << value;
}

void IRToNodestr::WriteLong(const char *field, long value) {
  output << " :" << field << " " << value;
}

void IRToNodestr::WriteFloat(const char *field, float value) {
  output << " :" << field << " " << std::fixed << std::setprecision(2) << value;
}

void IRToNodestr::WriteBool(const char *field, bool value) {
  output << " :" << field << " " << (value ? "true" : "false");
}

void IRToNodestr::WriteString(const char *field, const std::string &value) {
  if (value.empty()) {
    output << " :" << field << " <>";
  } else {
    output << " :" << field << " " << EscapeString(value);
  }
}

void IRToNodestr::WriteChar(const char *field, char value) {
  output << " :" << field << " ";
  if (value == '\0') {
    output << "<>";
  } else if (value == '\\' || value == ' ' || value == '\n' || value == '\t' ||
             value == '(' || value == ')' || value == '{' || value == '}') {
    output << "\\" << value;
  } else {
    output << value;
  }
}

void IRToNodestr::WriteNodeStart(const char *node_name) {
  output << "{" << node_name;
}

void IRToNodestr::WriteNodeEnd() { output << "}"; }

void IRToNodestr::WriteListStart() { output << "("; }

void IRToNodestr::WriteListEnd() { output << ")"; }

void IRToNodestr::WriteIntList(const char *field,
                               const std::vector<int> &values) {
  output << " :" << field << " (i";
  for (int val : values) {
    output << " " << val;
  }
  output << ")";
}

void IRToNodestr::WriteOidList(const char *field,
                               const std::vector<unsigned int> &values) {
  output << " :" << field << " (o";
  for (unsigned int val : values) {
    output << " " << val;
  }
  output << ")";
}

void IRToNodestr::WriteBitmapset(const char *field,
                                 const std::vector<int> &values) {
  output << " :" << field << " (b";
  for (int val : values) {
    output << " " << val;
  }
  output << ")";
}

std::string IRToNodestr::EscapeString(const std::string &str) {
  std::string result;
  for (char c : str) {
    if (c == '\\' || c == ' ' || c == '\n' || c == '\t' || c == '(' ||
        c == ')' || c == '{' || c == '}') {
      result += '\\';
    }
    result += c;
  }
  return result;
}

//===----------------------------------------------------------------------===//
// Write PostgreSQL PlannedStmt
//===----------------------------------------------------------------------===//

void IRToNodestr::WritePlannedStmt(const SimplestStmt &stmt) {
  WriteNodeStart("PLANNEDSTMT");

  // commandType: 1 = SELECT
  WriteInt("commandType", 1);
  // queryId
  WriteLong("queryId", 0);
  // hasReturning
  WriteBool("hasReturning", false);
  // hasModifyingCTE
  WriteBool("hasModifyingCTE", false);
  // canSetTag
  WriteBool("canSetTag", true);
  // transientPlan
  WriteBool("transientPlan", false);
  // dependsOnRole
  WriteBool("dependsOnRole", false);
  // parallelModeNeeded
  WriteBool("parallelModeNeeded", false);
  // jitFlags
  WriteInt("jitFlags", 0);

  // planTree
  output << " :planTree ";
  WritePlanNode(stmt);

  // rtable - range table entries
  output << " :rtable ";
  WriteListStart();
  // Collect all table indices and write RTEs
  std::vector<unsigned int> table_indices;
  CollectTableIndices(&stmt, table_indices);
  for (unsigned int idx : table_indices) {
    if (idx > 0 && idx <= table_col_names.size()) {
      const auto &table_entry = table_col_names[idx - 1];
      if (!table_entry.empty()) {
        std::string table_name = table_entry.begin()->first;
        WriteRangeTblEntry(idx, table_name);
      }
    }
  }
  WriteListEnd();

  // resultRelations
  output << " :resultRelations <>";
  // rootResultRelations
  output << " :rootResultRelations <>";
  // subplans
  output << " :subplans <>";
  // rewindPlanIDs
  WriteBitmapset("rewindPlanIDs", {});
  // rowMarks
  output << " :rowMarks <>";
  // relationOids
  output << " :relationOids ";
  WriteListStart();
  WriteListEnd();
  // invalItems
  output << " :invalItems <>";
  // paramExecTypes
  output << " :paramExecTypes <>";
  // utilityStmt
  output << " :utilityStmt <>";
  // stmt_location
  WriteInt("stmt_location", 0);
  // stmt_len
  WriteInt("stmt_len", 0);

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Common Plan Fields
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteCommonPlan(const SimplestStmt &stmt) {
  // startup_cost
  WriteFloat("startup_cost", 0.0f);
  // total_cost
  WriteFloat("total_cost", 0.0f);
  // plan_rows
  WriteLong("plan_rows", stmt.GetEstimatedCardinality());
  // plan_width
  WriteInt("plan_width", 0);
  // parallel_aware
  WriteBool("parallel_aware", false);
  // parallel_safe
  WriteBool("parallel_safe", false);
  // plan_node_id
  WriteInt("plan_node_id", 0);

  // targetlist
  output << " :targetlist ";
  WriteListStart();
  int resno = 1;
  for (const auto &attr : stmt.target_list) {
    WriteTargetEntry(*attr, resno++);
  }
  WriteListEnd();

  // qual
  output << " :qual ";
  if (stmt.qual_vec.empty()) {
    output << "<>";
  } else {
    WriteListStart();
    for (const auto &qual : stmt.qual_vec) {
      WriteOpExpr(*qual);
    }
    WriteListEnd();
  }

  // lefttree
  output << " :lefttree ";
  if (!stmt.children.empty() && stmt.children[0]) {
    WritePlanNode(*stmt.children[0]);
  } else {
    output << "<>";
  }

  // righttree
  output << " :righttree ";
  if (stmt.children.size() > 1 && stmt.children[1]) {
    WritePlanNode(*stmt.children[1]);
  } else {
    output << "<>";
  }

  // initPlan
  output << " :initPlan <>";
  // extParam
  WriteBitmapset("extParam", {});
  // allParam
  WriteBitmapset("allParam", {});
}

//===----------------------------------------------------------------------===//
// Write Plan Nodes
//===----------------------------------------------------------------------===//

void IRToNodestr::WritePlanNode(const SimplestStmt &stmt) {
  switch (stmt.GetNodeType()) {
  case ScanNode:
    WriteSeqScan(stmt.Cast<SimplestScan>());
    break;
  case JoinNode:
    WriteHashJoin(stmt.Cast<SimplestJoin>());
    break;
  case HashNode:
    WriteHash(stmt.Cast<SimplestHash>());
    break;
  case SortNode:
    WriteSort(stmt.Cast<SimplestSort>());
    break;
  case AggregateNode:
    WriteAgg(stmt.Cast<SimplestAggregate>());
    break;
  case FilterNode:
    WriteFilter(stmt.Cast<SimplestFilter>());
    break;
  case ProjectionNode:
    WriteProjection(stmt.Cast<SimplestProjection>());
    break;
  case StmtNode:
    // Generic statement node - write as a seq scan or nested structure
    if (!stmt.children.empty() && stmt.children[0]) {
      WritePlanNode(*stmt.children[0]);
    } else {
      // Fallback to writing common plan fields
      WriteNodeStart("RESULT");
      WriteCommonPlan(stmt);
      WriteNodeEnd();
    }
    break;
  default:
    std::cerr << "IRToNodestr: Unsupported node type: " << stmt.GetNodeType()
              << std::endl;
    assert(false);
  }
}

void IRToNodestr::WriteSeqScan(const SimplestScan &scan) {
  WriteNodeStart("SEQSCAN");
  WriteCommonPlan(scan);
  // scanrelid
  WriteUint("scanrelid", scan.GetTableIndex());
  WriteNodeEnd();
}

void IRToNodestr::WriteIndexScan(const SimplestScan &scan) {
  WriteNodeStart("INDEXSCAN");
  WriteCommonPlan(scan);
  // scanrelid
  WriteUint("scanrelid", scan.GetTableIndex());
  // indexid - we don't have this info in IR, use 0
  WriteUint("indexid", 0);
  // indexqual
  output << " :indexqual <>";
  // indexqualorig
  output << " :indexqualorig <>";
  // indexorderby
  output << " :indexorderby <>";
  // indexorderbyorig
  output << " :indexorderbyorig <>";
  // indexorderbyops
  output << " :indexorderbyops <>";
  // indexorderdir
  WriteInt("indexorderdir", 1); // ForwardScanDirection
  WriteNodeEnd();
}

void IRToNodestr::WriteHashJoin(const SimplestJoin &join) {
  WriteNodeStart("HASHJOIN");
  WriteCommonPlan(join);
  // jointype
  WriteInt("jointype", GetPgJoinType(join.GetSimplestJoinType()));
  // inner_unique
  WriteBool("inner_unique", false);
  // joinqual
  output << " :joinqual <>";

  // hashclauses - write join conditions
  output << " :hashclauses ";
  if (join.join_conditions.empty()) {
    output << "<>";
  } else {
    WriteListStart();
    for (const auto &cond : join.join_conditions) {
      WriteOpExpr(*cond);
    }
    WriteListEnd();
  }

  // hashoperators
  output << " :hashoperators ";
  WriteListStart();
  output << "o";
  for (size_t i = 0; i < join.join_conditions.size(); i++) {
    output << " " << GetPgComparisonOp(join.join_conditions[i]->GetSimplestExprType());
  }
  WriteListEnd();

  // hashcollations
  output << " :hashcollations ";
  WriteListStart();
  output << "o";
  for (size_t i = 0; i < join.join_conditions.size(); i++) {
    output << " 0";
  }
  WriteListEnd();

  // hashkeys
  output << " :hashkeys ";
  WriteListStart();
  for (const auto &cond : join.join_conditions) {
    WriteVar(*cond->right_attr);
  }
  WriteListEnd();

  WriteNodeEnd();
}

void IRToNodestr::WriteMergeJoin(const SimplestJoin &join) {
  WriteNodeStart("MERGEJOIN");
  WriteCommonPlan(join);
  // jointype
  WriteInt("jointype", GetPgJoinType(join.GetSimplestJoinType()));
  // inner_unique
  WriteBool("inner_unique", false);
  // joinqual
  output << " :joinqual <>";
  // skip_mark_restore
  WriteBool("skip_mark_restore", false);

  // mergeclauses - write join conditions
  output << " :mergeclauses ";
  if (join.join_conditions.empty()) {
    output << "<>";
  } else {
    WriteListStart();
    for (const auto &cond : join.join_conditions) {
      WriteOpExpr(*cond);
    }
    WriteListEnd();
  }

  int cond_num = join.join_conditions.size();
  // mergeFamilies
  output << " :mergeFamilies";
  for (int i = 0; i < cond_num; i++) {
    output << " 0";
  }
  // mergeCollations
  output << " :mergeCollations";
  for (int i = 0; i < cond_num; i++) {
    output << " 0";
  }
  // mergeStrategies
  output << " :mergeStrategies";
  for (int i = 0; i < cond_num; i++) {
    output << " 1";
  }
  // mergeNullsFirst
  output << " :mergeNullsFirst";
  for (int i = 0; i < cond_num; i++) {
    output << " false";
  }

  WriteNodeEnd();
}

void IRToNodestr::WriteNestLoop(const SimplestJoin &join) {
  WriteNodeStart("NESTLOOP");
  WriteCommonPlan(join);
  // jointype
  WriteInt("jointype", GetPgJoinType(join.GetSimplestJoinType()));
  // inner_unique
  WriteBool("inner_unique", false);
  // joinqual
  output << " :joinqual <>";

  // nestParams
  output << " :nestParams ";
  if (join.join_conditions.empty()) {
    output << "<>";
  } else {
    WriteListStart();
    // Write NestLoopParam for each condition
    int param_no = 0;
    for (const auto &cond : join.join_conditions) {
      WriteNodeStart("NESTLOOPPARAM");
      WriteInt("paramno", param_no++);
      output << " :paramval ";
      WriteVar(*cond->left_attr);
      WriteNodeEnd();
    }
    WriteListEnd();
  }

  WriteNodeEnd();
}

void IRToNodestr::WriteHash(const SimplestHash &hash) {
  WriteNodeStart("HASH");
  WriteCommonPlan(hash);

  // hashkeys
  output << " :hashkeys ";
  if (hash.hash_keys.empty()) {
    output << "<>";
  } else {
    WriteListStart();
    for (const auto &key : hash.hash_keys) {
      WriteVar(*key);
    }
    WriteListEnd();
  }

  // skewTable
  WriteUint("skewTable", 0);
  // skewColumn
  WriteInt("skewColumn", 0);
  // skewInherit
  WriteBool("skewInherit", false);
  // rows_total
  WriteFloat("rows_total", 0.0f);

  WriteNodeEnd();
}

void IRToNodestr::WriteSort(const SimplestSort &sort) {
  WriteNodeStart("SORT");
  WriteCommonPlan(sort);

  int num_cols = sort.GetOrderStructVec().size();
  // numCols
  WriteInt("numCols", num_cols);

  // sortColIdx
  output << " :sortColIdx";
  for (const auto &order : sort.GetOrderStructVec()) {
    output << " " << order.sort_col_idx;
  }

  // sortOperators
  output << " :sortOperators";
  for (const auto &order : sort.GetOrderStructVec()) {
    output << " " << GetPgComparisonOp(order.order_type);
  }

  // collations
  output << " :collations";
  for (const auto &order : sort.GetOrderStructVec()) {
    output << " " << GetPgTextOrder(order.text_order);
  }

  // nullsFirst
  output << " :nullsFirst";
  for (const auto &order : sort.GetOrderStructVec()) {
    output << " " << (order.nulls_first ? "true" : "false");
  }

  WriteNodeEnd();
}

void IRToNodestr::WriteAgg(const SimplestAggregate &agg) {
  WriteNodeStart("AGG");
  WriteCommonPlan(agg);

  // aggstrategy: 0 = AGG_PLAIN, 1 = AGG_SORTED, 2 = AGG_HASHED
  WriteInt("aggstrategy", agg.groups.empty() ? 0 : 2);
  // aggsplit: 0 = AGGSPLIT_SIMPLE
  WriteInt("aggsplit", 0);

  int num_cols = agg.groups.size();
  // numCols
  WriteInt("numCols", num_cols);

  // grpColIdx
  output << " :grpColIdx";
  for (int i = 1; i <= num_cols; i++) {
    output << " " << i;
  }

  // grpOperators
  output << " :grpOperators";
  for (int i = 0; i < num_cols; i++) {
    output << " 96"; // equality operator
  }

  // grpCollations
  output << " :grpCollations";
  for (int i = 0; i < num_cols; i++) {
    output << " 0";
  }

  // numGroups
  WriteLong("numGroups", 1);
  // aggParams
  WriteBitmapset("aggParams", {});
  // groupingSets
  output << " :groupingSets <>";
  // chain
  output << " :chain <>";

  WriteNodeEnd();
}

void IRToNodestr::WriteFilter(const SimplestFilter &filter) {
  // Filter is usually represented as a node with qual conditions
  // In PostgreSQL, this might be a Result node or simply part of another node
  WriteNodeStart("RESULT");
  WriteCommonPlan(filter);
  // resconstantqual
  output << " :resconstantqual <>";
  WriteNodeEnd();
}

void IRToNodestr::WriteProjection(const SimplestProjection &proj) {
  // PostgreSQL doesn't have a direct Projection node in the physical plan
  // It's typically represented through target lists
  // We write it as a Result node
  WriteNodeStart("RESULT");
  WriteCommonPlan(proj);
  // resconstantqual
  output << " :resconstantqual <>";
  WriteNodeEnd();
}

void IRToNodestr::WriteMaterial(const SimplestStmt &stmt) {
  WriteNodeStart("MATERIAL");
  WriteCommonPlan(stmt);
  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Target Entry
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteTargetEntry(const SimplestAttr &attr, int resno) {
  WriteNodeStart("TARGETENTRY");

  // expr
  output << " :expr ";
  WriteVar(attr);

  // resno
  WriteInt("resno", resno);
  // resname
  WriteString("resname", attr.GetColumnName());
  // ressortgroupref
  WriteUint("ressortgroupref", 0);
  // resorigtbl
  WriteUint("resorigtbl", 0);
  // resorigcol
  WriteInt("resorigcol", 0);
  // resjunk
  WriteBool("resjunk", false);

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Var
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteVar(const SimplestAttr &attr) {
  WriteNodeStart("VAR");

  // varno
  WriteUint("varno", attr.GetTableIndex());
  // varattno
  WriteInt("varattno", attr.GetColumnIndex());
  // vartype
  WriteUint("vartype", GetPgVarType(attr.GetType()));
  // vartypmod
  WriteInt("vartypmod", -1);
  // varcollid
  WriteUint("varcollid", 0);
  // varlevelsup
  WriteUint("varlevelsup", 0);
  // varnoold
  WriteUint("varnoold", attr.GetTableIndex());
  // varoattno
  WriteInt("varoattno", attr.GetColumnIndex());
  // location
  WriteInt("location", -1);

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Const
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteConst(const SimplestConstVar &const_var) {
  WriteNodeStart("CONST");

  unsigned int pg_type = GetPgVarType(const_var.GetType());
  // consttype
  WriteUint("consttype", pg_type);
  // consttypmod
  WriteInt("consttypmod", -1);
  // constcollid
  WriteUint("constcollid", 0);

  // constlen and constbyval depend on the type
  int const_len = 0;
  bool const_by_val = false;
  switch (const_var.GetType()) {
  case BoolVar:
    const_len = 1;
    const_by_val = true;
    break;
  case IntVar:
    const_len = 4;
    const_by_val = true;
    break;
  case FloatVar:
    const_len = 4;
    const_by_val = true;
    break;
  case StringVar:
    const_len = -1; // variable length
    const_by_val = false;
    break;
  case StringVarArr:
    const_len = -1;
    const_by_val = false;
    break;
  default:
    break;
  }

  // constlen
  WriteInt("constlen", const_len);
  // constbyval
  WriteBool("constbyval", const_by_val);
  // constisnull
  WriteBool("constisnull", false);
  // location
  WriteInt("location", -1);

  // constvalue - write as datum
  output << " :constvalue ";
  WriteDatum(const_var);

  WriteNodeEnd();
}

void IRToNodestr::WriteDatum(const SimplestConstVar &const_var) {
  switch (const_var.GetType()) {
  case BoolVar: {
    output << sizeof(bool) << " [ ";
    bool val = const_var.GetBoolValue();
    unsigned char *bytes = reinterpret_cast<unsigned char *>(&val);
    for (size_t i = 0; i < sizeof(long); i++) {
      output << (i < sizeof(bool) ? (int)bytes[i] : 0) << " ";
    }
    output << "]";
    break;
  }
  case IntVar: {
    output << sizeof(int) << " [ ";
    int val = const_var.GetIntValue();
    unsigned char *bytes = reinterpret_cast<unsigned char *>(&val);
    for (size_t i = 0; i < sizeof(long); i++) {
      output << (i < sizeof(int) ? (int)bytes[i] : 0) << " ";
    }
    output << "]";
    break;
  }
  case FloatVar: {
    output << sizeof(float) << " [ ";
    float val = const_var.GetFloatValue();
    unsigned char *bytes = reinterpret_cast<unsigned char *>(&val);
    for (size_t i = 0; i < sizeof(long); i++) {
      output << (i < sizeof(float) ? (int)bytes[i] : 0) << " ";
    }
    output << "]";
    break;
  }
  case StringVar: {
    std::string str = const_var.GetStringValue();
    // String in PostgreSQL has a 4-byte header (varlena)
    size_t total_len = sizeof(int) + str.length();
    output << total_len << " [ ";
    // Write the varlena header (length << 2)
    int header = (total_len << 2);
    unsigned char *header_bytes = reinterpret_cast<unsigned char *>(&header);
    for (size_t i = 0; i < sizeof(int); i++) {
      output << (int)header_bytes[i] << " ";
    }
    // Write the string bytes
    for (char c : str) {
      output << (int)(unsigned char)c << " ";
    }
    output << "]";
    break;
  }
  case StringVarArr: {
    // Array handling is more complex - simplified version
    std::vector<std::string> arr = const_var.GetStringVecValue();
    // Simplified: just write array header + elements
    output << "0 [ ]"; // Simplified for now
    break;
  }
  default:
    output << "0 [ ]";
    break;
  }
}

//===----------------------------------------------------------------------===//
// Write Param
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteParam(const SimplestParam &param) {
  WriteNodeStart("PARAM");

  // paramkind: 1 = PARAM_EXEC
  WriteInt("paramkind", 1);
  // paramid
  WriteUint("paramid", param.GetParamId());
  // paramtype
  WriteUint("paramtype", GetPgVarType(param.GetType()));
  // paramtypmod
  WriteInt("paramtypmod", -1);
  // paramcollid
  WriteUint("paramcollid", 0);
  // location
  WriteInt("location", -1);

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Expressions
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteOpExpr(const SimplestExpr &expr) {
  switch (expr.GetNodeType()) {
  case VarComparisonNode: {
    const auto &var_comp = expr.Cast<SimplestVarComparison>();
    WriteNodeStart("OPEXPR");
    // opno
    WriteUint("opno", GetPgComparisonOp(var_comp.GetSimplestExprType()));
    // opfuncid
    WriteUint("opfuncid", 0);
    // opresulttype: 16 = BOOLOID
    WriteUint("opresulttype", 16);
    // opretset
    WriteBool("opretset", false);
    // opcollid
    WriteUint("opcollid", 0);
    // inputcollid
    WriteUint("inputcollid", 0);
    // args
    output << " :args ";
    WriteListStart();
    WriteVar(*var_comp.left_attr);
    WriteVar(*var_comp.right_attr);
    WriteListEnd();
    // location
    WriteInt("location", -1);
    WriteNodeEnd();
    break;
  }
  case VarConstComparisonNode: {
    const auto &var_const = expr.Cast<SimplestVarConstComparison>();
    WriteNodeStart("OPEXPR");
    WriteUint("opno", GetPgComparisonOp(var_const.GetSimplestExprType()));
    WriteUint("opfuncid", 0);
    WriteUint("opresulttype", 16);
    WriteBool("opretset", false);
    WriteUint("opcollid", 0);
    WriteUint("inputcollid", 0);
    output << " :args ";
    WriteListStart();
    WriteVar(*var_const.attr);
    WriteConst(*var_const.const_var);
    WriteListEnd();
    WriteInt("location", -1);
    WriteNodeEnd();
    break;
  }
  case VarParamComparisonNode: {
    const auto &var_param = expr.Cast<SimplestVarParamComparison>();
    WriteNodeStart("OPEXPR");
    WriteUint("opno", GetPgComparisonOp(var_param.GetSimplestExprType()));
    WriteUint("opfuncid", 0);
    WriteUint("opresulttype", 16);
    WriteBool("opretset", false);
    WriteUint("opcollid", 0);
    WriteUint("inputcollid", 0);
    output << " :args ";
    WriteListStart();
    WriteVar(*var_param.attr);
    WriteParam(*var_param.param_var);
    WriteListEnd();
    WriteInt("location", -1);
    WriteNodeEnd();
    break;
  }
  case LogicalExprNode: {
    const auto &logical = expr.Cast<SimplestLogicalExpr>();
    WriteBoolExpr(logical);
    break;
  }
  case IsNullExprNode: {
    const auto &is_null = expr.Cast<SimplestIsNullExpr>();
    WriteNullTest(is_null);
    break;
  }
  case SingleAttrExprNode: {
    const auto &single_attr = expr.Cast<SimplestSingleAttrExpr>();
    WriteVar(*single_attr.attr);
    break;
  }
  default:
    std::cerr << "IRToNodestr: Unsupported expr type: " << expr.GetNodeType()
              << std::endl;
    assert(false);
  }
}

void IRToNodestr::WriteBoolExpr(const SimplestLogicalExpr &expr) {
  WriteNodeStart("BOOLEXPR");

  // boolop
  const char *op_str = nullptr;
  switch (expr.GetLogicalOp()) {
  case LogicalAnd:
    op_str = "and";
    break;
  case LogicalOr:
    op_str = "or";
    break;
  case LogicalNot:
    op_str = "not";
    break;
  default:
    op_str = "and";
    break;
  }
  output << " :boolop " << op_str;

  // args
  output << " :args ";
  WriteListStart();
  if (expr.left_expr) {
    WriteOpExpr(*expr.left_expr);
  }
  if (expr.right_expr) {
    WriteOpExpr(*expr.right_expr);
  }
  WriteListEnd();

  // location
  WriteInt("location", -1);

  WriteNodeEnd();
}

void IRToNodestr::WriteNullTest(const SimplestIsNullExpr &expr) {
  WriteNodeStart("NULLTEST");

  // arg
  output << " :arg ";
  WriteVar(*expr.attr);

  // nulltesttype: 0 = IS_NULL, 1 = IS_NOT_NULL
  WriteInt("nulltesttype", expr.GetSimplestExprType() == NullType ? 0 : 1);
  // argisrow
  WriteBool("argisrow", false);
  // location
  WriteInt("location", -1);

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Write Range Table Entry
//===----------------------------------------------------------------------===//

void IRToNodestr::WriteRangeTblEntry(unsigned int table_index,
                                     const std::string &table_name) {
  WriteNodeStart("RTE");

  // Write alias first
  output << " :alias ";
  if (table_index > 0 && table_index <= table_col_names.size()) {
    const auto &table_entry = table_col_names[table_index - 1];
    if (!table_entry.empty()) {
      std::vector<std::string> col_names;
      for (const auto &col : table_entry.begin()->second) {
        col_names.push_back(col->GetLiteralValue());
      }
      WriteAlias(table_name, col_names);
    } else {
      output << "<>";
    }
  } else {
    output << "<>";
  }

  // eref
  output << " :eref ";
  if (table_index > 0 && table_index <= table_col_names.size()) {
    const auto &table_entry = table_col_names[table_index - 1];
    if (!table_entry.empty()) {
      std::vector<std::string> col_names;
      for (const auto &col : table_entry.begin()->second) {
        col_names.push_back(col->GetLiteralValue());
      }
      WriteAlias(table_name, col_names);
    } else {
      output << "<>";
    }
  } else {
    output << "<>";
  }

  // rtekind: 0 = RTE_RELATION
  WriteInt("rtekind", 0);
  // relid - we don't have the actual OID, use table_index as placeholder
  WriteUint("relid", table_index);
  // relkind
  WriteChar("relkind", 'r');
  // rellockmode
  WriteInt("rellockmode", 1);
  // tablesample
  output << " :tablesample <>";
  // lateral
  WriteBool("lateral", false);
  // inh
  WriteBool("inh", true);
  // inFromCl
  WriteBool("inFromCl", true);
  // requiredPerms
  WriteUint("requiredPerms", 2);
  // checkAsUser
  WriteUint("checkAsUser", 0);
  // selectedCols
  WriteBitmapset("selectedCols", {});
  // insertedCols
  WriteBitmapset("insertedCols", {});
  // updatedCols
  WriteBitmapset("updatedCols", {});
  // extraUpdatedCols
  WriteBitmapset("extraUpdatedCols", {});
  // securityQuals
  output << " :securityQuals <>";

  WriteNodeEnd();
}

void IRToNodestr::WriteAlias(const std::string &alias_name,
                             const std::vector<std::string> &col_names) {
  WriteNodeStart("ALIAS");

  // aliasname
  WriteString("aliasname", alias_name);

  // colnames
  output << " :colnames ";
  WriteListStart();
  for (const auto &col : col_names) {
    output << "\"" << col << "\" ";
  }
  WriteListEnd();

  WriteNodeEnd();
}

//===----------------------------------------------------------------------===//
// Type Conversion Functions
//===----------------------------------------------------------------------===//

unsigned int IRToNodestr::GetPgVarType(SimplestVarType type) {
  switch (type) {
  case BoolVar:
    return 16; // BOOLOID
  case IntVar:
    return 23; // INT4OID
  case FloatVar:
    return 700; // FLOAT4OID
  case StringVar:
    return 25; // TEXTOID
  case StringVarArr:
    return 1009; // TEXTARRAYOID
  case Date:
    return 1082; // DATEOID
  default:
    return 0;
  }
}

unsigned int IRToNodestr::GetPgJoinType(SimplestJoinType type) {
  switch (type) {
  case Inner:
    return 0; // JOIN_INNER
  case Left:
    return 1; // JOIN_LEFT
  case Full:
    return 2; // JOIN_FULL
  case Right:
    return 3; // JOIN_RIGHT
  case Semi:
    return 4; // JOIN_SEMI
  case Anti:
    return 5; // JOIN_ANTI
  case UniqueOuter:
    return 6; // JOIN_UNIQUE_OUTER
  case UniqueInner:
    return 7; // JOIN_UNIQUE_INNER
  default:
    return 0;
  }
}

unsigned int IRToNodestr::GetPgComparisonOp(SimplestExprType type) {
  // Return OID of the operator from pg_operator.dat
  switch (type) {
  case Equal:
    return 96; // int4eq
  case NotEqual:
    return 518; // int4ne
  case LessThan:
    return 97; // int4lt
  case GreaterThan:
    return 521; // int4gt
  case LessEqual:
    return 523; // int4le
  case GreaterEqual:
    return 525; // int4ge
  case TextLike:
    return 1209; // textlike
  case Text_Not_Like:
    return 1210; // textnlike
  default:
    return 96;
  }
}

unsigned int IRToNodestr::GetPgTextOrder(SimplestTextOrder order) {
  switch (order) {
  case DefaultTextOrder:
    return 0;
  case UTF8:
    return 0;
  case C:
    return 0;
  default:
    return 0;
  }
}

unsigned int IRToNodestr::GetPgAggFnOid(SimplestAggFnType type) {
  switch (type) {
  case Min:
    return 2132; // min(int4)
  case Max:
    return 2129; // max(int4)
  case Sum:
    return 2108; // sum(int4)
  case Average:
    return 2100; // avg(int4)
  default:
    return 0;
  }
}

//===----------------------------------------------------------------------===//
// Utility Functions
//===----------------------------------------------------------------------===//

void IRToNodestr::CollectTableIndices(const SimplestStmt *stmt,
                                      std::vector<unsigned int> &indices) {
  if (!stmt)
    return;

  if (stmt->GetNodeType() == ScanNode) {
    const auto &scan = stmt->Cast<SimplestScan>();
    unsigned int idx = scan.GetTableIndex();
    // Check if not already in the list
    if (std::find(indices.begin(), indices.end(), idx) == indices.end()) {
      indices.push_back(idx);
    }
  }

  for (const auto &child : stmt->children) {
    if (child) {
      CollectTableIndices(child.get(), indices);
    }
  }
}

} // namespace ir_sql_converter
