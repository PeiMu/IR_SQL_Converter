//===----------------------------------------------------------------------===//
//                         IR_to_NodeStr
//
// ir_to_nodestr.h
// Convert SimplestIR to PostgreSQL's nodestring format
// (inverse of nodestr_to_ir.h)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <deque>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "simplest_ir.h"

namespace ir_sql_converter {

class IRToNodestr {
public:
  IRToNodestr() = default;
  ~IRToNodestr() = default;

  // Main entry point: convert AQPStmt to PostgreSQL nodestring
  std::string NodeToString(const std::unique_ptr<AQPStmt> &stmt);

  // Store table/column metadata for rtable generation
  std::deque<table_str> table_col_names;

  void Clear() {
    table_col_names.clear();
    output.str("");
    output.clear();
  }

private:
  std::ostringstream output;

  // Write helper functions
  void WriteToken(const char *token);
  void WriteInt(const char *field, int value);
  void WriteUint(const char *field, unsigned int value);
  void WriteLong(const char *field, long value);
  void WriteFloat(const char *field, float value);
  void WriteBool(const char *field, bool value);
  void WriteString(const char *field, const std::string &value);
  void WriteChar(const char *field, char value);

  // Write node structures
  void WriteNodeStart(const char *node_name);
  void WriteNodeEnd();
  void WriteListStart();
  void WriteListEnd();
  void WriteIntList(const char *field, const std::vector<int> &values);
  void WriteOidList(const char *field, const std::vector<unsigned int> &values);
  void WriteBitmapset(const char *field, const std::vector<int> &values);

  // Write PostgreSQL nodes
  void WritePlannedStmt(const AQPStmt &stmt);
  void WriteCommonPlan(const AQPStmt &stmt);
  void WriteTargetEntry(const SimplestAttr &attr, int resno);
  void WriteVar(const SimplestAttr &attr);
  void WriteConst(const SimplestConstVar &const_var);
  void WriteParam(const SimplestParam &param);
  void WriteOpExpr(const AQPExpr &expr);
  void WriteBoolExpr(const SimplestLogicalExpr &expr);
  void WriteNullTest(const SimplestIsNullExpr &expr);

  // Write plan nodes
  void WritePlanNode(const AQPStmt &stmt);
  void WriteSeqScan(const SimplestScan &scan);
  void WriteIndexScan(const SimplestScan &scan);
  void WriteHashJoin(const SimplestJoin &join);
  void WriteMergeJoin(const SimplestJoin &join);
  void WriteNestLoop(const SimplestJoin &join);
  void WriteHash(const SimplestHash &hash);
  void WriteSort(const SimplestSort &sort);
  void WriteAgg(const SimplestAggregate &agg);
  void WriteFilter(const SimplestFilter &filter);
  void WriteMaterial(const AQPStmt &stmt);
  void WriteProjection(const SimplestProjection &proj);

  // Write range table
  void WriteRangeTblEntry(unsigned int table_index, const std::string &table_name);
  void WriteAlias(const std::string &alias_name,
                  const std::vector<std::string> &col_names);

  // Write datum for CONST node
  void WriteDatum(const SimplestConstVar &const_var);

  // Type conversion functions (reverse of GetSimplestVarType, etc.)
  unsigned int GetPgVarType(SimplestVarType type);
  unsigned int GetPgJoinType(SimplestJoinType type);
  unsigned int GetPgComparisonOp(SimplestExprType type);
  unsigned int GetPgTextOrder(SimplestTextOrder order);
  unsigned int GetPgAggFnOid(SimplestAggFnType type);

  // Escape backslashes for string values
  std::string EscapeString(const std::string &str);

  // Collect all table indices from the plan tree
  void CollectTableIndices(const AQPStmt *stmt,
                           std::vector<unsigned int> &indices);
};

} // namespace ir_sql_converter
