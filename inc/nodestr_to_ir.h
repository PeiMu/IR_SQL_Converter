//===----------------------------------------------------------------------===//
//                         IR_SQL_Converter
//
// nodestr_to_ir.h
// from Postgres source code: src/backend/nodes/read.c
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstring>
#include <algorithm>
#include <cassert>

#include "nodes/pg_list.hpp"
#include "nodes/value.hpp"
#include "pg_functions.hpp"
#include "simplest_ir.h"

namespace ir_sql_converter {
	/* And a few guys need only the PG_strtok support fields */
#define READ_TEMP_LOCALS()                                                                                             \
  const char *token;                                                                                                 \
  int length

#define nullable_string(token, length) ((length) == 0 ? NULL : DeBackslash(token, length))

#define strtobool(x) ((*(x) == 't') ? true : false)

	using table_str = std::unordered_map<std::string, std::vector<std::unique_ptr<SimplestLiteral>>>;
	using Oid = unsigned int;

	class NodestrToIR {
	 public:
		NodestrToIR() = default;

		~NodestrToIR() = default;

		std::unique_ptr<SimplestNode> StringToNode(const char *str);

		std::deque<table_str> table_col_names;

		std::unique_ptr<SimplestStmt> GenerateProjHead(std::unique_ptr<SimplestStmt> postgres_stmt, size_t sub_plan_id);

		void Clear() {
			table_col_names.clear();
			index_conditions.clear();
			agg_fns.clear();
		}

	 private:
		const char *PG_strtok(int *length);

		std::unique_ptr<SimplestNode> NodeRead(const char *token, int tok_len, bool return_vector = false,
		                                       std::vector<std::unique_ptr<SimplestNode>> *node_vec = nullptr);

		char *DeBackslash(const char *token, int length);

		std::unique_ptr<SimplestNode> StringToNodeInternal(const char *str, bool restore_loc_fields);

		int StrToInt(const char *str, char **endptr, int base);

		duckdb_libpgquery::PGNodeTag NodeTokenType(const char *token, int length);

		std::unique_ptr<SimplestNode> ParseNodeString();

		// read postgres nodes
		void *ReadBitmapset();

		std::vector<int> ReadAttrNumberCols(int numCols);

		std::vector<int> ReadIntCols(int numCols);

		std::vector<bool> ReadBoolCols(int numCols);

		std::unique_ptr<SimplestStmt> ReadCommonPlan();

		std::unique_ptr<SimplestAggregate> ReadAgg();

		std::unique_ptr<SimplestAttr> ReadAggref();

		std::unique_ptr<SimplestNode> ReadTargetEntry();

		std::unique_ptr<SimplestParam> ReadParam();

		std::unique_ptr<SimplestAttr> ReadVar();

		std::unique_ptr<SimplestAttr> ReadRelabelType();

		std::unique_ptr<SimplestConstVar> ReadConst();

		void ReadGather();

		std::unique_ptr<SimplestJoin> ReadCommonJoin();

		std::unique_ptr<SimplestHash> ReadHash();

		std::unique_ptr<SimplestJoin> ReadHashJoin();

		std::unique_ptr<SimplestJoin> ReadMergeJoin();

		std::unique_ptr<SimplestJoin> ReadNestLoop();

		std::unique_ptr<SimplestVarComparison> ReadNestLoopParam();

		std::unique_ptr<SimplestScan> ReadCommonScan();

		std::unique_ptr<SimplestScan> ReadSeqScan();

		std::unique_ptr<SimplestScan> ReadBitmapHeapScan();

		std::unique_ptr<SimplestNode> ReadBitmapIndexScan();

		std::unique_ptr<SimplestScan> ReadIndexScan();

		std::unique_ptr<SimplestScan> ReadIndexOnlyScan();

		std::unique_ptr<SimplestSort> ReadSort();

		std::unique_ptr<SimplestExpr> ReadOpExpr();

		std::unique_ptr<SimplestLogicalExpr> ReadBoolExpr();

		std::unique_ptr<SimplestIsNullExpr> ReadNullTest();

		std::unique_ptr<SimplestExpr> ReadScalarArrayOpExpr();

		std::unique_ptr<SimplestStmt> ReadMaterial();

		std::unique_ptr<SimplestStmt> ReadPlannedStmt();

		void ReadRangeTblEntry();

		void ReadAlias();

		PGDatum ReadDatum(bool typbyval, unsigned int &datum_len);

		std::string ParseText(PGDatum datum, unsigned int datum_len);

		std::vector<std::string> ParseTextArray(PGDatum datum, unsigned int datum_len);

		SimplestVarType GetSimplestVarType(unsigned int type_id);

		SimplestJoinType GetSimplestJoinType(unsigned int type_id);

		SimplestExprType GetSimplestComparisonType(unsigned int type_id);

		SimplestTextOrder GetSimplestTextOrderType(int type_id);

		SimplestAggFnType GetSimplestAggFnType(unsigned int aggfnoid);

		std::vector<std::unique_ptr<SimplestVarParamComparison>> index_conditions;
		agg_fn_pair agg_fns;

		void PopulateTableNames(SimplestStmt *stmt);

		void PopulateColumnName(SimplestAttr *attr);

		void PopulateColumnNamesInExpr(SimplestExpr *expr);
	};
}