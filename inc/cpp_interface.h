#pragma once

#include <fstream>

#include "simplest_ir.h"
#include "nodestr_to_ir.h"
#include "ir_to_sql.h"

namespace ir_sql_converter {
	std::vector<std::unique_ptr<SimplestStmt>> ConvertNodeStrToIRFromFile(const std::string &nodestr_file_name);

	std::unique_ptr<SimplestStmt> ConvertNodeStrToIR(const std::string &nodestr, size_t query_id);

	std::string
	ConvertIRToSQL(SimplestStmt &simplest_stmt, int query_id, bool save_file = false,
	               const std::string &sql_path = "");
}