#include "c_interface.h"
#include "cpp_interface.h"
#include <cstring>
#include <memory>
#include <vector>

using namespace ir_sql_converter;

struct IRConverterStmtListInternal {
	std::vector<std::unique_ptr<SimplestStmt>> stmts;
	// todo
	std::string last_error;
	// todo
	int version{};
};

extern "C" {
IRConverterStmtList ConvertNodeStrToIR_C(const char *nodestr_file_name) {
	if (!nodestr_file_name) {
		return nullptr;
	}

	try {
		std::string filename(nodestr_file_name);
		auto stmts = ConvertNodeStrToIR(filename);

		auto *internal = new IRConverterStmtListInternal();
		internal->stmts = std::move(stmts);

		return static_cast<IRConverterStmtList>(internal);
	} catch (...) {
		return nullptr;
	}
}

size_t StmtListSize(IRConverterStmtList list) {
	if (!list) {
		return 0;
	}

	auto *internal = static_cast<IRConverterStmtListInternal *>(list);
	return internal->stmts.size();
}

IRConverterStmt GetStmtFromList(IRConverterStmtList list, size_t index) {
	if (!list) {
		return nullptr;
	}

	auto *internal = static_cast<IRConverterStmtListInternal *>(list);
	if (index >= internal->stmts.size()) {
		std::cout << "index exceed the list size!" << std::endl;
		return nullptr;
	}

	return static_cast<IRConverterStmt>(internal->stmts[index].get());
}

char *ConvertIRToSQL_C(IRConverterStmt stmt, int query_id, int save_file, const char *sql_path) {
	if (!stmt) {
		return nullptr;
	}

	try {
		auto *simplest_stmt = static_cast<SimplestStmt *>(stmt);
		std::string sql_path_str = sql_path ? std::string(sql_path) : "./";
		std::string sql = ConvertIRToSQL(*simplest_stmt, query_id, save_file == 1, sql_path_str);

		char *c_str = static_cast<char *>(malloc(sql.length() + 1));
		if (c_str) {
			strcpy(c_str, sql.c_str());
		} else {
			std::cout << "NO SQL for " << query_id << "!" << std::endl;
		}
		return c_str;
	} catch (...) {
		return nullptr;
	}
}

void FreeStmtList(IRConverterStmtList list) {
	if (!list) {
		return;
	}
	auto *internal = static_cast<IRConverterStmtListInternal *>(list);
	delete internal;
}

void FreeSQLString(char *str) {
	if (str) {
		free(str);
	}
}
}
