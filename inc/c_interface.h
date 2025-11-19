#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

	typedef void *IRConverterStmtList;
	typedef void *IRConverterStmt;

	IRConverterStmtList ConvertNodeStrToIR_C(const char *nodestr_file_name);

	size_t StmtListSize(IRConverterStmtList list);

	IRConverterStmt GetStmtFromList(IRConverterStmtList list, size_t index);

	char *ConvertIRToSQL_C(IRConverterStmt stmt, int query_id, int save_file, const char *sql_path);

	void FreeStmtList(IRConverterStmtList list);

	void FreeSQLString(char *str);

#ifdef __cplusplus
}
#endif