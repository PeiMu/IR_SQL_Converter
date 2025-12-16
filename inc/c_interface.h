#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *IRConverterStmtList;
typedef void *IRConverterStmt;

IRConverterStmtList ConvertNodeStrToIRFromFile_C(const char *nodestr_file_name);

IRConverterStmt ConvertNodeStrToIR_C(const char *nodestr, size_t query_id);

IRConverterStmt ConvertParseTreeToIR_C(const char *sql);

size_t StmtListSize(IRConverterStmtList list);

IRConverterStmt GetRawStmtFromList(IRConverterStmtList list, size_t index);

IRConverterStmt GetRawStmt(IRConverterStmt stmt);

char *ConvertIRToSQL_C(IRConverterStmt stmt, int query_id, int save_file,
                       const char *sql_path);

void FreeStmtList(IRConverterStmtList list);

void FreeStmt(IRConverterStmt stmt);

void FreeSQLString(char *str);

#ifdef __cplusplus
}
#endif