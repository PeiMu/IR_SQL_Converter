#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void *IRConverterStmtList;
typedef void *IRConverterStmt;

// Schema parser functions
int InitSchemaParser_C(const char *schema_path);
void CleanupSchemaParser_C(void);

IRConverterStmtList ConvertNodeStrToIRFromFile_C(const char *nodestr_file_name);

IRConverterStmt ConvertNodeStrToIR_C(const char *nodestr, size_t query_id);

char *ConvertIRToNodeStr_C(IRConverterStmt stmt);

// Convert parse tree JSON to IR (column indices will be 0 without schema)
IRConverterStmt ConvertParseTreeToIR_C(const char *parse_tree_json,
                                       unsigned int sub_plan_id);

// Convert parse tree JSON to IR using the global schema parser
// (must call InitSchemaParser_C first for correct column indices)
IRConverterStmt ConvertParseTreeToIRWithSchema_C(const char *parse_tree_json,
                                                 unsigned int sub_plan_id);

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