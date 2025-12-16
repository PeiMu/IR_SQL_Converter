#include <stdio.h>
#include <stdlib.h>

#include "c_interface.h"

int TestConvertNodeStrToIRFromFile_C() {
  IRConverterStmtList stmt_list =
      ConvertNodeStrToIRFromFile_C("../postgres_plan");
  if (!stmt_list) {
    fprintf(stderr, "Failed to convert nodestr to IR\n");
    return 1;
  }

  size_t query_count = StmtListSize(stmt_list);
  printf("Converted %zu queries\n", query_count);

  for (size_t i = 0; i < query_count; i++) {
    IRConverterStmt stmt = GetRawStmtFromList(stmt_list, i);
    if (!stmt) {
      fprintf(stderr, "Failed to get stmt %zu\n", i);
      return 1;
    }

    char *sql = ConvertIRToSQL_C(stmt, i, 1, "../query_");
    if (sql) {
      printf("Generated SQL code for query %zu\n", i);
      FreeSQLString(sql);
    } else {
      fprintf(stderr, "Failed to convert IR to SQL for query %zu\n", i);
    }
  }

  FreeStmtList(stmt_list);
  return 0;
}

int TestConvertNodeStrToIR_C() {
  FILE *fp;
  char *str;
  fp = fopen("../postgres_plan", "rb");
  if (!fp) {
    fprintf(stderr, "Failed to open file");
    return 1;
  }

  fseek(fp, 0L, SEEK_END);
  long size = ftell(fp);
  rewind(fp);

  str = malloc(size + 1);
  if (!str) {
    fclose(fp);
    fprintf(stderr, "Failed to alloc memory");
    return 1;
  }

  size_t id = 0;
  size_t len = 0;
  ssize_t read;
  while (-1 != (read = getline(&str, &len, fp))) {
    IRConverterStmt stmt = ConvertNodeStrToIR_C(str, id);
    if (!stmt) {
      fprintf(stderr, "Failed to convert nodestr to IR\n");
      return 1;
    }

    IRConverterStmt raw_stmt = GetRawStmt(stmt);

    char *sql = ConvertIRToSQL_C(raw_stmt, id, 1, "../query_");
    if (sql) {
      printf("Generated SQL code for query %zu\n", id);
      FreeSQLString(sql);
    } else {
      fprintf(stderr, "Failed to convert IR to SQL for query %zu\n", id);
    }

    id++;
    FreeStmt(stmt);
  }
  fclose(fp);
  free(str);

  return 0;
}

int TestConvertSQLToIR_C_PostgreSQL() {
  FILE *fp;
  char *str;
  fp = fopen("/home/pei/Project/benchmarks/imdb_job-postgres/queries/6d.sql", "rb");
  if (!fp) {
    fprintf(stderr, "Failed to open file\n");
    return 1;
  }

  fseek(fp, 0L, SEEK_END);
  long size = ftell(fp);
  rewind(fp);

  str = malloc(size + 1);
  if (!str) {
    fclose(fp);
    fprintf(stderr, "Failed to alloc memory\n");
    return 1;
  }

  size_t bytes_read = fread(str, 1, size, fp);
  str[bytes_read] = '\0';
  fclose(fp);

  printf("Testing ParseTree to IR conversion for SQL query:\n%s\n", str);

  IRConverterStmt stmt = ConvertParseTreeToIR_C(str, 1);
  if (!stmt) {
    fprintf(stderr, "Failed to convert SQL to IR\n");
    free(str);
    return 1;
  }

  IRConverterStmt raw_stmt = GetRawStmt(stmt);
  if (!raw_stmt) {
    fprintf(stderr, "Failed to get raw statement\n");
    FreeStmt(stmt);
    free(str);
    return 1;
  }

  char *sql = ConvertIRToSQL_C(raw_stmt, 0, 1, "../query_parsetree_");
  if (sql) {
    printf("Successfully generated SQL:\n%s\n", sql);
    FreeSQLString(sql);
  } else {
    fprintf(stderr, "Failed to convert IR to SQL\n");
    FreeStmt(stmt);
    free(str);
    return 1;
  }

  FreeStmt(stmt);
  free(str);
  return 0;
}

int main(void) {
  if (0 != TestConvertNodeStrToIRFromFile_C()) {
    return 1;
  }
  if (0 != TestConvertNodeStrToIR_C()) {
    return 1;
  }
  if (0 != TestConvertSQLToIR_C_PostgreSQL()) {
    return 1;
  }

  return 0;
}
