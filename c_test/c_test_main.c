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

int TestConvertIRToNodeStr_C() {
  FILE *fp_in;
  FILE *fp_out;
  char *str = NULL;
  size_t len = 0;
  ssize_t read;

  fp_in = fopen("../postgres_plan", "rb");
  if (!fp_in) {
    fprintf(stderr, "Failed to open input file ../postgres_plan\n");
    return 1;
  }

  fp_out = fopen("../postgres_plan_generated", "wb");
  if (!fp_out) {
    fprintf(stderr, "Failed to open output file ../postgres_plan_generated\n");
    fclose(fp_in);
    return 1;
  }

  size_t id = 0;
  while (-1 != (read = getline(&str, &len, fp_in))) {
    // Remove trailing newline if present
    if (read > 0 && str[read - 1] == '\n') {
      str[read - 1] = '\0';
    }

    // Convert nodestr to IR
    IRConverterStmt stmt = ConvertNodeStrToIR_C(str, id);
    if (!stmt) {
      fprintf(stderr, "Failed to convert nodestr to IR for query %zu\n", id);
      fclose(fp_in);
      fclose(fp_out);
      free(str);
      return 1;
    }

    // Convert IR back to nodestr
    char *generated_nodestr = ConvertIRToNodeStr_C(stmt);
    if (generated_nodestr) {
      // Write the generated nodestring to output file
      fprintf(fp_out, "%s\n", generated_nodestr);
      printf("Query %zu: Successfully converted IR to NodeStr\n", id);
      FreeSQLString(generated_nodestr);
    } else {
      fprintf(stderr, "Failed to convert IR to NodeStr for query %zu\n", id);
    }

    id++;
    FreeStmt(stmt);
  }

  printf("Processed %zu queries\n", id);

  fclose(fp_in);
  fclose(fp_out);
  free(str);

  return 0;
}

int main(void) {
  //  if (0 != TestConvertNodeStrToIRFromFile_C()) {
  //    return 1;
  //  }
  //  if (0 != TestConvertNodeStrToIR_C()) {
  //    return 1;
  //  }
  if (0 != TestConvertIRToNodeStr_C()) {
    return 1;
  }

  return 0;
}
