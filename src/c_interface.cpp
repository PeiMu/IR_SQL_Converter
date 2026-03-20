#include "c_interface.h"
#include "cpp_interface.h"
#include <cstring>
#include <memory>
#include <nlohmann/json.hpp>
#include <vector>

using json = nlohmann::json;

using namespace ir_sql_converter;

struct IRConverterStmtListInternal {
  std::vector<std::unique_ptr<AQPStmt>> stmts;
  // todo
  std::string last_error;
  // todo
  int version{};
};

struct IRConverterStmtInternal {
  std::unique_ptr<AQPStmt> stmt;
  // todo
  std::string last_error;
  // todo
  int version{};
};

extern "C" {

int InitSchemaParser_C(const char *schema_path) {
  if (!schema_path) {
    return 0;
  }
  try {
    std::string path(schema_path);
    return InitSchemaParser(path) ? 1 : 0;
  } catch (...) {
    return 0;
  }
}

void CleanupSchemaParser_C(void) { CleanupSchemaParser(); }

IRConverterStmtList
ConvertNodeStrToIRFromFile_C(const char *nodestr_file_name) {
  if (!nodestr_file_name) {
    return nullptr;
  }

  try {
    std::string filename(nodestr_file_name);
    auto stmts = ConvertNodeStrToIRFromFile(filename);

    auto *internal = new IRConverterStmtListInternal();
    internal->stmts = std::move(stmts);

    return static_cast<IRConverterStmtList>(internal);
  } catch (...) {
    return nullptr;
  }
}

IRConverterStmt ConvertNodeStrToIR_C(const char *nodestr, size_t query_id) {
  if (!nodestr) {
    return nullptr;
  }

  try {
    std::string nodestr_str(nodestr);
    auto stmt = ConvertNodeStrToIR(nodestr_str, query_id);

    auto *internal = new IRConverterStmtInternal();
    internal->stmt = std::move(stmt);

    return static_cast<IRConverterStmt>(internal);
  } catch (...) {
    return nullptr;
  }
}

char *ConvertIRToNodeStr_C(IRConverterStmt stmt) {
  if (!stmt) {
    return nullptr;
  }

  try {
    auto *internal = static_cast<IRConverterStmtInternal *>(stmt);
    std::string nodestr = ConvertIRToNodeStr(internal->stmt);

    char *c_str = static_cast<char *>(malloc(nodestr.length() + 1));
    if (c_str) {
      strcpy(c_str, nodestr.c_str());
    } else {
      std::cout << "Failed to allocate memory for nodestr!" << std::endl;
    }
    return c_str;
  } catch (...) {
    return nullptr;
  }
}

IRConverterStmt ConvertParseTreeToIR_C(const char *parse_tree_json,
                                       unsigned int sub_plan_id) {
  if (!parse_tree_json) {
    return nullptr;
  }

  try {
    // Parse the JSON string to nlohmann::json
    json parse_tree = json::parse(parse_tree_json);
    auto stmt = ConvertParseTreeToIR(parse_tree, sub_plan_id);

    auto *internal = new IRConverterStmtInternal();
    internal->stmt = std::move(stmt);

    return static_cast<IRConverterStmt>(internal);
  } catch (...) {
    return nullptr;
  }
}

IRConverterStmt ConvertParseTreeToIRWithSchema_C(const char *parse_tree_json,
                                                 unsigned int sub_plan_id) {
  if (!parse_tree_json) {
    return nullptr;
  }

  try {
    // Parse the JSON string to nlohmann::json
    json parse_tree = json::parse(parse_tree_json);
    auto stmt = ConvertParseTreeToIRWithSchema(parse_tree, sub_plan_id);

    auto *internal = new IRConverterStmtInternal();
    internal->stmt = std::move(stmt);

    return static_cast<IRConverterStmt>(internal);
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

IRConverterStmt GetRawStmtFromList(IRConverterStmtList list, size_t index) {
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

IRConverterStmt GetRawStmt(IRConverterStmt stmt) {
  if (!stmt) {
    return nullptr;
  }

  auto *internal = static_cast<IRConverterStmtInternal *>(stmt);
  return static_cast<IRConverterStmt>(internal->stmt.get());
}

char *ConvertIRToSQL_C(IRConverterStmt stmt, int query_id, int save_file,
                       const char *sql_path) {
  if (!stmt) {
    return nullptr;
  }

  try {
    auto *simplest_stmt = static_cast<AQPStmt *>(stmt);
    simplest_stmt->Print(true);
    std::string sql_path_str = sql_path ? std::string(sql_path) : "./";
    std::string sql =
        ConvertIRToSQL(*simplest_stmt, query_id, save_file == 1, sql_path_str);

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

void FreeStmt(IRConverterStmt stmt) {
  if (!stmt) {
    return;
  }
  auto *internal = static_cast<IRConverterStmt *>(stmt);
  delete internal;
}

void FreeSQLString(char *str) {
  if (str) {
    free(str);
  }
}
}
