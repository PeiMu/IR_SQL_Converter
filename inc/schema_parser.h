//===----------------------------------------------------------------------===//
//                         Schema Parser
//
// schema_parser.h
// Parses CREATE TABLE statements to extract column information
//
//===----------------------------------------------------------------------===//

#pragma once

#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace ir_sql_converter {

struct ColumnInfo {
  std::string name;
  std::string type;
  unsigned int ordinal_position; // 0-based
};

struct TableSchema {
  std::string table_name;
  std::vector<ColumnInfo> columns;
  std::unordered_map<std::string, unsigned int> column_name_to_index;
};

class SchemaParser {
public:
  SchemaParser() = default;
  ~SchemaParser() = default;

  // Load schema from a SQL file containing CREATE TABLE statements
  bool LoadFromFile(const std::string &schema_path);

  // Load schema from a string
  bool LoadFromString(const std::string &schema_sql);

  // Get column index (0-based) for a given table and column
  // Returns -1 if not found
  int GetColumnIndex(const std::string &table_name,
                     const std::string &column_name) const;

  // Get all columns for a table
  const TableSchema *GetTableSchema(const std::string &table_name) const;

  // Check if a table exists in the schema
  bool HasTable(const std::string &table_name) const;

  // Get all table names
  std::vector<std::string> GetTableNames() const;

private:
  // Parse a single CREATE TABLE statement
  bool ParseCreateTable(const std::string &create_stmt);

  // Normalize table/column names (lowercase)
  static std::string Normalize(const std::string &name);

  // Map: lowercase table name -> TableSchema
  std::unordered_map<std::string, TableSchema> tables_;
};

} // namespace ir_sql_converter
