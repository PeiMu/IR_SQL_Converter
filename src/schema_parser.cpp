//===----------------------------------------------------------------------===//
//                         Schema Parser
//
// schema_parser.cpp
// Implementation of schema parsing from CREATE TABLE statements
//
//===----------------------------------------------------------------------===//

#include "schema_parser.h"
#include <algorithm>
#include <cctype>
#include <iostream>

namespace ir_sql_converter {

std::string SchemaParser::Normalize(const std::string &name) {
  std::string result = name;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

bool SchemaParser::LoadFromFile(const std::string &schema_path) {
  std::ifstream file(schema_path);
  if (!file.is_open()) {
    std::cerr << "[SchemaParser] Failed to open schema file: " << schema_path
              << std::endl;
    return false;
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  return LoadFromString(buffer.str());
}

bool SchemaParser::LoadFromString(const std::string &schema_sql) {
  // Split by CREATE TABLE (case insensitive)
  std::regex create_table_regex(R"(CREATE\s+TABLE\s+(\w+)\s*\(([\s\S]*?)\);)",
                                std::regex::icase);

  auto begin = std::sregex_iterator(schema_sql.begin(), schema_sql.end(),
                                    create_table_regex);
  auto end = std::sregex_iterator();

  int table_count = 0;
  for (auto it = begin; it != end; ++it) {
    std::smatch match = *it;
    std::string table_name = match[1].str();
    std::string columns_str = match[2].str();

    TableSchema schema;
    schema.table_name = table_name;

    // Parse column definitions
    // Split by comma, but be careful about parentheses (e.g., VARCHAR(255))
    std::vector<std::string> column_defs;
    int paren_depth = 0;
    std::string current_def;

    for (char c : columns_str) {
      if (c == '(') {
        paren_depth++;
        current_def += c;
      } else if (c == ')') {
        paren_depth--;
        current_def += c;
      } else if (c == ',' && paren_depth == 0) {
        column_defs.push_back(current_def);
        current_def.clear();
      } else {
        current_def += c;
      }
    }
    if (!current_def.empty()) {
      column_defs.push_back(current_def);
    }

    unsigned int col_index = 0;
    for (const auto &def : column_defs) {
      // Trim whitespace
      std::string trimmed = def;
      trimmed.erase(0, trimmed.find_first_not_of(" \t\n\r"));
      trimmed.erase(trimmed.find_last_not_of(" \t\n\r") + 1);

      if (trimmed.empty())
        continue;

      // Skip constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, etc.)
      std::string upper_trimmed = trimmed;
      std::transform(upper_trimmed.begin(), upper_trimmed.end(),
                     upper_trimmed.begin(), ::toupper);

      if (upper_trimmed.find("PRIMARY KEY") == 0 ||
          upper_trimmed.find("FOREIGN KEY") == 0 ||
          upper_trimmed.find("UNIQUE") == 0 ||
          upper_trimmed.find("CHECK") == 0 ||
          upper_trimmed.find("CONSTRAINT") == 0) {
        continue;
      }

      // Extract column name (first word)
      std::regex col_regex(R"(^\s*(\w+)\s+(.+))");
      std::smatch col_match;
      if (std::regex_search(trimmed, col_match, col_regex)) {
        ColumnInfo col;
        col.name = col_match[1].str();
        col.type = col_match[2].str();
        col.ordinal_position = col_index;

        schema.columns.push_back(col);
        schema.column_name_to_index[Normalize(col.name)] = col_index;
        col_index++;
      }
    }

    tables_[Normalize(table_name)] = std::move(schema);
    table_count++;
  }
  return table_count > 0;
}

int SchemaParser::GetColumnIndex(const std::string &table_name,
                                 const std::string &column_name) const {
  auto table_it = tables_.find(Normalize(table_name));
  if (table_it == tables_.end()) {
    return -1;
  }

  auto col_it =
      table_it->second.column_name_to_index.find(Normalize(column_name));
  if (col_it == table_it->second.column_name_to_index.end()) {
    return -1;
  }

  return static_cast<int>(col_it->second);
}

const TableSchema *
SchemaParser::GetTableSchema(const std::string &table_name) const {
  auto it = tables_.find(Normalize(table_name));
  if (it == tables_.end()) {
    return nullptr;
  }
  return &it->second;
}

bool SchemaParser::HasTable(const std::string &table_name) const {
  return tables_.find(Normalize(table_name)) != tables_.end();
}

std::vector<std::string> SchemaParser::GetTableNames() const {
  std::vector<std::string> names;
  names.reserve(tables_.size());
  for (const auto &[name, schema] : tables_) {
    names.push_back(schema.table_name);
  }
  return names;
}

} // namespace ir_sql_converter
