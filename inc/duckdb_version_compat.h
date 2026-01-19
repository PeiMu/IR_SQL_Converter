#pragma once

#include "duckdb.hpp"

// Detect DuckDB version - check for features unique to 1.3.2
// ColumnIndex struct exists in 1.3.2 but not in 0.10.1
#if __has_include("duckdb/common/column_index.hpp")
#define DUCKDB_VERSION_MAJOR 1
#define DUCKDB_VERSION_MINOR 3
#include "duckdb/common/column_index.hpp"
#else
#define DUCKDB_VERSION_MAJOR 0
#define DUCKDB_VERSION_MINOR 10
#endif

namespace ir_sql_converter {
namespace compat {

//=============================================================================
// Column Index Compatibility
//=============================================================================
#if DUCKDB_VERSION_MAJOR >= 1

// v1.3.2: Use ColumnIndex directly
using column_index_t = duckdb::ColumnIndex;

inline duckdb::idx_t GetColumnId(const duckdb::ColumnIndex &col_idx) {
  return col_idx.GetPrimaryIndex();
}

inline duckdb::ColumnIndex MakeColumnIndex(duckdb::idx_t idx) {
  return duckdb::ColumnIndex(idx);
}

#else // v0.10.1

// v0.10.1: column_t is just idx_t
using column_index_t = duckdb::column_t;

inline duckdb::idx_t GetColumnId(duckdb::column_t col_idx) { return col_idx; }

inline duckdb::column_t MakeColumnIndex(duckdb::idx_t idx) { return idx; }

#endif

//=============================================================================
// LogicalGet Compatibility
//=============================================================================
#if DUCKDB_VERSION_MAJOR >= 1

// v1.3.2: Use accessor methods
inline const duckdb::vector<duckdb::ColumnIndex> &
GetLogicalGetColumnIds(const duckdb::LogicalGet &get_op) {
  return get_op.GetColumnIds();
}

inline void SetLogicalGetColumnIds(duckdb::LogicalGet &get_op,
                                   duckdb::vector<duckdb::ColumnIndex> &&ids) {
  get_op.SetColumnIds(std::move(ids));
}

// v1.3.2: Constructor with virtual_columns
inline duckdb::unique_ptr<duckdb::LogicalGet>
MakeLogicalGet(duckdb::idx_t table_index, duckdb::TableFunction function,
               duckdb::unique_ptr<duckdb::FunctionData> bind_data,
               duckdb::vector<duckdb::LogicalType> return_types,
               duckdb::vector<std::string> return_names) {
  return duckdb::make_uniq<duckdb::LogicalGet>(
      table_index, std::move(function), std::move(bind_data),
      std::move(return_types), std::move(return_names),
      duckdb::virtual_column_map_t()); // Add empty virtual_columns
}

#else // v0.10.1

// v0.10.1: Direct member access
inline const duckdb::vector<duckdb::column_t> &
GetLogicalGetColumnIds(const duckdb::LogicalGet &get_op) {
  return get_op.column_ids;
}

inline void SetLogicalGetColumnIds(duckdb::LogicalGet &get_op,
                                   duckdb::vector<duckdb::column_t> &&ids) {
  get_op.column_ids = std::move(ids);
}

// v0.10.1: Constructor without virtual_columns
inline duckdb::unique_ptr<duckdb::LogicalGet>
MakeLogicalGet(duckdb::idx_t table_index, duckdb::TableFunction function,
               duckdb::unique_ptr<duckdb::FunctionData> bind_data,
               duckdb::vector<duckdb::LogicalType> return_types,
               duckdb::vector<std::string> return_names) {
  return duckdb::make_uniq<duckdb::LogicalGet>(
      table_index, std::move(function), std::move(bind_data),
      std::move(return_types), std::move(return_names));
}

#endif

//=============================================================================
// ParamsToString Compatibility
//=============================================================================
#if DUCKDB_VERSION_MAJOR >= 1

// v1.3.2: Returns InsertionOrderPreservingMap<string>
inline std::string
GetTableNameFromLogicalGet(const duckdb::LogicalGet &get_op) {
  auto table_name = get_op.ParamsToString();
  return table_name["Table"];
}

#else // v0.10.1

// v0.10.1: Returns string - need to parse it
inline std::string
GetTableNameFromLogicalGet(const duckdb::LogicalGet &get_op) {
  // In v0.10.1, use get_op.function with get_op.bind_data
  auto table_name = get_op.function.to_string(get_op.bind_data.get());
  return table_name;
}

#endif

//=============================================================================
// Type aliases for table_column_ids_map
//=============================================================================
#if DUCKDB_VERSION_MAJOR >= 1
using column_ids_vector_t = duckdb::vector<duckdb::ColumnIndex>;
#else
using column_ids_vector_t = duckdb::vector<duckdb::column_t>;
#endif

} // namespace compat
} // namespace ir_sql_converter