#include "cpp_interface.h"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/planner.hpp"
#include "ir_to_duckdb_plan.h"
#include <iostream>

#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"

using namespace duckdb;

int main(int argc, char **argv) {
  std::string db_path = "/home/pei/Project/duckdb_132/measure/imdb.db";
  std::string plan_dir =
      "/home/pei/Project/duckdb_132/measure/logical_plan_v1.3.2_split_0.bin";

  DuckDB db(db_path);
  Connection conn(db);
  auto &fs = FileSystem::GetFileSystem(*conn.context);

  unique_ptr<QueryResult> final_result;

  try {
    // Start transaction for deserialization (needed for catalog access)
    conn.BeginTransaction();

    // Deserialize
    BufferedFileReader reader(fs, plan_dir.c_str());
    BinaryDeserializer deserializer(reader);
    deserializer.Set<ClientContext &>(*conn.context);

    deserializer.Begin();
    auto logical_plan = LogicalOperator::Deserialize(deserializer);
    deserializer.End();

    // Resolve types
    logical_plan->ResolveOperatorTypes();

    // print out logical plan
    std::cout << "original duckdb plan:\n";
    logical_plan->Print();
    auto context = conn.context;
    auto planner = std::make_unique<Planner>(*context);
    std::unordered_map<unsigned int, std::string> intermediate_table_map;

    auto simplest_ir = ir_sql_converter::ConvertDuckDBPlanToIR(
        *planner->binder, *context, logical_plan.get(), intermediate_table_map);
    // print out simplest ir
    std::cout << "simplest ir converted from duckdb plan:\n";
    simplest_ir->Print();

    // test Simplest IR serialization/deserialization
    ir_sql_converter::SaveSimplestIRToFile(simplest_ir, "test.ir");

    auto deserialized_simplest_ir =
        ir_sql_converter::LoadSimplestIRFromFile("test.ir");
    // print out simplest ir
    std::cout << "deserialized simplest ir:\n";
    deserialized_simplest_ir->Print();

    auto duckdb_plan = ir_sql_converter::ConvertIRToDuckDBPlan(
        *planner->binder, *context, deserialized_simplest_ir);
    // print out DuckDB plan
    std::cout << "duckdb plan converted from simplest ir:\n";
    duckdb_plan->Print();

    // Serialize the logical plan to binary file
    std::string filename = "/home/pei/Project/duckdb_132/measure/test.bin";

    BufferedFileWriter writer(FileSystem::GetFileSystem(*context), filename);
    BinarySerializer serializer(writer);

    serializer.Begin();
    duckdb_plan->Serialize(serializer);
    serializer.End();

    writer.Sync();

    return 0;

  } catch (std::exception &e) {
    std::cerr << "Error processing " << plan_dir << ": " << e.what() << '\n';

    // Try to rollback if we're in a transaction
    try {
      conn.Rollback();
    } catch (...) {
      // Ignore rollback errors
    }

    std::cout << "ERROR: " << "\"" << e.what() << "\"\n";
  }

  return 0;
}