cmake_minimum_required(VERSION 3.10)
project(Test_IR_SQL_Converter_CPP_PG)

set(CMAKE_CXX_STANDARD 14)

add_executable(Test_IR_SQL_Converter_CPP_PG cpp_test_pg.cpp)
add_executable(Test_IR_SQL_Converter_CPP_DuckDB cpp_test_duckdb.cpp)

# libpg_query
set(LIBPG_QUERY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../libpg_query" CACHE PATH "libpg_query source path")
if(EXISTS ${LIBPG_QUERY_ROOT})
    message(STATUS "Found libpg_query at: ${LIBPG_QUERY_ROOT}")

    include_directories(${LIBPG_QUERY_ROOT})

    find_library(LIBPG_QUERY_LIB
            NAMES pg_query libpg_query
            PATHS ${LIBPG_QUERY_ROOT}
            NO_DEFAULT_PATH
    )
else()
    message(WARNING "libpg_query not found at ${LIBPG_QUERY_ROOT}")
endif()

# DuckDB
set(DUCKDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb_132" CACHE PATH "DuckDB source path")
if(EXISTS ${DUCKDB_ROOT})
    message(STATUS "Found DuckDB source at: ${DUCKDB_ROOT}")

    find_library(DUCKDB_LIB
            NAMES duckdb libduckdb
            PATHS ${DUCKDB_ROOT}/build/release/src ${DUCKDB_ROOT}/build/debug/src
            NO_DEFAULT_PATH
    )
else()
    message(WARNING "DuckDB not found at ${DUCKDB_ROOT}")
endif()

find_package(nlohmann_json REQUIRED)

target_include_directories(Test_IR_SQL_Converter_CPP_PG PRIVATE ${CMAKE_SOURCE_DIR}/inc)
target_include_directories(Test_IR_SQL_Converter_CPP_DuckDB PRIVATE ${CMAKE_SOURCE_DIR}/inc)

target_link_libraries(Test_IR_SQL_Converter_CPP_PG ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json IR_SQL_Converter stdc++)
target_link_libraries(Test_IR_SQL_Converter_CPP_DuckDB ${DUCKDB_LIB} IR_SQL_Converter stdc++)
