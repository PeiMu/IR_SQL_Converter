cmake_minimum_required(VERSION 3.10)
project(IR_SQL_Converter)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
else()
    find_program(CCACHE_PROGRAM sccache)
    if(CCACHE_PROGRAM)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
    endif()
endif()

include_directories(third_party/libpg_query/include)
include_directories(inc)
add_subdirectory(third_party)

# DuckDB
set(DUCKDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../duckdb" CACHE PATH "DuckDB source path")
if(EXISTS ${DUCKDB_ROOT})
    message(STATUS "Found DuckDB source at: ${DUCKDB_ROOT}")

    find_library(DUCKDB_LIB
            NAMES duckdb libduckdb
            PATHS ${DUCKDB_ROOT}/build/release/src ${DUCKDB_ROOT}/build/debug/src
            NO_DEFAULT_PATH
    )
else()
    message(WARNING "DuckDB not found at ${LIBPG_QUERY_ROOT}")
endif()

# libpg_query
set(LIBPG_QUERY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../libpg_query" CACHE PATH "libpg_query source path")
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

find_package(nlohmann_json REQUIRED)

add_library(IR_SQL_Converter SHARED
        src/cpp_interface.cpp
        src/duckdb_plan_to_ir.cpp
        src/nodestr_to_ir.cpp
        src/parsetree_to_ir.cpp
        src/ir_to_sql.cpp
)
target_link_libraries(IR_SQL_Converter lib_pg_query ${DUCKDB_LIB} ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json)

add_library(IR_SQL_Converter_static STATIC
        src/cpp_interface.cpp
        src/duckdb_plan_to_ir.cpp
        src/nodestr_to_ir.cpp
        src/parsetree_to_ir.cpp
        src/ir_to_sql.cpp
)
target_link_libraries(IR_SQL_Converter_static lib_pg_query ${DUCKDB_LIB} ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json)

add_library(IR_SQL_Converter_C SHARED
        src/cpp_interface.cpp
        src/c_interface.cpp
        src/duckdb_plan_to_ir.cpp
        src/nodestr_to_ir.cpp
        src/parsetree_to_ir.cpp
        src/ir_to_sql.cpp)
target_link_libraries(IR_SQL_Converter_C lib_pg_query ${DUCKDB_LIB} ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json)

add_library(IR_SQL_Converter_C_static STATIC
        src/cpp_interface.cpp
        src/c_interface.cpp
        src/duckdb_plan_to_ir.cpp
        src/nodestr_to_ir.cpp
        src/parsetree_to_ir.cpp
        src/ir_to_sql.cpp)
target_link_libraries(IR_SQL_Converter_C_static lib_pg_query ${DUCKDB_LIB} ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json)

include_directories(${DUCKDB_ROOT}/src/include)

# Add DuckDB include directories to all targets
target_include_directories(IR_SQL_Converter PUBLIC ${DUCKDB_ROOT}/src/include)
target_include_directories(IR_SQL_Converter_static PUBLIC ${DUCKDB_ROOT}/src/include)
target_include_directories(IR_SQL_Converter_C PUBLIC ${DUCKDB_ROOT}/src/include)
target_include_directories(IR_SQL_Converter_C_static PUBLIC ${DUCKDB_ROOT}/src/include)

option(BUILD_TESTING "Build the testing tree" ON)
if(BUILD_TESTING)
    add_subdirectory(c_test)
#    add_subdirectory(cpp_test)
endif()