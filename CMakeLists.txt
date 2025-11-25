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

add_library(IR_SQL_Converter SHARED
        src/cpp_interface.cpp
        src/nodestr_to_ir.cpp
        src/ir_to_sql.cpp
)
target_link_libraries(IR_SQL_Converter lib_pg_query)

add_library(IR_SQL_Converter_static STATIC
        src/cpp_interface.cpp
        src/nodestr_to_ir.cpp
        src/ir_to_sql.cpp
)
target_link_libraries(IR_SQL_Converter_static lib_pg_query)

add_library(IR_SQL_Converter_C SHARED
        src/cpp_interface.cpp
        src/c_interface.cpp
        src/nodestr_to_ir.cpp
        src/ir_to_sql.cpp)
target_link_libraries(IR_SQL_Converter_C lib_pg_query)

add_library(IR_SQL_Converter_C_static STATIC
        src/cpp_interface.cpp
        src/c_interface.cpp
        src/nodestr_to_ir.cpp
        src/ir_to_sql.cpp)
target_link_libraries(IR_SQL_Converter_C_static lib_pg_query)

option(BUILD_TESTING "Build the testing tree" ON)
if(BUILD_TESTING)
    add_subdirectory(c_test)
#    add_subdirectory(cpp_test)
endif()