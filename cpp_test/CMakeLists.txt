cmake_minimum_required(VERSION 3.10)
project(Test_IR_SQL_Converter_CPP)

set(CMAKE_CXX_STANDARD 14)

add_executable(Test_IR_SQL_Converter_CPP cpp_test_main.cpp)

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

find_package(nlohmann_json REQUIRED)

target_include_directories(Test_IR_SQL_Converter_CPP PRIVATE ${CMAKE_SOURCE_DIR}/inc)

target_link_libraries(Test_IR_SQL_Converter_CPP ${LIBPG_QUERY_LIB} nlohmann_json::nlohmann_json IR_SQL_Converter stdc++)
