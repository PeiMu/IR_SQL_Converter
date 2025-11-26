cmake_minimum_required(VERSION 3.10)
project(Test_IR_SQL_Converter C)

set(CMAKE_C_STANDARD 11)

add_executable(Test_IR_SQL_Converter c_test_main.c)
target_include_directories(Test_IR_SQL_Converter PRIVATE ${CMAKE_SOURCE_DIR}/inc)

target_link_libraries(Test_IR_SQL_Converter IR_SQL_Converter_C stdc++)
