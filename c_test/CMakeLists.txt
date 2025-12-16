cmake_minimum_required(VERSION 3.10)
project(Test_IR_SQL_Converter_C C)

set(CMAKE_C_STANDARD 11)

add_executable(Test_IR_SQL_Converter_C c_test_main.c)
target_include_directories(Test_IR_SQL_Converter_C PRIVATE ${CMAKE_SOURCE_DIR}/inc)

target_link_libraries(Test_IR_SQL_Converter_C IR_SQL_Converter_C stdc++)
