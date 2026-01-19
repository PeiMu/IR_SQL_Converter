## How to compile
```bash
mkdir -p build && cd build/
cmake .. # third_party/libpg_query/CMakeLists.txt requires CMake 4.0 or higher
make -j32
```

### compatible with different duckdb version
compile with duckdb 0.10.1
```bash
mkdir -p build_duckdb_010 && cd build_duckdb_010/
cmake -DDUCKDB_VERSION_010=ON ..
make -j32
```
compile with duckdb 1.3.2
```bash
mkdir -p build_duckdb_010 && cd build_duckdb_010/
cmake -DDUCKDB_VERSION_010=OFF ..
make -j32
```

The test and library will be installed like
```bash
├── bin
│   └── Test_IR_SQL_Converter
├── lib
│   ├── libIR_SQL_Converter_C.so
│   ├── libIR_SQL_Converter_C_static.a
│   ├── libIR_SQL_Converter.so
│   ├── libIR_SQL_Converter_static.a
│   └── liblib_pg_query.a
```
