cmake_minimum_required(VERSION 2.8.12)
project(adbc-demo)

set(CMAKE_CXX_STANDARD 11)

include_directories(include)
link_directories(../../build/release/src)

add_executable(example main.cpp)
target_link_libraries(example duckdb)

add_executable(example_drivermanager main_drivermanager.cpp driver_manager.cpp)
