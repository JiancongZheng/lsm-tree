cmake_minimum_required(VERSION 3.10)
project(LSMTREE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# 设置动态库 静态库 可执行文件的输出路径
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# 链接GTEST库文件
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# 生成动态库文件
file(GLOB SKIPLIST ${CMAKE_SOURCE_DIR}/src/skiplist/*.cpp)
add_library(skiplist SHARED ${SKIPLIST})
target_link_libraries(skiplist)

file(GLOB MEMTABLE ${CMAKE_SOURCE_DIR}/src/memtable/*.cpp)
add_library(memtable SHARED ${MEMTABLE})
target_link_libraries(memtable skiplist)

# 生成可执行文件
add_executable(skiplist_test ${CMAKE_SOURCE_DIR}/test/skiplist_test.cpp)
target_link_libraries(skiplist_test skiplist GTest::gtest)

add_executable(memtable_test ${CMAKE_SOURCE_DIR}/test/memtable_test.cpp)
target_link_libraries(memtable_test memtable GTest::gtest)