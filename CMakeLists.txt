cmake_minimum_required(VERSION 3.10)
project(LSMTREE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# 设置动态库文件 静态库文件 可执行文件的输出路径
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 链接第三方库文件
add_subdirectory(third-party)

# 生成动态库文件
file(GLOB SKIPLIST ${CMAKE_SOURCE_DIR}/src/skiplist/*.cpp)
add_library(skiplist SHARED ${SKIPLIST})
target_link_libraries(skiplist)

file(GLOB MEMTABLE ${CMAKE_SOURCE_DIR}/src/memtable/*.cpp)
add_library(memtable SHARED ${MEMTABLE})
target_link_libraries(memtable skiplist)

file(GLOB BLOCK ${CMAKE_SOURCE_DIR}/src/block/*.cpp)
add_library(block SHARED ${BLOCK})
target_link_libraries(block)

# 生成可执行文件
add_executable(skiplist_test ${CMAKE_SOURCE_DIR}/test/skiplist_test.cpp)
target_link_libraries(skiplist_test skiplist GTest::gtest GTest::gmock)

add_executable(memtable_test ${CMAKE_SOURCE_DIR}/test/memtable_test.cpp)
target_link_libraries(memtable_test memtable GTest::gtest GTest::gmock)

add_executable(block_test ${CMAKE_SOURCE_DIR}/test/block_test.cpp)
target_link_libraries(block_test block GTest::gtest GTest::gmock)