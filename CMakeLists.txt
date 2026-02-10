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

# 生成测试文件
set(TEST_LIBS GTest::gtest GTest::gmock skiplist memtable block)

file(GLOB TEST_SRCS "${CMAKE_SOURCE_DIR}/test/test_*.cpp")

foreach(test_src ${TEST_SRCS})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src})
    target_link_libraries(${test_name} PRIVATE ${TEST_LIBS})
endforeach()
