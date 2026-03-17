cmake_minimum_required(VERSION 3.10)
project(LSMTREE VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置动态库文件 静态库文件 可执行文件的输出路径
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin)

# 链接第三方库文件
add_subdirectory(third-party)

# 源文件编译
set(SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)

include_directories(${SOURCE_DIR})

file(GLOB UTILS_SRCS ${SOURCE_DIR}/utils/*.cpp)
add_library(utils SHARED ${UTILS_SRCS})

file(GLOB CONFIG_SRCS ${SOURCE_DIR}/config/*.cpp)
add_library(config SHARED ${CONFIG_SRCS})
target_link_libraries(config PRIVATE tomlplusplus::tomlplusplus)

file(GLOB ITERATOR_SRCS ${SOURCE_DIR}/iterator/*.cpp)
add_library(iterator SHARED ${ITERATOR_SRCS})

file(GLOB SKIPLIST_SRCS ${SOURCE_DIR}/skiplist/*.cpp)
add_library(skiplist SHARED ${SKIPLIST_SRCS})
target_link_libraries(skiplist PUBLIC iterator)

file(GLOB MEMTABLE_SRCS ${SOURCE_DIR}/memtable/*.cpp)
add_library(memtable SHARED ${MEMTABLE_SRCS})
target_link_libraries(memtable PUBLIC skiplist)

file(GLOB BLOCK_SRCS ${SOURCE_DIR}/block/*.cpp)
add_library(block SHARED ${BLOCK_SRCS})
target_link_libraries(block PUBLIC iterator)

# 链接单元测试文件
add_subdirectory(test)
