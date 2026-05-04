cmake_minimum_required(VERSION 3.10)
project(LSMTREE VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置动态库文件 静态库文件 可执行文件的输出路径
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/build/bin)

# 源文件编译
set(SOURCE_DIR ${CMAKE_SOURCE_DIR}/src)

include_directories(${SOURCE_DIR})

file(GLOB SKIPLIST_SRCS ${SOURCE_DIR}/skiplist/*.cpp)
add_library(skiplist SHARED ${SKIPLIST_SRCS})
target_link_libraries(skiplist PUBLIC iterator)
