cmake_minimum_required(VERSION 3.14)

find_package(GTest REQUIRED)

set(TEST_LIBS GTest::gtest GTest::gmock iterator)

set(TEST_DIR ${CMAKE_SOURCE_DIR}/test)

add_executable(test_skiplist ${TEST_DIR}/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE ${TEST_LIBS} skiplist)
