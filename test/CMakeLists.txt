cmake_minimum_required(VERSION 3.14)

find_package(GTest REQUIRED)

set(TEST_LIBS GTest::gtest GTest::gmock iterator config)

set(TEST_DIR ${CMAKE_SOURCE_DIR}/test)

add_executable(test_skiplist ${TEST_DIR}/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE ${TEST_LIBS} skiplist)

add_executable(test_memtable ${TEST_DIR}/test_memtable.cpp)
target_link_libraries(test_memtable PRIVATE ${TEST_LIBS} memtable skiplist iterator)

add_executable(test_block ${TEST_DIR}/test_block.cpp)
target_link_libraries(test_block PRIVATE ${TEST_LIBS} block iterator)

add_executable(test_utils ${TEST_DIR}/test_utils.cpp)
target_link_libraries(test_utils PRIVATE ${TEST_LIBS} utils tomlplusplus::tomlplusplus)