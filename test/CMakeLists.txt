find_package(GTest REQUIRED)

set(TEST_LIBS GTest::gtest GTest::gmock)

add_executable(test_skiplist ${CMAKE_CURRENT_SOURCE_DIR}/test_skiplist.cpp)
target_link_libraries(test_skiplist PRIVATE ${TEST_LIBS} skiplist)

add_executable(test_memtable ${CMAKE_CURRENT_SOURCE_DIR}/test_memtable.cpp)
target_link_libraries(test_memtable PRIVATE ${TEST_LIBS} memtable)

add_executable(test_block ${CMAKE_CURRENT_SOURCE_DIR}/test_block.cpp)
target_link_libraries(test_block PRIVATE ${TEST_LIBS} block)

add_executable(test_utils ${CMAKE_CURRENT_SOURCE_DIR}/test_utils.cpp)
target_link_libraries(test_utils PRIVATE ${TEST_LIBS} utils config)

add_executable(test_sst ${CMAKE_CURRENT_SOURCE_DIR}/test_sst.cpp)
target_link_libraries(test_sst PRIVATE ${TEST_LIBS} sst)

add_executable(test_lsm ${CMAKE_CURRENT_SOURCE_DIR}/test_lsm.cpp)
target_link_libraries(test_lsm PRIVATE ${TEST_LIBS} lsm)
