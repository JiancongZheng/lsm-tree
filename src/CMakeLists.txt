file(GLOB UTILS_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cpp)
add_library(utils SHARED ${UTILS_SRCS})

file(GLOB CONFIG_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp)
add_library(config SHARED ${CONFIG_SRCS})
target_link_libraries(config PRIVATE tomlplusplus::tomlplusplus)

file(GLOB ITERATOR_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/iterator/*.cpp)
add_library(iterator SHARED ${ITERATOR_SRCS})

file(GLOB SKIPLIST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/skiplist/*.cpp)
add_library(skiplist SHARED ${SKIPLIST_SRCS})
target_link_libraries(skiplist PUBLIC iterator)

file(GLOB MEMTABLE_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/memtable/*.cpp)
add_library(memtable SHARED ${MEMTABLE_SRCS})
target_link_libraries(memtable PUBLIC skiplist sst config)

file(GLOB BLOCK_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/block/*.cpp)
add_library(block SHARED ${BLOCK_SRCS})
target_link_libraries(block PUBLIC iterator)

file(GLOB SST_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/sst/*.cpp)
add_library(sst SHARED ${SST_SRCS})
target_link_libraries(sst PUBLIC block utils config iterator)
