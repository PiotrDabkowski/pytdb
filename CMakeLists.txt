cmake_minimum_required(VERSION 3.8)
project(PyDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-g -Wall -std=c++17 -O2")
include(GoogleTest)


add_subdirectory(proto)
add_subdirectory(abseil-cpp)
add_subdirectory(googletest)


add_library(table_lib table.cc table.h)
target_link_libraries(table_lib proto ${Protobuf_LIBRARIES} absl::strings absl::str_format absl::flat_hash_map spdlog::spdlog stdc++fs)

find_package(spdlog REQUIRED)
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})


include_directories(${CMAKE_CURRENT_BINARY_DIR})


add_executable(pydb main.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(pydb proto ${Protobuf_LIBRARIES} spdlog::spdlog table_lib stdc++fs)
#

add_executable(tst test.cc)
target_link_libraries(tst proto ${Protobuf_LIBRARIES} table_lib gtest gmock gtest_main stdc++fs)
#
#TARGET_LINK_LIBRARIES(pdb proto ${Protobuf_LIBRAfggfRIES})

