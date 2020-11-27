
find_package(PythonInterp 3.6 REQUIRED)
set(PYBIND11_PYTHON_VERSION 3.6)
set(PYBIND11_CPP_STANDARD -std=c++1z)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_BINARY_DIR})

include(GoogleTest)

add_subdirectory(proto)

add_library(table_lib table.cc table.h)
target_link_libraries(table_lib PRIVATE proto ${Protobuf_LIBRARIES} absl::strings absl::str_format absl::flat_hash_map  ${LINUX_EXTRA_LIBS})

add_executable(TableTest test.cc)
target_link_libraries(TableTest proto ${Protobuf_LIBRARIES} table_lib gtest gmock gtest_main ${LINUX_EXTRA_LIBS} absl::flat_hash_map)

add_test(TableTest TableTest)

pybind11_add_module(pytdb_cc pywrap.cc)

target_link_libraries(pytdb_cc PRIVATE  table_lib proto ${Protobuf_LIBRARIES} absl::strings absl::str_format absl::flat_hash_map)