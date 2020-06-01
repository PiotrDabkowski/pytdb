cmake_minimum_required(VERSION 3.8)
project(PyDB)

set(CMAKE_CXX_STANDARD 17)

find_package(PythonInterp 3.6 REQUIRED)
set(PYBIND11_PYTHON_VERSION 3.6)
set(PYBIND11_CPP_STANDARD -std=c++1z)

if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_CXX_FLAGS " -Wall")
    message("Debug mode")
elseif(CMAKE_BUILD_TYPE MATCHES Release)
    set(CMAKE_CXX_FLAGS " -Wall -O2")
    message("Release mode")
endif ()

if(UNIX AND NOT APPLE)
    set(LINUX_EXTRA_LIBS "stdc++fs")
endif()

include(GoogleTest)


add_subdirectory(proto)
add_subdirectory(abseil-cpp)
add_subdirectory(googletest)
add_subdirectory(pybind11)


add_library(table_lib table.cc table.h)
target_link_libraries(table_lib proto ${Protobuf_LIBRARIES} absl::strings absl::str_format absl::flat_hash_map spdlog::spdlog ${LINUX_EXTRA_LIBS})

find_package(spdlog REQUIRED)
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})


include_directories(${CMAKE_CURRENT_BINARY_DIR})



add_executable(tst test.cc)
target_link_libraries(tst proto ${Protobuf_LIBRARIES} table_lib gtest gmock gtest_main ${LINUX_EXTRA_LIBS})

pybind11_add_module(pywrap pywrap.cc )
#target_compile_options(pywrap PUBLIC -Wno-register)
target_link_libraries(pywrap PRIVATE  table_lib proto ${Protobuf_LIBRARIES})

#target_link_libraries(${target_name} PRIVATE )
#
#TARGET_LINK_LIBRARIES(pdb proto ${Protobuf_LIBRAfggfRIES})

