cmake_minimum_required(VERSION 3.14)
project(PyDB)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_CXX_FLAGS " -Wall -fsanitize=leak -fPIC")
    message("Debug mode")
elseif(CMAKE_BUILD_TYPE MATCHES Release)
    set(CMAKE_CXX_FLAGS " -Wall -O2 -fPIC")
    message("Release mode ")
else()
    set(CMAKE_CXX_FLAGS " -Wall -O2 -fPIC")
    message("UNKNOWN CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
endif ()

if(UNIX AND NOT APPLE)
    set(LINUX_EXTRA_LIBS "stdc++fs")
endif()

add_subdirectory(abseil-cpp)
add_subdirectory(googletest)
add_subdirectory(pybind11)

enable_testing()

add_subdirectory(src)
