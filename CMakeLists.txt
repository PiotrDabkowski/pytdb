cmake_minimum_required(VERSION 3.8)
project(PyDB)

set(CMAKE_CXX_STANDARD 17)

if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_CXX_FLAGS " -Wall -fsanitize=leak -fPIC")
    message("Debug mode")
elseif(CMAKE_BUILD_TYPE MATCHES Release)
    set(CMAKE_CXX_FLAGS " -Wall -O2 -fPIC")
    message("Release mode")
endif ()

if(UNIX AND NOT APPLE)
    set(LINUX_EXTRA_LIBS "stdc++fs")
endif()


add_subdirectory(abseil-cpp)
add_subdirectory(googletest)
add_subdirectory(pybind11)

#enable_testing()

add_subdirectory(src)



