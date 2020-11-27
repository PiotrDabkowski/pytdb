#!/bin/bash

mkdir -p cmake-build-test
cd cmake-build-test

cmake ../ -DCMAKE_BUILD_TYPE=Debug
make -j 12
ctest --verbose || ! echo "Tests failed!" || exit 1
