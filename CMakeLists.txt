cmake_minimum_required(VERSION 3.13)
project(swh-compressor)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)

add_executable(benchmark main.cpp src/SpookyV2.cpp src/permutation.cpp src/simhash.cpp)
