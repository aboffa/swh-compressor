cmake_minimum_required(VERSION 3.13)
project(swh-compressor)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -pedantic -fopenmp")

include_directories(include)

add_executable(benchmark benchmark.cpp src/SpookyV2.cpp src/simhash.cpp)

add_executable(dmk_example dmk_example.cpp)

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(benchmark PUBLIC OpenMP::OpenMP_CXX)
endif()
