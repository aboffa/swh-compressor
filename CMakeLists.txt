cmake_minimum_required(VERSION 3.13)
project(swh-compressor)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -pedantic -fopenmp")

include_directories(include)

add_executable(benchmark benchmark.cpp src/SpookyV2.cpp src/simhash.cpp src/minhash.cpp src/Magic.cpp)

add_executable(dmk_example dmk_example.cpp)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.45.0 COMPONENTS compute)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(benchmark ${Boost_LIBRARIES})
endif()

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(benchmark PUBLIC OpenMP::OpenMP_CXX)
endif()

target_link_libraries(benchmark PUBLIC magic)
