cmake_minimum_required (VERSION 3.0)

project(tairhash_module)

set(ROOT_DIR ${CMAKE_SOURCE_DIR})

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -g -ggdb -std=c99 -march=native -O3 -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable")

if (GCOV_MODE) 
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)  

option(SORT_MODE "Use two-level sort index to to implement active expire" OFF)

if(SORT_MODE)
add_definitions(-DSORT_MODE)
endif(SORT_MODE)

option(SLAB_MODE "Use a memory friendly slab-based expiration algorithm to evict expired keys more efficient!" OFF)

if (SLAB_MODE)
message(STATUS "SLAB_API defined...")
add_definitions(-DSLAB_MODE)
endif(SLAB_MODE)

include_directories(${ROOT_DIR}/dep)
aux_source_directory(${ROOT_DIR}/dep USRC)
add_subdirectory(src)
