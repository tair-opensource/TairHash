cmake_minimum_required (VERSION 3.0)

project(tairhash_module)

set(ROOT_DIR ${CMAKE_SOURCE_DIR})

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -g -ggdb -O3 -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable")

# -march=native ties the library to the build host's CPU: an image built on one machine
# can die with SIGILL on another. Opt in only when building on the host that runs it.
option(TAIRHASH_NATIVE_ARCH "Optimize for the build host's CPU (-march=native)" OFF)
if(TAIRHASH_NATIVE_ARCH)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
endif()

option(TAIRHASH_LTO "Build with link-time optimization" OFF)
if(TAIRHASH_LTO)
    # check_ipo_supported() refuses to run unless CMP0069 is NEW, which a 3.0 minimum leaves unset.
    if(POLICY CMP0069)
        cmake_policy(SET CMP0069 NEW)
    endif()
    include(CheckIPOSupported)
    check_ipo_supported(RESULT TAIRHASH_LTO_SUPPORTED OUTPUT TAIRHASH_LTO_ERROR)
    if(NOT TAIRHASH_LTO_SUPPORTED)
        message(FATAL_ERROR "TAIRHASH_LTO requested but not supported: ${TAIRHASH_LTO_ERROR}")
    endif()
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
endif()

if (GCOV_MODE) 
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

if(SANITIZER_MODE MATCHES "address")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fsanitize=address -fno-omit-frame-pointer")
elseif(SANITIZER_MODE MATCHES "undefined")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=undefined -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fsanitize=undefined -fno-omit-frame-pointer")
elseif(SANITIZER_MODE MATCHES "thread")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=thread -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -fsanitize=thread -fno-omit-frame-pointer")
endif(SANITIZER_MODE MATCHES "address")

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)

SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)  

option(SORT_MODE "Use two-level sort index to to implement active expire" OFF)

if(SORT_MODE)
add_definitions(-DSORT_MODE)
endif(SORT_MODE)

option(SLAB_MODE "Use a memory friendly slab-based expiration algorithm to evict expired keys more efficient!" OFF)

if (SLAB_MODE)
add_definitions(-DSLAB_MODE)
endif(SLAB_MODE)

include_directories(${ROOT_DIR}/dep)
include_directories(${ROOT_DIR}/src)
aux_source_directory(${ROOT_DIR}/dep USRC)
add_subdirectory(src)
