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

# Active expire algorithm. sort and slab need Redis/Valkey 7.0+; scan also runs on Redis 5.0 and 6.x.
set(TAIRHASH_EXPIRE_MODE "" CACHE STRING "Active expire algorithm: sort (default), slab or scan")
set_property(CACHE TAIRHASH_EXPIRE_MODE PROPERTY STRINGS sort slab scan)

# SORT_MODE and SLAB_MODE selected the algorithm before TAIRHASH_EXPIRE_MODE did, and a build with
# both off was scan, so an explicit OFF still means scan rather than the new default.
if(SORT_MODE AND SLAB_MODE)
    message(FATAL_ERROR "SORT_MODE and SLAB_MODE are mutually exclusive: set TAIRHASH_EXPIRE_MODE instead")
endif()
set(TAIRHASH_LEGACY_EXPIRE_MODE "")
if(SORT_MODE)
    set(TAIRHASH_LEGACY_EXPIRE_MODE sort)
elseif(SLAB_MODE)
    set(TAIRHASH_LEGACY_EXPIRE_MODE slab)
elseif(DEFINED SORT_MODE OR DEFINED SLAB_MODE)
    set(TAIRHASH_LEGACY_EXPIRE_MODE scan)
endif()
if(NOT TAIRHASH_LEGACY_EXPIRE_MODE STREQUAL "")
    if(TAIRHASH_EXPIRE_MODE STREQUAL "")
        message(DEPRECATION "SORT_MODE and SLAB_MODE are deprecated: use -DTAIRHASH_EXPIRE_MODE=${TAIRHASH_LEGACY_EXPIRE_MODE}")
        set(TAIRHASH_EXPIRE_MODE ${TAIRHASH_LEGACY_EXPIRE_MODE})
    elseif((SORT_MODE OR SLAB_MODE) AND NOT TAIRHASH_EXPIRE_MODE STREQUAL TAIRHASH_LEGACY_EXPIRE_MODE)
        message(FATAL_ERROR "SORT_MODE/SLAB_MODE select ${TAIRHASH_LEGACY_EXPIRE_MODE}, which conflicts with TAIRHASH_EXPIRE_MODE=${TAIRHASH_EXPIRE_MODE}")
    endif()
endif()
# Compared as a string: if(NOT <variable>) would also take values such as "off" for unset.
if(TAIRHASH_EXPIRE_MODE STREQUAL "")
    set(TAIRHASH_EXPIRE_MODE sort)
endif()

if(TAIRHASH_EXPIRE_MODE STREQUAL "sort")
    add_definitions(-DSORT_MODE)
elseif(TAIRHASH_EXPIRE_MODE STREQUAL "slab")
    add_definitions(-DSLAB_MODE)
elseif(NOT TAIRHASH_EXPIRE_MODE STREQUAL "scan")
    message(FATAL_ERROR "TAIRHASH_EXPIRE_MODE must be sort, slab or scan, not '${TAIRHASH_EXPIRE_MODE}'")
endif()
message(STATUS "TairHash active expire mode: ${TAIRHASH_EXPIRE_MODE}")

include_directories(${ROOT_DIR}/dep)
include_directories(${ROOT_DIR}/src)
aux_source_directory(${ROOT_DIR}/dep USRC)
add_subdirectory(src)
