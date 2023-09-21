cmake_minimum_required (VERSION 3.0)

project(tairhash_module)

set(ROOT_DIR ${CMAKE_SOURCE_DIR})

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -g -ggdb -std=c99 -march=native -O3 -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable")

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

include(CheckCSourceRuns)

check_c_source_runs("
#include <immintrin.h>
int main()
{
  __m256i a, b, c;
  const int src[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
  int dst[8];
  a =  _mm256_loadu_si256( (__m256i*)src );
  b =  _mm256_loadu_si256( (__m256i*)src );
  c = _mm256_add_epi32( a, b );
  _mm256_storeu_si256( (__m256i*)dst, c );
  for( int i = 0; i < 8; i++ ){
    if( ( src[i] + src[i] ) != dst[i] ){
      return -1;
    }
  }
  return 0;
}" HAVE_AVX2_EXTENSIONS)

message(STATUS "SLAB_API defined...")

if (HAVE_AVX2_EXTENSIONS)
message(STATUS "SIMD acceleration...")
endif(HAVE_AVX2_EXTENSIONS)

add_definitions(-DSLAB_MODE)
endif(SLAB_MODE)

include_directories(${ROOT_DIR}/dep)
include_directories(${ROOT_DIR}/src)
aux_source_directory(${ROOT_DIR}/dep USRC)
add_subdirectory(src)
