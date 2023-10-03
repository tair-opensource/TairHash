set(TARGET tairhash_module)

FILE(GLOB_RECURSE SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/*.h
)
add_library(${TARGET} SHARED ${SRCS} ${USRC})

# OS X 11.x doesn't have /usr/lib/libSystem.dylib and needs an explicit setting.
if ( APPLE AND NOT EXISTS /usr/lib/libSystem.dylib)
    message(STATUS "apple...")
    set(CMAKE_C_COMPILER /usr/bin/clang)
    target_link_libraries(${TARGET} -L/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib)
    target_link_libraries(${TARGET} -lsystem)
endif()


set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
set_target_properties(${TARGET} PROPERTIES PREFIX "")