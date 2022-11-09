set(TARGET tairhash_module)

FILE(GLOB_RECURSE SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c
    ${CMAKE_CURRENT_SOURCE_DIR}/*.h
)
add_library(${TARGET} SHARED ${SRCS} ${USRC})

set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
set_target_properties(${TARGET} PROPERTIES PREFIX "")