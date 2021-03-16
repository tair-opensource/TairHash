set(TARGET tairhash_module)

set(SRCS
        tairhash.h
        tairhash.c
        redismodule.h )

add_library(${TARGET} SHARED ${SRCS} ${USRC})
set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
set_target_properties(${TARGET} PROPERTIES PREFIX "")