set(TARGET tairhash_module)
if (SLAB_MODE)
set(SRCS
        slab.h
        slab.c
        tairhash_skiplist.h
        tairhash_skiplist.c
        slabapi.h
        slabapi.c
        tairhash.h
        tairhash.c
        redismodule.h
)
add_library(${TARGET} SHARED ${SRCS} ${USRC})
else ()
set(SRCS
        tairhash.h
        tairhash.c
        redismodule.h )
add_library(${TARGET} SHARED ${SRCS} ${USRC})
endif()

set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
set_target_properties(${TARGET} PROPERTIES PREFIX "")