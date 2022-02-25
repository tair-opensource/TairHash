FROM redis

ENV TAIRHASH_URL https://github.com/alibaba/TairHash.git
RUN set -ex; \
    \
    BUILD_DEPS=' \
        ca-certificates \
        cmake \
        gcc \
        git \
        g++ \
        make \
    '; \
    apt-get update; \
    apt-get install -y $BUILD_DEPS --no-install-recommends; \
    rm -rf /var/lib/apt/lists/*; \
    git clone "$TAIRHASH_URL"; \
    cd TairHash; \
    mkdir -p build; \
    cd build; \
    cmake ..; \
    make -j; \
    cd ..; \
    cp lib/tairhash_module.so /usr/local/lib/; \
    \
    apt-get purge -y --auto-remove $BUILD_DEPS
ADD http://download.redis.io/redis-stable/redis.conf /usr/local/etc/redis/redis.conf
RUN sed -i '1i loadmodule /usr/local/lib/tairhash_module.so' /usr/local/etc/redis/redis.conf; \
    chmod 644 /usr/local/etc/redis/redis.conf; \
    sed -i 's/^bind 127.0.0.1/#bind 127.0.0.1/g' /usr/local/etc/redis/redis.conf; \
    sed -i 's/^protected-mode yes/protected-mode no/g' /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]