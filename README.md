# TairHash
## Introduction  [中文说明](README-CN.md)
     TairHash is a hash data structure developed based on the redis module. TairHash not only has the same rich data interface and high performance as the native hash, but also can set the expiration and version for the field. TairHash provides an active expiration mechanism, even if the field is not accessed after expiration, it can be actively deleted to release the memory.


### The main features：

- Field supports setting expire and version
- Support efficient active expire and passivity expire for field
- The cmd is similar to the native hash data type
- Support redis swapdb, rename, move and copy command

### Note this module can only be used for versions after redis 6.2

### The principle of efficient expiration:
![avatar](imgs/tairhash_index.png)
- Use a two-level sort index, the first level sorts the main key of tairhash, and the second level sorts the fields inside each tairhash
- The first-level uses the smallest ttl in the second-level sorting index for sorting, so the main key is globally ordered
- The built-in timer will periodically scan the first-level index to find out a key that has expired, and then check the secondary index of these keys to eliminate the expired fields. This is the active expire
- Every time a write operation to tairhash, the first level index will be checked first, and at most three fields will be expired, Note these fields may not belong to the key currently being operated, so in theory, the faster you write, the faster the elimination
- Every time you read or write a field, it will also trigger the expiration of the field itself

<br/>

At the same time, we also open sourced an enhanced string structure, which can set the version number for value and support memcached semantics. For details, please refer to [here](https://github.com/alibaba/TairString)

## Quick Start

```go
127.0.0.1:6379> EXHSET k f v ex 10
(integer) 1
127.0.0.1:6379> EXHGET k f
"v"
127.0.0.1:6379> EXISTS k
(integer) 1
127.0.0.1:6379> debug sleep 10
OK
(10.00s)
127.0.0.1:6379> EXISTS k
(integer) 0
127.0.0.1:6379> EXHGET k f
(nil)
127.0.0.1:6379> EXHSET k f v px 10000
(integer) 1
127.0.0.1:6379> EXHGET k f
"v"
127.0.0.1:6379> EXISTS k
(integer) 1
127.0.0.1:6379> debug sleep 10
OK
(10.00s)
127.0.0.1:6379> EXISTS k
(integer) 0
127.0.0.1:6379> EXHGET k f
(nil)
127.0.0.1:6379> EXHSET k f v  VER 1
(integer) 1
127.0.0.1:6379> EXHSET k f v  VER 1
(integer) 0
127.0.0.1:6379> EXHSET k f v  VER 1
(error) ERR update version is stale
127.0.0.1:6379> EXHSET k f v  ABS 1
(integer) 0
127.0.0.1:6379> EXHSET k f v  ABS 2
(integer) 0
127.0.0.1:6379> EXHVER k f
(integer) 2
```  
## BUILD

```
mkdir build  
cd build  
cmake ../ && make -j
```
then the tairhash_module.so library file will be generated in the lib directory

## TEST

1. Modify the path in the tairhash.tcl file in the `tests` directory to `set testmodule [file your_path/tairhash_module.so]`
2. Add the path of the tairhash.tcl file in the `tests` directory to the all_tests of redis test_helper.tcl
3. run ./runtest --single tairhash


## Client
### Java : https://github.com/aliyun/alibabacloud-tairjedis-sdk

## API
[Reference](CMDDOC.md)

