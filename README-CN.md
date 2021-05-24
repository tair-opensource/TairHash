# TairHash: field带有过期时间和版本的hash数据结构
## 简介  [英文说明](README.md)
     TairHash是基于redis module开发的一种hash数据结构，和redis原生的hash数据结构相比，TairHash不但和原生hash一样具有丰富的数据接口和高性能，还可以为field设置过期时间和版本，这极大的提高了hash数据结构的灵活性，在很多场景下可以大大的简化业务开发。TairHash提供active expire机制，即使field在过期后没有被访问也可以被主动删除，释放内存。


### 主要的特性如下：

- field支持单独设置expire和version
- 针对field支持高效的active expire和passivity expire
- 语法和原生hash数据类型类似

### 高效过期实现原理：
![avatar](imgs/tairhash_index.png)

- 使用两级排序索引，第一级对tairhash主key进行排序，第二级针对每个tairhash内部的field进行排序
- 第一级排序使用第二级排序里最小的ttl进行排序，因此主key一定是全局有序的
- 内置定时器，会间隔1秒扫描第一级索引，找出一分部已经过期的key，然后分别对检查这些key的二级索引，进行field的淘汰，这就是active expire
- 每一次执行tairhash的写命令的时候，也会先检查第一级索引，看看是否有已经过期的key，如果有，则先进行淘汰
- 每一次读写field，也会触发对这个field自身的过期检查


<br/>

同时，我们还开源了一个增强型的string结构，它可以给value设置版本号并支持memcached语义，具体可参见[这里](https://github.com/alibaba/TairString)

<br/>


## 快速开始

```go
127.0.0.1:6379> EXHSET exhashkey f v ex 10
(integer) 1
127.0.0.1:6379> EXHGET exhashkey f
"v"
127.0.0.1:6379> EXISTS exhashkey
(integer) 1
127.0.0.1:6379> debug sleep 10
OK
(10.00s)
127.0.0.1:6379> EXISTS exhashkey
(integer) 0
127.0.0.1:6379> EXHGET exhashkey f
(nil)
127.0.0.1:6379> EXHSET exhashkey f v px 10000
(integer) 1
127.0.0.1:6379>
127.0.0.1:6379> EXHGET exhashkey f
"v"
127.0.0.1:6379> EXISTS exhashkey
(integer) 1
127.0.0.1:6379> debug sleep 10
OK
(10.00s)
127.0.0.1:6379> EXISTS exhashkey
(integer) 0
127.0.0.1:6379> EXHGET exhashkey f
(nil)
127.0.0.1:6379> EXHSET exhashkey f v  VER 1
(integer) 1
127.0.0.1:6379> EXHSET exhashkey f v  VER 1
(integer) 0
127.0.0.1:6379> EXHSET exhashkey f v  VER 1
(error) ERR update version is stale
127.0.0.1:6379> EXHSET exhashkey f v  ABS 1
(integer) 0
127.0.0.1:6379> EXHSET exhashkey f v  ABS 2
(integer) 0
127.0.0.1:6379> EXHVER exhashkey f
(integer) 2
```

## 命令介绍

#### EXHSET


语法及复杂度：


> EXHSET <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [NX/XX] [VER/ABS version] 
> 时间复杂度：O(1)



命令描述：


> 向key指定的TairHash中插入一个field，如果TairHash不存在则自动创建一个，如果field已经存在则覆盖其值。在插入的时候，可以使用EX/EXAT/PX/PXAT给field设置过期时间，当field过期后会被主动（active expire）或被动（passivity expire）删除掉。如果指定了NX选项，则只有在field不存在的时候才会插入成功，同理，如果指定了XX选项，则只有在field存在的时候才能插入成功。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以插入成功，如果field不存在或者field当前版本为0则不进行检查，总是可以插入成功。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功, ABS指定的版本号不能为0。该命令会触发对field的被动淘汰检查



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: TairHash中的一个元素对应的值  
> EX: 指定field的相对过期时间，单位为秒，为0表示不过期  
> EXAT: 指定field的绝对过期时间，单位为秒，为0表示不过期  
> PX: 指定field的相对过期时间，单位为毫秒，为0表示不过期  
> PXAT: 指定field的绝对过期时间，单位为毫秒 ，为0表示不过期  
> NX/XX: NX表示当要插入的field不存在的时候才允许插入，XX表示只有当field存在的时候才允许插入  
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号  


返回值：


> 成功：新创建field并成功为它设置值时，命令返回1,如果field已经存在并且成功覆盖旧值，那么命令返回0 ；如果指定了XX且field不存在则返回-1，如果指定了NX且field已经存在返回-1；如果指定了VER且版本和当前版本不匹配则返回异常信息"ERR update version is stale"    
> 失败：返回相应异常信息  



#### EXHGET


语法及复杂度：


> EXHGET <key> <field>
> 时间复杂度：O(1)



命令描述：


> 获取key指定的TairHash一个field的值，如果TairHash不存在或者field不存在，则返回nil  



参数：


> key: 用于查找该TairHash的键  
> field:   



返回值：


> 成功：当field存在时返回其对应的值，当TairHash不存在或者field不存在时返回nil  
> 失败：返回相应异常信息  



#### EXHMSET


语法及复杂度：


> EXHMSET <key> <field> <value> [field value...]  
> 时间复杂度：O(1)  



命令描述：


> 同时向key指定的TairHash中插入多个field，如果TairHash不存在则自动创建一个，如果field已经存在则覆盖其值  



参数：

 
> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: TairHash中的一个元素对应的值  



返回值：


> 成功：返回OK  
> 失败：返回相应异常信息  



#### EXHPEXPIREAT


语法及复杂度：


> EXHPEXPIREAT <key> <field> <milliseconds-timestamp> [VER/ABS version] 
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置绝对过期时间，单位为毫秒。当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，注意版本号不能为0。该命令会触发对field的被动淘汰检查  



参数：


> key: 用于查找该TairHash的键    
> field: TairHash中的一个元素    
> milliseconds-timestamp: 以毫秒为单位的时间戳    
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号    


返回值：


> 成功：当field存在时返回1，当field不存在时返回0    
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息     



#### EXHPEXPIRE


语法及复杂度：


> EXHPEXPIRE <key> <field> <milliseconds> [VER/ABS version]
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置相对过期时间，单位为毫秒。当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，注意版本号不能为0。该命令会触发对field的被动淘汰  



参数：


> key: 用于查找该TairHash的键    
> field: TairHash中的一个元素    
> milliseconds: 以毫秒为单位的过期时间    
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号    


返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  



#### EXHEXPIREAT


语法及复杂度：


> EXHEXPIREAT <key> <field> <timestamp> [VER/ABS version]
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置绝对过期时间，单位为秒，当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，注意版本号不能为0。该命令会触发对field的被动淘汰  

参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> timestamp: 以秒为单位的时间戳  
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号  



返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  



#### EXHEXPIRE


语法及复杂度：


> EXHEXPIRE <key> <field> <seconds> [VER/ABS version]
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置相对过期时间，单位为秒，当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，注意版本号不能为0。该命令会触发对field的被动淘汰  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> seconds: 以秒为单位的过期时间  
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号  



返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  



#### EXHPTTL


语法及复杂度：


> EXHPTTL <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 查看key指定的TairHash中一个field的剩余过期时间，单位为毫秒  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：当TairHash或者field不存在时返回-2，当field存在但是没有设置过期时间时返回-1，当field存在且设置过期时间时时返回对应过期时间，单位为毫秒  
> 失败：返回相应异常信息  



#### EXHTTL


语法及复杂度：


> EXHTTL <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 查看key指定的TairHash中一个field的剩余过期时间，单位为秒  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
 


返回值：


> 成功：当TairHash或者field不存在时返回-2，当field存在但是没有设置过期时间时返回-1，当field存在且设置过期时间时时返回对应过期时间，单位为秒  
> 失败：返回相应异常信息  



#### EXHVER


语法及复杂度：


> EXHVER <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 查看key指定的TairHash中一个field的当前版本号  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：当TairHash不存在时返回-1，当field不存在时返回-2，否则返回field版本号  
> 失败：返回相应异常信息  



#### EXHSETVER


语法及复杂度：


> EXHSETVER <key> <field> <version>  
> 时间复杂度：O(1)  



命令描述：


> 设置key指定的TairHash中一个field的版本号  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：当TairHash或者field不存在则返回0，否则返回1  
> 失败：返回相应异常信息  



#### EXHINCRBY


语法及复杂度：


> EXHINCRBY <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS version]   
> [MIN minval] [MAX maxval]  
> 时间复杂度：O(1)  



命令描述：


> 将key指定的TairHash中一个field的值加上整数value。如果TairHash不存在则自动新创建一个，如果指定的field不存在，则在加之前先将field的值初始化为0。同时还可以使用EX/EXAT/PX/PXAT为field设置过期时间。  
> 如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以设置成功，同时将field当前版本号设置为ABS指定的版本号，注意ABS指定的版本号不能为0。MIN/MAX用户给field提供一个边界，只有本次incr操作后field的值还在此边界时incr才会被执行，否则返回overflow的错误。该命令会触发对field的被动淘汰检查  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: field对应的值  
> EX: 指定field的相对过期时间，单位为秒 ，为0表示不过期  
> EXAT: 指定field的绝对过期时间，单位为秒 ，为0表示不过期  
> PX: 指定field的相对过期时间，单位为毫秒，为0表示不过期  
> PXAT: 指定field的绝对过期时间，单位为毫秒，为0表示不过期  
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号  


返回值：


> 成功：返回相加之后的值  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息（如原来的field值不是浮点型）  



#### EXHINCRBYFLOAT


语法及复杂度：


> EXHINCRBYFLOAT <key> <field> <value> [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS version] [MIN minval] [MAX maxval]  
> 时间复杂度：O(1)  



命令描述：


> 将key指定的TairHash中一个field的值加上浮点型value。如果TairHash不存在则自动新创建一个，如果指定的field不存在，则在加之前先将field的值初始化为0。同时还可以使用EX/EXAT/PX/PXAT为field设置过期时间。  
> 如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以设置成功，同时将field当前版本号设置为ABS指定的版本号，注意ABS指定的版本号不能为0。MIN/MAX用户给field提供一个边界，只有本次incr操作后field的值还在此边界时incr才会被执行，否则返回overflow的错误。该命令会触发对field的被动淘汰检查  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: field对应的值  
> EX: 指定field的相对过期时间，单位为秒，为0表示不过期  
> EXAT: 指定field的绝对过期时间，单位为秒，为0表示不过期  
> PX: 指定field的相对过期时间，单位为毫秒，为0表示不过期  
> PXAT: 指定field的绝对过期时间，单位为毫秒，为0表示不过期  
> VER/ABS: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号  


返回值：


> 成功：返回相加之后的值  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息（如原来的field值不是浮点型）  



#### EXHGETWITHVER


语法及复杂度：


> EXHGETWITHVER <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 同时获取key指定的TairHash一个field的值和版本，如果TairHash不存在或者field不存在，则返回nil  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：如果TairHash不存在或者field不存在，则返回nil，否则返回field对应的值和版本  
> 失败：返回相应异常信息  



#### EXHMGET


语法及复杂度：


> EXHMGET <key> <field> [field ...]  
> 时间复杂度：O(n)  



命令描述：


> 同时获取key指定的TairHash多个field的值，如果TairHash不存在或者field不存在，则返回nil  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：返回一个数组，数组的每一个元素对应一个field, 如果TairHash不存在或者field不存在，则为nil，否则为field对应的值  
> 失败：返回相应异常信息  



#### EXHMGETWITHVER


语法及复杂度：


> EXHMGETWITHVER <key> <field> [field ...]  
> 时间复杂度：O(n)  



命令描述：


> 同时获取key指定的TairHash多个field的值和版本，如果TairHash不存在或者field不存在，则返回nil



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：返回一个数组，数组的每一个元素对应一个field, 如果TairHash不存在或者field不存在，则为nil，否则为field对应的值和版本  
> 失败：返回相应异常信息  



#### EXHDEL


语法及复杂度：


> EXHDEL <key> <field> <field> <field> ...  
> 时间复杂度：O(1)  



命令描述：


> 删除key指定的TairHash一个field，如果TairHash不存在或者field不存在则返回0 ，成功删除返回1



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：如果TairHash不存则返回0 ，成功怎返回成功删除的filed的个数  
> 失败：返回相应异常信息  



#### EXHLEN


语法及复杂度：


> EXHLEN <key> [noexp]  
> 时间复杂度：不是noexp选项时是O(1)，带noexp选项时是O(N)  



命令描述：


> 获取key指定的TairHash中field的个数，该命令默认不会触发对过期field的被动淘汰，也不会将其过滤掉，所以结果中可能包含已经过期但还未被删除的field。如果只想返回当前没有过期的field个数，那么可以最后带一个noexp参数，注意，带有该参数时，exhlen的RT将受到exhash大小的影响（因为要循环遍历），同时exhlen不会触发对field的淘汰，它只是把过期的field过滤了一下而已  



参数：


> key: 用于查找该TairHash的键  



返回值：


> 成功：如果TairHash不存在或者field不存在则返回0 ，成功删除返回TairHash中field个数  
> 失败：返回相应异常信息  



#### EXHEXISTS


语法及复杂度：


> EXHEXISTS <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 查询key指定的TairHash中是否存在对应的field  



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：如果TairHash不存在或者field不存在则返回0 ，如果field存在则返回1  
> 失败：返回相应异常信息



#### EXHSTRLEN


语法及复杂度：


> EXHSTRLEN <key> <field>  
> 时间复杂度：O(1)  



命令描述：


> 获取key指定的TairHash一个field的值的长度



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 成功：如果TairHash不存在或者field不存在则返回0 ，否则返回field对应值的长度  
> 失败：返回相应异常信息  



#### EXHKEYS


语法及复杂度：


> EXHKEYS <key>  
> 时间复杂度：O(1)  



命令描述：


> 获取key指定的TairHash中所有field的键  



参数：


> key: 用于查找该TairHash的键  



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHVALS


语法及复杂度：


> EXHVALS <key>  
> 时间复杂度：O(1)  



命令描述：


> 获取key指定的TairHash中所有field的值  



参数：


> key: 用于查找该TairHash的键  



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field的值，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHGETALL


语法及复杂度：


> EXHGETALL <key>  
> 时间复杂度：O(1)  



命令描述：


> 获取key指定的TairHash中所有field的键值对  



参数：


> key: 用于查找该TairHash的键



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field的键值对，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHSCAN


语法及复杂度：


> EXHSCAN <key> <op> <subkey> [MATCH pattern] [COUNT count]  
> 时间复杂度：O(1)、O(N)  



命令描述：


> 扫描key指定的TairHash,扫描的方式op可以为>、>=、<、<=、==、^、$，扫描的同时可以根据MATCH指定的pattern对结果集进行过滤，还可以使用COUNT对单次扫描个数进行限制，如果不指定则默认值为10。TairHash和Redis云生Hash的SCAN/HSCAN等不太一致，它没有cursor的概念，相反用户可以使用subkey直接定位扫描的开始位置，相比对业务毫无意义的cursor而言，subkey对业务更加友好。同时，原生的Redis Hash扫描算法可能在rehash的时候返回大量的已经扫描过的field，而TairHash则没有这个问题，渐近扫描的过程中，无论TairHash中的field增加还是减少，已经被扫描过的field绝对不会被再次扫描和返回。



参数：


> key: 用于查找该TairHash的键    
> op: 用于定位扫描的起点，可以是:>(从第一个大于subkey的field开始),>=(从第一个大于等于subkey的field位置开始),<(从第一个小于subkey的field开始)，<=(从第一个小于等于subkey的field开始)，==(从第一个等于subkey的field开始)，^(从第一个field开始)，\$(从最后一个field开始)      
> subkey: 用于搜索扫描起始位置的键，当op为^或$时该值将被忽略  
> MATCH: 用于对扫描结果进行过滤的规则  
> COUNT: 用于规定单次扫描field的个数，注意，COUNT仅表示每次扫描TairHash的feild的个数，不代表最终一定会返回COUNT个field结果集，结果集的大小还要根据TairHash中当前field个数和是否指定MATCH进行过滤而定。COUNT默认值为10。  



返回值：


> 成功：返回一个具有两个元素的数组，数组第一个元素是一个字符串，表示本次扫描结束后下一个待扫描的field，如果本次扫描结束后已经没有field可以扫描，那么该元素为一个空的字符串。第二个数组元素还是一个数组，数组包含了所有本次被迭代的field/value。如果扫描到一个空的TairHash或者是TairHash不存在，那么这两个数组元素都为空。  
> 失败：返回相应异常信息  



**使用示例：**
1、如何使用渐进式扫描整个TairHash:   
```
127.0.0.1:6379> exhmset exhashkey field1 val1 field2 val2 field3 val3 field4 val4 field5 val5 field6 val6 field7 val7 field8 val8 field9 val9
OK
127.0.0.1:6379> exhscan exhashkey ^ xxx COUNT 3
1) "field4"
2) 1) "field1"
   2) "val1"
   3) "field2"
   4) "val2"
   5) "field3"
   6) "val3"
127.0.0.1:6379> exhscan exhashkey >= field4 COUNT 3
1) "field7"
2) 1) "field4"
   2) "val4"
   3) "field5"
   4) "val5"
   5) "field6"
   6) "val6"
127.0.0.1:6379> exhscan exhashkey >= field7 COUNT 3
1)
2) 1) "field7"
   2) "val7"
   3) "field8"
   4) "val8"
   5) "field9"
   6) "val9"
127.0.0.1:6379>
```

2、如何使用MATCH对结果集进行过滤 
精确匹配：


```
127.0.0.1:6379> exhmset exhashkey field1_1 val1_1 field1_2 val1_2 field1_3 val1_3 field1_4 val1_4 field1_5 val1_5 field2_1 val2_1 field2_2 val2_2 field2_3 val2_3 field6_1 val6_1 field6_2 val6_2 field6_3 val6_3 field6_4 val6_4 field8_1 val8_1 field8_2 val8_4
OK
127.0.0.1:6379> exhscan exhashkey ^ xxx COUNT 3 MATCH field1_1
1) "field1_4"
2) 1) "field1_1"
   2) "val1_1"
127.0.0.1:6379> exhscan exhashkey >= field1_4 COUNT 3 MATCH field1_1
1) "field2_2"
2) (empty list or set)
127.0.0.1:6379> exhscan exhashkey >= field2_2 COUNT 3 MATCH field1_1
1) "field6_2"
2) (empty list or set)
127.0.0.1:6379> exhscan exhashkey >= field6_2 COUNT 3 MATCH field1_1
1) "field8_1"
2) (empty list or set)
127.0.0.1:6379> exhscan exhashkey >= field8_1 COUNT 3 MATCH field1_1
1)
2) (empty list or set)
```


模糊匹配：

```
127.0.0.1:6379> exhmset exhashkey field1_1 val1_1 field1_2 val1_2 field1_3 val1_3 field1_4 val1_4 field1_5 val1_5 field2_1 val2_1 field2_2 val2_2 field2_3 val2_3 field6_1 val6_1 field6_2 val6_2 field6_3 val6_3 field6_4 val6_4 field8_1 val8_1 field8_2 val8_4
OK
127.0.0.1:6379> exhscan exhashkey ^ xxx COUNT 3 MATCH field6_*
1) "field1_4"
2) (empty list or set)
127.0.0.1:6379> exhscan exhashkey >= field1_4 COUNT 3 MATCH field6_*
1) "field2_2"
2) (empty list or set)
127.0.0.1:6379> exhscan exhashkey >= field2_2 COUNT 3 MATCH field6_*
1) "field6_2"
2) 1) "field6_1"
   2) "val6_1"
127.0.0.1:6379> exhscan exhashkey >= field6_2 COUNT 3 MATCH field6_*
1) "field8_1"
2) 1) "field6_2"
   2) "val6_2"
   3) "field6_3"
   4) "val6_3"
   5) "field6_4"
   6) "val6_4"
127.0.0.1:6379> exhscan exhashkey >= field8_1 COUNT 3 MATCH field6_*
1)
2) (empty list or set)
```

<br/>
  
## 编译及使用

```
mkdir build  
cd build  
cmake ../ && make -j
```
编译成功后会在lib目录下产生tairhash_module.so库文件
## 测试方法

1. 修改`tests`目录下tairhash.tcl文件中的路径为`set testmodule [file your_path/tairhash_module.so]`
2. 将`tests`目录下tairhash.tcl文件路径加入到redis的test_helper.tcl的all_tests中
3. 在redis根目录下运行./runtest --single tairhash

<br/>

##
## 客户端
### Java : https://github.com/aliyun/alibabacloud-tairjedis-sdk
### 其他语言：可以参考java的sendcommand自己封装