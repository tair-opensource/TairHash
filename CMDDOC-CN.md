
## 命令介绍

#### EXHSET


语法及复杂度：


> EXHSET key field value [EX time] [EXAT time] [PX time] [PXAT time] [NX/XX] [VER/ABS/GT version] [KEEPTTL]  
> 时间复杂度：O(1)



命令描述：


> 向key指定的TairHash中插入一个field，如果TairHash不存在则自动创建一个，如果field已经存在则覆盖其值。在插入的时候，可以使用EX/EXAT/PX/PXAT给field设置过期时间，当field过期后会被主动（active expire）或被动（passivity expire）删除掉。如果指定了NX选项，则只有在field不存在的时候才会插入成功，同理，如果指定了XX选项，则只有在field存在的时候才能插入成功。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以插入成功，如果field不存在或者field当前版本为0则不进行检查，总是可以插入成功。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: TairHash中的一个元素对应的值  
> EX: 指定field的相对过期时间，单位为秒，0表示立刻过期
> EXAT: 指定field的绝对过期时间，单位为秒，0表示立刻过期
> PX: 指定field的相对过期时间，单位为毫秒，0表示立刻过期
> PXAT: 指定field的绝对过期时间，单位为毫秒 ，0表示立刻过期
> NX/XX: NX表示当要插入的field不存在的时候才允许插入，XX表示只有当field存在的时候才允许插入  
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0  
> KEEPTTL: 当未指定EX/EXAT/PX/PXAT时保留field的过期时间

返回值：


> 成功：新创建field并成功为它设置值时，命令返回1,如果field已经存在并且成功覆盖旧值，那么命令返回0 ；如果指定了XX且field不存在则返回-1，如果指定了NX且field已经存在返回-1；如果指定了VER且版本和当前版本不匹配则返回异常信息"ERR update version is stale"    
> 失败：返回相应异常信息  



#### EXHGET


语法及复杂度：


> EXHGET key field  
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


> EXHMSET key field value [field value...]    
> 时间复杂度：O(n)  



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


> EXHPEXPIREAT key field milliseconds-timestamp [VER/ABS/GT version]   
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置绝对过期时间，单位为毫秒。当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0



参数：


> key: 用于查找该TairHash的键    
> field: TairHash中的一个元素    
> milliseconds-timestamp: 以毫秒为单位的时间戳，0表示立刻过期   
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0


返回值：


> 成功：当field存在时返回1，当field不存在时返回0    
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息     



#### EXHPEXPIRE


语法及复杂度：


> EXHPEXPIRE key field milliseconds [VER/ABS/GT version]  
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置相对过期时间，单位为毫秒。当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0

参数：


> key: 用于查找该TairHash的键    
> field: TairHash中的一个元素    
> milliseconds: 以毫秒为单位的过期时间，0表示立刻过期       
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0


返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  



#### EXHEXPIREAT


语法及复杂度：


> EXHEXPIREAT key field timestamp [VER/ABS/GT version]  
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置绝对过期时间，单位为秒，当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0

参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> timestamp: 以秒为单位的时间戳，0表示立刻过期   
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0



返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  



#### EXHEXPIRE


语法及复杂度：


> EXHEXPIRE key field seconds [VER/ABS/GT version]  
> 时间复杂度：O(1)  



命令描述：


> 在key指定的TairHash中为一个field设置相对过期时间，单位为秒，当过期时间到时，该field会被主动删除。如果field不存在则直接返回0。如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以插入成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> seconds: 以秒为单位的过期时间，0表示立刻过期     
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0



返回值：


> 成功：当field存在时返回1，当field不存在时返回0  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息  

#### EXHPERSIST


语法及复杂度：


> EXHEXPIRE key field
> 时间复杂度：O(1)  



命令描述：


> 将一个field设置为永不过期



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  



返回值：


> 1：成功移除field上的过期设置
> 0：key或field不存在，或field存在但当前没有过期设置



#### EXHPTTL


语法及复杂度：


> EXHPTTL key field    
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


> EXHTTL key field    
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


> EXHVER key field     
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


> EXHSETVER key field version    
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


> EXHINCRBY key field value [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS/GT version] [MIN minval] [MAX maxval] [KEEPTTL]    
> 时间复杂度：O(1)  



命令描述：


> 将key指定的TairHash中一个field的值加上整数value。如果TairHash不存在则自动新创建一个，如果指定的field不存在，则在加之前先将field的值初始化为0。同时还可以使用EX/EXAT/PX/PXAT为field设置过期时间。  
> 如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以设置成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0，MIN/MAX用户给field提供一个边界，只有本次incr操作后field的值还在此边界时incr才会被执行，否则返回overflow的错误



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: field对应的值  
> EX: 指定field的相对过期时间，单位为秒 ，0表示立刻过期
> EXAT: 指定field的绝对过期时间，单位为秒 ，0表示立刻过期
> PX: 指定field的相对过期时间，单位为毫秒，0表示立刻过期
> PXAT: 指定field的绝对过期时间，单位为毫秒，0表示立刻过期
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0
> MAX/MIN: 设置最大最小边界，本次incr操作后，field的值在此边界时incr才会被执行，否则返回overflow的错误。
> KEEPTTL: 当未指定EX/EXAT/PX/PXAT时保留field的过期时间

返回值：


> 成功：返回相加之后的值  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息（如原来的field值不是浮点型）  



#### EXHINCRBYFLOAT


语法及复杂度：


> EXHINCRBYFLOAT key field value [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS/GT version] [MIN minval] [MAX maxval] [KEEPTTL]   
> 时间复杂度：O(1)  



命令描述：


> 将key指定的TairHash中一个field的值加上浮点型value。如果TairHash不存在则自动新创建一个，如果指定的field不存在，则在加之前先将field的值初始化为0。同时还可以使用EX/EXAT/PX/PXAT为field设置过期时间。  
> 如果指定了VER参数，则VER所携带的版本号必须和field当前版本号一致才可以设置成功，或者如果VER参数所携带的版本号为0，则不进行版本校验。ABS参数用于强制给field设置版本号，而不管field当前的版本号，总是可以设置成功，同时将field当前版本号设置为ABS指定的版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0，MIN/MAX用户给field提供一个边界，只有本次incr操作后field的值还在此边界时incr才会被执行，否则返回overflow的错误



参数：


> key: 用于查找该TairHash的键  
> field: TairHash中的一个元素  
> value: field对应的值  
> EX: 指定field的相对过期时间，单位为秒，0表示立刻过期
> EXAT: 指定field的绝对过期时间，单位为秒，0表示立刻过期
> PX: 指定field的相对过期时间，单位为毫秒，0表示立刻过期
> PXAT: 指定field的绝对过期时间，单位为毫秒，0表示立刻过期
> VER/ABS/GT: VER表示只有指定的版本和field当前的版本一致时才允许设置，如果VER指定的版本为0则表示不进行版本检查，ABS表示无论field当前的版本是多少都强制设置并修改版本号，GT表示只有指定的版本大于当前版本时才允许设置，ABS和GT指定的版本号不能为0
> MAX/MIN: 设置最大最小边界，本次incr操作后，field的值在此边界时incr才会被执行，否则返回overflow的错误。
> KEEPTTL: 当未指定EX/EXAT/PX/PXAT时保留field的过期时间

返回值：


> 成功：返回相加之后的值  
> 失败：当版本校验失败时返回update version is stale错误，其他错误返回相应异常信息（如原来的field值不是浮点型）  



#### EXHGETWITHVER


语法及复杂度：


> EXHGETWITHVER key field    
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


> EXHMGET key field [field ...]    
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


> EXHMGETWITHVER key field [field ...]    
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


> EXHDEL key field [field...]   
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


> EXHLEN key [noexp]   
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


> EXHEXISTS key field  
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


> EXHSTRLEN key field   
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


> EXHKEYS key  
> 时间复杂度：O(n)  



命令描述：


> 获取key指定的TairHash中所有field的键  



参数：


> key: 用于查找该TairHash的键  



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHVALS


语法及复杂度：


> EXHVALS key  
> 时间复杂度：O(n)  



命令描述：


> 获取key指定的TairHash中所有field的值  



参数：


> key: 用于查找该TairHash的键  



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field的值，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHGETALL


语法及复杂度：


> EXHGETALL key  
> 时间复杂度：O(n)  



命令描述：


> 获取key指定的TairHash中所有field的键值对  



参数：


> key: 用于查找该TairHash的键



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field的键值对，如果TairHash不存则返回空的数组  
> 失败：返回相应异常信息  



#### EXHGETALLWITHVER


语法及复杂度：


> EXHGETALLWITHVER key
> 时间复杂度：O(n)



命令描述：


> 获取key指定的TairHash中所有field的键值对和版本



参数：


> key: 用于查找该TairHash的键



返回值：


> 成功：返回一个数组，数组的每一位对应TairHash中的每一个field的键值对和版本，如果TairHash不存则返回空的数组
> 失败：返回相应异常信息



#### EXHSCAN


语法及复杂度：


> EXHSCAN key cursor [MATCH pattern] [COUNT count]   
> 时间复杂度：O(1)、O(N)  



命令描述：


> 扫描key指定的TairHash



参数：


> key: 用于查找该TairHash的键        
> cursor: 扫描的游标，从0开始，每次扫描后会返回下一次扫描的cursor，直到返回0表示扫描结束    
> MATCH: 用于对扫描结果进行过滤的规则      
> COUNT: 用于规定单次扫描field的个数，注意，COUNT仅表示每次扫描TairHash的feild的个数，不代表最终一定会返回COUNT个field结果集，结果集的大小还要根据TairHash中当前field个数和是否指定MATCH进行过滤而定。COUNT默认值为10     



返回值：


> 成功：返回一个具有两个元素的数组，数组第一个元素是下一次扫描需要使用的cursor，为0表示整个扫描结束。第二个数组元素还是一个数组，数组包含了所有本次被迭代的field/value。如果扫描到一个空的TairHash或者是TairHash不存在，那么这两个数组元素都为空。      
> 失败：返回相应异常信息  



**使用示例：**
1、如何使用渐进式扫描整个TairHash:   
```
127.0.0.1:6379> exhmset exhashkey field1 val1 field2 val2 field3 val3 field4 val4 field5 val5 field6 val6 field7 val7 field8 val8 field9 val9
OK
127.0.0.1:6379> exhscan exhashkey 0 COUNT 3
1) (integer) 4
2) 1) "field6"
   2) "val6"
   3) "field5"
   4) "val5"
127.0.0.1:6379> exhscan exhashkey 4 COUNT 3
1) (integer) 1
2) 1) "field8"
   2) "val8"
   3) "field2"
   4) "val2"
127.0.0.1:6379> exhscan exhashkey 1 COUNT 3
1) (integer) 13
2) 1) "field9"
   2) "val9"
   3) "field7"
   4) "val7"
127.0.0.1:6379> exhscan exhashkey 13 COUNT 3
1) (integer) 11
2) 1) "field3"
   2) "val3"
   3) "field4"
   4) "val4"
127.0.0.1:6379> exhscan exhashkey 11 COUNT 3
1) (integer) 0
2) 1) "field1"
   2) "val1"
```

2、如何使用MATCH对结果集进行过滤 
精确匹配：


```
127.0.0.1:6379> exhmset exhashkey field1_1 val1_1 field1_2 val1_2 field1_3 val1_3 field1_4 val1_4 field1_5 val1_5 field2_1 val2_1 field2_2 val2_2 field2_3 val2_3 field6_1 val6_1 field6_2 val6_2 field6_3 val6_3 field6_4 val6_4 field8_1 val8_1 field8_2 val8_4
OK
127.0.0.1:6379> exhscan exhashkey 0 COUNT 3 MATCH field1_1
1) (integer) 8
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 8 COUNT 3 MATCH field1_1
1) (integer) 12
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 12 COUNT 3 MATCH field1_1
1) (integer) 9
2) 1) "field1_1"
   2) "val1_1"
127.0.0.1:6379> exhscan exhashkey 9 COUNT 3 MATCH field1_1
1) (integer) 5
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 5 COUNT 3 MATCH field1_1
1) (integer) 11
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 11 COUNT 3 MATCH field1_1
1) (integer) 0
2) (empty array)
```


模糊匹配：

```
127.0.0.1:6379> exhmset exhashkey field1_1 val1_1 field1_2 val1_2 field1_3 val1_3 field1_4 val1_4 field1_5 val1_5 field2_1 val2_1 field2_2 val2_2 field2_3 val2_3 field6_1 val6_1 field6_2 val6_2 field6_3 val6_3 field6_4 val6_4 field8_1 val8_1 field8_2 val8_4
OK
127.0.0.1:6379> exhscan exhashkey 0 COUNT 3 MATCH field6_*
1) (integer) 8
2) 1) "field6_4"
   2) "val6_4"
   3) "field6_1"
   4) "val6_1"
127.0.0.1:6379> exhscan exhashkey 8 COUNT 3 MATCH field6_*
1) (integer) 12
2) 1) "field6_2"
   2) "val6_2"
127.0.0.1:6379> exhscan exhashkey 12 COUNT 3 MATCH field6_*
1) (integer) 9
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 9 COUNT 3 MATCH field6_*
1) (integer) 5
2) (empty array)
127.0.0.1:6379> exhscan exhashkey 5 COUNT 3 MATCH field6_*
1) (integer) 11
2) 1) "field6_3"
   2) "val6_3"
127.0.0.1:6379> exhscan exhashkey 11 COUNT 3 MATCH field6_*
1) (integer) 0
2) (empty array)
```

<br/>

## Last-write-wins写入与数据源同步

以下命令用于把TairHash作为外部数据源（例如保存每个field最新值的数据库表）的缓存，并且不会把不完整或过期的数据当作完整数据返回：

1. 写入方使用 `EXHMSETLWW` 写入每次变更、使用 `EXHDELLWW` 写入每次删除，以数据源的写入时间戳作为版本。被更新的写入覆盖是预期结果，不是错误。
2. 读取方调用 `EXHSYNCGET`。未同步的TairHash（新建、被淘汰，或因重启、故障切换失效，见下文）视为未命中：读取方调用 `EXHSYNCBEGIN`，然后从数据源读取全部field，连同 `EXHSYNCBEGIN` 返回的token一起交给 `EXHSYNC`。
3. `EXHSYNC` 合并快照，删除数据源中已不存在的field，并把TairHash标记为已同步。在token之后写入的field与快照读取存在竞争，无论版本如何都会保留。

已同步的TairHash依靠以下机制保持正确：

- **墓碑。** `EXHDELLWW` 会留下一个记录删除版本的墓碑，保留 `lww_tombstone_ttl` 秒（默认3600）。版本相同或更低的写入和快照条目都会输给墓碑，因此在删除之前读取的快照不会让field复活。只剩墓碑的TairHash会获得一个在最后一个墓碑到期时结束的key过期时间。已过保留期的墓碑不再阻止任何写入，但在之后对该TairHash执行的 `EXHMSETLWW`、`EXHDELLWW` 或 `EXHSYNC` 将其清除之前（从节点在应用对该TairHash的写入时清除自己的墓碑），或在该TairHash重新从RDB加载之前，它仍然占用内存：不再有写入的TairHash会一直保留其过期的墓碑。过期的墓碑不会写入RDB、AOF重写和全量同步。其他命令（`EXHSET`、`EXHSETNX`、`EXHMSET`、`EXHMSETWITHOPTS`、`EXHINCRBY`、`EXHINCRBYFLOAT`）写入带有墓碑的field时会移除墓碑，并像在已有field上一样从删除版本继续：field的版本为删除版本加一，因此被墓碑阻止的写入仍然会失败。`GT` 与墓碑的版本比较；`ABS` 直接设置给定的版本，因此会覆盖墓碑。
- **租约。** 只有当TairHash仍持有 `EXHSYNCBEGIN` 以该token开启的租约时，`EXHSYNC` 才会成功：key被删除、淘汰或重建之后，另一次同步完成之后，同步纪元变化之后（见下文），或超过 `sync_lease_ttl` 秒（默认60）之后都会失败，调用方需重新调用 `EXHSYNCBEGIN`。同一TairHash的并发同步共享一个租约，最先完成的 `EXHSYNC` 生效。对不存在的key，`EXHSYNCBEGIN` 会创建一个过期时间为 `sync_lease_ttl` 的空占位TairHash；没有field且未同步的TairHash在key自身没有过期时间时也会获得该过期时间，因此被放弃的同步不会留下任何数据。以空数据同步过、在纪元变化后不再处于已同步状态的TairHash，只有在对它调用 `EXHSYNCBEGIN` 时才会成为这样的占位TairHash，未命中它的 `EXHSYNCGET` 则会将其删除：如果之后既不被读取也不再同步，它会作为空key一直保留，除非key自身设置了过期时间。
- **同步纪元。** TairHash只在 `EXHSYNC` 标记它时的纪元内处于已同步状态。主节点在模块加载时、加载RDB或AOF之后（重启、`DEBUG RELOAD`）以及被提升为主节点时开始一个新纪元，因为这些情况都可能丢失已同步TairHash所反映的写入；此后已同步的TairHash在重新同步之前都视为未命中，其数据、墓碑和写入序号仍会保留。主节点会把新纪元写入复制流，排在它在上一个纪元内复制的所有内容之后，因此从节点拥有的是主节点在其已应用的复制流位置上的纪元：无论它是实时跟随复制流、在部分重同步后从积压缓冲区重放，还是从另一个从节点复制，它判定某个TairHash已同步的结果都与主节点在该位置上的判定一致。全量同步以及从节点重启时加载的RDB也携带纪元。主节点变为从节点后，在加载或应用新主节点的纪元之前没有纪元，所有TairHash都视为未命中。

模块设置的过期时间（占位TairHash的，或只剩墓碑的TairHash的）只在TairHash没有field时有效：第一个添加field的命令以及 `EXHSYNC` 会移除它。key本身已有的过期时间不受影响。缓存的过期时间应在 `EXHSYNC` 之后设置，而不是在同步过程中设置，否则可能被同步移除。

同步状态属于TairHash本身，与其key的名称无关。`RENAME` 和 `MOVE` 会连同同步状态、墓碑以及进行中的同步一起移动TairHash，该同步的token在新名称下继续有效。`COPY` 创建的是一个新的TairHash：它拥有相同的field，但未同步，也没有墓碑和进行中的同步，因为之后的写入只会到达原来的TairHash。`RESTORE` 同样不会使TairHash成为已同步状态，因为其payload可能早于TairHash在 `DUMP` 之后接收的写入；payload中的墓碑会被恢复，因此迁移后的key仍能阻止迟到的写入。

`EXHSYNCBEGIN`、`EXHSYNC` 和 `EXHSYNCGET` 需要Redis 6.0及以上（或Valkey）的服务端事件来更新纪元，在更早的服务端上不会注册。`EXHMSETLWW` 和 `EXHDELLWW` 在所有版本上可用。

所有写入value的命令都会为field记录一个由服务端分配的写入序号：取微秒级时钟与上一个序号加一中的较大值。序号严格递增，会被复制和持久化；只要各节点时钟的偏差小于重启或故障切换所需的时间，它就大于之前的进程或故障切换前的主节点分配的序号。同步token和纪元取自同一序列。

#### EXHMSETLWW


语法及复杂度：


> EXHMSETLWW key field value version [field value version ...]
> 时间复杂度：O(N)，N为field个数



命令描述：


> 以last-write-wins语义写入key指定的TairHash中的field，TairHash不存在时自动创建。当field不存在且没有版本相同或更高的墓碑（见EXHDELLWW）、版本更高，或版本相同且value按字节比较更大（一个value是另一个的前缀时，较短的更小）时写入生效。版本相同时的比较规则与Cassandra/Scylla相同，因此结果与写入到达的顺序无关。生效的写入会替换value和版本，清除field的过期时间，并删除其墓碑。field按参数顺序写入。



参数：


> key: 用于查找该TairHash的键
> field: TairHash中的一个元素
> value: 写入的值
> version: 写入的版本，正整数（例如数据源的微秒级写入时间戳）



返回值：


> 成功：返回数组，每个field对应一个整数，1表示写入生效，0表示field已有更新（或比较更大）的写入，或已在相同或更高的版本被删除
> 失败：版本不是正整数时返回syntax error（不会写入任何数据）；key为其他类型时返回WRONGTYPE



#### EXHDELLWW


语法及复杂度：


> EXHDELLWW key field version [field version ...]
> 时间复杂度：O(N)，N为field个数



命令描述：


> 以last-write-wins语义删除key指定的TairHash中的field。除非field持有版本更高的写入，删除都会生效（版本相同时删除优先），并留下一个记录删除版本的墓碑，在 `lww_tombstone_ttl` 秒内，版本相同或更低的写入和EXHSYNC快照条目都会输给它。key不存在时会被创建，这样先于被覆盖写入到达的删除仍能阻止该写入；只剩墓碑的TairHash会获得一个在最后一个墓碑到期时结束的key过期时间（key已有自身的过期时间时除外），并保留到那时。field按参数顺序处理。



参数：


> key: 用于查找该TairHash的键
> field: TairHash中的一个元素
> version: 删除的版本，正整数



返回值：


> 成功：返回数组，每个field对应一个整数，1表示field已在该版本被删除（由本次删除，或之前一次版本相同或更高的删除），0表示更新的写入保留了该field
> 失败：版本不是正整数时返回syntax error（不会删除任何数据）；key为其他类型时返回WRONGTYPE



#### EXHSYNCGET


语法及复杂度：


> EXHSYNCGET key [field ...]
> 时间复杂度：O(N)，N为TairHash中的field个数或指定的field个数



命令描述：


> 如果key指定的TairHash在当前纪元已同步（见EXHSYNC），返回其内容。TairHash不存在或未同步时视为未命中：不返回field，调用方使用EXHSYNCBEGIN和EXHSYNC同步它。指定field时只返回存在的field，顺序与请求一致；对已同步的TairHash，field不存在表示数据源中没有该field。已过期的field不会返回。与其他读命令一样，它只会删除已过期的field，因此除非有field过期，否则不会使WATCH失效。未命中时，它还会删除已经一无所有的TairHash：没有field、墓碑和进行中的同步，例如在之前的纪元中以空数据同步过的TairHash。



参数：


> key: 用于查找该TairHash的键
> field: 要返回的field（默认返回全部）



返回值：


> 成功：返回两个元素的数组：已同步为1否则为0；field、value、version三元组组成的数组（未命中时为空）
> 失败：key为其他类型时返回WRONGTYPE



#### EXHSYNCBEGIN


语法及复杂度：


> EXHSYNCBEGIN key
> 时间复杂度：O(1)



命令描述：


> 开启key指定的TairHash与数据源的一次同步，并返回其token。应在此调用之后读取数据源，并把快照和token交给EXHSYNC。此调用之后的所有写入都排在token之后。租约持续 `sync_lease_ttl` 秒，直到使用该token的EXHSYNC完成或key被删除为止；租约有效期间再次调用会返回相同的token。key不存在时会创建一个过期时间为 `sync_lease_ttl` 的空的、未同步的占位TairHash。没有field且未同步的TairHash（在之前的纪元中以空数据同步过，或只剩墓碑）按占位TairHash处理：key没有过期时间时获得该过期时间，模块设置的过期时间至少会延长到该时长。key自身的过期时间不受影响。租约不会被持久化或复制：跨越重启或故障切换的同步会失败并重新开始。



参数：


> key: 用于查找该TairHash的键



返回值：


> 成功：返回同步token，正整数
> 失败：key为其他类型时返回WRONGTYPE



#### EXHSYNC


语法及复杂度：


> EXHSYNC key token [field value version ...]
> 时间复杂度：O(N + M + T)，N为快照中的field个数，M为TairHash中的field个数，T为其墓碑个数



命令描述：


> 使用在EXHSYNCBEGIN之后读取的数据源完整快照，完成该token开启的同步：快照中的field按EXHMSETLWW语义合并，因此在相同或更高版本被删除的field保持删除；不在快照中的field会被删除，除非它们在token之后写入；然后把TairHash标记为在当前纪元已同步，关闭租约，并移除模块设置的过期时间。已同步的TairHash即使没有field也会保留，因此空快照可以缓存“数据源中没有该key的数据”。已同步状态一直保持到key被删除或纪元变化。TairHash不持有该token的租约时失败，不做任何修改。



参数：


> key: 用于查找该TairHash的键
> token: 读取快照之前EXHSYNCBEGIN返回的token
> field value version: 快照内容，版本为正整数



返回值：


> 成功：返回两个整数的数组：写入生效的快照field个数，以及删除的field个数
> 失败：token或版本不是正整数时返回syntax error，TairHash不持有该token的租约时返回 `ERR no sync lease for this token: call EXHSYNCBEGIN again`（两种情况都不做任何修改）；key为其他类型时返回WRONGTYPE

<br/>

## 复制与持久化

写命令复制的是本次写入的每个field的结果状态，而不是原始请求，因此从节点或AOF重放可以精确还原值、版本和过期时间，与其自身的时钟和状态无关。模块参数 `replication_format` 决定使用的命令：

- `state`（默认）：每条写命令复制为一条内部命令 `EXHMSETREPL key 2 field value version expire seq [field value version expire seq ...]`，expire为毫秒级unix时间戳，0表示不过期，seq为field的写入序号。超过8192个field的写入会拆成多条，在一个MULTI/EXEC中一起复制。不修改值的写入（`EXHEXPIRE` 系列、`EXHPERSIST`、`EXHSETVER`）改为发送不带值的 `EXHMETAREPL key 1 field version expire [...]`。key之后的数字表示参数布局，节点会拒绝其不认识的布局并在日志中记录丢弃的写入；对于 `EXHMSETREPL` 布局1（TairHash 2.0，不含写入序号），节点会自行分配序号。如果 `EXHMETAREPL` 更新的field在本节点上不存在，说明该节点已经与主节点不一致：节点会更新存在的field，把其余的记入日志，并返回错误。hash的同步状态（其纪元，未同步时为0，以及key的过期时间是否由模块设置）通过 `EXHSYNCREPL key epoch owns_ttl` 发送，其后是该过期时间的 `PEXPIREAT` 或 `PERSIST`；`EXHDELLWW` 发送 `EXHTOMBREPL key 1 field version deadline [...]`，携带其设置的墓碑到期时间。AOF重写也会输出这些命令，以及每条最多64个field的 `EXHMSETREPL`。主节点开始新的同步纪元时会发送 `EXHSYNCEPOCH epoch`，从节点以它作为自己的纪元；只有在该节点上同步过TairHash之后才会立即发送，否则推迟到第一个TairHash被同步之前，因此不使用同步命令的主节点不会发送旧版本TairHash不认识的命令。当前的同步纪元同时以模块aux数据保存在RDB中，因此即使没有TairHash类型的key，TairHash 2.1的RDB也需要加载本模块。这些命令只接受来自复制流或加载过程的调用，普通客户端会收到 `ERR only accepted from the replication stream or while loading`（缺少Redis 6.0.9加入的 `RedisModule_GetContextFlagsAll`、无法报告所支持的上下文标志的服务端总是接受）。因此state格式的AOF或复制流只能通过加载或从节点应用，不能通过 `redis-cli --pipe` 等客户端重放；需要时请使用 `compat`。
- `compat`：每个field复制为一条 `EXHSET key field value ABS <version> [PXAT <expire>]`；版本为0的field（只有TairHash 1.x写入的数据会有）`EXHSET` 无法设置，复制为 `EXHSETNX key field value`。来自复制流或AOF的 `EXHSETNX` 把新field的版本置为0，客户端发送的 `EXHSETNX` 则从版本1开始。同一写命令涉及的多个field在一个MULTI/EXEC中一起复制。TairHash 1.x可以识别这些命令，升级依赖于此，见README。这些命令不携带写入序号：从节点或AOF重放会自行分配更大的序号，因此使用之前获取的token执行 `EXHSYNC` 时会保留而不是删除这些field。`EXHMSETLWW`、`EXHDELLWW` 和 `EXHSYNC` 在不支持它们的版本上无法执行，因此总是以state格式复制。
