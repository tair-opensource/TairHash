
## Command introduction

#### EXHSET

Grammar and complexity：


> EXHSET key field value [EX time] [EXAT time] [PX time] [PXAT time] [NX/XX] [VER/ABS/GT version] [KEEPTTL]
> time complexity：O(1)

Command Description：
> Insert a field into the TairHash specified by the key. If TairHash does not exist, it will automatically create one, and if the field already exists, its value will be overwritten. When inserting, you can use EX/EXAT/PX/PXAT to set the expiration time for the field. When the field expires, it will be deleted actively (active expire) or passive (passivity expire). If the NX option is specified, the insertion will be successful only when the field does not exist. Similarly, if the XX option is specified, the insertion will be successful only when the field exists. If the VER parameter is specified, the version number carried by the VER must be consistent with the current version number of the field before it can be inserted successfully. If the field does not exist or the current version of the field is 0, no check is performed, and the insertion can always be successful. The ABS parameter is used to forcibly set the version number for the field, regardless of the current version number of the field, it can always be inserted successfully, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. This command will trigger the passive elimination check of the field


parameter：

> key: The key used to find the TairHash
> field: An element in TairHash
> value: The value corresponding to an element in TairHash
> EX: The relative expiration time of the specified field, in seconds, 0 means expire immediately
> EXAT: Specify the absolute expiration time of the field, in seconds, 0 means expire immediately
> PX: The relative expiration time of the specified field, in milliseconds, 0 means expire immediately
> PXAT: Specify the absolute expiration time of the field, in milliseconds, 0 means expire immediately
> NX/XX: NX means inserting is allowed only when the field to be inserted does not exist, XX means inserting is allowed only when the field exists
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.

> KEEPTTL: Retain the time to live associated with the field. KEEPTTL cannot be used together with EX/EXAT/PX/PXAT

Return：

> When a new field is created and the value is successfully set for it, the command returns 1, if the field already exists and successfully overwrites the old value, the command returns 0; if XX is specified and the field does not exist, it returns -1, if NX is specified and the field is already If exists, return -1; if VER is specified and the version does not match the current version, the exception message "ERR update version is stale" is returned

#### EXHGET


Grammar and complexity：


> EXHGET key field
> time complexity：O(1)



Command Description：


> Get the value of a field in TairHash specified by key, if TairHash does not exist or the field does not exist, return nil



parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> When the field exists, return its corresponding value, when TairHash does not exist or the field does not exist, return nil



#### EXHMSET


Grammar and complexity：


> EXHMSET key field value [field value...]
> time complexity：O(n)



Command Description：


> At the same time, insert multiple fields into TairHash specified by key. If TairHash does not exist, one will be created automatically, and if the field already exists, its value will be overwritten



parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> value: The value corresponding to an element in TairHash



Return：


> Return OK



#### EXHPEXPIREAT


Grammar and complexity：


> EXHPEXPIREAT key field milliseconds-timestamp [VER/ABS/GT version]
> time complexity: (1)



Command Description：


> Set the absolute expiration time for a field in the TaiHash specified by the key, in milliseconds. When the expiration time is up, the field will be deleted actively. If the field does not exist, return 0 directly. If VER parameter is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to force the field to set the version number, regardless of the current version number of the field, it can always be inserted successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. This command will trigger the passive elimination check of the field



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> milliseconds-timestamp: Timestamp in milliseconds, 0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.


Return：


> Success: return 1 when the field exists, and return 0 when the field does not exist
> When the version verification fails, the update version is stale error is returned



#### EXHPEXPIRE


Grammar and complexity：


> EXHPEXPIRE key field milliseconds [VER/ABS/GT version]
> time complexity：O(1)



Command Description：


> Set the relative expiration time for a field in the TairHash specified by the key, in milliseconds. When the expiration time is up, the field will be deleted actively. If the field does not exist, return 0 directly. If VER parameter is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to force the field to set the version number, regardless of the current version number of the field, it can always be inserted successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. This command will trigger the passive elimination of the field



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> milliseconds: Expiration time in milliseconds, 0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.



Return：


> Return 1 when the field exists, and 0 when the field does not exist
> When the version verification fails, the update version is stale error will be returned



#### EXHEXPIREAT


Grammar and complexity：


> EXHEXPIREAT key field timestamp [VER/ABS/GT version]
> time complexity：O(1)



Command Description：


> Set the absolute expiration time for a field in the TairHash specified by the key, in seconds. When the expiration time expires, the field will be actively deleted. If the field does not exist, return 0 directly. If VER parameter is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to force the field to set the version number, regardless of the current version number of the field, it can always be inserted successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. This command will trigger the passive elimination of the field

Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> timestamp: Timestamp in seconds，0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.



Return：


> Return 1 when the field exists, and 0 when the field does not exist
> When the version verification fails, the update version is stale error will be returned, and the corresponding exception information will be returned for other errors.



#### EXHEXPIRE


Grammar and complexity：


> EXHEXPIRE key field seconds [VER/ABS/GT version]
> time complexity：O(1)



Command Description：


> Set the relative expiration time for a field in the TairHash specified by the key, in seconds. When the expiration time expires, the field will be actively deleted. If the field does not exist, return 0 directly. If VER parameter is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to force the field to set the version number, regardless of the current version number of the field, it can always be inserted successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. This command will trigger the passive elimination of the field



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> timestamp: Timestamp in seconds, 0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.



Return：


> Return 1 when the field exists, and 0 when the field does not exist
> When the version verification fails, the update version is stale error will be returned, and the corresponding exception information will be returned for other errors.

#### EXHPERSIST


Grammar and complexity：


> EXHEXPIRE key field
> time complexity：O(1)



Command Description：


> Set a field to never expire



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> 1：Successfully removed the expiration setting on the field
> 0：The key or field does not exist, or the field exists but does not have an expiration setting


#### EXHPTTL


Grammar and complexity:


> EXHPTTL key field
> time complexity：O(1)



Command Description：


> View the remaining expiration time of a field in TaiHash specified by key, in milliseconds


Parameter:


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> When TairHash or the field does not exist, it returns -2, when the field exists but no expiration time is set, it returns -1. When the field exists and the expiration time is set, it returns the corresponding expiration time, in milliseconds.



#### EXHTTL


Grammar and complexity：


> EXHTTL key field
> time complexity：O(1)



Command Description：


> View the remaining expiration time of a field in TaiHash specified by key, in seconds



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> When TairHash or the field does not exist, it returns -2. When the field exists but no expiration time is set, it returns -1. When the field exists and the expiration time is set, it returns the corresponding expiration time, in seconds.



#### EXHVER


Grammar and complexity：


> EXHVER key field
> time complexity：O(1)



Command Description：


> View the current version number of a field in TairHash specified by key



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> Returns -1 when TairHash does not exist, returns -2 when the field does not exist, otherwise returns the field version number


#### EXHSETVER


Grammar and complexity：


> EXHSETVER key field version
> time complexity：O(1)



Command Description：


> Set the version number of a field in TairHash specified by key



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> version: version number



Return：


> When TairHash or field does not exist, return 0, otherwise return 1




#### EXHINCRBY


Grammar and complexity：


> EXHINCRBY key field value [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS/GT version] [MIN minval] [MAX maxval] [KEEPTTL]
> time complexity：O(1)



Command Description：


> Add the integer value to the value of a field in TairHash specified by key. If TairHash does not exist, it will automatically create a new one. If the specified field does not exist, initialize the value of the field to 0 before adding it. At the same time, you can also use EX/EXAT/PX/PXAT to set the expiration time for the field
> If VER is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to forcibly set the version number for the field, regardless of the current version number of the field, it can always be set successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. MIN/MAX users provide a boundary for the field. Incr will be executed only when the value of the field is still on this boundary after this incr operation, otherwise an overflow error will be returned. This command will trigger the passive elimination check of the field



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> value: The value to be increased
> EX: The relative expiration time of the specified field, in seconds, 0 means expire immediately
> EXAT: Specify the absolute expiration time of the field, in seconds, 0 means expire immediately
> PX: The relative expiration time of the specified field, in milliseconds, 0 means expire immediately
> PXAT: Specify the absolute expiration time of the field, in milliseconds, 0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.
> MAX/MIN: Specify the boundary, incr will be executed only when the value of the field is still on this boundary after this incr operation,otherwise an overflow error will be returned
> KEEPTTL: Retain the time to live associated with the field. KEEPTTL cannot be used together with EX/EXAT/PX/PXAT


Return：


> Return the added value
> When the version verification fails, an update version is stale error is returned



#### EXHINCRBYFLOAT


Grammar and complexity：


> EXHINCRBYFLOAT key field value [EX time] [EXAT time] [PX time] [PXAT time] [VER/ABS/GT version] [MIN minval] [MAX maxval] [KEEPTTL]
> time complexity：O(1)



Command Description：


> Add the floating-point value to the value of a field in TairHash specified by key. If TairHash does not exist, it will automatically create a new one. If the specified field does not exist, initialize the value of the field to 0 before adding it. At the same time, you can also use EX/EXAT/PX/PXAT to set the expiration time for the field
> If VER is specified, the version number carried by VER must be consistent with the current version number of the field before it can be set successfully, or if the version number carried by VER parameter is 0, no version verification is performed. ABS parameter is used to forcibly set the version number for the field, regardless of the current version number of the field, it can always be set successfully. At the same time, the current version number of the field is set to the version number specified by ABS, GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0. MIN/MAX users provide a boundary for the field. Incr will be executed only when the value of the field is still on this boundary after this incr operation, otherwise an overflow error will be returned. This command will trigger the passive elimination check of the field



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> value: The value to be increased
> EX: The relative expiration time of the specified field, in seconds, 0 means expire immediately
> EXAT: Specify the absolute expiration time of the field, in seconds, 0 means expire immediately
> PX: The relative expiration time of the specified field, in milliseconds, 0 means expire immediately
> PXAT: Specify the absolute expiration time of the field, in milliseconds, 0 means expire immediately
> VER/ABS/GT: VER means that the setting is allowed only when the specified version is consistent with the current version of the field. If the version specified by VER is 0, it means that no version check will be performed. ABS means that the version number is forced to be set and modified regardless of the current version of the field., GT means that the setting is only allowed when the specified version is greater than the current version of the field, the version specified by GT and ABS cannot be 0.
> MAX/MIN: Specify the boundary, incr will be executed only when the value of the field is still on this boundary after this incr operation,otherwise an overflow error will be returned
> KEEPTTL: Retain the time to live associated with the field. KEEPTTL cannot be used together with EX/EXAT/PX/PXAT

Return：


> Return the added value
> When the version verification fails, the update version is stale error is returned, and the corresponding exception information is returned for other errors (for example, the original field value is not a floating point)



#### EXHGETWITHVER


Grammar and complexity：


> EXHGETWITHVER key field
> time complexity：O(1)



Command Description：


> At the same time, get the value and version of a field of TairHash specified by key. If TairHash does not exist or the field does not exist, return nil



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> If TairHash does not exist or the field does not exist, return nil, otherwise return the value and version corresponding to the field



#### EXHMGET


Grammar and complexity：


> EXHMGET key field [field ...]
> time complexity：O(n)



Command Description：


> At the same time get the value of multiple fields of TairHash specified by key, if TairHash does not exist or the field does not exist, return nil



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> Returns an array, each element of the array corresponds to a field, if TairHash does not exist or the field does not exist, it is nil, otherwise it is the value corresponding to the field



#### EXHMGETWITHVER


Grammar and complexity：


> EXHMGETWITHVER key field [field ...]
> time complexity：O(n)



Command Description：


> Get the value and version of multiple fields of TairHash specified by key at the same time. If TairHash does not exist or the field does not exist, return nil



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> Returns an array, each element of the array corresponds to a field, if TairHash does not exist or the field does not exist, it is nil, otherwise, the value and version corresponding to the field



#### EXHDEL


Grammar and complexity：


> EXHDEL key field [field...]
> time complexity：O(1)



Command Description：


> Delete a field of TairHash specified by key, if TairHash does not exist or the field does not exist, return 0, and return 1 if successfully deleted



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> If TairHash does not exist, return 0. How to return the number of files deleted successfully?



#### EXHLEN


Grammar and complexity：


> EXHLEN key [noexp]
> time complexity：It is O(1) if it is not a noexp option, and O(N) if it is a noexp option



Command Description：


> Get the number of fields in TairHash specified by key. By default, this command will not trigger the passive elimination of expired fields, nor will it filter them out, so the result may include fields that have expired but have not been deleted. If you only want to return the number of fields that have not expired, you can bring a noexpParameter at the end. Note that with this parameter, the RT of exhlen will be affected by the size of exhash (because it needs to be traversed), and exhlen will not trigger the field Is eliminated, it just filters out the expired fields



Parameter：


> key: The key used to find the TairHash



Return：


> If TairHash does not exist or the field does not exist, return 0, if successful deletion returns the number of fields in TairHash



#### EXHEXISTS


Grammar and complexity：


> EXHEXISTS key field
> time complexity：O(1)



Command Description：


> Query whether there is a corresponding field in TairHash specified by key



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> If TairHash does not exist or the field does not exist, return 0, if the field exists, return 1



#### EXHSTRLEN


Grammar and complexity：


> EXHSTRLEN key field
> time complexity：O(1)



Command Description：


> Get the length of the value of a field in TaiHash specified by key



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash



Return：


> If TairHash does not exist or the field does not exist, return 0, otherwise return the length of the corresponding value of the field



#### EXHKEYS


Grammar and complexity：


> EXHKEYS key
> time complexity：O(n)



Command Description：


> Get the keys of all fields in TairHash specified by key



Parameter：


> key: The key used to find the TairHash



Return：


> Return an array, each bit of the array corresponds to each field in TairHash, if TairHash does not exist, return an empty array



#### EXHVALS


Grammar and complexity：


> EXHVALS key
> time complexity：O(n)



Command Description：


> Get the value of all fields in TairHash specified by key



Parameter：


> key: The key used to find the TairHash



Return：


> Returns an array, each bit of the array corresponds to the value of each field in TairHash, if TairHash does not exist, it returns an empty array



#### EXHGETALL


Grammar and complexity：


> EXHGETALL key
> time complexity：O(n)



Command Description：


> Get the key-value pairs of all fields in TairHash specified by key



Parameter：


> key: The key used to find the TairHash



Return：


> Returns an array, each bit of the array corresponds to the key-value pair of each field in TairHash, if TairHash does not exist, it returns an empty array

```sh
127.0.0.1:6379> EXHSET k0 f1 1 ABS 2
(integer) 1
127.0.0.1:6379> EXHSET k0 f2 2 ABS 1
(integer) 1
127.0.0.1:6379> EXHGETALL k0
1) "f2"
2) "2"
3) "f1"
4) "1"
```

#### EXHGETALLWITHVER


Grammar and complexity：


> EXHGETALLWITHVER key
> time complexity：O(n)



Command Description：


> Get the key-value-version tuples of all fields in TairHash specified by key



Parameter：


> key: The key used to find the TairHash



Return：


> Returns an array, each bit of the array corresponds to the key-value-version tuple of each field in TairHash, if TairHash does not exist, it returns an empty array

```sh
127.0.0.1:6379> EXHSET k0 f1 1 ABS 2
(integer) 1
127.0.0.1:6379> EXHSET k0 f2 2 ABS 1
(integer) 1
127.0.0.1:6379> EXHGETALLWITHVER k0
1) "f2"
2) "2"
3) (integer) 1
4) "f1"
5) "1"
6) (integer) 2
```

#### EXHSCAN


Grammar and complexity：


> EXHSCAN key cursor [MATCH pattern] [COUNT count]
> time complexity：O(1)、O(N)



Command Description：


> Scan the TairHash specified by the key


Parameter：


> key: The key used to find the TairHash
> cursor: Scan cursor, starting from 0, after each scan, it will return to the next scan cursor, until it returns 0 to indicate the end of the scan
> MATCH: Rules for filtering scan results
> COUNT: It is used to specify the number of fields in a single scan. Note that COUNT only represents the number of feilds of TairHash scanned each time. It does not mean that COUNT field result sets will be returned in the end. The size of the result set depends on the current fields in TaiHash. The number and whether to specify MATCH for filtering depends. The default value of COUNT is 10



Return：


> Returns an array with two elements. The first element of the array is the cursor to be used in the next scan, and 0 means the end of the entire scan. The second array element is still an array, and the array contains all the field/values that are iterated this time. If an empty TairHash is found or TairHash does not exist, then both array elements are empty.



**example：**
1、How to progressively scan the entire TaiHash:
```
127.0.0.1:6379> exhmset exhashkey field1 val1 field2 val2 field3 val3 field4 val4 field5 val5 field6 val6 field7 val7 field8 val8 field9 val9
OK
127.0.0.1:6379> exhscan exhashkey 0 COUNT 3
1) (integer) 4
2) 1) "field6"
   1) "val6"
   2) "field5"
   3) "val5"
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

2、How to use MATCH to filter the result set
Exact match：


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


Fuzzy matching：

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
