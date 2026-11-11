
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

## Last-write-wins and source-of-truth sync

These commands keep a TairHash as a cache of an external source of truth (for example a
database table holding the latest value per field) without ever serving a partial or stale
copy as complete:

1. Writers apply each change with `EXHMSETLWW` and each delete with `EXHDELLWW`, using the
   source's write timestamp as the version. A write that loses to a newer one is an expected
   outcome, not an error.
2. Readers call `EXHSYNCGET`. A hash that is not synced (new, evicted, or invalidated by a
   restart or failover, see below) is a miss: the reader calls `EXHSYNCBEGIN`, then reads the
   full set of fields from the source, and hands it to `EXHSYNC` together with the token
   `EXHSYNCBEGIN` returned.
3. `EXHSYNC` merges the snapshot, removes fields the source no longer has, and marks the
   hash synced. Fields written after the token raced the snapshot read and are kept,
   whatever their version.

What keeps a synced hash correct:

- **Tombstones.** `EXHDELLWW` leaves a tombstone holding the delete's version for
  `lww_tombstone_ttl` seconds (default 3600). A write or snapshot entry with the same or a
  lower version loses to it, so a snapshot read before a delete cannot bring the field back.
  A hash left with only tombstones gets a key TTL ending with its last tombstone. A
  tombstone whose time has passed holds nothing off any more, but it stays in memory until a
  later `EXHMSETLWW`, `EXHDELLWW` or `EXHSYNC` of the hash drops it (a replica drops its own
  when it applies writes to the hash) or the hash is loaded from an RDB again: a hash that
  takes no further write keeps its expired tombstones. They are left out of an RDB, an AOF
  rewrite and a full sync. A write by another command (`EXHSET`, `EXHSETNX`, `EXHMSET`,
  `EXHMSETWITHOPTS`, `EXHINCRBY`, `EXHINCRBYFLOAT`) to a field that has a tombstone removes
  the tombstone and continues from the delete's version as it would from a field's: the
  field gets that version plus one, so the writes the tombstone held off still lose. `GT`
  compares its version with the tombstone's; `ABS` sets the version it is given and so
  overrides the tombstone.
- **Leases.** `EXHSYNC` succeeds only while the hash holds the lease `EXHSYNCBEGIN` opened
  with that token: not after the key was deleted, evicted or recreated, after another sync
  completed, after the sync epoch changed (see below), or after `sync_lease_ttl` seconds
  (default 60). The caller then starts over with `EXHSYNCBEGIN`. Concurrent syncs of a hash
  share one lease; the first `EXHSYNC` wins. For a missing key, `EXHSYNCBEGIN` creates an
  empty placeholder with a TTL of `sync_lease_ttl`, and a TairHash without fields that is
  not synced gets that TTL too unless the key has one of its own, so an abandoned sync
  leaves nothing behind. A TairHash that was synced without fields and lost its sync when
  the epoch changed becomes such a placeholder only when `EXHSYNCBEGIN` is called for it,
  and is deleted by an `EXHSYNCGET` that misses it: one that is never read or synced again
  stays as an empty key, unless the key has a TTL of its own.
- **Sync epochs.** A hash is synced only in the epoch `EXHSYNC` marked it in. A master starts
  a new epoch when the module loads, after it loads an RDB or AOF (a restart, `DEBUG
  RELOAD`) and when it is promoted, since each can lose writes the synced hashes reflected;
  synced hashes then read as misses until they are synced again. Their data, tombstones and
  write sequences are kept. The master puts the new epoch in the replication stream, after
  everything it replicated under the previous one, so a replica has its master's epoch as of
  the point of the stream it has applied: it reads a hash as synced exactly when its master
  did at that point, whether it follows the stream, replays it from the backlog after a
  partial resync or replicates from another replica. A full sync, and the RDB a replica
  restarts from, carry the epoch as well. A master that becomes a replica has no epoch, and
  reads every hash as a miss, until it has loaded or applied its new master's.

A TTL the module set (a placeholder's, or that of a hash with only tombstones) lasts only
while the hash has no fields: the first command that adds a field, and `EXHSYNC`, remove it.
A TTL the key had of its own is left alone. Set a cache TTL after `EXHSYNC`, not during a
sync, as the sync may remove it.

The sync state belongs to the hash, whatever its key is called. `RENAME` and `MOVE` move a
TairHash with its sync state, its tombstones and a sync that is open, whose token is then
accepted under the new name. `COPY` makes a new TairHash that has the fields but is not
synced and has no tombstones or open sync: writes keep going to the original only. `RESTORE`
does not make a hash synced either, since a payload can be older than writes the hash has
taken since it was dumped; the tombstones in the payload are restored, so a migrated key keeps
holding off late writes.

`EXHSYNCBEGIN`, `EXHSYNC` and `EXHSYNCGET` need the server events of Redis 6.0 or later (or
Valkey) to renew the epoch, and are not registered on older servers. `EXHMSETLWW` and
`EXHDELLWW` work everywhere.

Every value write (by any command) is stamped with a server-assigned write sequence: the
larger of the wall clock in microseconds and the previous sequence plus one. It is strictly
increasing, is replicated and persisted, and stays above sequences handed out by an earlier
process or a failed-over master as long as the nodes' clocks agree better than the time a
restart or failover takes. Sync tokens and epochs are drawn from the same sequence.

#### EXHMSETLWW


Grammar and complexity：


> EXHMSETLWW key field value version [field value version ...]
> time complexity：O(N), N is the number of fields



Command Description：


> Apply last-write-wins writes to the fields of the TairHash specified by key, creating it if it does not exist. A write lands when the field does not exist and has no tombstone with the same or a higher version (see EXHDELLWW), when its version is higher than the field's, or when the versions are equal and its value is byte-wise greater (shorter values sort first when one is a prefix of the other). The tie-break is the one Cassandra/Scylla use for equal write timestamps, so the result does not depend on the order writes arrive in. A landed write replaces the value and version, clears the field's TTL and drops its tombstone. Fields are applied in argument order.



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> value: The value to write
> version: The version of the write, a positive integer (for example the source's write timestamp in microseconds)



Return：


> An array with one integer per field: 1 if the write landed, 0 if the field already held a winning write or was deleted at the same or a higher version
> Error: syntax error if a version is not a positive integer (nothing is written); WRONGTYPE if the key holds another type

```sh
127.0.0.1:6379> EXHMSETLWW k0 f1 21.5 1731300000000001 f2 7 1731300000000001
1) (integer) 1
2) (integer) 1
127.0.0.1:6379> EXHMSETLWW k0 f1 20.9 1731299999000000 f2 8 1731300000000001
1) (integer) 0
2) (integer) 1
127.0.0.1:6379> EXHGETWITHVER k0 f2
1) "8"
2) (integer) 1731300000000001
```

#### EXHDELLWW


Grammar and complexity：


> EXHDELLWW key field version [field version ...]
> time complexity：O(N), N is the number of fields



Command Description：


> Apply last-write-wins deletes to the fields of the TairHash specified by key. A delete removes the field unless it holds a write with a higher version (a delete wins a tie), and leaves a tombstone with the delete's version that writes and EXHSYNC snapshot entries with the same or a lower version lose to, until `lww_tombstone_ttl` seconds have passed. The key is created if it does not exist, so that a delete that arrives before the write it supersedes still holds it off; a TairHash left with only tombstones gets a key TTL ending with its last tombstone, unless the key has a TTL of its own, and is kept until then. Fields are applied in argument order.



Parameter：


> key: The key used to find the TairHash
> field: An element in TairHash
> version: The version of the delete, a positive integer



Return：


> An array with one integer per field: 1 if the field is deleted at that version (by this delete, or already by one with the same or a higher version), 0 if a newer write keeps it
> Error: syntax error if a version is not a positive integer (nothing is deleted); WRONGTYPE if the key holds another type

```sh
127.0.0.1:6379> EXHMSETLWW k3 f1 a 10 f2 b 20
1) (integer) 1
2) (integer) 1
127.0.0.1:6379> EXHDELLWW k3 f1 11 f2 15
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> EXHMSETLWW k3 f1 late 11
1) (integer) 0
```

#### EXHSYNCGET


Grammar and complexity：


> EXHSYNCGET key [field ...]
> time complexity：O(N), N is the number of fields in the TairHash, or of the fields given



Command Description：


> Read the TairHash specified by key if it is synced in the current epoch (see EXHSYNC). A TairHash that does not exist or is not synced is a miss: no fields are returned, and the caller syncs it with EXHSYNCBEGIN and EXHSYNC. With fields given, only those that exist are returned, in request order; on a synced TairHash a missing field means the source has no value for it. Expired fields are not returned. Like the other reads, it changes the TairHash only to remove expired fields, so it does not abort a WATCH unless a field expired. On a miss it also deletes a TairHash that is left with nothing: no fields, tombstones or open sync, as one that was synced empty in an earlier epoch.



Parameter：


> key: The key used to find the TairHash
> field: Fields to return (default: all fields)



Return：


> An array of two elements: 1 if the TairHash is synced and 0 otherwise, and an array of field, value, version triples (empty on a miss)
> Error: WRONGTYPE if the key holds another type

```sh
127.0.0.1:6379> EXHSYNCGET k1
1) (integer) 0
2) (empty array)
127.0.0.1:6379> EXHSYNCBEGIN k1
(integer) 1794387046202298
127.0.0.1:6379> EXHSYNC k1 1794387046202298 f1 21.5 1731300000000001
1) (integer) 1
2) (integer) 0
127.0.0.1:6379> EXHSYNCGET k1 f1 missing
1) (integer) 1
2) 1) "f1"
   2) "21.5"
   3) (integer) 1731300000000001
```

#### EXHSYNCBEGIN


Grammar and complexity：


> EXHSYNCBEGIN key
> time complexity：O(1)



Command Description：


> Open a sync of the TairHash specified by key with the source of truth, and return its token. Read the source after this call and pass the snapshot and the token to EXHSYNC. Every write after this call sorts above the token. The lease lasts `sync_lease_ttl` seconds, until an EXHSYNC with the token completes or until the key goes away; a call while a lease is open returns the same token. A missing key is created as an empty, unsynced placeholder with a TTL of `sync_lease_ttl`. A TairHash that has no fields and is not synced (one that was synced empty in an earlier epoch, or holds only tombstones) is treated as one: it gets that TTL if the key has none, and a TTL the module set is extended to at least that. A TTL the key has of its own is left alone. The lease is neither persisted nor replicated: a sync that spans a restart or a failover fails and starts over.



Parameter：


> key: The key used to find the TairHash



Return：


> The sync token, a positive integer
> Error: WRONGTYPE if the key holds another type

#### EXHSYNC


Grammar and complexity：


> EXHSYNC key token [field value version ...]
> time complexity：O(N + M + T), N is the number of fields in the snapshot, M the number of fields in the TairHash, T the number of its tombstones



Command Description：


> Complete the sync EXHSYNCBEGIN opened with token, using a full snapshot of the source of truth read after that call. Snapshot fields are merged with EXHMSETLWW semantics, so fields deleted at the same or a higher version stay deleted. Fields not in the snapshot are removed unless they were written after token. The TairHash is then marked synced in the current epoch, the lease is closed, and a TTL the module set is removed. A synced TairHash is kept even when it has no fields, so an empty snapshot caches "the source has nothing for this key". It stays synced until the key is deleted or the epoch changes. Fails without changing anything unless the TairHash holds the lease of token.



Parameter：


> key: The key used to find the TairHash
> token: The token EXHSYNCBEGIN returned before the snapshot was read
> field value version: The snapshot; versions are positive integers



Return：


> An array of two integers: the number of snapshot fields whose write landed, and the number of fields removed
> Error: syntax error if the token or a version is not a positive integer, and `ERR no sync lease for this token: call EXHSYNCBEGIN again` if the TairHash does not hold the lease of token (nothing is changed in either case); WRONGTYPE if the key holds another type

```sh
127.0.0.1:6379> EXHSYNCBEGIN k2
(integer) 1794387046210665
127.0.0.1:6379> EXHSYNC k2 1794387046210665
1) (integer) 0
2) (integer) 0
127.0.0.1:6379> EXHSYNCGET k2
1) (integer) 1
2) (empty array)
127.0.0.1:6379> EXHSYNC k2 1794387046210665
(error) ERR no sync lease for this token: call EXHSYNCBEGIN again
```

<br/>

## Replication and persistence

Write commands replicate the resulting state of each field they wrote rather than the
request, so a replica or an AOF replay reproduces values, versions and TTLs exactly,
independently of its own clock and state. The `replication_format` module option chooses
the commands:

- `state` (the default): one internal
  `EXHMSETREPL key 2 field value version expire seq [field value version expire seq ...]`
  per write command, with the expire as an absolute unix time in milliseconds, 0 for none,
  and the field's write sequence. A write of more than 8192 fields is split into several,
  propagated together in a MULTI/EXEC. A write that leaves the values alone (the `EXHEXPIRE`
  family, `EXHPERSIST`, `EXHSETVER`) sends `EXHMETAREPL key 1 field version expire [...]`
  instead, without the value. The number after the key names the argument layout; a node
  rejects a layout it does not know and logs the write it drops, and accepts `EXHMSETREPL`
  layout 1 (TairHash 2.0, without the write sequence) by assigning sequences itself.
  An `EXHMETAREPL` for a field the node does not have means that the node has diverged from
  its master: it updates the fields it has, logs the others and replies an error.
  The sync state of a hash (its epoch, 0 when not synced, and whether the module owns the
  key's TTL) is sent as `EXHSYNCREPL key epoch owns_ttl`, followed by the `PEXPIREAT` or
  `PERSIST` of such a TTL, and `EXHDELLWW` sends
  `EXHTOMBREPL key 1 field version deadline [...]` with the tombstone deadlines it set. AOF
  rewrite emits these too, and `EXHMSETREPL` with up to 64 fields each. A master that starts
  a new sync epoch sends `EXHSYNCEPOCH epoch`, which a replica takes as its own epoch. It
  does so once a hash has been synced on it, and before the first one otherwise, so a master
  whose sync commands are not used sends nothing an older TairHash does not know. The
  current sync epoch is also saved in the RDB as module aux data, so a TairHash 2.1 RDB needs
  the module to load even without TairHash keys. These commands are
  accepted only from the replication stream or while loading; a regular client gets
  `ERR only accepted from the replication stream or while loading` (a server that cannot
  report which context flags it supports, because it lacks `RedisModule_GetContextFlagsAll`,
  which Redis has from 6.0.9 on, always accepts them). So a state-format AOF or replication
  stream can be applied only by loading it or by a replica, not replayed through a client
  such as `redis-cli --pipe`; use `compat` where that is needed.
- `compat`: one `EXHSET key field value ABS <version> [PXAT <expire>]` per field, or
  `EXHSETNX key field value` for a field at version 0, which `EXHSET` cannot set (only data
  written by TairHash 1.x has it). From the replication stream or the AOF, `EXHSETNX` leaves
  the new field at version 0; sent by a client, it starts the field at version 1. The fields
  of one write command are propagated together in a MULTI/EXEC. TairHash 1.x accepts these
  commands, which is what an upgrade relies on; see the README. They carry no write sequence:
  a replica or an AOF replay assigns its own, later ones, so `EXHSYNC` with a token from
  before keeps such fields rather than removing them. `EXHMSETLWW`, `EXHDELLWW` and
  `EXHSYNC`, which no version without them can apply, always replicate in the state format.
