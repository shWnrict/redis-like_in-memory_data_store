#### Core Operations: Basic key-value operations

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| PING | PING | PONG |
| FLUSHDB | FLUSHDB | OK |
| SET | SET mykey "Hello" | OK |
| GET | GET mykey | "Hello" |
| DEL | DEL mykey | (integer) 1 |
| EXISTS | EXISTS mykey | (integer) 1 |
| EXPIRE | EXPIRE mykey 60 | (integer) 1 |
| TTL | TTL mykey | (integer) 60 |
| PERSIST | PERSIST mykey | (integer) 1 |

#### Transaction Operations: Atomic operation groups

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| MULTI | MULTI | OK |
| EXEC | EXEC | [OK, OK] |
| DISCARD | DISCARD | OK |

#### Publish/Subscribe

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| PUBLISH | PUBLISH channel "Hello, Redis!" | (integer) 1 |
| SUBSCRIBE | SUBSCRIBE channel | Waiting for messages... |

#### String Operations: String manipulation and atomic counters

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| APPEND | APPEND mykey " World" | (integer) 11 |
| STRLEN | STRLEN mykey | (integer) 11 |
| INCR | INCR counter | (integer) 1 |
| DECR | DECR counter | (integer) -1 |
| INCRBY | INCRBY counter 5 | (integer) 4 |
| DECRBY | DECRBY counter 2 | (integer) 2 |
| GETRANGE | GETRANGE mykey 0 4 | "Hello" |
| SETRANGE | SETRANGE mykey 6 "Redis" | (integer) 11 |

#### List Operations: Queue and stack operations

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| LPUSH | LPUSH mylist "One" | (integer) 1 |
| RPUSH | RPUSH mylist "Two" | (integer) 2 |
| LPOP | LPOP mylist | "One" |
| RPOP | RPOP mylist | "Two" |
| LRANGE | LRANGE mylist 0 1 | 1) "One" 2) "Two" |
| LINDEX | LINDEX mylist 0 | "One" |
| LSET | LSET mylist 0 "Updated" | OK |

#### Set Operations: Unique element collections

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| SADD | SADD myset "Apple" | (integer) 1 |
| SREM | SREM myset "Apple" | (integer) 1 |
| SISMEMBER | SISMEMBER myset "Apple" | (integer) 0 |
| SMEMBERS | SMEMBERS myset | 1) "Apple" |
| SINTER | SINTER set1 set2 | 1) "CommonValue" |
| SUNION | SUNION set1 set2 | 1) "Apple" 2) "Banana" |
| SDIFF | SDIFF set1 set2 | 1) "Orange" |

#### Hash Operations: Field-value pair storage

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| HSET | HSET myhash field1 "value1" | (integer) 1 |
| HGET | HGET myhash field1 | "value1" |
| HMSET | HMSET myhash field1 "value1" field2 "value2" | OK |
| HGETALL | HGETALL myhash | 1) "field1" 2) "value1" 3) "field2" 4) "value2" |
| HDEL | HDEL myhash field1 | (integer) 1 |
| HEXISTS | HEXISTS myhash field1 | (integer) 0 |

#### Sorted Set Operations: Scored member management

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| ZADD | ZADD myzset 1 "one" 2 "two" | (integer) 2 |
| ZRANGE | ZRANGE myzset 0 -1 | 1) "one" 2) "two" |
| ZRANK | ZRANK myzset "two" | (integer) 1 |
| ZREM | ZREM myzset "one" | (integer) 1 |
| ZRANGEBYSCORE | ZRANGEBYSCORE myzset 1 2 | 1) "two" |

#### JSON document storage

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| JSON.SET | JSON.SET myjson . '{"name": "Redis", "type": "Database"}' | OK |
| JSON.GET | JSON.GET myjson . | {"name": "Redis", "type": "Database"} |
| JSON.DEL | JSON.DEL myjson .name | (integer) 1 |
| JSON.ARRAPPEND | JSON.ARRAPPEND myjson .tags "fast" "scalable" | (integer) 3 |

#### Stream Operations: Append-only log structures

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| XADD | XADD mystream * field1 value1 | "1683552795445-0" |
| XREAD | XREAD STREAMS mystream 0 | 1) 1) "mystream" 2) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XRANGE | XRANGE mystream - + | 1) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XLEN | XLEN mystream | (integer) 1 |
| XGROUP | XGROUP CREATE mystream mygroup $ | OK |
| XREADGROUP | XREADGROUP GROUP mygroup consumer mystream > | 1) 1) "mystream" 2) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XACK | XACK mystream mygroup 1683552795445-0 | (integer) 1 |

#### Geospatial Operations: Location-based features

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| GEOADD | GEOADD mygeoset 13.361389 38.115556 "Palermo" | (integer) 1 |
| GEOSEARCH | GEOSEARCH mygeoset FROMLONLAT 13.361389 38.115556 BYRADIUS 200 km | 1) "Palermo" |
| GEODIST | GEODIST mygeoset "Palermo" "Catania" km | "166.2741" |

#### Bitmap & Bitfield Operations: Bit-level manipulations

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| SETBIT | SETBIT mybitkey 7 1 | OK |
| GETBIT | GETBIT mybitkey 7 | (integer) 1 |
| BITCOUNT | BITCOUNT mybitkey | (integer) 1 |
| BITOP | BITOP AND destkey mybitkey1 mybitkey2 | (integer) 5 |
| BITFIELD | BITFIELD mybitkey GET i8 0 | (array) [ 5 ] |

#### Probabilistic Operations: Bloom filters and HyperLogLog

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| BF.RESERVE | BF.RESERVE mybloomfilter 0.01 1000 | OK |
| BF.ADD | BF.ADD mybloomfilter "item1" | (integer) 1 |
| BF.EXISTS | BF.EXISTS mybloomfilter "item1" | (integer) 1 |
| PFADD | PFADD myhyperloglog "element1" | (integer) 1 |
| PFCOUNT | PFCOUNT myhyperloglog | (integer) 1 |
| PFMERGE | PFMERGE mymerged myhyperloglog1 myhyperloglog2 | OK |

#### Time Series Operations: Time-based data management

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| TS.CREATE | TS.CREATE mytimeseries | OK |
| TS.ADD | TS.ADD mytimeseries 1609459200 42.0 | OK |
| TS.GET | TS.GET mytimeseries | 1) 1609459200 2) "42.0" |
| TS.RANGE | TS.RANGE mytimeseries 1609459200 1609545600 | 1) 1609459200 2) "42.0" |

#### Input command
##### String operation example
```shell
SET mykey "Hello"
GET mykey
APPEND mykey " World"
STRLEN mykey
SET mycounter 0
INCR mycounter
DECR mycounter
INCRBY mycounter 5
DECRBY mycounter 3
GETRANGE mykey 0 4
SETRANGE mykey 6 "Redis"
GET mykey
```
Sure! Below is the markdown that combines the input commands, examples, and shell code for all the Redis commands you listed, organized by command categories.

```markdown
#### Core Operations: Basic key-value operations
##### String operation example
```shell
SET mykey "Hello"
GET mykey
DEL mykey
EXISTS mykey
EXPIRE mykey 60
TTL mykey
PERSIST mykey
```

#### Transaction Operations: Atomic operation groups
##### Transaction operation example
```shell
MULTI
SET mykey "Hello"
INCR mycounter
EXEC
DISCARD
```

#### Publish/Subscribe:
##### Publish/Subscribe operation example
```shell
PUBLISH channel "Hello, Redis!"
SUBSCRIBE channel
```

#### String Operations: String manipulation and atomic counters
##### String operation example
```shell
SET mykey "Hello"
GET mykey
APPEND mykey " World"
STRLEN mykey
SET mycounter 0
INCR mycounter
DECR mycounter
INCRBY mycounter 5
DECRBY mycounter 3
GETRANGE mykey 0 4
SETRANGE mykey 6 "Redis"
GET mykey
```

#### List Operations: Queue and stack operations
##### List operation example
```shell
LPUSH mylist "One"
RPUSH mylist "Two"
LPOP mylist
RPOP mylist
LRANGE mylist 0 -1
LINDEX mylist 0
LSET mylist 0 "Updated"
```

#### Set Operations: Unique element collections
##### Set operation example
```shell
SADD myset "Apple"
SREM myset "Apple"
SISMEMBER myset "Apple"
SMEMBERS myset
SINTER set1 set2
SUNION set1 set2
SDIFF set1 set2
```

#### Hash Operations: Field-value pair storage
##### Hash operation example
```shell
HSET myhash field1 "value1"
HGET myhash field1
HMSET myhash field1 "value1" field2 "value2"
HGETALL myhash
HDEL myhash field1
HEXISTS myhash field1
```

#### Sorted Set Operations: Scored member management
##### Sorted Set operation example
```shell
ZADD myzset 1 "one" 2 "two"
ZRANGE myzset 0 -1
ZRANK myzset "two"
ZREM myzset "one"
ZRANGEBYSCORE myzset 1 2
```

#### JSON document storage
##### JSON operation example
```shell
JSON.SET myjson . '{"name": "Redis", "type": "Database"}'
JSON.GET myjson .
JSON.DEL myjson .name
JSON.ARRAPPEND myjson .tags "fast" "scalable"
```

#### Stream Operations: Append-only log structures
##### Stream operation example
```shell
XADD mystream * field1 value1
XREAD STREAMS mystream 0
XRANGE mystream - +
XLEN mystream
XGROUP CREATE mystream mygroup $
XREADGROUP GROUP mygroup consumer mystream >
XACK mystream mygroup 1683552795445-0
```

#### Geospatial Operations: Location-based features
##### Geospatial operation example
```shell
GEOADD mygeoset 13.361389 38.115556 "Palermo"
GEOSEARCH mygeoset FROMLONLAT 13.361389 38.115556 BYRADIUS 200 km
GEODIST mygeoset "Palermo" "Catania" km
```

#### Bitmap & Bitfield Operations: Bit-level manipulations
##### Bitmap operation example
```shell
SETBIT mybitkey 7 1
GETBIT mybitkey 7
BITCOUNT mybitkey
BITOP AND destkey mybitkey1 mybitkey2
BITFIELD mybitkey GET i8 0
```

#### Probabilistic Operations: Bloom filters and HyperLogLog
##### Probabilistic operation example
```shell
BF.RESERVE mybloomfilter 0.01 1000
BF.ADD mybloomfilter "item1"
BF.EXISTS mybloomfilter "item1"
PFADD myhyperloglog "element1"
PFCOUNT myhyperloglog
PFMERGE mymerged myhyperloglog1 myhyperloglog2
```

#### Time Series Operations: Time-based data management
##### Time Series operation example
```shell
TS.CREATE mytimeseries
TS.ADD mytimeseries 1609459200 42.0
TS.GET mytimeseries
TS.RANGE mytimeseries 1609459200 1609545600
```
```