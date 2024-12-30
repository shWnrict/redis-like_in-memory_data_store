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

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| MULTI | MULTI | OK |
| EXEC | EXEC | [OK, OK] |
| DISCARD | DISCARD | OK |

```shell
MULTI
SET mykey "Hello"
INCR mycounter
EXEC
DISCARD
```

#### Publish/Subscribe

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| PUBLISH | PUBLISH channel "Hello, Redis!" | (integer) 1 |
| SUBSCRIBE | SUBSCRIBE channel | Waiting for messages... |

```shell
PUBLISH channel "Hello, Redis!"
SUBSCRIBE channel
```

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

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| LPUSH | LPUSH mylist "One" | (integer) 1 |
| RPUSH | RPUSH mylist "Two" | (integer) 2 |
| LPOP | LPOP mylist | "One" |
| RPOP | RPOP mylist | "Two" |
| LRANGE | LRANGE mylist 0 1 | 1) "One" 2) "Two" |
| LINDEX | LINDEX mylist 0 | "One" |
| LSET | LSET mylist 0 "Updated" | OK |

```shell
LPUSH mylist "one"
LPUSH mylist "two"
RPUSH mylist "three"
RPUSH mylist "four"
LPOP mylist
RPOP mylist
LRANGE mylist 0 -1
LINDEX mylist 1
LSET mylist 1 "new_value"

```

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

```shell
SADD myset "one"
SADD myset "two"
SADD myset "three"
SREM myset "two"
SISMEMBER myset "one"
SISMEMBER myset "four"
SMEMBERS myset
SADD myset2 "three"
SADD myset2 "four"
SINTER myset myset2
SUNION myset myset2
SDIFF myset myset2

```

#### Hash Operations: Field-value pair storage

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| HSET | HSET myhash field1 "value1" | (integer) 1 |
| HGET | HGET myhash field1 | "value1" |
| HMSET | HMSET myhash field1 "value1" field2 "value2" | OK |
| HGETALL | HGETALL myhash | 1) "field1" 2) "value1" 3) "field2" 4) "value2" |
| HDEL | HDEL myhash field1 | (integer) 1 |
| HEXISTS | HEXISTS myhash field1 | (integer) 0 |

```shell
HSET myhash field1 "value1"
HGET myhash field1
HMSET myhash field1 "value1" field2 "value2"
HGETALL myhash
HDEL myhash field1
HEXISTS myhash field1
```

#### Sorted Set Operations: Scored member management

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| ZADD | ZADD myzset 1 "one" 2 "two" | (integer) 2 |
| ZRANGE | ZRANGE myzset 0 -1 | 1) "one" 2) "two" |
| ZRANK | ZRANK myzset "two" | (integer) 1 |
| ZREM | ZREM myzset "one" | (integer) 1 |
| ZRANGEBYSCORE | ZRANGEBYSCORE myzset 1 2 | 1) "two" |

```shell
ZADD myzset 1 "one" 2 "two"
ZRANGE myzset 0 -1
ZRANK myzset "two"
ZREM myzset "one"
ZRANGEBYSCORE myzset 1 2
```

#### JSON document storage
**Examples**
Replace an existing value
```shell
JSON.SET doc $ '{"a":2}'
JSON.SET doc $.a '3'
JSON.GET doc $
```

Add a new value
```shell
JSON.SET doc $ '{"a":2}'
JSON.SET doc $.b '8'
JSON.GET doc $
```

Update multi-paths
```shell
JSON.SET doc $ '{"f1": {"a":1}, "f2":{"a":2}}'
JSON.SET doc $..a 3
JSON.GET doc
```

Delete specified values
```shell
redis> JSON.SET doc $ '{"a": 1, "nested": {"a": 2, "b": 3}}'
redis> JSON.DEL doc $..a
redis> JSON.GET doc $
```

Use case: Add a new color to a list of product colors
```shell
JSON.SET item:1 $ '{"name":"Noise-cancelling Bluetooth headphones","description":"Wireless Bluetooth headphones with noise-cancelling technology","connection":{"wireless":true,"type":"Bluetooth"},"price":99.98,"stock":25,"colors":["black","silver"]}'
JSON.ARRAPPEND item:1 $.colors '"blue"'
JSON.GET item:1
```

#### Stream Operations: Append-only log structures

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| XADD | XADD mystream * field1 value1 | "1683552795445-0" |
| XREAD | XREAD STREAMS mystream 0 | 1) 1) "mystream" 2) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XRANGE | XRANGE mystream - + | 1) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XLEN | XLEN mystream | (integer) 1 |

```shell
XADD mystream * name Sara surname OConnor
XADD mystream * field1 value1 field2 value2 field3 value3
XLEN mystream
XRANGE mystream - +
```

#### Geospatial Operations: Location-based features
https://redis.io/docs/latest/commands/geoadd/
https://redis.io/docs/latest/commands/geosearch/
https://redis.io/docs/latest/commands/geodist/

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| GEOADD | GEOADD mygeoset 13.361389 38.115556 "Palermo" | (integer) 1 |
| GEOSEARCH | GEOSEARCH mygeoset FROMLONLAT 13.361389 38.115556 BYRADIUS 200 km | 1) "Palermo" |
| GEODIST | GEODIST mygeoset "Palermo" "Catania" km | "166.2741" |

```shell
GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
GEODIST Sicily Palermo Catania
GEORADIUS Sicily 15 37 100 km
GEORADIUS Sicily 15 37 200 km
```

#### Bitmap & Bitfield Operations: Bit-level manipulations

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| SETBIT | SETBIT mybitkey 7 1 | OK |
| GETBIT | GETBIT mybitkey 7 | (integer) 1 |
| BITCOUNT | BITCOUNT mybitkey | (integer) 1 |
| BITOP | BITOP AND destkey mybitkey1 mybitkey2 | (integer) 5 |
| BITFIELD | BITFIELD mybitkey GET i8 0 | (array) [ 5 ] |

```shell
SETBIT mybitkey 7 1
GETBIT mybitkey 7
BITCOUNT mybitkey
BITOP AND destkey mybitkey1 mybitkey2
BITFIELD mybitkey GET i8 0
```

#### Probabilistic Operations: Bloom filters and HyperLogLog

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| BF.RESERVE | BF.RESERVE mybloomfilter 0.01 1000 | OK |
| BF.ADD | BF.ADD mybloomfilter "item1" | (integer) 1 |
| BF.EXISTS | BF.EXISTS mybloomfilter "item1" | (integer) 1 |
| PFADD | PFADD myhyperloglog "element1" | (integer) 1 |
| PFCOUNT | PFCOUNT myhyperloglog | (integer) 1 |
| PFMERGE | PFMERGE mymerged myhyperloglog1 myhyperloglog2 | OK |

```shell
BF.RESERVE mybloomfilter 0.01 1000
BF.ADD mybloomfilter "item1"
BF.EXISTS mybloomfilter "item1"
PFADD myhyperloglog "element1"
PFCOUNT myhyperloglog
PFMERGE mymerged myhyperloglog1 myhyperloglog2
```

#### Time Series Operations: Time-based data management

| Command | Sample Input | Expected Output |
|---------|--------------|-----------------|
| TS.CREATE | TS.CREATE mytimeseries | OK |
| TS.ADD | TS.ADD mytimeseries 1609459200 42.0 | OK |
| TS.GET | TS.GET mytimeseries | 1) 1609459200 2) "42.0" |
| TS.RANGE | TS.RANGE mytimeseries 1609459200 1609545600 | 1) 1609459200 2) "42.0" |

```shell
TS.CREATE mytimeseries
TS.ADD mytimeseries 1609459200 42.0
TS.GET mytimeseries
TS.RANGE mytimeseries 1609459200 1609545600
```