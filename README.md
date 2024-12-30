# Redis-like In-Memory Data Store

## Overview
A custom implementation of a redis-like in-memory data store using Python. This project provides a simple and efficient way to store and manage data in memory with support for basic Redis-like commands and data structures.

## Key Features
- **Core Key-Value Store**
- **Key Expiration**
- **Transactions**
- **Comprehensive Data Types**
    - Strings
    - Lists
    - Sets
    - Hashes
    - Sorted Sets
    - JSON
    - Streams
    - Geospatial
    - Bitmaps
    - HyperLogLog
    - Time-Series data
- **Pub/Sub Mechanism**
- **Data Persistence support** (AOF, Snapshots)

#### Data Persistence
##### Append Only File (AOF)
- All write operations are logged
- Recovery on restart

##### Snapshots
- Point-in-time snapshots of the dataset (300 seconds)

## Setup Instructions

### Prerequisites
- Python 3:  https://www.python.org/downloads/
- Redis-cli: https://github.com/microsoftarchive/redis/releases (for Windows)

### Installation
1. Clone the repository:
```bash
git clone https://github.com/shWnrict/redis-like_in-memory_data_store.git
cd redis-like_in-memory_data_store
```

2. Create a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Quick Start
1. Start the Server
```bash
python -m src.main
```
2. In another terminal, use redis-cli to connect
```bash
redis-cli
```

### Supported Commands and Examples

#### Core Operations: Basic key-value operations

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| PING | Test connection | PING | PONG |
| FLUSHDB | Clear database | FLUSHDB | OK |
| SET | Set key to hold string value | SET mykey "Hello" | OK |
| GET | Get value of key | GET mykey | "Hello" |
| DEL | Delete a key | DEL mykey | (integer) 1 |
| EXISTS | Determine if key exists | EXISTS mykey | (integer) 1 |
| EXPIRE | Set key timeout | EXPIRE mykey 60 | (integer) 1 |
| TTL | Get key timeout | TTL mykey | (integer) 60 |
| PERSIST | Remove timeout | PERSIST mykey | (integer) 1 |

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

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| MULTI | Start transaction | MULTI | OK |
| EXEC | Execute transaction | EXEC | [OK, OK] |
| DISCARD | Discard transaction | DISCARD | OK |

```shell
MULTI
SET mykey "Hello"
INCR mycounter
EXEC
DISCARD
```

#### Publish/Subscribe

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| PUBLISH | Send message to channel | PUBLISH channel "Hello, Redis!" | (integer) 1 |
| SUBSCRIBE | Listen for messages | SUBSCRIBE channel | Waiting for messages... |

```shell
PUBLISH channel "Hello, Redis!"
SUBSCRIBE channel
```

#### String Operations: String manipulation and atomic counters

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| APPEND | Append to value of key | APPEND mykey " World" | (integer) 11 |
| STRLEN | Get length of string value | STRLEN mykey | (integer) 11 |
| INCR | Increment value by one | INCR counter | (integer) 1 |
| DECR | Decrement value by one | DECR counter | (integer) -1 |
| INCRBY | Increment by specified amount | INCRBY counter 5 | (integer) 4 |
| DECRBY | Decrement by specified amount | DECRBY counter 2 | (integer) 2 |
| GETRANGE | Get substring of string | GETRANGE mykey 0 4 | "Hello" |
| SETRANGE | Overwrite part of string | SETRANGE mykey 6 "Redis" | (integer) 11 |

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

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| LPUSH | Push element to head of list | LPUSH mylist "One" | (integer) 1 |
| RPUSH | Push element to tail of list | RPUSH mylist "Two" | (integer) 2 |
| LPOP | Remove and get first element | LPOP mylist | "One" |
| RPOP | Remove and get last element | RPOP mylist | "Two" |
| LRANGE | Get range of elements | LRANGE mylist 0 1 | 1) "One" 2) "Two" |
| LINDEX | Get element by index | LINDEX mylist 0 | "One" |
| LSET | Set element at index | LSET mylist 0 "Updated" | OK |

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

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| SADD | Add member to set | SADD myset "Apple" | (integer) 1 |
| SREM | Remove member from set | SREM myset "Apple" | (integer) 1 |
| SISMEMBER | Test if member in set | SISMEMBER myset "Apple" | (integer) 0 |
| SMEMBERS | Get all members | SMEMBERS myset | 1) "Apple" |
| SINTER | Intersect multiple sets | SINTER set1 set2 | 1) "CommonValue" |
| SUNION | Add multiple sets | SUNION set1 set2 | 1) "Apple" 2) "Banana" |
| SDIFF | Subtract multiple sets | SDIFF set1 set2 | 1) "Orange" |

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

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| HSET | Set field in hash | HSET myhash field1 "value1" | (integer) 1 |
| HGET | Get field in hash | HGET myhash field1 | "value1" |
| HMSET | Set multiple fields | HMSET myhash field1 "value1" field2 "value2" | OK |
| HGETALL | Get all fields and values | HGETALL myhash | 1) "field1" 2) "value1" 3) "field2" 4) "value2" |
| HDEL | Delete field | HDEL myhash field1 | (integer) 1 |
| HEXISTS | Test if field exists | HEXISTS myhash field1 | (integer) 0 |

```shell
HSET myhash field1 "value1"
HGET myhash field1
HMSET myhash field1 "value1" field2 "value2"
HGETALL myhash
HDEL myhash field1
HEXISTS myhash field1
```

#### Sorted Set Operations: Scored member management

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| ZADD | Add member with score | ZADD myzset 1 "one" 2 "two" | (integer) 2 |
| ZRANGE | Get range of members | ZRANGE myzset 0 -1 | 1) "one" 2) "two" |
| ZRANK | Get rank of member | ZRANK myzset "two" | (integer) 1 |
| ZREM | Remove member | ZREM myzset "one" | (integer) 1 |
| ZRANGEBYSCORE | Get range by score | ZRANGEBYSCORE myzset 1 2 | 1) "two" |

```shell
ZADD myzset 1 "one" 2 "two"
ZRANGE myzset 0 -1
ZRANK myzset "two"
ZREM myzset "one"
ZRANGEBYSCORE myzset 1 2
```

#### JSON document storage

| Command | Purpose | Syntax |
|---------|---------|--------|
| JSON.SET | Set JSON value | JSON.SET key path value |
| JSON.GET | Get JSON value | JSON.GET key [path ...] |
| JSON.DEL | Delete JSON value | JSON.DEL key [path] |
| JSON.ARRAPPEND | Append to JSON array | JSON.ARRAPPEND key path value [value ...] |

**Examples**

Replace an existing value:
```shell
JSON.SET doc $ '{"a":2}'
JSON.SET doc $.a '3'
JSON.GET doc $
```

Add a new value:
```shell
JSON.SET doc $ '{"a":2}'
JSON.SET doc $.b '8'
JSON.GET doc $
```

Update multi-paths:
```shell
JSON.SET doc $ '{"f1": {"a":1}, "f2":{"a":2}}'
JSON.SET doc $..a 3
JSON.GET doc
```

Delete specified values:
```shell
JSON.SET doc $ '{"a": 1, "nested": {"a": 2, "b": 3}}'
JSON.DEL doc $..a
JSON.GET doc $
```

Use case - Add a new color to a list of product colors:
```shell
JSON.SET item:1 $ '{"name":"Noise-cancelling Bluetooth headphones","description":"Wireless Bluetooth headphones with noise-cancelling technology","connection":{"wireless":true,"type":"Bluetooth"},"price":99.98,"stock":25,"colors":["black","silver"]}'
JSON.ARRAPPEND item:1 $.colors '"blue"'
JSON.GET item:1
```

#### Stream Operations: Append-only log structures

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| XADD | Add entry to stream | XADD mystream * field1 value1 | "1683552795445-0" |
| XREAD | Read stream entries | XREAD STREAMS mystream 0 | 1) 1) "mystream" 2) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XRANGE | Get range of entries | XRANGE mystream - + | 1) 1) "1683552795445-0" 2) "field1" 3) "value1" |
| XLEN | Get length of stream | XLEN mystream | (integer) 1 |

```shell
XADD mystream * name Sara surname OConnor
XADD mystream * field1 value1 field2 value2 field3 value3
XLEN mystream
XRANGE mystream - +
```

#### Geospatial Operations: Location-based features

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| GEOADD | Add geospatial item | GEOADD mygeoset 13.361389 38.115556 "Palermo" | (integer) 1 |
| GEOSEARCH | Search radius | GEOSEARCH mygeoset FROMLONLAT 13.361389 38.115556 BYRADIUS 200 km | 1) "Palermo" |
| GEODIST | Get distance between points | GEODIST mygeoset "Palermo" "Catania" km | "166.2741" |

```shell
GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
GEODIST Sicily Palermo Catania
GEOSEARCH Sicily FROMLONLAT 15 37 BYRADIUS 100 km WITHCOORD WITHDIST
```

#### Bitmap: Bit-level manipulations

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| SETBIT | Set bit value | SETBIT mybitkey 7 1 | OK |
| GETBIT | Get bit value | GETBIT mybitkey 7 | (integer) 1 |
| BITCOUNT | Count set bits | BITCOUNT mybitkey | (integer) 1 |
| BITOP | Bitwise operations | BITOP AND destkey mybitkey1 mybitkey2 | (integer) 5 |

```shell
SETBIT mybitkey 7 1
GETBIT mybitkey 7
BITCOUNT mybitkey
```

BITCOUNT example:
```shell
SET mykey "foobar"
BITCOUNT mykey
BITCOUNT mykey 0 0
BITCOUNT mykey 1 1
BITCOUNT mykey 1 1 BYTE
BITCOUNT mykey 5 30 BIT
```

#### HyperLogLog: Probabilistic Operations

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| PFADD | Add to HyperLogLog | PFADD myhyperloglog "element1" | (integer) 1 |
| PFCOUNT | Get HyperLogLog count | PFCOUNT myhyperloglog | (integer) 1 |
| PFMERGE | Merge HyperLogLogs | PFMERGE mymerged myhyperloglog1 myhyperloglog2 | OK |

```shell
PFADD myhyperloglog "element1"
PFCOUNT myhyperloglog
PFMERGE mymerged myhyperloglog1 myhyperloglog2
```

#### Time Series Operations: Time-based data management

| Command | Purpose | Sample Input | Expected Output |
|---------|---------|--------------|-----------------|
| TS.CREATE | Create time series | TS.CREATE mytimeseries | OK |
| TS.ADD | Add time series entry | TS.ADD mytimeseries 1609459200 42.0 | OK |
| TS.GET | Get time series entry | TS.GET mytimeseries | 1) 1609459200 2) "42.0" |
| TS.RANGE | Get time series range | TS.RANGE mytimeseries 1609459200 1609545600 | 1) 1609459200 2) "42.0" |

```shell
TS.CREATE mytimeseries
TS.ADD mytimeseries 1609459200 42.0
TS.GET mytimeseries
TS.RANGE mytimeseries 1609459200 1609545600
```

## Project Structure
```
└── 📁src
    ├── main.py                      # Main entry point
    ├── protocol.py                  # Protocol handling
    ├── pubsub.py                    # Publish/Subscribe functionality
    ├── server.py                    # Server setup and configuration
    └── 📁core
        ├── database.py              # Core database functionality
        ├── expiry.py                # Expiry management
        ├── persistence.py           # Persistence mechanisms
        ├── transaction.py           # Transaction management
    └── 📁datatypes
        ├── 📁advanced
            ├── bitfield.py          # Bitfield data type
            ├── bitmap.py            # Bitmap data type
            ├── geo.py               # Geospatial data type
            ├── json.py              # JSON data type
            ├── probabilistic.py     # Probabilistic data structures
            ├── stream.py            # Stream data type
            ├── timeseries.py        # Timeseries data type
        ├── base.py                  # Base data type
        ├── hash.py                  # Hash data type
        ├── list.py                  # List data type
        ├── set.py                   # Set data type
        ├── string.py                # String data type
        ├── zset.py                  # Sorted set data type
    └── 📁commands
        ├── base_handler.py          # Base class for command handlers
        ├── bitfield_handler.py      # Handler for bitfield commands
        ├── bitmap_handler.py        # Handler for bitmap commands
        ├── core_handler.py          # Core command handler
        ├── geo_handler.py           # Handler for geospatial commands
        ├── hash_handler.py          # Handler for hash commands
        ├── json_handler.py          # Handler for JSON commands
        ├── list_handler.py          # Handler for list commands
        ├── probabilistic_handler.py # Handler for probabilistic data structures
        ├── set_handler.py           # Handler for set commands
        ├── stream_handler.py        # Handler for stream commands
        ├── string_handler.py        # Handler for string commands
        ├── timeseries_handler.py    # Handler for timeseries commands
        ├── transaction_handler.py   # Handler for transaction commands
        ├── zset_handler.py          # Handler for sorted set commands
├── appendonly.aof                   # Data persistence AOF
├── snapshot.rdb                     # Data persistence RDB
├── README.md                        # README file
├── requirements.txt                 # To import dependencies
```

## Known Limitations
- Tested with redis-cli only
- Limited command set implementation
- AOF has no rewrite operation
- No authentication or authorization
- No support for multiple databases
- No cluster support
- No Lua scripting support
- No unit tests or benchmarking

## Future Development
- [ ] Client library implementation
- [ ] Vector Database functionality
- [ ] Document Database capabilities
- [ ] Security
- [ ] Support for clustering
- [ ] Monitoring and Management
- [ ] Enhanced Client-side Caching
- [ ] Pipelining Optimization
- [ ] Keyspace Notifications
- [ ] Redis Patterns Implementation

## Notes
- This is a learning project. Implementation focuses on understanding core concepts rather than performance optimization
- Not recommended for production use without thorough testing and security review 

## Acknowledgments
- Inspired by Redis architecture and commands
- Project structure based on provided requirements document
- Resources: 
  - Official Redis Documentation - https://redis.io/docs/latest/
  - Guide to install Redis-cli for Windows - https://www.youtube.com/watch?v=188Fy-oCw4w