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
    - Bitfields
    - Probabilistic (HyperLogLog, Bloom Filter)
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

### Supported Commands

#### Core Operations: Basic key-value operations

| Command | Purpose | Syntax |
|---------|---------|--------|
| PING | Test connection | PING |
| FLUSHDB | Clear database | FLUSHDB |
| SET | Set key to hold string value | SET key value|
| GET | Get value of key | GET key |
| DEL | Delete a key | DEL key [key ...] |
| EXISTS | Determine if key exists | EXISTS key |
| EXPIRE | Set key timeout | EXPIRE key seconds |
| TTL | Get key timeout | TTL key |
| PERSIST | Remove timeout | PERSIST key |

#### Transaction Operations: Atomic operation groups

| Command | Purpose | Syntax |
|---------|---------|--------|
| MULTI | Start transaction | MULTI |
| EXEC | Execute transaction | EXEC |
| DISCARD | Discard transaction | DISCARD |

#### Publish/Subscribe:

| Command | Purpose | Syntax |
|---------|---------|--------|
| PUBLISH | Send message to channel | PUBLISH channel message |
| SUBSCRIBE | Listen for messages | SUBSCRIBE channel |

#### String Operations: String manipulation and atomic counters

| Command | Purpose | Syntax |
|---------|---------|--------|
| APPEND | Append to value of key | APPEND key value |
| STRLEN | Get length of string value | STRLEN key |
| INCR | Increment value by one | INCR key |
| DECR | Decrement value by one | DECR key |
| INCRBY | Increment by specified amount | INCRBY key increment |
| DECRBY | Decrement by specified amount | DECRBY key decrement |
| GETRANGE | Get substring of string | GETRANGE key start end |
| SETRANGE | Overwrite part of string | SETRANGE key offset value |

#### List Operations: Queue and stack operations

| Command | Purpose | Syntax |
|---------|---------|--------|
| LPUSH | Push element to head of list | LPUSH key value |
| RPUSH | Push element to tail of list | RPUSH key value |
| LPOP | Remove and get first element | LPOP key |
| RPOP | Remove and get last element | RPOP key |
| LRANGE | Get range of elements | LRANGE key start stop |
| LINDEX | Get element by index | LINDEX key index |
| LSET | Set element at index | LSET key index value |

#### Set Operations: Unique element collections

| Command | Purpose | Syntax |
|---------|---------|--------|
| SADD | Add member to set | SADD key member |
| SREM | Remove member from set | SREM key member |
| SISMEMBER | Test if member in set | SISMEMBER key member |
| SMEMBERS | Get all members | SMEMBERS key |
| SINTER | Intersect multiple sets | SINTER key |
| SUNION | Add multiple sets | SUNION key |
| SDIFF | Subtract multiple sets | SDIFF key |

#### Hash Operations: Field-value pair storage

| Command | Purpose | Syntax |
|---------|---------|--------|
| HSET | Set field in hash | HSET key field value |
| HGET | Get field in hash | HGET key field |
| HMSET | Set multiple fields | HMSET key field value [field value ...] |
| HGETALL | Get all fields and values | HGETALL key |
| HDEL | Delete field | HDEL key field |
| HEXISTS | Test if field exists | HEXISTS key field |

#### Sorted Set Operations: Scored member management

| Command | Purpose | Syntax |
|---------|---------|--------|
| ZADD | Add member with score | ZADD key score member |
| ZRANGE | Get range of members | ZRANGE key start stop [WITHSCORES] |
| ZRANK | Get rank of member | ZRANK key member |
| ZREM | Remove member | ZREM key member |
| ZRANGEBYSCORE | Get range by score | ZRANGEBYSCORE key min max [WITHSCORES] |

#### JSON document storage

| Command | Purpose | Syntax |
|---------|---------|--------|
| JSON.SET | Set JSON value | JSON.SET key path value |
| JSON.GET | Get JSON value | JSON.GET key [path ...] |
| JSON.DEL | Delete JSON value | JSON.DEL key [path] |
| JSON.ARRAPPEND | Append to JSON array | JSON.ARRAPPEND key path value [value ...] |

#### Stream Operations: Append-only log structures

| Command | Purpose | Syntax |
|---------|---------|--------|
| XADD | Add entry to stream | XADD key ID field value [field value ...] |
| XREAD | Read stream entries | XREAD [COUNT count] STREAMS key ID |
| XRANGE | Get range of entries | XRANGE key start end [COUNT count] |
| XLEN | Get length of stream | XLEN key |
| XGROUP | Manage consumer groups | XGROUP CREATE key groupname ID |
| XREADGROUP | Read from consumer group | XREADGROUP GROUP group consumer STREAMS key ID |
| XACK | Acknowledge message | XACK key group ID [ID ...] |

#### Geospatial Operations: Location-based features

| Command | Purpose | Syntax |
|---------|---------|--------|
| GEOADD | Add geospatial item | GEOADD key longitude latitude member |
| GEOSEARCH | Search radius | GEOSEARCH key FROMLONLAT longitude latitude BYRADIUS radius unit |
| GEODIST | Get distance between points | GEODIST key member1 member2 [unit] |

#### Bitmap & Bitfield Operations: Bit-level manipulations

| Command | Purpose | Syntax |
|---------|---------|--------|
| SETBIT | Set bit value | SETBIT key offset value |
| GETBIT | Get bit value | GETBIT key offset |
| BITCOUNT | Count set bits | BITCOUNT key [start end] |
| BITOP | Bitwise operations | BITOP operation destkey key [key ...] |
| BITFIELD | Operate on bitmap segments | BITFIELD key GET type offset |

#### Probabilistic Operations: Bloom filters and HyperLogLog

| Command | Purpose | Syntax |
|---------|---------|--------|
| BF.RESERVE | Create Bloom filter | BF.RESERVE key error_rate initial_size |
| BF.ADD | Add to Bloom filter | BF.ADD key item |
| BF.EXISTS | Check Bloom filter | BF.EXISTS key item |
| PFADD | Add to HyperLogLog | PFADD key element [element ...] |
| PFCOUNT | Get HyperLogLog count | PFCOUNT key [key ...] |
| PFMERGE | Merge HyperLogLogs | PFMERGE destkey sourcekey [sourcekey ...] |

#### Time Series Operations: Time-based data management

| Command | Purpose | Syntax |
|---------|---------|--------|
| TS.CREATE | Create time series | TS.CREATE key |
| TS.ADD | Add time series entry | TS.ADD key timestamp value |
| TS.GET | Get time series entry | TS.GET key |
| TS.RANGE | Get time series range | TS.RANGE key fromTimestamp toTimestamp |

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