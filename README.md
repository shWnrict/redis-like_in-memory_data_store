# Redis-like In-Memory Data Store

## Overview
A custom implementation of a redis-like in-memory data store using Python. This project provides a simple and efficient way to store and manage data in memory with support for basic Redis-like commands and data structures.

## Key Features
- Core KeyValue Store
- Key Expiration
- Transactions
- Comprehensive Data Types
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
- Pub/Sub Mechanism
- Replication (master-slave)
- Data Persistence support (AOF, Snapshots)

### Data Persistence
#### Append Only File (AOF)
- All write operations are logged
- Recovery on restart

#### Snapshots
- Point-in-time snapshots of the dataset
- Configurable snapshot intervals
- Manual snapshot through SAVE command

## Setup Instructions

### Prerequisites
- Python 3.8 or higher

### Installation
1. Clone the repository:
```bash
git clone https://github.com/shWnrict/redis-like_in-memory_data_store.git
cd redis-like_in-memory_data_store
```

2. Create a virtual environment (optional but recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Testing
To run the test suite:
```bash
python -m pytest tests/
```

## Usage
Currently, the project can be tested using redis-cli:

### Quick Start
1. Start the Server
```bash
python src/server.py
```
2. In another terminal, use redis-cli to connect
```bash
redis-cli -p 6379
```

### Supported Commands

#### Core Operations
**Basic key-value operations**

| Command | Purpose | Syntax | Output |
|---------|---------|---------|---------|
| SET | Set key to hold string value | SET key value | "OK" |
| GET | Get value of key | GET key | value |
| DEL | Delete a key | DEL key [key ...] | (integer) 1 |
| EXISTS | Determine if key exists | EXISTS key [key ...] | (integer) 1 |
| EXPIRE | Set key timeout | EXPIRE key seconds | (integer) 1 |
| TTL | Get key timeout | TTL key | (integer) seconds |
| PERSIST | Remove timeout | PERSIST key | (integer) 1 |

Example commands:
```
SET mykey "Hello"
GET mykey
DEL mykey
EXISTS mykey
EXPIRE mykey 60
TTL mykey
PERSIST mykey
```

#### Transaction Operations
**Atomic operation groups**

#### Server Operations
**Server management and replication**

#### **Publish/Subscribe**
****

#### String Operations
**String manipulation and atomic counters**

| Command | Purpose | Syntax | Output |
|---------|---------|---------|---------|
| APPEND | Append to value of key | APPEND key value | (integer) length |
| STRLEN | Get length of string value | STRLEN key | (integer) length |
| INCR | Increment value by one | INCR key | (integer) value |
| DECR | Decrement value by one | DECR key | (integer) value |
| INCRBY | Increment by specified amount | INCRBY key increment | (integer) value |
| DECRBY | Decrement by specified amount | DECRBY key decrement | (integer) value |
| GETRANGE | Get substring of string | GETRANGE key start end | string |
| SETRANGE | Overwrite part of string | SETRANGE key offset value | (integer) length |

Example commands:
```
SET mykey "Hello"
APPEND mykey " World"
STRLEN mykey
INCR counter
DECRBY counter 5
GETRANGE mykey 0 4
SETRANGE mykey 6 "Redis"
```

#### List Operations
**Queue and stack operations**

| Command | Purpose | Syntax | Output |
|---------|---------|---------|---------|
| LPUSH | Push element to head of list | LPUSH key value [value ...] | (integer) length |
| RPUSH | Push element to tail of list | RPUSH key value [value ...] | (integer) length |
| LPOP | Remove and get first element | LPOP key | element |
| RPOP | Remove and get last element | RPOP key | element |
| LRANGE | Get range of elements | LRANGE key start stop | array |
| LINDEX | Get element by index | LINDEX key index | element |
| LSET | Set element at index | LSET key index value | "OK" |

Example commands:
```
LPUSH mylist "world"
RPUSH mylist "hello"
LPOP mylist
LRANGE mylist 0 -1
LINDEX mylist 0
LSET mylist 0 "redis"
```

#### Set Operations
**Unique element collections**

| Command | Purpose | Syntax | Output |
|---------|---------|---------|---------|
| SADD | Add member to set | SADD key member [member ...] | (integer) added |
| SREM | Remove member from set | SREM key member [member ...] | (integer) removed |
| SISMEMBER | Test if member in set | SISMEMBER key member | (integer) exists |
| SMEMBERS | Get all members | SMEMBERS key | array |
| SINTER | Intersect multiple sets | SINTER key [key ...] | array |
| SUNION | Add multiple sets | SUNION key [key ...] | array |
| SDIFF | Subtract multiple sets | SDIFF key [key ...] | array |

Example commands:
```
SADD myset "one"
SADD myset "two"
SISMEMBER myset "one"
SMEMBERS myset
SINTER set1 set2
SUNION set1 set2
```
#### Hash Operations
**Field-value pair storage**

#### Sorted Set Operations
**Scored member management**

#### JSON
**JSON document storage**

#### Stream Operations
**Append-only log structures**

#### Geospatial Operations
**Location-based features**

#### Bitmap & Bitfield Operations
**Bit-level manipulations**

#### Probabilistic Operations
**Bloom filters and HyperLogLog**

#### Time Series Operations
**Time-based data management**

## Project Structure -Enhance the Project Structure section with descriptions:
```
project_root/
├── src/
│   ├── __init__.py
│   ├── server.py          # Server implementation
│   ├── store/
│   │   ├── __init__.py
│   │   ├── base.py       # Core storage implementation
│   │   ├── strings.py    # String data type implementation
│   │   ├── lists.py      # List data type implementation
│   │   ├── sets.py       # Set data type implementation
│   │   ├── hashes.py     # Hash data type implementation
│   │   ├── sorted_sets.py # Sorted Set data type implementation
│   │   └── types/        # Other data type implementations
│   ├── protocol/         # Client-server communication
│   │   ├── __init__.py
│   │   ├── parser.py     # Command parser implementation
│   │   ├── responder.py  # Response handler implementation
│   │   └── connection.py # Client connection handler
│   └── persistence/      # Data persistence implementations
│       ├── __init__.py
│       ├── aof.py        # Append-only file persistence
│       └── snapshot.py   # Snapshot persistence
├── tests/
│   ├── __init__.py
│   └── test_store.py
├── requirements.txt
└── README.md
```

## Known Limitations
- Limited command set implementation
- No authentication/authorization
- No cluster support
- No Lua scripting support

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
- This is a learning project
- Not recommended for production use without thorough testing and security review
- Implementation focuses on understanding core concepts rather than performance optimization (Only a limited set of commands are implemented. And there is no support for multiple databases or authentication.)

## Acknowledgments
- Inspired by Redis architecture and commands
- Project structure based on provided requirements document
- Resources: 
  - https://redis.io/docs/latest/