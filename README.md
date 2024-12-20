# Redis-like In-Memory Data Store
## Overview
This project aims to build a Redis-like in-memory data store from scratch. It includes a variety of features and data types commonly found in Redis, such as key-value storage, strings, lists, sets, hashes, sorted sets, JSON documents, streams, and more. The project is designed to help you understand the inner workings of in-memory data stores and enhance your skills in data structures, algorithms, and system design.

## Features
- Core Key-Value Store
- Comprehensive Data Types
  - Strings
  - JSON Documents
  - Lists
  - Sets
  - Hashes
  - Sorted Sets
  - Streams
  - Geospatial Data
  - Bitmaps
  - Bitfields
  - Probabilistic Data Structures
  - Time-Series Data
- Key Expiration
- Server Functionality
- Transactions
- Publish/Subscribe Mechanism
- Persistence
- Replication
- Client Development
- Vector Database Functionality
- Document Database Capabilities
- Performance Optimization
- Security
- Advanced Features
- Monitoring and Management
- Testing and Benchmarking

## Project Structure
redis-like_in-memory_data_store/
├── src/
│   ├── __init__.py
│   ├── server.py                  # Main server implementation
│   ├── protocol.py                # RESP protocol implementation
│   ├── core/
│   │   ├── __init__.py
│   │   ├── data_store.py          # Main key-value store implementation
│   │   ├── expiry_manager.py      # Key expiration handling
│   │   ├── transaction_manager.py # Transaction handling (MULTI/EXEC)
│   │   └── keyspace_manager.py    # Keyspace management (SELECT, FLUSHDB)
│   ├── datatypes/
│   │   ├── __init__.py
│   │   ├── base.py                # Base class for all data types
│   │   ├── strings.py             # String operations
│   │   ├── lists.py               # List operations
│   │   ├── sets.py                # Set operations
│   │   ├── hashes.py              # Hash operations
│   │   ├── sorted_sets.py         # Sorted set operations
│   │   ├── json_type.py           # JSON document operations
│   │   ├── streams.py             # Stream operations
│   │   ├── vectors.py             # Vector operations and indexing
│   │   ├── documents.py           # Document database operations
│   │   ├── geospatial.py          # Geospatial operations
│   │   ├── bitmaps.py             # Bitmap operations
│   │   ├── bitfields.py           # Bitfield operations
│   │   ├── hyperloglog.py         # HyperLogLog implementation
│   │   └── timeseries.py          # Time series operations
│   ├── persistence/
│   │   ├── __init__.py
│   │   ├── aof.py                 # Append-only file implementation
│   │   └── snapshot.py            # Point-in-time snapshot implementation
│   ├── pubsub/
│   │   ├── __init__.py
│   │   ├── publisher.py           # Publisher implementation
│   │   ├── subscriber.py          # Subscriber implementation
│   │   └── keyspace_notif.py      # Keyspace notifications
│   ├── client/
│   │   ├── __init__.py
│   │   ├── client.py              # Client implementation
│   │   ├── connection.py          # Connection handling
│   │   ├── pipeline.py            # Pipelining implementation
│   │   └── cache.py               # Client-side caching
│   ├── security/
│   │   ├── __init__.py
│   │   ├── authentication.py      # Basic auth mechanisms
│   │   ├── acl.py                 # Access Control Lists
│   │   └── tls.py                 # TLS support
│   ├── cluster/
│   │   ├── __init__.py
│   │   ├── node.py                # Cluster node implementation
│   │   ├── slot_manager.py        # Hash slot management
│   │   └── replication.py         # Master-slave replication
│   ├── scripting/
│   │   ├── __init__.py
│   │   ├── lua_engine.py          # Lua scripting support
│   │   └── script_cache.py        # Script storage and caching
│   ├── monitoring/
│   │   ├── __init__.py
│   │   ├── stats.py               # Server statistics (INFO)
│   │   ├── slowlog.py             # Slow query logging
│   │   └── metrics.py             # Real-time metrics
│   ├── patterns/
│   │   ├── __init__.py
│   │   ├── distributed_lock.py    # Distributed locks
│   │   ├── rate_limiter.py        # Rate limiting
│   │   ├── message_queue.py       # Message queue system
│   │   └── cache_layer.py         # Caching patterns
│   └── utils/
│       ├── __init__.py
│       ├── serializer.py          # Data serialization utilities
│       ├── indexing.py            # Vector/document indexing utilities
│       ├── config.py              # Configuration management
│       └── memory_tracker.py      # Memory usage tracking
├── tests/
│   ├── __init__.py
│   ├── test_server.py
│   ├── test_protocol.py
│   ├── test_datatypes/
│   │   ├── __init__.py
│   │   ├── test_strings.py
│   │   ├── test_lists.py
│   │   ├── test_sets.py
│   │   ├── test_hashes.py
│   │   ├── test_sorted_sets.py
│   │   ├── test_json_type.py
│   │   ├── test_streams.py
│   │   ├── test_vectors.py
│   │   ├── test_documents.py
│   │   ├── test_geospatial.py
│   │   ├── test_bitmaps.py
│   │   ├── test_bitfields.py
│   │   ├── test_hyperloglog.py
│   │   └── test_timeseries.py
│   ├── test_persistence/
│   │   ├── __init__.py
│   │   ├── test_aof.py
│   │   └── test_snapshot.py
│   ├── test_pubsub/
│   │   ├── __init__.py
│   │   ├── test_publisher.py
│   │   ├── test_subscriber.py
│   │   └── test_keyspace_notif.py
│   ├── test_security/
│   │   ├── __init__.py
│   │   ├── test_auth.py
│   │   └── test_acl.py
│   ├── test_cluster/
│   │   ├── __init__.py
│   │   └── test_replication.py
│   ├── test_monitoring/
│   │   ├── __init__.py
│   │   └── test_metrics.py
│   ├── test_patterns/
│   │   ├── __init__.py
│   │   ├── test_distributed_lock.py
│   │   ├── test_rate_limiter.py
│   │   ├── test_message_queue.py
│   │   └── test_cache_layer.py
│   └── benchmarks/
│       ├── __init__.py
│       ├── benchmark_ops.py
│       └── compare_redis.py
├── web_interface/               # OPTIONAL ENHANCEMENT
│   ├── __init__.py
│   ├── app.py                   # Web application
│   ├── static/                  # Static files
│   └── templates/               # HTML templates
├── examples/
│   ├── basic_usage.py
│   ├── vector_operations.py
│   ├── document_operations.py
│   ├── cluster_setup.py
│   └── pattern_examples.py
├── docs/
│   ├── api_reference/
│   ├── user_guide/
│   └── protocol_spec/
├── requirements.txt
├── setup.py
└── README.md


## Getting Started
### Prerequisites
- Python 3.8+
- Git

### Installation
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/redis-like_in-memory_data_store.git
   cd redis-like_in-memory_data_store
   ```

2. **Set up a virtual environment**
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment**
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Server
To start the Redis-like server, run the following command:
```bash
python src/server.py
```
The server will start and listen for incoming connections on the default port 6379.

### Running the Tests
To run the tests, use the following command:
```bash
python -m unittest discover -s tests
```
This command will discover and run all the test cases in the `tests` directory.

## Usage
You can interact with the server using a Redis client or a simple socket connection. Here are a few examples of commands you can use:

### Set a key
```bash
SET key value
```

### Get a key
```bash
GET key
```

### Delete a key
```bash
DEL key
```

### Check if a key exists
```bash
EXISTS key
```
---

Instructions
    1. Project Setup
        a. Create a project directory and initialize version control
        b. Choose your preferred programming language
        c. Set up the main server file as an application entry point. So just like Redis, we are using this app as a third-party application
    2. Core KeyValue Store
        a. Design a class for data storing using hash table or dictionary
        b. Implement core operations: SET, GET, DEL, EXISTS
    3. Comprehensive Data Types
        a. Strings
            i. Implement basic operations: GET, SET, APPEND, STRLEN
            ii. Add incremental operations: INCR, DECR, INCRBY, DECRBY
            iii. Implement substring operations: GETRANGE, SETRANGE
        b. JSON (similar to RedisJSON)
            i. Design structure to store and manipulate JSON documents
            ii. Implement operations: JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND
            iii. Add support for JSONPathlike queries
        c. Lists
            i. Implement as dynamic arrays or linked lists
            ii. Add operations: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LSET
        d. Sets
            i. Implement using hash tables for O(1) lookups
            ii. Add operations: SADD, SREM, SISMEMBER, SMEMBERS, SINTER, SUNION, SDIFF
        e. Hashes
            i. Use nested hash tables for efficient field-value pair storage
            ii. Implement HSET, HGET, HMSET, HGETALL, HDEL, HEXISTS
        f. Sorted Sets
            i. Implement using a balanced tree or skip list
            ii. Add operations: ZADD, ZRANGE, ZRANK, ZREM, ZRANGEBYSCORE
        g. Streams
            i. Design append-only loglike data structure
            ii. Implement: XADD, XREAD, XRANGE, XLEN
            iii. Add consumer group support: XGROUP CREATE, XREADGROUP, XACK
        h. Geospatial
            i. Implement geospatial indexing (consider using Geohash system)
            ii. Add commands: GEOADD, GGEOSEARCH, GEODIST
        i. Bitmaps
            i. Implement bit-level operations on strings
            ii. Add commands: SETBIT, GETBIT, BITCOUNT, BITOP
        j. Bitfields
            i. Allow efficient storage and manipulation of multiple counters in a single string
            ii. Implement: BITFIELD GET, BITFIELD SET, BITFIELD INCRBY
        k. Probabilistic Data Structures
            i. Implement HyperLogLog for cardinality estimation
            ii. Add commands: PFADD, PFCOUNT, PFMERGE
            iii. Consider implementing Bloom filters or Cuckoo filters
        l. Time-Series (similar to RedisTimeSeries module)
            i. Design structure for time series data storage
            ii. Implement: TS.CREATE, TS.ADD, TS.RANGE, TS.GET
            iii. Add support for downsampling and aggregation
    4. Key Expiration
        a. Associate expiration time with key-value pairs
        b. Create background task to remove expired keys
        c. Modify GET to check for expiration before returning value
    5. Server Functionality
        a. Build server that listens for client connections
        b. Define protocol for client-server communication (similar to RESP)
        c. Parse incoming commands and route to appropriate functions
        d. Send responses back to clients
    6. Transactions
        a. Implement MULTI to begin a transaction
        b. Queue commands received after MULTI
        c. Execute queued commands atomically with EXEC
        d. Provide DISCARD to clear command queue
    7. Publish/Subscribe (Pub/Sub) Mechanism
        a. Manage Pub/Sub channels and subscribers
        b. Implement PUBLISH to send messages to channel subscribers
        c. Support SUBSCRIBE for clients to listen to chanel messages
        d. Build message distribution system
    8. Persistence
        a. AppendOnly File (AOF): Log write operations, implement replay on restart
        b. PointinTime Snapshots: Periodically serialize dataset, load on startup
    9. Replication
        a. Create master and slave modes
        b. Transfer data from master to slave on initial connection
        c. Forward write operations from master to slave in real-time
    10. Client Development
        a. Develop client class to connect to server
        b. Implement methods for all supported server operations
        c. Include error handling and automatic reconnection logic
    11. Vector Database Functionality
        a. Design data structure for efficient vector storage
        b. Implement vector indexing (e.g.. HNSW or LSH) for fast similarity search
        c. Add similarity search operations (K-nearest Neighbors)
        d. Support different distance metrics (Euclidean, Cosine similarity)
        e. Implement basic vector operations (addition, subtraction, dot product)
        f. Design interface for integration with machine learning libraries
    12. Document Database Capabilities
        a. Design flexible schema for JSON-like document storage
        b. Implement indexing mechanisms for efficient document retrieval
        c. Create simple query language for documents
        d. Add support for partial updates, array operations, and nested field access
        e. Implement a basic aggregation framework (count, sum, average)
    13. Performance Optimization
        a. Implement client-side caching with server assisted invalidation
        b. Add support for pipelining to reduce network round trips
        c. Optimize vector operations for high-dimensional data
        d. Implement efficient serialization/deserialization for document storage
    14. Security
        a. Implement basic authentication mechanisms
        b. Add support for Access Control Lists (ACLs)
        c. Implement TKS support for encrypted client-server communication
    15. Advanced Features
        a. Add support for Lua scripting for complex operations
        b. Implement basic cluster mode using hash slots for data distribution
        c. Add support for geospatial indexing and querying
        d. Implement time series data structure with retention policies and aggregations
    16. Monitoring and Management
        a. Implement INFO command for server statistics
        b. Add SLOWLOG for identifying and logging slow queries
        c. Create simple monitoring interface for real-time server metrics
    17. Testing and Benchmarking
        a. Develop a comprehensive test suite for all components
        b. Implement benchmarking tools to measure performance
        c. Compare implementation with Redis for similar operations
    18. Keyspace Functionality
        a. Design logical database structure using Redis keyspaces
        b. Implement commands for keyspace management (SELECT, FLUSHDB, RANDOMKEY)
        c. Add support for key expiration and TTL commands (EXPIRE, TTL, PERSIST)
    19. Enhanced Client-side Caching
        a. Implement server-assisted clientside caching mechanisms
        b. Add support for cache invalidation messages
        c. Implement tracking of clientside caches on server
    20. Pipelining Optimization
        a. Implement pipelining mechanism to send multiple commands in single request
        b. Add support for handling pipelined responses efficiently
        c. Create client abstractions for easy pipelining usage
    21. Keyspace Notifications
        a. Design pub/sub system for keyspace events
        b. Add support for configurable notification types (string modifications, list operations, etc.)
        c. Implement client-side listeners for keyspace notifications
    22. Redis Patterns Implementation
        a. Implement common Redis patterns like distributed locks
        b. Add support for rate limiting using Redis data structures
        c. Design and implement simple message queue system
        d. Create basic caching layer using Redis as backing store
        
Optional Enhancements
    • Implement LRU eviction for memory management
    • Create a web-based management interface for the data store
    
Testing & Documentation
    • Write unit tests to cover all data structures and operations
    • Create integration tests to validate the interaction between the server and the client
    • Document the comunication protocol and all supported commands
    • Write a user guide explaining how to set up, run, and use your Redislike data store
    
Notes: 
    • Avoid using existing libraries or framework that handle core functionality. 
    • Ensure the originality in design. So in terms of designing, it's better to use your own way of coding or your design.
    • Do not use third party persistence or replication modules.
