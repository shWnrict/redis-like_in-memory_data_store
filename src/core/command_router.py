# src/core/command_router.py
from src.logger import setup_logger
from src.protocol import RESPProtocol  # Import RESPProtocol
from src.datatypes.sorted_sets import SortedSets
from src.datatypes.hashes import Hashes
from src.datatypes.base import BaseDataType
from src.monitoring.stats import Stats  # Ensure correct import
from src.monitoring.slowlog import SlowLog  # Ensure correct import
from src.datatypes.geospatial import Geospatial
from src.scripting.lua_engine import LuaEngine  # Import LuaEngine
from src.patterns.distributed_locks import DistributedLock
from src.patterns.rate_limiter import RateLimiter
from src.patterns.message_queue import MessageQueue

logger = setup_logger("command_router")

class CommandRouter:
    """
    Routes parsed commands to the appropriate server methods.
    """

    def __init__(self, server):
        self.server = server
        self.transaction_queue = []
        self.in_transaction = False

        # Initialize pattern handlers
        self.distributed_lock = DistributedLock(self.server.data_store)
        self.rate_limiter = RateLimiter(self.server.data_store, max_requests=100, window_seconds=60)
        self.message_queue = MessageQueue(self.server.data_store, queue_name='default')
        
        self.handlers = {
            "SET": self.server.strings.handle_command,
            "GET": self.server.strings.handle_command,
            "DEL": self.server.strings.handle_command,
            "EXISTS": self.server.strings.handle_command,
            "EXPIRE": self.handle_expire,
            "LPUSH": self.server.lists.handle_command,
            "RPUSH": self.server.lists.handle_command,
            "LPOP": self.server.lists.handle_command,
            "RPOP": self.server.lists.handle_command,
            "SADD": self.server.sets.handle_command,
            "SREM": self.server.sets.handle_command,
            "SISMEMBER": self.server.sets.handle_command,
            "SMEMBERS": self.server.sets.handle_command,
            "HSET": self.server.hashes.handle_command,
            "HGET": self.server.hashes.handle_command,
            "HMSET": self.server.hashes.handle_command,
            "HGETALL": self.server.hashes.handle_command,
            "HDEL": self.server.hashes.handle_command,
            "HEXISTS": self.server.hashes.handle_command,
            "ZADD": self.server.sorted_sets.handle_command,
            "ZRANGE": self.server.sorted_sets.handle_command,
            "ZRANK": self.server.sorted_sets.handle_command,
            "ZREM": self.server.sorted_sets.handle_command,
            "ZRANGEBYSCORE": self.server.sorted_sets.handle_command,
            "TTL": self.handle_ttl,
            "PERSIST": self.handle_persist,
            "SINTER": self.server.sets.handle_command,
            "SUNION": self.server.sets.handle_command,
            "SDIFF": self.server.sets.handle_command,
            "INFO": self.handle_info,  # Add INFO command handler
            "EVAL": self.handle_eval,
            "EVALSHA": self.handle_evalsha,
            "LOCK": self.handle_lock,
            "UNLOCK": self.handle_unlock,
            "RATE.LIMIT": self.handle_rate_limit,
            "MQ.PUSH": self.handle_mq_push,
            "MQ.POP": self.handle_mq_pop,
            "MQ.PEEK": self.handle_mq_peek,
            "XADD": self.server.streams.handle_command,        # Add Streams commands
            "XREAD": self.server.streams.handle_command,
            "XRANGE": self.server.streams.handle_command,
            "XLEN": self.server.streams.handle_command,
            # Add other Lua scripting commands as needed
        }
        
        self.expiry_manager = server.expiry_manager  # Initialize ExpiryManager
        self.stats = server.stats  # Initialize Stats
        self.slowlog = server.slowlog  # Initialize SlowLog
        self.lua_engine = LuaEngine(server)  # Initialize LuaEngine

    def route(self, command, client_socket):
        if not command:
            return "ERR Empty command"

        cmd = command[0].upper()
        args = command[1:]

        handler = self.handlers.get(cmd, None)
        if handler:
            return handler(cmd, self.server.data_store.store, *args)
        else:
            return "ERR Unknown command"

    def handle_info(self, cmd, args):
        """
        Handle the INFO command to provide server statistics.
        """
        if args:
            return "ERR INFO command does not take any arguments"
        return self.stats.handle_info()

    def handle_expire(self, cmd, args):
        if len(args) != 2:
            return "-ERR wrong number of arguments for 'EXPIRE' command\r\n"
        key, ttl = args
        try:
            ttl = int(ttl)
            result = self.expiry_manager.handle_expire(key, ttl)
            return f":{result}\r\n"
        except ValueError:
            return "-ERR invalid TTL value\r\n"

    def handle_ttl(self, cmd, args):
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'TTL' command\r\n"
        key = args[0]
        result = self.expiry_manager.handle_ttl(key)
        return f":{result}\r\n"

    def handle_persist(self, cmd, args):
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'PERSIST' command\r\n"
        key = args[0]
        result = self.expiry_manager.handle_persist(key)
        return f":{result}\r\n"

    def handle_lock(self, cmd, args):
        """
        Handle the LOCK command to acquire a distributed lock.
        LOCK lock_name ttl
        """
        if len(args) != 2:
            return "ERR wrong number of arguments for 'LOCK' command"
        lock_name, ttl = args
        try:
            ttl = int(ttl)
        except ValueError:
            return "ERR TTL must be an integer"

        success = self.distributed_lock.acquire(lock_name, ttl)
        return "OK\r\n" if success else "ERR Failed to acquire lock\r\n"

    def handle_unlock(self, cmd, args):
        """
        Handle the UNLOCK command to release a distributed lock.
        UNLOCK lock_name
        """
        if len(args) != 1:
            return "ERR wrong number of arguments for 'UNLOCK' command"
        lock_name = args[0]
        success = self.distributed_lock.release(lock_name)
        return "OK\r\n" if success else "ERR Failed to release lock\r\n"

    def handle_rate_limit(self, cmd, args):
        """
        Handle the RATE.LIMIT command to check if a request is allowed.
        RATE.LIMIT client_id
        """
        if len(args) != 1:
            return "ERR wrong number of arguments for 'RATE.LIMIT' command"
        client_id = args[0]
        allowed = self.rate_limiter.is_allowed(client_id)
        return "OK\r\n" if allowed else "ERR Rate limit exceeded\r\n"

    def handle_mq_push(self, cmd, args):
        """
        Handle the MQ.PUSH command to enqueue a message.
        MQ.PUSH queue_name message
        """
        if len(args) != 2:
            return "ERR wrong number of arguments for 'MQ.PUSH' command"
        queue_name, message = args
        mq = MessageQueue(self.server.data_store, queue_name)
        mq.enqueue(message)
        return "+OK\r\n"

    def handle_mq_pop(self, cmd, args):
        """
        Handle the MQ.POP command to dequeue a message.
        MQ.POP queue_name
        """
        if len(args) != 1:
            return "ERR wrong number of arguments for 'MQ.POP' command"
        queue_name = args[0]
        mq = MessageQueue(self.server.data_store, queue_name)
        message = mq.dequeue()
        return RESPProtocol.format_response(message)

    def handle_mq_peek(self, cmd, args):
        """
        Handle the MQ.PEEK command to view the next message without dequeuing.
        MQ.PEEK queue_name
        """
        if len(args) != 1:
            return "ERR wrong number of arguments for 'MQ.PEEK' command"
        queue_name = args[0]
        mq = MessageQueue(self.server.data_store, queue_name)
        message = mq.peek()
        return RESPProtocol.format_response(message)

    def handle_eval(self, cmd, args):
        """
        Handle the EVAL command to execute Lua scripts.
        EVAL script numkeys key [key ...] arg [arg ...]
        """
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'EVAL' command\r\n"

        script = args[0]
        try:
            numkeys = int(args[1])
        except ValueError:
            return "-ERR numkeys must be an integer\r\n"

        if len(args) < 2 + numkeys:
            return "-ERR not enough arguments for 'EVAL' command\r\n"

        keys = args[2:2+numkeys]
        arguments = args[2+numkeys:]

        result = self.lua_engine.execute_script(script, keys, arguments)
        return RESPProtocol.format_response(result)

    def handle_evalsha(self, cmd, args):
        """
        Handle the EVALSHA command to execute cached Lua scripts by SHA1 hash.
        EVALSHA sha1 numkeys key [key ...] arg [arg ...]
        """
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'EVALSHA' command\r\n"

        sha1 = args[0]
        try:
            numkeys = int(args[1])
        except ValueError:
            return "-ERR numkeys must be an integer\r\n"

        if len(args) < 2 + numkeys:
            return "-ERR not enough arguments for 'EVALSHA' command\r\n"

        keys = args[2:2+numkeys]
        arguments = args[2+numkeys:]

        # Retrieve the script by SHA1 hash (implementation depends on script caching mechanism)
        script = self.server.lua_engine.get_script_by_sha1(sha1)
        if not script:
            return "-ERR No script found with the given SHA1 hash\r\n"

        result = self.lua_engine.execute_script(script, keys, arguments)
        return RESPProtocol.format_response(result)

    def evict_keys_if_needed(self):
        """
        Check memory usage and trigger eviction if necessary.
        """
        current_memory = self.server.memory_manager.get_current_memory()
        if current_memory > self.server.memory_manager.max_memory:
            self.server.memory_manager.evict_keys()
