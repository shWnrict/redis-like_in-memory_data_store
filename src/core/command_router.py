# src/core/command_router.py
from src.logger import setup_logger

logger = setup_logger("command_router")

class CommandRouter:
    """
    Routes parsed commands to the appropriate server methods.
    """

    def __init__(self, server):
        self.server = server
        self.transaction_queue = []
        self.in_transaction = False
        
        # Command categories and mappings
        self.command_categories = {
            "CORE": {
                "SET", "GET", "DEL", "EXISTS", "EXPIRE"
            },
            "STRING": {
                "APPEND", "STRLEN", "INCR", "DECR", 
                "INCRBY", "DECRBY", "GETRANGE", "SETRANGE"
            },
            "LIST": {
                "LPUSH", "RPUSH", "LPOP", "RPOP",
                "LRANGE", "LINDEX", "LSET", "LLEN"
            },
            "SET": {
                "SADD", "SREM", "SISMEMBER", "SMEMBERS",
                "SINTER", "SUNION", "SDIFF"
            },
            "HASH": {
                "HSET", "HGET", "HMSET", "HGETALL",
                "HDEL", "HEXISTS"
            },
            "SORTED_SET": {
                "ZADD", "ZRANGE", "ZRANK", "ZREM",
                "ZRANGEBYSCORE"
            },
            "STREAM": {
                "XADD", "XREAD", "XRANGE", "XLEN",
                "XGROUP", "XREADGROUP", "XACK"
            },
            "JSON": {
                "JSON.SET", "JSON.GET", "JSON.DEL",
                "JSON.ARRAPPEND"
            },
            "GEOSPATIAL": {
                "GEOADD", "GEOSEARCH", "GEODIST"
            },
            "BITMAP": {
                "SETBIT", "GETBIT", "BITCOUNT", "BITOP"
            },
            "BITFIELD": {
                "BITFIELD"
            },
            "HYPERLOGLOG": {
                "PFADD", "PFCOUNT", "PFMERGE"
            },
            "TIMESERIES": {
                "TS.CREATE", "TS.ADD", "TS.RANGE", "TS.GET"
            },
            "VECTOR": {
                "VECTOR.ADD", "VECTOR.SEARCH", "VECTOR.OP"
            },
            "DOCUMENT": {
                "DOC.INSERT", "DOC.FIND", "DOC.UPDATE",
                "DOC.DELETE", "DOC.AGGREGATE"
            },
            "TRANSACTION": {
                "MULTI", "EXEC", "DISCARD"
            },
            "PUBSUB": {
                "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH"
            },
            "PERSISTENCE": {
                "SAVE", "RESTORE"
            }
        }

        # Create a flat command to handler mapping
        self.command_handlers = {
            cmd: self._get_handler_for_category(category)
            for category, commands in self.command_categories.items()
            for cmd in commands
        }

    def _get_handler_for_category(self, category):
        """Map category to corresponding handler method"""
        category_handlers = {
            "CORE": lambda cmd, args: self.server.strings.handle_command(cmd, self.server.data_store.store, *args),
            "STRING": lambda cmd, args: self.server.strings.handle_command(cmd, self.server.data_store.store, *args),
            "LIST": lambda cmd, args: self.server.lists.handle_command(cmd, self.server.data_store.store, *args),
            "SET": lambda cmd, args: self.server.sets.handle_command(cmd, self.server.data_store.store, *args),
            "HASH": lambda cmd, args: self.server.hashes.handle_command(cmd, self.server.data_store.store, *args),
            "SORTED_SET": lambda cmd, args: self.server.sorted_sets.handle_command(cmd, self.server.data_store.store, *args),
            "STREAM": lambda cmd, args: self.server.streams.handle_command(cmd, self.server.data_store.store, *args),
            "JSON": lambda cmd, args: self.server.json_type.handle_command(cmd, self.server.data_store.store, *args),
            "GEOSPATIAL": lambda cmd, args: self.server.geospatial.handle_command(cmd, self.server.data_store.store, *args),
            "BITMAP": lambda cmd, args: self.server.bitmaps.handle_command(cmd, self.server.data_store.store, *args),
            "BITFIELD": lambda cmd, args: self.server.bitfields.handle_command(cmd, self.server.data_store.store, *args),
            "HYPERLOGLOG": lambda cmd, args: self.server.hyperloglogs.handle_command(cmd, self.server.data_store.store, *args),
            "TIMESERIES": lambda cmd, args: self.server.timeseries.handle_command(cmd, self.server.data_store.store, *args),
            "VECTOR": lambda cmd, args: self.server.vectors.handle_command(cmd, self.server.data_store.store, *args),
            "DOCUMENT": lambda cmd, args: self.server.documents.handle_command(cmd, self.server.data_store.store, *args),
            "TRANSACTION": lambda cmd, args: self._handle_transaction(cmd, args),
            "PUBSUB": lambda cmd, args: self.server.handle_pubsub(cmd, args, None),
            "PERSISTENCE": lambda cmd, args: self.server.handle_persistence(cmd, args, None)
        }
        return category_handlers.get(category)

    def route_command(self, command_parts):
        """Route the parsed command to the corresponding handler"""
        if not command_parts:
            return "-ERR Empty command\r\n"

        cmd = command_parts[0].upper()
        args = command_parts[1:]

        logger.info(f"Routing command: {cmd} with args: {args}")

        # Handle transactions first
        if self.in_transaction and cmd != "EXEC" and cmd != "DISCARD":
            self.transaction_queue.append([cmd] + args)
            return "+QUEUED\r\n"

        # Get the appropriate handler for the command
        handler = self.command_handlers.get(cmd)
        if handler:
            try:
                return handler(cmd, args)
            except Exception as e:
                logger.error(f"Error executing command {cmd}: {e}")
                return f"-ERR {str(e)}\r\n"
        
        return f"-ERR Unknown command '{cmd}'\r\n"

    def _handle_transaction(self, cmd, args):
        """Handle transaction-related commands"""
        if cmd == "MULTI":
            self.in_transaction = True
            return "+OK\r\n"
        elif cmd == "EXEC":
            if not self.in_transaction:
                return "-ERR No transaction started\r\n"
            return self._execute_transaction()
        elif cmd == "DISCARD":
            self.transaction_queue.clear()
            self.in_transaction = False
            return "+OK\r\n"
        return "-ERR Unknown transaction command\r\n"

    def _execute_transaction(self):
        """Execute all commands in the transaction queue"""
        results = []
        try:
            for cmd in self.transaction_queue:
                result = self.server.process_command(cmd)
                results.append(result)
        finally:
            self.transaction_queue.clear()
            self.in_transaction = False
        return f"*{len(results)}\r\n{''.join(results)}"
