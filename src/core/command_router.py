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
        
        self.handlers = {
            "SET": self.server.strings.handle_command,
            "GET": self.server.strings.handle_command,
            "DEL": self.server.strings.handle_command,
            "EXISTS": self.server.strings.handle_command,
            "EXPIRE": self.server.strings.handle_command,
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
            "ZADD": self.server.sorted_sets.handle_command,
            "ZRANGE": self.server.sorted_sets.handle_command,
            # Add other command mappings as needed
        }

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
        if self.in_transaction and cmd not in {"EXEC", "DISCARD"}:
            self.transaction_queue.append([cmd] + args)
            return "+QUEUED\r\n"

        # Get the appropriate handler for the command
        handler = self.handlers.get(cmd)
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
