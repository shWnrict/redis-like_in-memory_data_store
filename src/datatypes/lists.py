# src/datatypes/lists.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType
from src.protocol import RESPProtocol  # Import RESPProtocol

logger = setup_logger("lists")

class Lists(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)

    def _validate_key_is_list(self, key):
        if key not in self.store:
            logger.error(f"ERR no such key: {key}")
            return False
        if not isinstance(self.store[key], list):
            logger.error(f"ERR Operation against a key holding the wrong kind of value")
            return False
        return True

    def lpush(self, cmd, store, *args):
        """
        Pushes values to the left of the list.
        LPUSH key value [value ...]
        """
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'LPUSH' command\r\n"
        key, *values = args
        with self.lock:
            current_list = store.setdefault(key, [])
            current_list = list(current_list)  # Ensure it's mutable
            for value in values:
                current_list.insert(0, value)
                logger.debug(f"LPUSH {key} <- {value}")
            store[key] = current_list
            logger.info(f"LPUSH {key} -> {len(current_list)} elements")
            return f":{len(current_list)}\r\n"

    def rpush(self, cmd, store, *args):
        """
        Pushes values to the right of the list.
        RPUSH key value [value ...]
        """
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'RPUSH' command\r\n"
        key, *values = args
        with self.lock:
            current_list = store.setdefault(key, [])
            for value in values:
                current_list.append(value)
                logger.debug(f"RPUSH {key} -> {value}")
            logger.info(f"RPUSH {key} -> {len(current_list)} elements")
            return f":{len(current_list)}\r\n"

    def lpop(self, cmd, store, *args):
        """
        Removes and returns the first element of the list.
        LPOP key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'LPOP' command\r\n"
        key = args[0]
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            if not store[key]:
                return "(nil)\r\n"
            value = store[key].pop(0)
            logger.info(f"LPOP {key} -> {value}")
            return RESPProtocol.format_response(value)

    def rpop(self, cmd, store, *args):
        """
        Removes and returns the last element of the list.
        RPOP key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'RPOP' command\r\n"
        key = args[0]
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            if not store[key]:
                return "(nil)\r\n"
            value = store[key].pop()
            logger.info(f"RPOP {key} -> {value}")
            return RESPProtocol.format_response(value)

    def lrange(self, cmd, store, *args):
        """
        Returns a range of elements from the list.
        LRANGE key start stop
        """
        if len(args) != 3:
            return "-ERR wrong number of arguments for 'LRANGE' command\r\n"
        key, start, stop = args
        try:
            start = int(start)
            stop = int(stop)
        except ValueError:
            return "-ERR start and stop must be integers\r\n"
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            lst = store[key]
            sliced = lst[start:stop+1]  # stop is inclusive
            logger.info(f"LRANGE {key} {start} {stop} -> {len(sliced)} elements")
            return RESPProtocol.format_array(sliced)

    def lindex(self, cmd, store, *args):
        """
        Returns the element at the specified index in the list.
        LINDEX key index
        """
        if len(args) != 2:
            return "-ERR wrong number of arguments for 'LINDEX' command\r\n"
        key, index = args
        try:
            index = int(index)
        except ValueError:
            return "-ERR index must be an integer\r\n"
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            lst = store[key]
            if -len(lst) <= index < len(lst):
                value = lst[index]
                logger.info(f"LINDEX {key} {index} -> {value}")
                return RESPProtocol.format_response(value)
            logger.info(f"LINDEX {key} {index} -> (nil)")
            return "(nil)\r\n"

    def lset(self, cmd, store, *args):
        """
        Sets the element at a specified index in the list.
        LSET key index value
        """
        if len(args) != 3:
            return "-ERR wrong number of arguments for 'LSET' command\r\n"
        key, index, value = args
        try:
            index = int(index)
        except ValueError:
            return "-ERR index must be an integer\r\n"
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            lst = store[key]
            if -len(lst) <= index < len(lst):
                lst[index] = value
                logger.info(f"LSET {key} {index} -> {value}")
                return "+OK\r\n"
            return "-ERR index out of range\r\n"

    def llen(self, cmd, store, *args):
        """
        Returns the length of the list.
        LLEN key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'LLEN' command\r\n"
        key = args[0]
        with self.lock:
            if not self._validate_key_is_list(key):
                return "-ERR Operation against a key holding the wrong kind of value\r\n"
            length = len(store.get(key, []))
            logger.info(f"LLEN {key} -> {length}")
            return f":{length}\r\n"

    def handle_command(self, cmd, store, *args):
        """
        Route the L* commands to their respective methods.
        """
        cmd = cmd.upper()
        if cmd == "LPUSH":
            return self.lpush(cmd, store, *args)
        elif cmd == "RPUSH":
            return self.rpush(cmd, store, *args)
        elif cmd == "LPOP":
            return self.lpop(cmd, store, *args)
        elif cmd == "RPOP":
            return self.rpop(cmd, store, *args)
        elif cmd == "LRANGE":
            return self.lrange(cmd, store, *args)
        elif cmd == "LINDEX":
            return self.lindex(cmd, store, *args)
        elif cmd == "LSET":
            return self.lset(cmd, store, *args)
        elif cmd == "LLEN":
            return self.llen(cmd, store, *args)
        else:
            return "-ERR Unknown list command\r\n"
