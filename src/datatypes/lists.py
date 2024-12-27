# src/datatypes/lists.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType

logger = setup_logger("lists")

class Lists(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)

    def _validate_key_is_list(self, key):
        if key not in self.store:
            self.store[key] = []
        if not isinstance(self.store[key], list):
            logger.error("ERR Key is not a list")
            return False
        return True

    def lpush(self, key, *values):
        """
        Pushes values to the left of the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "ERR Key is not a list"
            self.store[key] = list(values) + self.store[key]
            logger.info(f"LPUSH {key} -> {self.store[key]}")
            return len(self.store[key])

    def rpush(self, key, *values):
        """
        Pushes values to the right of the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "ERR Key is not a list"
            self.store[key].extend(values)
            logger.info(f"RPUSH {key} -> {self.store[key]}")
            return len(self.store[key])

    def lpop(self, key):
        """
        Removes and returns the first element of the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "(nil)"
            if not self.store[key]:
                return "(nil)"
            value = self.store[key].pop(0)
            logger.info(f"LPOP {key} -> {value}")
            return value

    def rpop(self, key):
        """
        Removes and returns the last element of the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "(nil)"
            if not self.store[key]:
                return "(nil)"
            value = self.store[key].pop()
            logger.info(f"RPOP {key} -> {value}")
            return value

    def lrange(self, key, start, end):
        """
        Returns a range of elements from the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "(nil)"
            try:
                start, end = int(start), int(end)
            except ValueError:
                return "ERR start or end is not an integer"
            
            # Adjust negative indices
            if start < 0:
                start = max(0, len(self.store[key]) + start)
            if end < 0:
                end = len(self.store[key]) + end

            result = self.store[key][start:end + 1]
            logger.info(f"LRANGE {key} [{start}:{end}] -> {result}")
            return result

    def lindex(self, key, index):
        """
        Returns the element at the specified index in the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "(nil)"
            try:
                index = int(index)
            except ValueError:
                return "ERR index is not an integer"
            if index < 0 or index >= len(self.store[key]):
                return "(nil)"
            value = self.store[key][index]
            logger.info(f"LINDEX {key} [{index}] -> {value}")
            return value

    def lset(self, key, index, value):
        """
        Sets the element at a specified index in the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return "ERR Key is not a list"
            try:
                index = int(index)
            except ValueError:
                return "ERR index is not an integer"
            if index < 0 or index >= len(self.store[key]):
                return "ERR Index out of range"
            self.store[key][index] = value
            logger.info(f"LSET {key} [{index}] -> {value}")
            return "OK"

    def llen(self, key):
        """
        Returns the length of the list.
        """
        with self.lock:
            if not self._validate_key_is_list(key):
                return 0
            length = len(self.store[key])
            logger.info(f"LLEN {key} -> {length}")
            return length

    def handle_command(self, cmd, store, *args):
        if cmd == "LPUSH":
            return self.lpush(args[0], *args[1:])
        elif cmd == "RPUSH":
            return self.rpush(args[0], *args[1:])
        elif cmd == "LPOP":
            return self.lpop(args[0])
        elif cmd == "RPOP":
            return self.rpop(args[0])
        elif cmd == "LRANGE":
            return self.lrange(args[0], args[1], args[2])
        elif cmd == "LINDEX":
            return self.lindex(args[0], args[1])
        elif cmd == "LSET":
            return self.lset(args[0], args[1], args[2])
        elif cmd == "LLEN":
            return self.llen(args[0])
        return "ERR Unknown command"
