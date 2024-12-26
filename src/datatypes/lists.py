# src/datatypes/lists.py
from src.logger import setup_logger
import threading

logger = setup_logger("lists")

class Lists:
    def __init__(self):
        self.lock = threading.Lock()

    def _validate_key_is_list(self, store, key):
        if key not in store:
            store[key] = []
        if not isinstance(store[key], list):
            return "ERR Key is not a list"
        return None

    def lpush(self, store, key, *values):
        """
        Pushes values to the left of the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return error
            store[key] = list(values) + store[key]
            logger.info(f"LPUSH {key} -> {store[key]}")
            return len(store[key])

    def rpush(self, store, key, *values):
        """
        Pushes values to the right of the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return error
            store[key].extend(values)
            logger.info(f"RPUSH {key} -> {store[key]}")
            return len(store[key])

    def lpop(self, store, key):
        """
        Removes and returns the first element of the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return "(nil)"
            if not store[key]:
                return "(nil)"
            value = store[key].pop(0)
            logger.info(f"LPOP {key} -> {value}")
            return value

    def rpop(self, store, key):
        """
        Removes and returns the last element of the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return "(nil)"
            if not store[key]:
                return "(nil)"
            value = store[key].pop()
            logger.info(f"RPOP {key} -> {value}")
            return value

    def lrange(self, store, key, start, end):
        """
        Returns a range of elements from the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return "(nil)"
            try:
                start, end = int(start), int(end)
            except ValueError:
                return "ERR start or end is not an integer"
            
            # Adjust negative indices
            if start < 0:
                start = max(0, len(store[key]) + start)
            if end < 0:
                end = len(store[key]) + end

            result = store[key][start:end + 1]
            logger.info(f"LRANGE {key} [{start}:{end}] -> {result}")
            return result

    def lindex(self, store, key, index):
        """
        Returns the element at the specified index in the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return "(nil)"
            try:
                index = int(index)
            except ValueError:
                return "ERR index is not an integer"
            if index < 0 or index >= len(store[key]):
                return "(nil)"
            value = store[key][index]
            logger.info(f"LINDEX {key} [{index}] -> {value}")
            return value

    def lset(self, store, key, index, value):
        """
        Sets the element at a specified index in the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return error
            try:
                index = int(index)
            except ValueError:
                return "ERR index is not an integer"
            if index < 0 or index >= len(store[key]):
                return "ERR Index out of range"
            store[key][index] = value
            logger.info(f"LSET {key} [{index}] -> {value}")
            return "OK"

    def llen(self, store, key):
        """
        Returns the length of the list.
        """
        with self.lock:
            error = self._validate_key_is_list(store, key)
            if error:
                return 0
            length = len(store[key])
            logger.info(f"LLEN {key} -> {length}")
            return length

    def handle_command(self, cmd, store, *args):
        if cmd == "LPUSH":
            return self.lpush(store, args[0], *args[1:])
        elif cmd == "RPUSH":
            return self.rpush(store, args[0], *args[1:])
        elif cmd == "LPOP":
            return self.lpop(store, args[0])
        elif cmd == "RPOP":
            return self.rpop(store, args[0])
        elif cmd == "LRANGE":
            return self.lrange(store, args[0], args[1], args[2])
        elif cmd == "LINDEX":
            return self.lindex(store, args[0], args[1])
        elif cmd == "LSET":
            return self.lset(store, args[0], args[1], args[2])
        elif cmd == "LLEN":
            return self.llen(store, args[0])
        return "ERR Unknown command"
