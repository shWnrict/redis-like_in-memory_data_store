# src/datatypes/lists.py
from src.logger import setup_logger
import threading

logger = setup_logger("lists")

class Lists:
    def __init__(self):
        self.lock = threading.Lock()

    def lpush(self, store, key, *values):
        """
        Pushes values to the left of the list.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            if not isinstance(store[key], list):
                return "ERR Key is not a list"
            store[key] = list(values) + store[key]
            logger.info(f"LPUSH {key} -> {store[key]}")
            return len(store[key])

    def rpush(self, store, key, *values):
        """
        Pushes values to the right of the list.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            if not isinstance(store[key], list):
                return "ERR Key is not a list"
            store[key].extend(values)
            logger.info(f"RPUSH {key} -> {store[key]}")
            return len(store[key])

    def lpop(self, store, key):
        """
        Removes and returns the first element of the list.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list) or not store[key]:
                return "(nil)"
            value = store[key].pop(0)
            logger.info(f"LPOP {key} -> {value}")
            return value

    def rpop(self, store, key):
        """
        Removes and returns the last element of the list.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list) or not store[key]:
                return "(nil)"
            value = store[key].pop()
            logger.info(f"RPOP {key} -> {value}")
            return value

    def lrange(self, store, key, start, end):
        """
        Returns a range of elements from the list.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "(nil)"
            start, end = int(start), int(end)
            result = store[key][start:end + 1]
            logger.info(f"LRANGE {key} [{start}:{end}] -> {result}")
            return result

    def lindex(self, store, key, index):
        """
        Returns the element at the specified index in the list.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "(nil)"
            index = int(index)
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
            if key not in store or not isinstance(store[key], list):
                return "ERR Key is not a list"
            index = int(index)
            if index < 0 or index >= len(store[key]):
                return "ERR Index out of range"
            store[key][index] = value
            logger.info(f"LSET {key} [{index}] -> {value}")
            return "OK"
