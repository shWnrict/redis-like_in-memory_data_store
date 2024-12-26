# src/datatypes/strings.py
from src.logger import setup_logger
import threading

logger = setup_logger("strings")

class Strings:
    def __init__(self):
        self.lock = threading.Lock()

    def set(self, store, key, value):
        """
        Sets a string value for a key.
        """
        with self.lock:
            store[key] = value
            logger.info(f"SET {key} -> {value}")
            return "OK"

    def get(self, store, key):
        """
        Gets the string value for a key.
        """
        with self.lock:
            value = store.get(key)
            logger.info(f"GET {key} -> {value}")
            return value if value is not None else "(nil)"

    def append(self, store, key, value):
        """
        Appends a value to an existing string or sets it if key does not exist.
        """
        with self.lock:
            if key not in store:
                store[key] = value
            else:
                store[key] += value
            logger.info(f"APPEND {key} -> {store[key]}")
            return len(store[key])

    def strlen(self, store, key):
        """
        Returns the length of the string for a key.
        """
        with self.lock:
            if key not in store:
                return 0
            length = len(store[key])
            logger.info(f"STRLEN {key} -> {length}")
            return length

    def incr(self, store, key):
        return self._increment(store, key, 1)

    def decr(self, store, key):
        return self._increment(store, key, -1)

    def incrby(self, store, key, increment):
        try:
            increment = int(increment)
        except ValueError:
            return "ERR increment is not an integer"
        return self._increment(store, key, increment)

    def decrby(self, store, key, decrement):
        try:
            decrement = int(decrement)
        except ValueError:
            return "ERR decrement is not an integer"
        return self._increment(store, key, -decrement)

    def _increment(self, store, key, delta):
        """
        Helper method to increment or decrement a string value as an integer.
        """
        with self.lock:
            if key not in store:
                store[key] = "0"
            try:
                store[key] = str(int(store[key]) + delta)
                logger.info(f"INCR/DECR {key} -> {store[key]}")
                return store[key]
            except ValueError:
                return "ERR Value is not an integer"

    def getrange(self, store, key, start, end):
        """
        Gets a substring of the string value for a key.
        """
        with self.lock:
            if key not in store:
                return "(nil)"
            start, end = int(start), int(end)
            value = store[key][start:end + 1]
            logger.info(f"GETRANGE {key} [{start}:{end}] -> {value}")
            return value

    def setrange(self, store, key, offset, value):
        """
        Overwrites part of the string value at a key starting at the specified offset.
        """
        with self.lock:
            if key not in store:
                store[key] = "\0" * offset + value
            else:
                original = store[key]
                store[key] = (original[:offset] + value +
                              original[offset + len(value):])
            logger.info(f"SETRANGE {key} [{offset}] -> {store[key]}")
            return len(store[key])
