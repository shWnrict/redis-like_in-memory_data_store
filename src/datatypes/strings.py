# src/datatypes/strings.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType

logger = setup_logger("strings")

class Strings(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)

    def set(self, key, value):
        """
        Sets a string value for a key.
        """
        with self.lock:
            self.store[key] = value
            logger.info(f"SET {key} -> {value}")
            return "OK"

    def get(self, key):
        """
        Gets the string value for a key.
        """
        with self.lock:
            value = self.store.get(key)
            logger.info(f"GET {key} -> {value}")
            return value if value is not None else "(nil)"

    def append(self, key, value):
        """
        Appends a value to an existing string or sets it if key does not exist.
        """
        with self.lock:
            if key not in self.store:
                self.store[key] = value
            else:
                self.store[key] += value
            logger.info(f"APPEND {key} -> {self.store[key]}")
            return len(self.store[key])

    def strlen(self, key):
        """
        Returns the length of the string for a key.
        """
        with self.lock:
            if key not in self.store:
                return 0
            length = len(self.store[key])
            logger.info(f"STRLEN {key} -> {length}")
            return length

    def incr(self, key):
        return int(self._increment(key, 1))

    def decr(self, key):
        return int(self._increment(key, -1))

    def incrby(self, key, increment):
        try:
            increment = int(increment)
        except ValueError:
            return "ERR increment is not an integer"
        return int(self._increment(key, increment))

    def decrby(self, key, decrement):
        try:
            decrement = int(decrement)
        except ValueError:
            return "ERR decrement is not an integer"
        return int(self._increment(key, -decrement))

    def _increment(self, key, delta):
        """
        Helper method to increment or decrement a string value as an integer.
        """
        with self.lock:
            if key not in self.store:
                self.store[key] = "0"
            try:
                self.store[key] = str(int(self.store[key]) + delta)
                logger.info(f"INCR/DECR {key} -> {self.store[key]}")
                return self.store[key]
            except ValueError:
                return "ERR Value is not an integer"

    def getrange(self, key, start, end):
        """
        Gets a substring of the string value for a key.
        """
        with self.lock:
            if key not in self.store:
                return "(nil)"
            start, end = int(start), int(end)
            value = self.store[key][start:end + 1]
            logger.info(f"GETRANGE {key} [{start}:{end}] -> {value}")
            return value

    def setrange(self, key, offset, value):
        """
        Overwrites part of the string value at a key starting at the specified offset.
        """
        with self.lock:
            offset = int(offset)
            if key not in self.store:
                self.store[key] = "\0" * offset + value
            else:
                original = self.store[key]
                self.store[key] = (original[:offset] + value +
                              original[offset + len(value):])
            logger.info(f"SETRANGE {key} [{offset}] -> {self.store[key]}")
            return len(self.store[key])

    def delete(self, key):
        """
        Deletes a key from the store.
        """
        with self.lock:
            if key in self.store:
                del self.store[key]
                logger.info(f"DEL {key}")
                return 1
            return 0

    def exists(self, key):
        """
        Checks if a key exists in the store.
        """
        with self.lock:
            exists = key in self.store
            logger.info(f"EXISTS {key} -> {exists}")
            return 1 if exists else 0

    def expire(self, key, ttl):
        """
        Sets an expiration time for a key.
        """
        with self.lock:
            if key in self.store:
                if self.expiry_manager:
                    self.expiry_manager.set_expiry(key, ttl)
                    logger.info(f"EXPIRE {key} {ttl}")
                    return 1
                else:
                    logger.error("Expiry manager not set")
                    return "ERR Expiry manager not set"
            return 0

    def handle_command(self, cmd, store, *args):
        if cmd == "SET":
            return self.set(args[0], args[1])
        elif cmd == "GET":
            return self.get(args[0])
        elif cmd == "DEL":
            return self.delete(args[0])
        elif cmd == "EXISTS":
            return self.exists(args[0])
        elif cmd == "EXPIRE":
            return self.expire(args[0], int(args[1]))
        elif cmd == "INCR":
            return self.incr(args[0])
        elif cmd == "DECR":
            return self.decr(args[0])
        elif cmd == "INCRBY":
            return self.incrby(args[0], args[1])
        elif cmd == "DECRBY":
            return self.decrby(args[0], args[1])
        elif cmd == "APPEND":
            return self.append(args[0], args[1])
        elif cmd == "STRLEN":
            return self.strlen(args[0])
        elif cmd == "GETRANGE":
            return self.getrange(args[0], args[1], args[2])
        elif cmd == "SETRANGE":
            return self.setrange(args[0], args[1], args[2])
        return "ERR Unknown command"
