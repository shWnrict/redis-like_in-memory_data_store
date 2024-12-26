# src/datatypes/strings.py
from src.logger import setup_logger
import threading

logger = setup_logger("strings")

class Strings:
    def __init__(self, expiry_manager=None):
        self.lock = threading.Lock()
        self.expiry_manager = expiry_manager

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
        return int(self._increment(store, key, 1))

    def decr(self, store, key):
        return int(self._increment(store, key, -1))

    def incrby(self, store, key, increment):
        try:
            increment = int(increment)
        except ValueError:
            return "ERR increment is not an integer"
        return int(self._increment(store, key, increment))

    def decrby(self, store, key, decrement):
        try:
            decrement = int(decrement)
        except ValueError:
            return "ERR decrement is not an integer"
        return int(self._increment(store, key, -decrement))

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
            offset = int(offset)
            if key not in store:
                store[key] = "\0" * offset + value
            else:
                original = store[key]
                store[key] = (original[:offset] + value +
                              original[offset + len(value):])
            logger.info(f"SETRANGE {key} [{offset}] -> {store[key]}")
            return len(store[key])

    def delete(self, store, key):
        """
        Deletes a key from the store.
        """
        with self.lock:
            if key in store:
                del store[key]
                logger.info(f"DEL {key}")
                return 1
            return 0

    def exists(self, store, key):
        """
        Checks if a key exists in the store.
        """
        with self.lock:
            exists = key in store
            logger.info(f"EXISTS {key} -> {exists}")
            return 1 if exists else 0

    def expire(self, store, key, ttl):
        """
        Sets an expiration time for a key.
        """
        with self.lock:
            if key in store:
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
            return self.set(store, args[0], args[1])
        elif cmd == "GET":
            return self.get(store, args[0])
        elif cmd == "DEL":
            return self.delete(store, args[0])
        elif cmd == "EXISTS":
            return self.exists(store, args[0])
        elif cmd == "EXPIRE":
            return self.expire(store, args[0], int(args[1]))
        elif cmd == "INCR":
            return self.incr(store, args[0])
        elif cmd == "DECR":
            return self.decr(store, args[0])
        elif cmd == "INCRBY":
            return self.incrby(store, args[0], args[1])
        elif cmd == "DECRBY":
            return self.decrby(store, args[0], args[1])
        elif cmd == "APPEND":
            return self.append(store, args[0], args[1])
        elif cmd == "STRLEN":
            return self.strlen(store, args[0])
        elif cmd == "GETRANGE":
            return self.getrange(store, args[0], args[1], args[2])
        elif cmd == "SETRANGE":
            return self.setrange(store, args[0], args[1], args[2])
        return "ERR Unknown command"
