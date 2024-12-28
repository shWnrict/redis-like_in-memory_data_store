# src/datatypes/strings.py
from src.logger import setup_logger
import threading
from src.datatypes.base import BaseDataType
from src.protocol import RESPProtocol  # Import RESPProtocol

logger = setup_logger("strings")

class Strings(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

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

    def append(self, store, key, value):
        with self.lock:
            if key not in store:
                store[key] = ""
            store[key] = str(store[key]) + str(value)
            return len(store[key])

    def strlen(self, store, key):
        with self.lock:
            if key not in store:
                return 0
            return len(str(store[key]))

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

    def getrange(self, store, key, start, end):
        with self.lock:
            if key not in store:
                return ""
            value = str(store[key])
            length = len(value)
            
            # Handle negative indices
            start = length + start if start < 0 else start
            end = length + end if end < 0 else end
            
            # Bound indices
            start = max(0, min(start, length))
            end = max(0, min(end + 1, length))
            
            return value[start:end]

    def setrange(self, store, key, offset, value):
        with self.lock:
            if key not in store:
                store[key] = "\0" * offset
            current = str(store[key])
            prefix = current[:offset]
            suffix = current[offset + len(value):]
            store[key] = prefix + value + suffix
            return len(store[key])

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
        """
        Route string commands to their respective methods.
        """
        cmd = cmd.upper()
        if cmd == "SET":
            return self.set(cmd, store, *args)
        elif cmd == "GET":
            return self.get(cmd, store, *args)
        elif cmd == "DEL":
            return self.delete(cmd, store, *args)
        elif cmd == "EXISTS":
            return self.exists(cmd, store, *args)
        elif cmd == "INCR":
            return self.incr(cmd, store, *args)
        elif cmd == "DECR":
            return self.decr(cmd, store, *args)
        elif cmd == "APPEND":
            if len(args) != 2:
                return "-ERR wrong number of arguments\r\n"
            key, value = args
            length = self.append(store, key, value)
            return f":{length}\r\n"
        elif cmd == "STRLEN":
            if len(args) != 1:
                return "-ERR wrong number of arguments\r\n"
            key = args[0]
            length = self.strlen(store, key)
            return f":{length}\r\n"
        elif cmd == "GETRANGE":
            if len(args) != 3:
                return "-ERR wrong number of arguments\r\n"
            key, start, end = args
            try:
                result = self.getrange(store, key, int(start), int(end))
                return f"${len(result)}\r\n{result}\r\n"
            except ValueError:
                return "-ERR value is not an integer\r\n"
        elif cmd == "SETRANGE":
            if len(args) != 3:
                return "-ERR wrong number of arguments\r\n"
            key, offset, value = args
            try:
                length = self.setrange(store, key, int(offset), value)
                return f":{length}\r\n"
            except ValueError:
                return "-ERR value is not an integer\r\n"
        else:
            return "-ERR Unknown string command\r\n"

    def set(self, cmd, store, *args):
        """
        Set the string value of a key.
        SET key value [EX seconds] [PX milliseconds] [NX|XX]
        """
        if len(args) < 2:
            return "-ERR wrong number of arguments for 'SET' command\r\n"
        key, value, *options = args
        ttl = None
        nx = False
        xx = False
        i = 0
        while i < len(options):
            option = options[i].upper()
            if option == "EX" and i + 1 < len(options):
                ttl = int(options[i + 1])
                i += 2
            elif option == "PX" and i + 1 < len(options):
                ttl = int(options[i + 1]) / 1000  # Convert milliseconds to seconds
                i += 2
            elif option == "NX":
                nx = True
                i += 1
            elif option == "XX":
                xx = True
                i += 1
            else:
                return "-ERR syntax error\r\n"

        with self.lock:
            if nx and key in store:
                return "-ERR key already exists\r\n"
            if xx and key not in store:
                return "-ERR key does not exist\r\n"
            store[key] = value
            if ttl:
                self.expiry_manager.handle_expire(key, ttl)
            logger.info(f"SET {key} -> {value} (TTL={ttl}, NX={nx}, XX={xx})")
            return "+OK\r\n"

    def get(self, cmd, store, *args):
        """
        Get the value of a key.
        GET key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'GET' command\r\n"
        key = args[0]
        with self.lock:
            if self.expiry_manager and self.expiry_manager.is_expired(key):
                del store[key]
                return "(nil)\r\n"
            value = store.get(key)
            logger.info(f"GET {key} -> {value}")
            return RESPProtocol.format_response(value) if value is not None else "(nil)\r\n"

    def delete(self, cmd, store, *args):
        """
        Delete a key.
        DEL key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'DEL' command\r\n"
        key = args[0]
        with self.lock:
            if key in store:
                del store[key]
                if self.expiry_manager:
                    self.expiry_manager.handle_persist(key)
                logger.info(f"DEL {key} -> 1")
                return ":1\r\n"
            logger.info(f"DEL {key} -> 0")
            return ":0\r\n"

    def exists(self, cmd, store, *args):
        """
        Determine if a key exists.
        EXISTS key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'EXISTS' command\r\n"
        key = args[0]
        with self.lock:
            exists = 1 if key in store and not (self.expiry_manager and self.expiry_manager.is_expired(key)) else 0
            logger.info(f"EXISTS {key} -> {exists}")
            return f":{exists}\r\n"

    def incr(self, cmd, store, *args):
        """
        Increment the integer value of a key by one.
        INCR key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'INCR' command\r\n"
        key = args[0]
        with self.lock:
            if self.expiry_manager and self.expiry_manager.is_expired(key):
                store[key] = 0
            value = store.get(key, 0)
            try:
                new_value = int(value) + 1
                store[key] = new_value
                logger.info(f"INCR {key} -> {new_value}")
                return f":{new_value}\r\n"
            except ValueError:
                logger.error(f"INCR {key} -> ERR value is not an integer\r\n")
                return "-ERR value is not an integer\r\n"

    def decr(self, cmd, store, *args):
        """
        Decrement the integer value of a key by one.
        DECR key
        """
        if len(args) != 1:
            return "-ERR wrong number of arguments for 'DECR' command\r\n"
        key = args[0]
        with self.lock:
            if self.expiry_manager and self.expiry_manager.is_expired(key):
                store[key] = 0
            value = store.get(key, 0)
            try:
                new_value = int(value) - 1
                store[key] = new_value
                logger.info(f"DECR {key} -> {new_value}")
                return f":{new_value}\r\n"
            except ValueError:
                logger.error(f"DECR {key} -> ERR value is not an integer\r\n")
                return "-ERR value is not an integer\r\n"
