# src/datatypes/timeseries.py
import threading  # Added import
import time  # Added import

from src.logger import setup_logger
from src.datatypes.base import BaseDataType  # Import BaseDataType
from src.protocol import RESPProtocol

logger = setup_logger("timeseries")

class TimeSeries(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    @classmethod
    def create(cls, store, key):
        """
        Create a new time series key.
        """
        with cls(store).lock:
            if key in store:
                logger.warning(f"TS.CREATE failed: Key '{key}' already exists.")
                return "ERR Key already exists"
            store[key] = []
            logger.info(f"TS.CREATE {key} -> OK")
            return "OK"

    def add(self, store, key, value, timestamp=None):
        """
        Adds a value with an optional timestamp to the time series.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            if timestamp is None:
                timestamp = int(time.time() * 1000)
            store[key].append((timestamp, value))
            logger.info(f"TS.ADD {key} {timestamp} -> {value}")
            return timestamp

    def range(self, store, key, start, end, count=None):
        """
        Retrieves a range of entries from the time series.
        """
        with self.lock:
            if key not in store:
                return []
            entries = store[key]
            filtered = [entry for entry in entries if start <= entry[0] <= end]
            if count:
                filtered = filtered[:count]
            logger.info(f"TS.RANGE {key} {start} {end} -> {filtered}")
            return filtered

    def len(self, store, key):
        """
        Returns the number of entries in the time series.
        """
        with self.lock:
            if key not in store:
                return 0
            length = len(store[key])
            logger.info(f"TS.LEN {key} -> {length}")
            return length

    def get(self, store, key, timestamp):
        """
        Get the value at a specific timestamp.
        """
        with self.lock:
            if key not in store:
                return "(nil)"
            for ts, val in store[key]:
                if ts == timestamp:
                    logger.info(f"TS.GET {key} {timestamp} -> {val}")
                    return val
            logger.info(f"TS.GET {key} {timestamp} -> (nil)")
            return "(nil)"

    def handle_command(self, cmd, store, *args):
        """
        Dispatch time series commands to the appropriate methods.
        """
        cmd = cmd.upper()
        if cmd == "TS.CREATE":
            if len(args) != 1:
                return "-ERR wrong number of arguments for 'TS.CREATE' command\r\n"
            key = args[0]
            result = self.create(store, key)
            return f"+{result}\r\n"
        elif cmd == "TS.ADD":
            key, value = args[:2]
            timestamp = int(args[2]) if len(args) > 2 else None
            return self.add(store, key, value, timestamp)
        elif cmd == "TS.RANGE":
            key, start, end = args[:3]
            count = int(args[3]) if len(args) > 3 else None
            return self.range(store, key, start, end, count)
        elif cmd == "TS.LEN":
            key = args[0]
            return self.len(store, key)
        elif cmd == "TS.GET":
            if len(args) != 2:
                return "-ERR wrong number of arguments for 'TS.GET' command\r\n"
            key, timestamp = args
            result = self.ts_get(store, key, int(timestamp))
            return RESPProtocol.format_response(result)
        return "-ERR Unknown TS command\r\n"
