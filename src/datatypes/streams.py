# src/datatypes/streams.py
from src.logger import setup_logger
import threading
import time

logger = setup_logger("streams")

class Streams:
    def __init__(self):
        self.lock = threading.Lock()

    def xadd(self, store, key, entry_id, **fields):
        """
        Appends an entry to the stream.
        """
        with self.lock:
            if key not in store:
                store[key] = {}

            if not isinstance(store[key], dict):
                return "ERR Key is not a stream"

            # Auto-generate ID if not provided
            if entry_id == "*":
                entry_id = f"{int(time.time() * 1000)}-{len(store[key])}"

            if entry_id in store[key]:
                return "ERR Entry ID already exists"

            store[key][entry_id] = fields
            logger.info(f"XADD {key} {entry_id} -> {fields}")
            return entry_id

    def xread(self, store, key, count=None, last_id="0-0"):
        """
        Reads entries from the stream starting after the specified last_id.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            entries = sorted(store[key].items())
            start_index = 0

            # Find the starting point based on last_id
            for idx, (entry_id, _) in enumerate(entries):
                if entry_id > last_id:
                    start_index = idx
                    break

            result = entries[start_index: start_index + int(count)] if count else entries[start_index:]
            logger.info(f"XREAD {key} from {last_id} -> {result}")
            return result

    def xrange(self, store, key, start="0-0", end="+", count=None):
        """
        Retrieves a range of entries from the stream.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            entries = sorted(store[key].items())

            # Convert + to the max possible value
            if end == "+":
                end = entries[-1][0] if entries else "0-0"

            result = [(entry_id, data) for entry_id, data in entries if start <= entry_id <= end]
            if count:
                result = result[:int(count)]

            logger.info(f"XRANGE {key} {start} {end} -> {result}")
            return result

    def xlen(self, store, key):
        """
        Returns the number of entries in the stream.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return 0
            length = len(store[key])
            logger.info(f"XLEN {key} -> {length}")
            return length
