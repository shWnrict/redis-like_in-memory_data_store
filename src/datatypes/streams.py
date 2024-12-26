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
                store[key] = {"entries": {}, "group_data": {}}

            if not isinstance(store[key], dict):
                return "ERR Key is not a stream"

            # Auto-generate ID if not provided
            if entry_id == "*":
                entry_id = f"{int(time.time() * 1000)}-{len(store[key]['entries'])}"

            if entry_id in store[key]["entries"]:
                return "ERR Entry ID already exists"

            store[key]["entries"][entry_id] = fields
            logger.info(f"XADD {key} {entry_id} -> {fields}")
            return entry_id

    def xread(self, store, key, count=None, last_id="0-0"):
        """
        Reads entries from the stream starting after the specified last_id.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            entries = sorted(store[key]["entries"].items())
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

            entries = sorted(store[key]["entries"].items())

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
            length = len(store[key]["entries"])
            logger.info(f"XLEN {key} -> {length}")
            return length
        
    def __initialize_stream(self, store, key):
        if key not in store:
            store[key] = {"entries": {}, "group_data": {}}

    def xgroup_create(self, store, key, group_name, start_id="0-0"):
        """
        Creates a consumer group for the stream.
        """
        with self.lock:
            self.__initialize_stream(store, key)
            stream = store[key]

            if group_name in stream["group_data"]:
                return "ERR Group already exists"
            
            stream["group_data"][group_name] = {"consumers": {}, "pending": {}}
            logger.info(f"XGROUP CREATE {key} {group_name} -> Start at {start_id}")
            return "OK"

    def xreadgroup(self, store, group_name, consumer_name, key, count=None, last_id=">"):
        """
        Reads messages for a specific consumer in the group.
        """
        with self.lock:
            if key not in store:
                return "ERR Stream does not exist"
            stream = store[key]

            if group_name not in stream["group_data"]:
                return "ERR Group does not exist"

            group = stream["group_data"][group_name]

            if consumer_name not in group["consumers"]:
                group["consumers"][consumer_name] = []

            entries = list(stream["entries"].items())
            result = []

            for entry_id, entry_data in entries:
                if entry_id > last_id or entry_id in group["pending"]:
                    group["pending"][entry_id] = {"data": entry_data, "consumer": consumer_name}
                    group["consumers"][consumer_name].append(entry_id)
                    result.append((entry_id, entry_data))
                    if count and len(result) >= int(count):
                        break

            logger.info(f"XREADGROUP {group_name} {consumer_name} {key} -> {result}")
            return result

    def xack(self, store, key, group_name, *entry_ids):
        """
        Acknowledges the processing of messages in the group.
        """
        with self.lock:
            if key not in store or group_name not in store[key]["group_data"]:
                return 0
            group = store[key]["group_data"][group_name]

            acked = 0
            for entry_id in entry_ids:
                if entry_id in group["pending"]:
                    del group["pending"][entry_id]
                    acked += 1

            logger.info(f"XACK {key} {group_name} -> {acked} entries acknowledged")
            return acked