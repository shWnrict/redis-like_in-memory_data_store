# src/datatypes/streams.py
from src.logger import setup_logger
from src.datatypes.base import BaseDataType  # Ensure correct import
import threading
import time

logger = setup_logger("streams")

class Streams(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def xadd(self, store, key, entry_id, *args):
        """
        Appends an entry to the stream.
        """
        nomkstream = False
        maxlen = None
        fields = {}

        # Parse optional arguments
        i = 0
        while i < len(args):
            if args[i].upper() == "NOMKSTREAM":
                nomkstream = True
                i += 1
            elif args[i].upper() == "MAXLEN":
                if i + 1 < len(args) and args[i + 1] == "~":
                    i += 1  # Skip the "~" argument
                if i + 1 < len(args):
                    maxlen = int(args[i + 1])
                    i += 2
                else:
                    return "-ERR Invalid MAXLEN argument"
            else:
                break

        # Parse field-value pairs
        while i < len(args):
            if i + 1 < len(args):
                fields[args[i]] = args[i + 1]
                i += 2
            else:
                return "-ERR Invalid field-value pairs"

        with self.lock:
            if key not in store:
                if nomkstream:
                    return "-ERR Stream does not exist"
                store[key] = {"entries": {}, "group_data": {}}

            if not isinstance(store[key], dict):
                return "-ERR Key is not a stream"

            # Auto-generate ID if not provided
            if entry_id == "*":
                entry_id = f"{int(time.time() * 1000)}-{len(store[key]['entries'])}"

            if entry_id in store[key]["entries"]:
                return "-ERR Entry ID already exists"

            store[key]["entries"][entry_id] = fields
            logger.info(f"XADD {key} {entry_id} -> {fields}")

            # Trim the stream if MAXLEN is specified
            if maxlen is not None:
                self._trim_stream(store[key]["entries"], maxlen)

            return entry_id

    def _trim_stream(self, entries, maxlen):
        """
        Trim the stream to the specified maximum length.
        """
        while len(entries) > maxlen:
            oldest_entry = min(entries.keys())
            del entries[oldest_entry]

    def xread(self, store, *args):
        """
        Reads entries from one or multiple streams.
        """
        count = None
        block = None
        streams = {}
        
        # Parse optional arguments
        i = 0
        while i < len(args):
            if args[i].upper() == "COUNT":
                if i + 1 < len(args):
                    count = int(args[i + 1])
                    i += 2
                else:
                    return "-ERR Invalid COUNT argument"
            elif args[i].upper() == "BLOCK":
                if i + 1 < len(args):
                    block = int(args[i + 1])
                    i += 2
                else:
                    return "-ERR Invalid BLOCK argument"
            elif args[i].upper() == "STREAMS":
                i += 1
                break
            else:
                return "-ERR Invalid argument"

        # Parse streams and IDs
        while i < len(args):
            stream_key = args[i]
            last_id = args[i + 1] if i + 1 < len(args) else "0-0"
            streams[stream_key] = last_id
            i += 2

        results = []
        with self.lock:
            for stream_key, last_id in streams.items():
                if stream_key not in store or not isinstance(store[stream_key], dict):
                    continue

                entries = sorted(store[stream_key]["entries"].items())
                start_index = 0

                # Find the starting point based on last_id
                for idx, (entry_id, _) in enumerate(entries):
                    if entry_id > last_id:
                        start_index = idx
                        break

                result = entries[start_index: start_index + count] if count else entries[start_index:]
                if result:
                    formatted_entries = []
                    for entry_id, data in result:
                        fields = []
                        for field, value in data.items():
                            fields.extend([field, value])
                        formatted_entries.append([entry_id, fields])
                    results.append([stream_key, formatted_entries])

        logger.info(f"XREAD {streams} -> {results}")
        return results

    def xrange(self, store, key, start="0-0", end="+", count=None):
        """
        Retrieves a range of entries from the stream.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return []

            entries = sorted(store[key]["entries"].items())

            # Convert special IDs
            if start == "-":
                start = "0-0"
            if end == "+":
                end = entries[-1][0] if entries else "0-0"

            # Handle exclusive ranges
            if start.startswith("("):
                start = start[1:]
                entries = [(entry_id, data) for entry_id, data in entries if entry_id > start]
            else:
                entries = [(entry_id, data) for entry_id, data in entries if entry_id >= start]

            if end.startswith("("):
                end = end[1:]
                entries = [(entry_id, data) for entry_id, data in entries if entry_id < end]
            else:
                entries = [(entry_id, data) for entry_id, data in entries if entry_id <= end]

            # Apply count if specified
            if count is not None:
                entries = entries[:count]

            formatted_entries = []
            for entry_id, data in entries:
                fields = []
                for field, value in data.items():
                    fields.extend([field, value])
                formatted_entries.append([entry_id, fields])

            logger.info(f"XRANGE {key} {start} {end} -> {formatted_entries}")
            return formatted_entries

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
                return "-ERR Group already exists"
            
            stream["group_data"][group_name] = {"consumers": {}, "pending": {}}
            logger.info(f"XGROUP CREATE {key} {group_name} -> Start at {start_id}")
            return "OK"

    def xreadgroup(self, store, group_name, consumer_name, key, count=None, last_id=">"):
        """
        Reads messages for a specific consumer in the group.
        """
        with self.lock:
            if key not in store:
                return "-ERR Stream does not exist"
            stream = store[key]

            if group_name not in stream["group_data"]:
                return "-ERR Group does not exist"

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

    def handle_command(self, cmd, store, *args):
        """
        Dispatch stream commands to the appropriate methods.
        """
        cmd = cmd.upper()
        if cmd == "XADD":
            key, entry_id, *field_pairs = args
            fields = dict(zip(field_pairs[::2], field_pairs[1::2]))
            return self.xadd(store, key, entry_id, *field_pairs)
        elif cmd == "XREAD":
            return self.xread(store, *args)
        elif cmd == "XRANGE":
            key, start, end, count = args
            return self.xrange(store, key, start, end, count)
        elif cmd == "XLEN":
            key = args[0]
            return self.xlen(store, key)
        else:
            return "-ERR Unknown stream command"