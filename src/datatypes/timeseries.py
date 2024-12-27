# src/datatypes/timeseries.py
from src.logger import setup_logger
import threading
import time
from src.datatypes.base import BaseDataType  # Import BaseDataType

logger = setup_logger("timeseries")

class TimeSeries(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def ts_add(self, store, key, timestamp, value):
        """
        Add a data point to the time series.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            store[key].append((timestamp, value))
            store[key].sort(key=lambda x: x[0])  # Ensure sorted by timestamp
            logger.info(f"TS.ADD {key} {timestamp} -> {value}")
            return "OK"

    def ts_range(self, store, key, start, end, aggregation=None, bucket=None):
        """
        Retrieve data points within a range. Supports optional aggregation.
        """
        with self.lock:
            if key not in store:
                return []
            data = store[key]
            filtered = [(ts, val) for ts, val in data if start <= ts <= end]
            
            if aggregation and bucket:
                aggregated = {}
                for ts, val in filtered:
                    bucket_ts = ts - (ts % bucket)
                    if bucket_ts not in aggregated:
                        aggregated[bucket_ts] = []
                    aggregated[bucket_ts].append(val)
                if aggregation == "avg":
                    result = [(ts, sum(vals)/len(vals)) for ts, vals in aggregated.items()]
                elif aggregation == "sum":
                    result = [(ts, sum(vals)) for ts, vals in aggregated.items()]
                else:
                    return "ERR Unsupported aggregation"
                logger.info(f"TS.RANGE {key} {start} {end} with {aggregation} -> {result}")
                return result
            logger.info(f"TS.RANGE {key} {start} {end} -> {filtered}")
            return filtered

    def ts_get(self, store, key, timestamp):
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
        if cmd == "TS.ADD":
            key, timestamp, value = args
            return self.ts_add(store, key, int(timestamp), float(value))
        elif cmd == "TS.RANGE":
            key, start, end = args[:3]
            if len(args) > 3:
                aggregation = args[3].upper()
                bucket = int(args[4])
                return self.ts_range(store, key, int(start), int(end), aggregation, bucket)
            return self.ts_range(store, key, int(start), int(end))
        elif cmd == "TS.GET":
            key, timestamp = args
            return self.ts_get(store, key, int(timestamp))
        return "ERR Unknown command"
