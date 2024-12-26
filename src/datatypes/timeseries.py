# src/datatypes/timeseries.py
from src.logger import setup_logger
import threading

logger = setup_logger("timeseries")

class TimeSeries:
    def __init__(self):
        self.lock = threading.Lock()

    def _validate_timestamp(self, timestamp):
        try:
            return int(timestamp)
        except ValueError:
            return None

    def _validate_value(self, value):
        try:
            return float(value)
        except ValueError:
            return None

    def create(self, store, key):
        """
        Creates a time series structure.
        """
        with self.lock:
            if key in store:
                return "ERR Key already exists"
            store[key] = []
            logger.info(f"TS.CREATE {key}")
            return "OK"

    def add(self, store, key, timestamp, value):
        """
        Adds a data point to the time series.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Time series does not exist"

            timestamp = self._validate_timestamp(timestamp)
            value = self._validate_value(value)

            if timestamp is None or value is None:
                return "ERR Invalid timestamp or value"

            store[key].append((timestamp, value))
            store[key].sort(key=lambda x: x[0])  # Ensure sorted order
            logger.info(f"TS.ADD {key} {timestamp} -> {value}")
            return "OK"

    def get(self, store, key):
        """
        Gets the latest data point in the time series.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list) or not store[key]:
                return "(nil)"
            latest = store[key][-1]
            logger.info(f"TS.GET {key} -> {latest}")
            return latest

    def range(self, store, key, start, end, aggregation=None):
        """
        Retrieves data points in a given time range, with optional aggregation.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return []

            start, end = self._validate_timestamp(start), self._validate_timestamp(end)
            if start is None or end is None:
                return "ERR Invalid start or end timestamp"

            result = [(ts, val) for ts, val in store[key] if start <= ts <= end]

            if aggregation:
                return self._apply_aggregation(result, aggregation)

            logger.info(f"TS.RANGE {key} [{start}:{end}] -> {result}")
            return result

    def _apply_aggregation(self, result, aggregation):
        """
        Apply an aggregation function to the result.
        """
        if aggregation.upper() == "SUM":
            aggregated = sum(val for _, val in result)
            logger.info(f"TS.RANGE AGGREGATION SUM -> {aggregated}")
            return aggregated
        elif aggregation.upper() == "AVG":
            aggregated = sum(val for _, val in result) / len(result) if result else 0
            logger.info(f"TS.RANGE AGGREGATION AVG -> {aggregated}")
            return aggregated
        else:
            return "ERR Unknown aggregation type"
