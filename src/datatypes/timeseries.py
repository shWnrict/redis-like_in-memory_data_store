# src/datatypes/timeseries.py
from src.logger import setup_logger
import threading
import time

logger = setup_logger("timeseries")

class TimeSeries:
    def __init__(self):
        self.lock = threading.Lock()
        self.retention_policies = {}
        self.labels = {}

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

    def create(self, store, key, retention=None, labels=None):
        """
        Creates a time series structure with optional retention period and labels.
        """
        with self.lock:
            if key in store:
                return "ERR Key already exists"
            store[key] = []
            if retention:
                self.retention_policies[key] = int(retention)
            if labels:
                self.labels[key] = labels
            logger.info(f"TS.CREATE {key} (Retention={retention}, Labels={labels})")
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
            self._apply_retention_policy(store, key)
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
        elif aggregation.upper() == "MIN":
            aggregated = min(val for _, val in result) if result else None
            logger.info(f"TS.RANGE AGGREGATION MIN -> {aggregated}")
            return aggregated
        elif aggregation.upper() == "MAX":
            aggregated = max(val for _, val in result) if result else None
            logger.info(f"TS.RANGE AGGREGATION MAX -> {aggregated}")
            return aggregated
        else:
            return "ERR Unknown aggregation type"

    def _apply_retention_policy(self, store, key):
        """
        Apply the retention policy to the time series.
        """
        if key in self.retention_policies:
            retention_period = self.retention_policies[key]
            current_time = int(time.time() * 1000)
            store[key] = [(ts, val) for ts, val in store[key] if current_time - ts <= retention_period]

    def query_by_labels(self, store, labels):
        """
        Query time series by labels.
        """
        with self.lock:
            result = []
            for key, ts_labels in self.labels.items():
                if all(ts_labels.get(k) == v for k, v in labels.items()):
                    result.append(key)
            logger.info(f"TS.QUERY {labels} -> {result}")
            return result

    def handle_command(self, cmd, store, *args):
        if cmd == "TS.CREATE":
            key, *options = args
            retention = None
            labels = {}
            i = 0
            while i < len(options):
                if options[i].upper() == "RETENTION":
                    retention = options[i + 1]
                    i += 2
                elif options[i].upper() == "LABELS":
                    i += 1
                    while i < len(options):
                        labels[options[i]] = options[i + 1]
                        i += 2
                else:
                    i += 1
            return self.create(store, key, retention, labels)
        elif cmd == "TS.ADD":
            key, timestamp, value = args
            return self.add(store, key, timestamp, value)
        elif cmd == "TS.GET":
            key = args[0]
            return self.get(store, key)
        elif cmd == "TS.RANGE":
            key, start, end, *aggregation = args
            aggregation = aggregation[0] if aggregation else None
            return self.range(store, key, start, end, aggregation)
        elif cmd == "TS.QUERYINDEX":
            labels = dict(zip(args[::2], args[1::2]))
            return self.query_by_labels(store, labels)
        return "ERR Unknown time series command"
