import time
from typing import List, Tuple, Dict, Optional
from enum import Enum
import statistics

class TSAggregationType(Enum):
    AVG = 'avg'
    SUM = 'sum'
    MIN = 'min'
    MAX = 'max'
    COUNT = 'count'
    FIRST = 'first'
    LAST = 'last'

class TimeSeries:
    """
    A class to represent a time series data structure with support for retention policies,
    duplicate handling, and downsampling.
    Attributes:
    -----------
    samples : list
        A list of (timestamp, value) tuples representing the time series data points.
    retention_ms : int
        The retention period in milliseconds. Samples older than this period will be removed.
    duplicate_policy : str
        The policy for handling duplicate timestamps. Options are 'LAST', 'FIRST', and 'BLOCK'.
    labels : dict
        Metadata labels associated with the time series.
    rules : list
        Downsampling rules defined as (dest_key, aggregation, bucket_size_ms) tuples.
    Methods:
    --------
    _cleanup_samples(current_time_ms: int):
        Remove samples older than the retention period.
    add_sample(timestamp_ms: int, value: float) -> bool:
        Add a new sample to the time series, handling duplicates according to the policy.
    get_sample(timestamp_ms: Optional[int] = None) -> Optional[Tuple[int, float]]:
        Get the sample at or closest to the given timestamp.
    range(from_ts: int, to_ts: int, aggregation: Optional[TSAggregationType] = None,
        Get a range of samples with optional downsampling.
    """
    def __init__(self, retention_ms: int = 0, duplicate_policy: str = 'LAST'):
        self.samples = []  # List of (timestamp, value) tuples
        self.retention_ms = retention_ms
        self.duplicate_policy = duplicate_policy
        self.labels = {}  # Metadata labels
        self.rules = []  # Downsampling rules: (dest_key, aggregation, bucket_size_ms)

    def _cleanup_samples(self, current_time_ms: int):
        """Remove samples older than retention period."""
        if self.retention_ms > 0:
            min_timestamp = current_time_ms - self.retention_ms
            self.samples = [(ts, val) for ts, val in self.samples if ts >= min_timestamp]

    def add_sample(self, timestamp_ms: int, value: float) -> bool:
        """Add a new sample to the time series."""
        # Handle duplicate timestamps
        for i, (ts, _) in enumerate(self.samples):
            if ts == timestamp_ms:
                if self.duplicate_policy == 'BLOCK':
                    return False
                elif self.duplicate_policy == 'FIRST':
                    return True
                else:  # LAST
                    self.samples[i] = (timestamp_ms, value)
                    return True

        self.samples.append((timestamp_ms, value))
        self.samples.sort()  # Keep samples sorted by timestamp
        
        # Apply retention policy
        self._cleanup_samples(timestamp_ms)
        return True

    def get_sample(self, timestamp_ms: Optional[int] = None) -> Optional[Tuple[int, float]]:
        """Get the sample at or closest to the given timestamp."""
        if not self.samples:
            return None
            
        if timestamp_ms is None:
            return self.samples[-1]  # Return latest sample

        # Find closest sample
        closest = min(self.samples, key=lambda x: abs(x[0] - timestamp_ms))
        return closest

    def range(self, from_ts: int, to_ts: int, 
              aggregation: Optional[TSAggregationType] = None,
              bucket_size_ms: Optional[int] = None) -> List[Tuple[int, float]]:
        """Get range of samples with optional downsampling."""
        # Filter samples within range
        filtered = [(ts, val) for ts, val in self.samples if from_ts <= ts <= to_ts]
        
        if not aggregation or not bucket_size_ms:
            return filtered

        # Group samples into buckets for downsampling
        buckets: Dict[int, List[float]] = {}
        for ts, val in filtered:
            bucket_ts = (ts // bucket_size_ms) * bucket_size_ms
            if bucket_ts not in buckets:
                buckets[bucket_ts] = []
            buckets[bucket_ts].append(val)

        # Apply aggregation function to each bucket
        result = []
        for ts, values in sorted(buckets.items()):
            if aggregation == TSAggregationType.AVG:
                agg_value = statistics.mean(values)
            elif aggregation == TSAggregationType.SUM:
                agg_value = sum(values)
            elif aggregation == TSAggregationType.MIN:
                agg_value = min(values)
            elif aggregation == TSAggregationType.MAX:
                agg_value = max(values)
            elif aggregation == TSAggregationType.COUNT:
                agg_value = len(values)
            elif aggregation == TSAggregationType.FIRST:
                agg_value = values[0]
            elif aggregation == TSAggregationType.LAST:
                agg_value = values[-1]
            else:
                agg_value = statistics.mean(values)  # Default to average
                
            result.append((ts, agg_value))
            
        return result

class TimeSeriesDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_ts(self, key: str) -> TimeSeries:
        """Ensure value at key is a TimeSeries."""
        if not self.db.exists(key):
            ts = TimeSeries()
            self.db.store[key] = ts
            return ts
        value = self.db.get(key)
        if not isinstance(value, TimeSeries):
            raise ValueError("Value is not a TimeSeries")
        return value

    def create(self, key: str, retention_ms: int = 0, 
               duplicate_policy: str = 'LAST', labels: Dict = None) -> bool:
        """Create a new time series."""
        try:
            if self.db.exists(key):
                return False
                
            ts = TimeSeries(retention_ms, duplicate_policy)
            if labels:
                ts.labels = labels
                
            self.db.store[key] = ts
            
            if not self.db.replaying:
                cmd_parts = [
                    "TS.CREATE", key,
                    "RETENTION", str(retention_ms),
                    "DUPLICATE_POLICY", duplicate_policy
                ]
                if labels:
                    for k, v in labels.items():
                        cmd_parts.extend(["LABELS", k, str(v)])
                self.db.persistence_manager.log_command(" ".join(cmd_parts))
                
            return True
        except ValueError:
            return False

    def add(self, key: str, timestamp_ms: int, value: float) -> bool:
        """Add a sample to the time series."""
        try:
            ts = self._ensure_ts(key)
            result = ts.add_sample(timestamp_ms, value)
            
            if result and not self.db.replaying:
                self.db.persistence_manager.log_command(
                    f"TS.ADD {key} {timestamp_ms} {value}")
                    
            return result
        except ValueError:
            return False

    def get(self, key: str, timestamp_ms: Optional[int] = None) -> Optional[Tuple[int, float]]:
        """Get the sample at or closest to timestamp."""
        try:
            ts = self._ensure_ts(key)
            return ts.get_sample(timestamp_ms)
        except ValueError:
            return None

    def range(self, key: str, from_ts: int, to_ts: int,
              agg_type: Optional[str] = None,
              bucket_size_ms: Optional[int] = None) -> List[Tuple[int, float]]:
        """Get range of samples with optional aggregation."""
        try:
            ts = self._ensure_ts(key)
            aggregation = TSAggregationType(agg_type.lower()) if agg_type else None
            return ts.range(from_ts, to_ts, aggregation, bucket_size_ms)
        except (ValueError, AttributeError):
            return []
