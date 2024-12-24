# src/datatypes/probabilistic.py
from src.logger import setup_logger
import threading
import hashlib
import math

logger = setup_logger("probabilistic")

class HyperLogLog:
    def __init__(self, precision=14):
        self.lock = threading.Lock()
        self.precision = precision
        self.num_buckets = 1 << precision
        self.buckets = [0] * self.num_buckets

    def __hash(self, value):
        """
        Hash a value into a 64-bit integer.
        """
        return int(hashlib.sha256(value.encode()).hexdigest(), 16)

    def __get_bucket_and_position(self, hash_value):
        """
        Extract the bucket index and zero-run-length position.
        """
        bucket_index = hash_value & (self.num_buckets - 1)
        hash_value >>= self.precision
        position = len(bin(hash_value)) - len(bin(hash_value).rstrip('0'))
        return bucket_index, position

    def add(self, value):
        """
        Add a value to the HyperLogLog structure.
        """
        with self.lock:
            hash_value = self.__hash(value)
            bucket_index, position = self.__get_bucket_and_position(hash_value)
            self.buckets[bucket_index] = max(self.buckets[bucket_index], position)
            logger.info(f"PFADD -> Added {value} to bucket {bucket_index} with position {position}")
            return 1

    def count(self):
        """
        Estimate the cardinality of the HyperLogLog structure.
        """
        with self.lock:
            # Estimate cardinality using the harmonic mean of bucket values
            alpha = 0.7213 / (1 + 1.079 / self.num_buckets)
            harmonic_mean = sum(2 ** -b for b in self.buckets)
            raw_estimate = alpha * self.num_buckets ** 2 / harmonic_mean

            # Apply small range correction
            if raw_estimate <= 2.5 * self.num_buckets:
                zeros = self.buckets.count(0)
                if zeros > 0:
                    raw_estimate = self.num_buckets * math.log(self.num_buckets / zeros)

            # Apply large range correction
            if raw_estimate > 1 << 32:
                raw_estimate = -(1 << 32) * math.log(1 - raw_estimate / (1 << 32))

            logger.info(f"PFCOUNT -> Estimated cardinality: {int(raw_estimate)}")
            return int(raw_estimate)

    def merge(self, other):
        """
        Merge another HyperLogLog structure into this one.
        """
        with self.lock:
            if len(self.buckets) != len(other.buckets):
                raise ValueError("HyperLogLogs must have the same precision to merge")
            self.buckets = [max(a, b) for a, b in zip(self.buckets, other.buckets)]
            logger.info("PFMERGE -> Merged two HyperLogLog structures")
            return "OK"
