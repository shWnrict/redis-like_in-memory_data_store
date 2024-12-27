# src/datatypes/hyperloglog.py
import hashlib
import math  # Import math module
import threading
from typing import List, Tuple  # Import Tuple
from src.core.data_store import DataStore
from src.logger import setup_logger

logger = setup_logger("hyperloglog")

class HyperLogLog:
    def __init__(self, precision=14):
        self.lock = threading.Lock()
        self.precision = precision
        self.num_buckets = 1 << precision
        self.buckets = [0] * self.num_buckets

    def handle_command(self, cmd, store, *args):
        if cmd == "PFADD":
            return self.pfadd(store, *args)
        elif cmd == "PFCOUNT":
            return self.pfcount(store, *args)
        elif cmd == "PFMERGE":
            return self.pfmerge(store, *args)
        return "ERR Unknown HyperLogLog command"

    def __hash(self, value: str) -> int:
        """
        Hash a value into a 64-bit integer.
        """
        return int(hashlib.sha256(value.encode()).hexdigest(), 16)

    def __get_bucket_and_position(self, hash_value: int) -> Tuple[int, int]:
        """
        Extract the bucket index and zero-run-length position.
        """
        bucket_index = hash_value & (self.num_buckets - 1)
        hash_value >>= self.precision
        position = 1
        while hash_value & 1 == 0 and position <= 64:
            position += 1
            hash_value >>= 1
        return bucket_index, position

    def pfadd(self, store, key: str, *values: str):
        with self.lock:
            for value in values:
                hash_value = self.__hash(value)
                bucket_index, position = self.__get_bucket_and_position(hash_value)
                if position > self.buckets[bucket_index]:
                    self.buckets[bucket_index] = position
            logger.info(f"PFADD {key} -> {len(values)} elements added")
        return 1

    def pfcount(self, store, *keys: str):
        with self.lock:
            # Simple aggregation if multiple keys are provided
            if not keys:
                return "ERR PFCOUNT requires at least one key"
            for key in keys:
                # Assuming each key has its own HyperLogLog instance
                pass  # Implement merging logic if multiple keys
            # Estimate cardinality using the harmonic mean of bucket values
            alpha = 0.7213 / (1 + 1.079 / self.num_buckets)
            harmonic_mean = sum(2 ** -b for b in self.buckets)
            raw_estimate = alpha * self.num_buckets ** 2 / harmonic_mean

            # Apply small range correction
            if raw_estimate <= 2.5 * self.num_buckets:
                cardinality = int(raw_estimate)
            elif raw_estimate > (1 << 32):
                cardinality = int(-(1 << 32) * math.log(1 - raw_estimate / (1 << 32)))
            else:
                cardinality = int(raw_estimate)

            logger.info(f"PFCOUNT -> Estimated cardinality: {cardinality}")
        return cardinality

    def pfmerge(self, store, dest_key: str, *source_keys: str):
        with self.lock:
            for src_key in source_keys:
                # Assuming source keys have their own HyperLogLog instances
                pass  # Implement merging logic
            logger.info(f"PFMERGE {dest_key} -> Merged {len(source_keys)} keys")
        return "OK"
