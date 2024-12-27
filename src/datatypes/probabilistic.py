# src/datatypes/probabilistic.py
from src.logger import setup_logger
import threading
import hashlib
import math
import bitarray

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
        position = 1
        while hash_value & 1 == 0 and position <= 64:
            position += 1
            hash_value >>= 1
        return bucket_index, position

    def add(self, value):
        """
        Add a value to the HyperLogLog structure.
        """
        with self.lock:
            hash_value = self.__hash(value)
            bucket_index, position = self.__get_bucket_and_position(hash_value)
            if position > self.buckets[bucket_index]:
                self.buckets[bucket_index] = position
                logger.info(f"PFADD -> Added {value} to bucket {bucket_index} with position {position}")
                return 1
            return 0

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

    @staticmethod
    def merge_multiple(hlls):
        """
        Merge multiple HyperLogLog structures and return the merged result.
        """
        if not hlls:
            return HyperLogLog()

        precision = hlls[0].precision
        merged_hll = HyperLogLog(precision)
        for hll in hlls:
            if hll.precision != precision:
                raise ValueError("HyperLogLogs must have the same precision to merge")
            merged_hll.buckets = [max(a, b) for a, b in zip(merged_hll.buckets, hll.buckets)]
        return merged_hll

class BloomFilter:
    def __init__(self, size=1000, hash_count=3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray.bitarray(size)
        self.bit_array.setall(0)
        self.lock = threading.Lock()

    def __hashes(self, value):
        """
        Generate multiple hash values for the given value.
        """
        hash1 = int(hashlib.md5(value.encode()).hexdigest(), 16)
        hash2 = int(hashlib.sha1(value.encode()).hexdigest(), 16)
        for i in range(self.hash_count):
            yield (hash1 + i * hash2) % self.size

    def add(self, value):
        """
        Add a value to the Bloom filter.
        """
        with self.lock:
            for hash_value in self.__hashes(value):
                self.bit_array[hash_value] = True
            logger.info(f"BloomFilter -> Added {value}")
            return 1

    def contains(self, value):
        """
        Check if a value is in the Bloom filter.
        """
        with self.lock:
            result = all(self.bit_array[hash_value] for hash_value in self.__hashes(value))
            logger.info(f"BloomFilter -> Contains {value}: {result}")
            return result

    @staticmethod
    def calculate_size_and_hash_count(error_rate, capacity):
        """
        Calculate the size and number of hash functions for the Bloom filter.
        """
        size = -int(capacity * math.log(error_rate) / (math.log(2) ** 2))
        hash_count = int(math.ceil((size / capacity) * math.log(2)))
        return size, hash_count

    def handle_command(self, cmd, store, *args):
        if cmd == "PFADD":
            key, *values = args
            if key not in store or not isinstance(store[key], HyperLogLog):
                store[key] = HyperLogLog()
            hll = store[key]
            return 1 if any(hll.add(value) for value in values) else 0
        elif cmd == "PFCOUNT":
            keys = args
            if not keys:
                return 0
            hlls = [store[key] for key in keys if key in store and isinstance(store[key], HyperLogLog)]
            if not hlls:
                return 0
            if len(hlls) == 1:
                return hlls[0].count()
            merged_hll = HyperLogLog.merge_multiple(hlls)
            return merged_hll.count()
        elif cmd == "PFMERGE":
            destkey, *sourcekeys = args
            if destkey not in store or not isinstance(store[destkey], HyperLogLog):
                store[destkey] = HyperLogLog()
            dest_hll = store[destkey]
            source_hlls = [store[key] for key in sourcekeys if key in store and isinstance(store[key], HyperLogLog)]
            if not source_hlls:
                return "ERR No source keys found"
            merged_hll = HyperLogLog.merge_multiple([dest_hll] + source_hlls)
            store[destkey] = merged_hll
            return "OK"
        elif cmd == "BFADD":
            key, value = args
            if key not in store:
                store[key] = BloomFilter()
            bf = store[key]
            return bf.add(value)
        elif cmd == "BFEXISTS":
            key, value = args
            if key not in store:
                return 0
            bf = store[key]
            return 1 if bf.contains(value) else 0
        elif cmd == "BF.RESERVE":
            key, error_rate, capacity = args
            error_rate = float(error_rate)
            capacity = int(capacity)
            size, hash_count = BloomFilter.calculate_size_and_hash_count(error_rate, capacity)
            store[key] = BloomFilter(size=size, hash_count=hash_count)
            logger.info(f"BF.RESERVE -> Created BloomFilter {key} with size {size} and {hash_count} hash functions")
            return "OK"
        return "ERR Unknown probabilistic command"
