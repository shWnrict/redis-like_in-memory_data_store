# src/datatypes/bloomfilter.py
import hashlib
import threading
from bitarray import bitarray
from typing import List, Tuple
from src.core.data_store import DataStore
from src.logger import setup_logger

logger = setup_logger("bloomfilter")

class BloomFilter:
    def __init__(self, size=1000, hash_count=3):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = bitarray(size)
        self.bit_array.setall(0)
        self.lock = threading.Lock()

    def handle_command(self, cmd, store, *args):
        if cmd == "BFADD":
            return self.bfadd(store, *args)
        elif cmd == "BFEXISTS":
            return self.bfexists(store, *args)
        elif cmd == "BF.RESERVE":
            return self.bfreserve(store, *args)
        return "ERR Unknown BloomFilter command"

    def __hashes(self, value: str) -> List[int]:
        """
        Generate multiple hash values for the given value.
        """
        hash1 = int(hashlib.md5(value.encode()).hexdigest(), 16)
        hash2 = int(hashlib.sha1(value.encode()).hexdigest(), 16)
        return [(hash1 + i * hash2) % self.size for i in range(self.hash_count)]

    def bfadd(self, store, key: str, *values: str):
        with self.lock:
            for value in values:
                for hv in self.__hashes(value):
                    self.bit_array[hv] = 1
            logger.info(f"BFADD {key} -> {len(values)} elements added")
        return len(values)

    def bfexists(self, store, key: str, *values: str):
        with self.lock:
            for value in values:
                if not all(self.bit_array[hv] for hv in self.__hashes(value)):
                    return 0
            return 1

    def bfreserve(self, store, key: str, error_rate: float, capacity: int):
        # Calculate size and hash count
        size, hash_count = self.calculate_size_and_hash_count(error_rate, capacity)
        with self.lock:
            self.size = size
            self.hash_count = hash_count
            self.bit_array = bitarray(size)
            self.bit_array.setall(0)
            logger.info(f"BF.RESERVE {key} -> size={size}, hash_count={hash_count}")
        return "OK"

    @staticmethod
    def calculate_size_and_hash_count(error_rate: float, capacity: int) -> Tuple[int, int]:
        """
        Calculate the size of the bit array and the number of hash functions.
        """
        from math import ceil, log
        size = ceil(-(capacity * log(error_rate)) / (log(2) ** 2))
        hash_count = ceil((size / capacity) * log(2))
        return size, hash_count