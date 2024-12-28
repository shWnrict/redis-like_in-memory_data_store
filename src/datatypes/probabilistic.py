# src/datatypes/probabilistic.py
from src.datatypes.base import BaseDataType
import math
import mmh3  # MurmurHash3 for efficient hashing
from src.logger import setup_logger

logger = setup_logger("probabilistic")

class HyperLogLog(BaseDataType):
    def __init__(self, store, expiry_manager=None, precision=14):
        super().__init__(store, expiry_manager)
        self.precision = precision
        self.m = 1 << precision
        self.alpha = self._get_alpha(self.m)

    def _get_alpha(self, m):
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        return 0.7213 / (1 + 1.079 / m)

    def pfadd(self, store, key, *elements):
        """Add elements to HyperLogLog"""
        with self.lock:
            if key not in store:
                store[key] = [0] * self.m
            
            modified = False
            for element in elements:
                hash_val = mmh3.hash(str(element).encode())
                bucket = hash_val & (self.m - 1)
                pattern = bin(hash_val >> self.precision)[2:]
                rank = len(pattern) - len(pattern.rstrip('0'))
                if store[key][bucket] < rank:
                    store[key][bucket] = rank
                    modified = True
            return 1 if modified else 0

    def pfcount(self, store, *keys):
        """Estimate cardinality of set(s)"""
        with self.lock:
            if len(keys) == 1:
                return self._estimate(store[keys[0]])
            
            # Merge multiple HLLs
            merged = [0] * self.m
            for key in keys:
                if key in store:
                    for i in range(self.m):
                        merged[i] = max(merged[i], store[key][i])
            return self._estimate(merged)

    def _estimate(self, registers):
        """Calculate cardinality estimate"""
        sum_inv = 0
        zeros = 0
        for val in registers:
            sum_inv += math.pow(2, -val)
            if val == 0:
                zeros += 1
        
        estimate = self.alpha * self.m * self.m / sum_inv
        if estimate <= 2.5 * self.m:
            if zeros > 0:
                estimate = self.m * math.log(self.m / zeros)
        return int(estimate)

class BloomFilter(BaseDataType):
    def __init__(self, store, expiry_manager=None, size=1000000, hash_count=7):
        super().__init__(store, expiry_manager)
        self.size = size
        self.hash_count = hash_count

    def bfadd(self, store, key, element):
        """Add element to Bloom filter"""
        with self.lock:
            if key not in store:
                store[key] = [0] * self.size
            
            for seed in range(self.hash_count):
                idx = mmh3.hash(str(element).encode(), seed) % self.size
                store[key][idx] = 1
        return 1

    def bfexists(self, store, key, element):
        """Check if element might exist in Bloom filter"""
        with self.lock:
            if key not in store:
                return 0
            
            for seed in range(self.hash_count):
                idx = mmh3.hash(str(element).encode(), seed) % self.size
                if not store[key][idx]:
                    return 0
            return 1

    def handle_command(self, cmd, store, *args):
        """Handle probabilistic commands"""
        cmd = cmd.upper()
        if cmd == "PFADD":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            key, *elements = args
            return f":{self.pfadd(store, key, *elements)}\r\n"
        elif cmd == "PFCOUNT":
            if not args:
                return "-ERR wrong number of arguments\r\n"
            return f":{self.pfcount(store, *args)}\r\n"
        # ... implement other commands
