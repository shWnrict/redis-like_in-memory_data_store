import math
import mmh3  # MurmurHash3 for better hash distribution
from typing import List, Set, Optional
import struct

class HyperLogLog:
    def __init__(self, precision: int = 14):
        """Initialize HLL with given precision (p)."""
        self.p = precision
        self.m = 1 << precision  # Number of registers
        self.registers = bytearray(self.m)
        self.alpha = self._get_alpha(self.m)

    def _get_alpha(self, m: int) -> float:
        """Get alpha constant based on number of registers."""
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        return 0.7213 / (1 + 1.079 / m)

    def add(self, item: str) -> bool:
        """Add an item to the HLL."""
        x = mmh3.hash(item, 42)  # Use seed 42
        j = x & (self.m - 1)  # Register index
        w = x >> self.p  # Remaining bits
        rho = min(self.p, (w | 1).bit_length())  # Count leading zeros + 1
        if self.registers[j] < rho:
            self.registers[j] = rho
            return True
        return False

    def count(self) -> int:
        """Estimate cardinality."""
        sum_inv = 0
        zeros = 0
        for val in self.registers:
            sum_inv += 2.0 ** -val
            if val == 0:
                zeros += 1

        estimate = (self.alpha * self.m ** 2) / sum_inv

        # Apply corrections
        if estimate <= 2.5 * self.m:  # Small range correction
            if zeros > 0:
                estimate = self.m * math.log(self.m / zeros)
        elif estimate > 2**32 / 30:  # Large range correction
            estimate = -2**32 * math.log(1 - estimate / 2**32)

        return int(estimate)

    def merge(self, other: 'HyperLogLog') -> bool:
        """Merge another HLL into this one."""
        if self.p != other.p:
            return False
        for i in range(self.m):
            self.registers[i] = max(self.registers[i], other.registers[i])
        return True

class BloomFilter:
    def __init__(self, size: int = 1000, num_hashes: int = 4):
        """Initialize Bloom filter with given size and hash functions."""
        self.size = size
        self.num_hashes = num_hashes
        self.bits = bytearray(size)

    def _get_hash_values(self, item: str) -> List[int]:
        """Get hash values for item using different seeds."""
        return [mmh3.hash(item, seed) % self.size for seed in range(self.num_hashes)]

    def add(self, item: str) -> bool:
        """Add an item to the Bloom filter."""
        changed = False
        for pos in self._get_hash_values(item):
            if not self.bits[pos]:
                self.bits[pos] = 1
                changed = True
        return changed

    def contains(self, item: str) -> bool:
        """Check if item might be in the set."""
        return all(self.bits[pos] for pos in self._get_hash_values(item))

class ProbabilisticDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_hll(self, key: str) -> HyperLogLog:
        """Ensure value at key is a HyperLogLog."""
        if not self.db.exists(key):
            hll = HyperLogLog()
            self.db.store[key] = hll
            return hll
        value = self.db.get(key)
        if not isinstance(value, HyperLogLog):
            raise ValueError("Value is not a HyperLogLog")
        return value

    def pfadd(self, key: str, *elements: str) -> int:
        """Add elements to HyperLogLog."""
        try:
            hll = self._ensure_hll(key)
            changed = False
            for elem in elements:
                if hll.add(elem):
                    changed = True
            
            if changed and not self.db.replaying:
                self.db.persistence_manager.log_command(f"PFADD {key} {' '.join(elements)}")
            
            return 1 if changed else 0
        except ValueError:
            return 0

    def pfcount(self, *keys: str) -> int:
        """Get cardinality estimate for one or more HLLs."""
        try:
            if len(keys) == 1:
                return self._ensure_hll(keys[0]).count()
            
            # Merge multiple HLLs
            merged = HyperLogLog()
            for key in keys:
                hll = self._ensure_hll(key)
                merged.merge(hll)
            return merged.count()
        except ValueError:
            return 0

    def pfmerge(self, destkey: str, *sourcekeys: str) -> bool:
        """Merge multiple HLLs into a new one."""
        try:
            merged = HyperLogLog()
            for key in sourcekeys:
                hll = self._ensure_hll(key)
                if not merged.merge(hll):
                    return False
            
            self.db.store[destkey] = merged
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"PFMERGE {destkey} {' '.join(sourcekeys)}")
            
            return True
        except ValueError:
            return False
