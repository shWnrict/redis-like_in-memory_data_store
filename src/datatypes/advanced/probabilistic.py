import math
import mmh3  # MurmurHash3 for better hash distribution
from typing import List

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
    """
    ProbabilisticDataType is a class that provides methods to interact with probabilistic data structures
    such as HyperLogLog and Bloom Filter within an in-memory database. 
    Methods:
        __init__(database):
            Initializes the ProbabilisticDataType with a reference to the database.
        _ensure_hll(key: str) -> HyperLogLog:
            Ensures that the value at the given key is a HyperLogLog. If not, it initializes a new HyperLogLog.
        pfadd(key: str, *elements: str) -> int:
            Adds elements to the HyperLogLog at the specified key. Returns 1 if the HyperLogLog was modified, otherwise 0.
        pfcount(*keys: str) -> int:
            Returns the cardinality estimate for one or more HyperLogLogs. If multiple keys are provided, it merges them before counting.
        pfmerge(destkey: str, *sourcekeys: str) -> bool:
            Merges multiple HyperLogLogs into a new one at the destination key. Returns True if successful, otherwise False.
        _ensure_bloom(key: str, size: int = None, num_hashes: int = None) -> BloomFilter:
            Ensures that the value at the given key is a BloomFilter. If not, it initializes a new BloomFilter with the specified size and number of hash functions.
        bf_reserve(key: str, size: int, num_hashes: int) -> bool:
            Creates a new Bloom filter with the specified parameters at the given key. Returns True if successful, otherwise False.
        bf_add(key: str, item: str) -> int:
            Adds an item to the Bloom filter at the specified key. Returns 1 if the Bloom filter was modified, otherwise 0.
        bf_exists(key: str, item: str) -> bool:
            Checks if an item might exist in the Bloom filter at the specified key. Returns True if the item might exist, otherwise False.
    """
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

    def _ensure_bloom(self, key: str, size: int = None, num_hashes: int = None) -> BloomFilter:
        """Ensure value at key is a BloomFilter."""
        if not self.db.exists(key):
            if size is None:
                size = 1000  # Default size
            if num_hashes is None:
                num_hashes = 4  # Default number of hash functions
            bf = BloomFilter(size, num_hashes)
            self.db.store[key] = bf
            return bf
        value = self.db.get(key)
        if not isinstance(value, BloomFilter):
            raise ValueError("Value is not a BloomFilter")
        return value

    def bf_reserve(self, key: str, size: int, num_hashes: int) -> bool:
        """Create a new Bloom filter with specified parameters."""
        try:
            if self.db.exists(key):
                return False
            bf = BloomFilter(size, num_hashes)
            self.db.store[key] = bf
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"BF.RESERVE {key} {size} {num_hashes}")
            return True
        except ValueError:
            return False

    def bf_add(self, key: str, item: str) -> int:
        """Add item to Bloom filter."""
        try:
            bf = self._ensure_bloom(key)
            result = bf.add(item)
            if result and not self.db.replaying:
                self.db.persistence_manager.log_command(f"BF.ADD {key} {item}")
            return 1 if result else 0
        except ValueError:
            return 0

    def bf_exists(self, key: str, item: str) -> bool:
        """Check if item might exist in Bloom filter."""
        try:
            bf = self._ensure_bloom(key)
            return bf.contains(item)
        except ValueError:
            return False
