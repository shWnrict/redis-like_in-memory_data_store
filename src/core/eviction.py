from collections import OrderedDict
from src.logger import setup_logger

logger = setup_logger("eviction")

class EvictionPolicy:
    def __init__(self, data_store):
        self.data_store = data_store

    def evict(self):
        # Placeholder for eviction logic
        pass

class LRUEvictionPolicy(EvictionPolicy):
    def __init__(self, data_store):
        super().__init__(data_store)
        self.access_order = OrderedDict()

    def record_access(self, key):
        if key in self.access_order:
            self.access_order.move_to_end(key)
        else:
            self.access_order[key] = None

    def evict(self):
        if not self.access_order:
            logger.warning("No keys to evict under LRU policy.")
            return
        oldest_key, _ = self.access_order.popitem(last=False)
        self.data_store.delete(oldest_key)
        logger.info(f"Evicted key '{oldest_key}' using LRU policy.")

class LFUEvictionPolicy(EvictionPolicy):
    def __init__(self, data_store):
        super().__init__(data_store)
        self.usage_counts = {}

    def record_access(self, key):
        if key in self.usage_counts:
            self.usage_counts[key] += 1
        else:
            self.usage_counts[key] = 1

    def evict(self):
        if not self.usage_counts:
            logger.warning("No keys to evict under LFU policy.")
            return
        least_used_key = min(self.usage_counts, key=self.usage_counts.get)
        self.data_store.delete(least_used_key)
        del self.usage_counts[least_used_key]
        logger.info(f"Evicted key '{least_used_key}' using LFU policy.")
