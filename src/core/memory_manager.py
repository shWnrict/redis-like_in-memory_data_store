# src/core/memory_manager.py
import threading
import psutil
from src.logger import setup_logger
from src.config import Config
import time

logger = setup_logger("memory_manager")

class MemoryManager:
    def __init__(self, data_store):
        self.data_store = data_store
        self.max_memory = Config.MAX_MEMORY  # Maximum memory in bytes
        self.monitor_interval = Config.MONITOR_INTERVAL  # Interval in seconds
        self.running = False
        self.thread = threading.Thread(target=self.monitor_memory)
        self.thread.daemon = True

    def start(self):
        self.running = True
        self.thread.start()
        logger.info("MemoryManager started.")

    def stop(self):
        self.running = False
        self.thread.join()
        logger.info("MemoryManager stopped.")

    def monitor_memory(self):
        while self.running:
            current_memory = psutil.Process().memory_info().rss
            logger.debug(f"Current memory usage: {current_memory} bytes.")
            if current_memory > self.max_memory:
                logger.warning("Memory limit exceeded. Initiating eviction.")
                self.evict_keys()
            time.sleep(self.monitor_interval)

    def evict_keys(self):
        """
        Evict keys based on the configured eviction policy.
        """
        eviction_policy = Config.EVICTION_POLICY
        if eviction_policy == "LRU":
            self.data_store.evict_lru()
        elif eviction_policy == "LFU":
            self.data_store.evict_lfu()
        else:
            logger.error(f"Unknown eviction policy: {eviction_policy}")