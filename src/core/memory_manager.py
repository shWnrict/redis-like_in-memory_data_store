# src/core/memory_manager.py
from src.config import Config
import threading
import time
import sys

class MemoryManager:
    def __init__(self):
        self.max_memory = Config.DATA_LIMIT
        self.current_memory = 0
        self.gc_threshold = 0.8 * self.max_memory  # 80% threshold
        self.lock = threading.Lock()
        self._start_gc_thread()

    def _start_gc_thread(self):
        def gc_routine():
            while True:
                if self.current_memory > self.gc_threshold:
                    self._collect_garbage()
                time.sleep(60)  # Check every minute
                
        threading.Thread(target=gc_routine, daemon=True).start()

    def _collect_garbage(self):
        with self.lock:
            # Remove expired keys first
            self._remove_expired()
            # If still above threshold, remove least recently used
            if self.current_memory > self.gc_threshold:
                self._remove_lru()

    def calculate_size(self, value):
        return sys.getsizeof(value)

    def can_store(self, value):
        return self.current_memory + self.calculate_size(value) <= self.max_memory

    def add(self, value):
        size = self.calculate_size(value)
        if not self.can_store(value):
            raise MemoryError("Not enough memory to store the value")
        self.current_memory += size

    def remove(self, value):
        self.current_memory -= self.calculate_size(value)