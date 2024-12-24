from src.config import Config
# src/core/memory_manager.py
import sys

class MemoryManager:
    def __init__(self, max_memory=Config.DATA_LIMIT):
        self.max_memory = max_memory
        self.current_memory = 0

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