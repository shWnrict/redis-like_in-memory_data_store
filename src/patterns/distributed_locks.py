import time
import uuid

class DistributedLock:
    def __init__(self, data_store, lock_name, ttl=10):
        self.data_store = data_store
        self.lock_name = lock_name
        self.ttl = ttl
        self.lock_value = str(uuid.uuid4())

    def acquire(self):
        if self.data_store.set(self.lock_name, self.lock_value, nx=True, ex=self.ttl):
            return True
        return False

    def release(self):
        if self.data_store.get(self.lock_name) == self.lock_value:
            self.data_store.delete(self.lock_name)
