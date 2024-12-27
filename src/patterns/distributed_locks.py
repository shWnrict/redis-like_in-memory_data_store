import time
import uuid
from src.logger import setup_logger

logger = setup_logger("distributed_locks")

class DistributedLock:
    def __init__(self, data_store):
        self.data_store = data_store
        self.locks = {}
    
    def acquire(self, lock_name, ttl):
        """
        Acquire a distributed lock with the given name and TTL.
        """
        lock_value = str(uuid.uuid4())
        acquired = self.data_store.set(lock_name, lock_value, ttl=ttl)
        if acquired == "OK":
            self.locks[lock_name] = lock_value
            logger.info(f"Lock '{lock_name}' acquired with TTL={ttl} seconds.")
            return True
        logger.warning(f"Failed to acquire lock '{lock_name}'.")
        return False

    def release(self, lock_name):
        """
        Release the distributed lock with the given name.
        """
        lock_value = self.locks.get(lock_name)
        if not lock_value:
            logger.warning(f"Attempted to release non-existent lock '{lock_name}'.")
            return False

        current_value = self.data_store.get(lock_name)
        if current_value == lock_value:
            deleted = self.data_store.delete(lock_name)
            if deleted == 1:
                logger.info(f"Lock '{lock_name}' released successfully.")
                del self.locks[lock_name]
                return True
            logger.error(f"Failed to delete lock '{lock_name}'.")
        else:
            logger.warning(f"Lock '{lock_name}' value mismatch. Cannot release.")
        return False
