
# src/clustering/keyspace_manager.py
from src.logger import setup_logger
from threading import Lock
import random

logger = setup_logger("keyspace_manager")

class KeyspaceManager:
    def __init__(self):
        self.databases = {i: {} for i in range(16)}  # Redis default 16 databases
        self.current_db = 0
        self.lock = Lock()

    def select(self, db_index):
        with self.lock:
            if db_index in self.databases:
                self.current_db = db_index
                logger.info(f"Selected database {db_index}")
                return "OK"
            else:
                return "ERR Invalid database index"

    def flushdb(self):
        with self.lock:
            self.databases[self.current_db].clear()
            logger.info(f"Flushed database {self.current_db}")
            return "OK"

    def randomkey(self):
        with self.lock:
            db = self.databases[self.current_db]
            if not db:
                return "(nil)"
            key = random.choice(list(db.keys()))
            return key