# src/monitoring/slowlog.py

from src.logger import setup_logger
from typing import List, Tuple
import time
from threading import Lock

logger = setup_logger("slowlog")

class SlowLog:
    def __init__(self, threshold_ms: int = 100):
        """
        Initialize the SlowLog with a threshold in milliseconds.
        """
        self.threshold_ms = threshold_ms
        self.log: List[Tuple[float, str, List[str]]] = []
        self.lock = Lock()
        logger.info(f"SlowLog initialized with threshold {self.threshold_ms} ms.")

    def add_entry(self, execution_time: float, command: str, args: List[str]):
        """
        Add a command to the slow log if it exceeds the threshold.
        """
        if execution_time > self.threshold_ms:
            with self.lock:
                timestamp = time.time()
                self.log.append((timestamp, command, args))
                logger.warning(f"Slow command logged: {command} {' '.join(args)} took {execution_time:.2f} ms.")

    def get_slowlog(self) -> List[Tuple[float, str, List[str]]]:
        """
        Retrieve the list of slow commands.
        """
        with self.lock:
            return self.log.copy()

    def clear_slowlog(self) -> int:
        """
        Clear the slow log and return the number of entries removed.
        """
        with self.lock:
            count = len(self.log)
            self.log.clear()
            logger.info("SlowLog cleared.")
            return count

# No changes needed as `SlowLog` is already integrated in server.py and command_router.py

# ...existing code...