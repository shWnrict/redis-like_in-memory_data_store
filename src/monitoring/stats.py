# src/monitoring/stats.py
from src.logger import setup_logger
import time
import psutil  # Ensure to install psutil via pip
from src.core.data_store import DataStore

logger = setup_logger("stats")

class Stats:
    def __init__(self, data_store: DataStore):
        self.data_store = data_store
        self.start_time = time.time()
        logger.info("Stats monitoring initialized.")

    def info(self) -> str:
        """
        Generate an INFO payload containing memory usage, uptime, and command statistics.
        """
        uptime_seconds = int(time.time() - self.start_time)
        memory = psutil.Process().memory_info().rss  # in bytes
        memory_mb = memory / (1024 * 1024)

        info_segments = [
            "# Server",
            f"uptime_in_seconds:{uptime_seconds}",
            f"used_memory:{memory_mb:.2f}MB",
            f"connected_clients:{len(self.data_store.store)}",
            # Add more stats as needed
        ]

        info_payload = "\n".join(info_segments)
        logger.info("Generated INFO payload.")
        return info_payload

    def handle_info(self) -> str:
        """
        Handle the INFO command and return the statistics.
        """
        return self.info()
