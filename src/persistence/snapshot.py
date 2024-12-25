# src/persistence/snapshot.py
import json
import os
from src.logger import setup_logger

logger = setup_logger("snapshot")

# Extend src/persistence/snapshot.py for periodic saving
class Snapshot:
    def __init__(self, file_path="snapshot.rdb"):
        self.file_path = file_path

    def save(self, data_store):
        temp_path = f"{self.file_path}.tmp"
        try:
            with open(temp_path, "w") as f:
                json.dump(data_store, f)
            os.replace(temp_path, self.file_path)
            return "OK"
        except IOError as e:
            logger.error(f"Snapshot save failed: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return "ERR Snapshot save failed"
    
    def load(self):
        """
        Load the data store state from a snapshot file.
        """
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("No snapshot file found")
            return {}
        except Exception as e:
            logger.error(f"Snapshot load failed: {e}")
            return {}
