# src/persistence/snapshot.py
import json
from src.logger import setup_logger

logger = setup_logger("snapshot")

class Snapshot:
    def __init__(self, file_path="snapshot.rdb"):
        self.file_path = file_path

    def save(self, data_store):
        """
        Save the current state of the data store to a snapshot file.
        """
        try:
            with open(self.file_path, "w") as f:
                json.dump(data_store, f)
            logger.info(f"Snapshot saved to {self.file_path}")
            return "OK"
        except Exception as e:
            logger.error(f"Failed to save snapshot: {e}")
            return "ERR Snapshot save failed"

    def load(self):
        """
        Load the data store state from a snapshot file.
        """
        try:
            with open(self.file_path, "r") as f:
                data_store = json.load(f)
            logger.info(f"Snapshot loaded from {self.file_path}")
            return data_store
        except FileNotFoundError:
            logger.warning(f"No snapshot file found at {self.file_path}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load snapshot: {e}")
            return {}
