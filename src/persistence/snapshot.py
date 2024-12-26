# src/persistence/snapshot.py
import json
import os
from src.logger import setup_logger

logger = setup_logger("snapshot")

class Snapshot:
    def __init__(self, file_path="snapshot.rdb"):
        self.file_path = file_path

    def save(self, data_store):
        temp_path = f"{self.file_path}.tmp"
        try:
            # Convert the data store to a serializable format
            serializable_data = {}
            for key, value in data_store.items():
                # Convert complex types to a serializable format
                if isinstance(value, (dict, list, str, int, float, bool)):
                    serializable_data[key] = value
                else:
                    # Handle other types if needed
                    serializable_data[key] = str(value)

            with open(temp_path, "w") as f:
                json.dump(serializable_data, f)
            os.replace(temp_path, self.file_path)
            return "OK"
        except IOError as e:
            logger.error(f"Snapshot save failed: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return "ERR Snapshot save failed"
    
    def load(self):
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("No snapshot file found")
            return {}
        except Exception as e:
            logger.error(f"Snapshot load failed: {e}")
            return {}