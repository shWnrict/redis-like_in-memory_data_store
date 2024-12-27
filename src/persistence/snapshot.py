# src/persistence/snapshot.py
import json
import os
import threading
import time
from src.logger import setup_logger
from src.config import Config

logger = setup_logger("snapshot")

class Snapshot:
    def __init__(self, data_store, file_path="snapshot.rdb", save_interval=Config.SAVE_INTERVAL):
        self.data_store = data_store
        self.file_path = file_path
        self.save_interval = save_interval
        self.running = False
        self.save_thread = threading.Thread(target=self._periodic_save, daemon=True)
        self.stop_event = threading.Event()

    def start(self):
        self.running = True
        self.save_thread.start()

    def stop(self):
        self.running = False
        self.stop_event.set()  # Wake up the thread if it's sleeping
        self.save_thread.join()

    def save(self, data_store):
        temp_path = f"{self.file_path}.tmp"
        try:
            # Convert the data store to a serializable format
            serializable_data = {}
            for key, value in data_store.store.items():  # Access the internal store dictionary
                # Convert complex types to a serializable format
                if isinstance(value, (dict, list, str, int, float, bool)):
                    serializable_data[key] = value
                else:
                    # Handle other types if needed
                    serializable_data[key] = str(value)

            with open(temp_path, "w") as f:
                json.dump(serializable_data, f)
            os.replace(temp_path, self.file_path)
            logger.info("Snapshot saved successfully")
            return "OK"
        except IOError as e:
            logger.error(f"Snapshot save failed: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return "ERR Snapshot save failed"
    
    def load(self):
        try:
            with open(self.file_path, "r") as f:
                logger.info("Snapshot loaded successfully")
                return json.load(f)
        except FileNotFoundError:
            logger.warning("No snapshot file found")
            return {}
        except Exception as e:
            logger.error(f"Snapshot load failed: {e}")
            return {}

    def clear(self):
        try:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
                logger.info("Snapshot file cleared successfully")
                return "OK"
            else:
                logger.warning("No snapshot file found to clear")
                return "ERR No snapshot file found"
        except Exception as e:
            logger.error(f"Failed to clear snapshot file: {e}")
            return f"ERR {e}"

    def _periodic_save(self):
        while self.running:
            if self.stop_event.wait(self.save_interval):
                break
            self.save(self.data_store)