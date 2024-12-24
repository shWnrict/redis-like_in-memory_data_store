# src/datatypes/json_type.py
from src.logger import setup_logger
import threading
import json

logger = setup_logger("json")

class JSONType:
    def __init__(self):
        self.lock = threading.Lock()

    def json_set(self, store, key, path, value):
        """
        Sets a JSON value at the specified path.
        """
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a JSON object"

            current = store[key]
            keys = path.split(".")
            for k in keys[:-1]:
                if k not in current or not isinstance(current[k], dict):
                    current[k] = {}
                current = current[k]

            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass  # Assume the value is a primitive

            current[keys[-1]] = value
            logger.info(f"JSON.SET {key} {path} -> {value}")
            return "OK"

    def json_get(self, store, key, path):
        """
        Retrieves a JSON value at the specified path.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return "(nil)"
            current = store[key]
            keys = path.split(".")
            for k in keys:
                if k not in current:
                    return "(nil)"
                current = current[k]
            logger.info(f"JSON.GET {key} {path} -> {current}")
            return json.dumps(current)

    def json_del(self, store, key, path):
        """
        Deletes a JSON value at the specified path.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return 0
            current = store[key]
            keys = path.split(".")
            for k in keys[:-1]:
                if k not in current:
                    return 0
                current = current[k]
            if keys[-1] in current:
                del current[keys[-1]]
                logger.info(f"JSON.DEL {key} {path}")
                return 1
            return 0

    def json_arrappend(self, store, key, path, *values):
        """
        Appends values to an array at the specified path.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], dict):
                return "ERR Key is not a JSON object"
            current = store[key]
            keys = path.split(".")
            for k in keys[:-1]:
                if k not in current or not isinstance(current[k], dict):
                    return "ERR Path not found or invalid"
                current = current[k]
            if keys[-1] not in current or not isinstance(current[keys[-1]], list):
                return "ERR Path does not point to an array"
            current[keys[-1]].extend(values)
            logger.info(f"JSON.ARRAPPEND {key} {path} -> {values}")
            return len(current[keys[-1]])
