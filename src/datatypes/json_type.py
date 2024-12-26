# src/datatypes/json_type.py
from src.logger import setup_logger
import threading
import json

logger = setup_logger("json")

class JSONType:
    def __init__(self):
        self.lock = threading.Lock()

    def _validate_json(self, value):
        """
        Validate and parse JSON strings.
        """
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None

    def _navigate_to_path(self, store, key, path):
        """
        Helper method to navigate to the specified path in the JSON object.
        """
        if key not in store or not isinstance(store[key], dict):
            return None, "ERR Key is not a JSON object"

        current = store[key]
        keys = path.split(".")
        for k in keys[:-1]:
            if k not in current or not isinstance(current[k], dict):
                return None, "ERR Path not found or invalid"
            current = current[k]

        return current, None

    def json_set(self, store, key, path, value):
        """
        Sets a JSON value at the specified path.
        """
        with self.lock:
            if key not in store:
                store[key] = {}
            if not isinstance(store[key], dict):
                return "ERR Key is not a JSON object"

            current, error = self._navigate_to_path(store, key, path)
            if error:
                return error

            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass  # Assume the value is a primitive

            current[path.split(".")[-1]] = value
            logger.info(f"JSON.SET {key} {path} -> {value}")
            return "OK"

    def json_get(self, store, key, path):
        """
        Retrieves a JSON value at the specified path.
        """
        with self.lock:
            current, error = self._navigate_to_path(store, key, path)
            if error:
                return error

            target_key = path.split(".")[-1]
            if target_key not in current:
                return "(nil)"

            value = current[target_key]
            logger.info(f"JSON.GET {key} {path} -> {value}")
            return json.dumps(value)

    def json_del(self, store, key, path):
        """
        Deletes a JSON value at the specified path.
        """
        with self.lock:
            current, error = self._navigate_to_path(store, key, path)
            if error:
                return error

            target_key = path.split(".")[-1]
            if target_key in current:
                del current[target_key]
                logger.info(f"JSON.DEL {key} {path}")
                return 1
            return 0

    def json_arrappend(self, store, key, path, *values):
        """
        Appends values to an array at the specified path.
        """
        with self.lock:
            current, error = self._navigate_to_path(store, key, path)
            if error:
                return error

            target_key = path.split(".")[-1]
            if target_key not in current or not isinstance(current[target_key], list):
                return "ERR Path does not point to an array"

            try:
                parsed_values = [json.loads(v) for v in values]
            except json.JSONDecodeError:
                return "ERR Values must be JSON serializable"

            current[target_key].extend(parsed_values)
            logger.info(f"JSON.ARRAPPEND {key} {path} -> {parsed_values}")
            return len(current[target_key])
