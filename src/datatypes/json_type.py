# src/datatypes/json_type.py
from src.logger import setup_logger
import threading
import json
import re

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

    def _get_value_at_path(self, data, path):
        if path == ".":
            return data
        
        keys = path.strip(".").split(".")
        current = data
        
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current

    def json_set(self, store, key, path, value):
        with self.lock:
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return "ERR Invalid JSON value"

            if path == ".":
                store[key] = value
                return "OK"

            if key not in store:
                store[key] = {}
            
            current = store[key]
            keys = path.strip(".").split(".")
            
            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]
            
            current[keys[-1]] = value
            return "OK"

    def json_get(self, store, key, path):
        if key not in store:
            return None  # Changed from "(nil)"
        
        value = self._get_value_at_path(store[key], path)
        if value is None:
            return None  # Changed from "(nil)"
            
        return value

    def json_del(self, store, key, path):
        if key not in store:
            return 0

        if path == ".":
            del store[key]
            return 1

        keys = path.strip(".").split(".")
        current = store[key]
        
        for k in keys[:-1]:
            if not isinstance(current, dict) or k not in current:
                return 0
            current = current[k]

        if not isinstance(current, dict) or keys[-1] not in current:
            return 0
            
        del current[keys[-1]]
        return 1

    def json_arrappend(self, store, key, path, *values):
        with self.lock:
            if key not in store:
                store[key] = {}

            # If path doesn't exist, create an empty array
            keys = path.strip(".").split(".")
            current = store[key]
            
            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]
                
            last_key = keys[-1]
            if last_key not in current:
                current[last_key] = []
            elif not isinstance(current[last_key], list):
                return "ERR Path does not point to an array"

            try:
                parsed_values = [json.loads(v) for v in values]
            except json.JSONDecodeError:
                return "ERR Values must be JSON serializable"

            current[last_key].extend(parsed_values)
            return len(current[last_key])
        
    def json_query(self, store, key, json_path):
        """
        Retrieve data using a simple JSONPath-like syntax.
        Supports paths like $.store.book[0].title
        """
        with self.lock:
            if key not in store or not isinstance(store[key], (dict, list)):
                return None

            data = store[key]
            # Simple regex to parse JSONPath
            tokens = re.findall(r'\w+|\[\d+\]', json_path)
            try:
                for token in tokens:
                    if token.startswith('[') and token.endswith(']'):
                        index = int(token[1:-1])
                        data = data[index]
                    else:
                        data = data[token]
                return data
            except (KeyError, IndexError, TypeError):
                return None

    def handle_command(self, cmd, store, *args):
        if cmd == "JSON.SET":
            return self.json_set(store, args[0], args[1], args[2])
        elif cmd == "JSON.GET":
            return self.json_get(store, args[0], args[1]) if len(args) > 1 else self.json_get(store, args[0], ".")
        elif cmd == "JSON.DEL":
            return self.json_del(store, args[0], args[1])
        elif cmd == "JSON.ARRAPPEND":
            return self.json_arrappend(store, args[0], args[1], *args[2:])
        elif cmd == "JSON.QUERY":
            return self.json_query(store, args[0], args[1])
        return "ERR Unknown command"
