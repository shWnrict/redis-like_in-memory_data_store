# src/datatypes/json_type.py
from src.datatypes.base import BaseDataType
import json
import threading
import jsonpath_ng
from src.logger import setup_logger

logger = setup_logger("json_type")

class JSONType(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()

    def json_set(self, store, key, path, value):
        with self.lock:
            try:
                json_value = json.loads(value)
                if path == "$":
                    store[key] = json_value
                else:
                    if key not in store:
                        store[key] = {}
                    jsonpath_expr = jsonpath_ng.parse(path)
                    jsonpath_expr.update(store[key], json_value)
                return "OK"
            except json.JSONDecodeError:
                return "ERR Invalid JSON string"
            except Exception as e:
                return f"ERR {str(e)}"

    def json_get(self, store, key, path="$"):
        with self.lock:
            if key not in store:
                return None
            try:
                jsonpath_expr = jsonpath_ng.parse(path)
                matches = [match.value for match in jsonpath_expr.find(store[key])]
                return matches[0] if matches else None
            except Exception as e:
                return f"ERR {str(e)}"

    def json_del(self, store, key, path="$"):
        with self.lock:
            if key not in store:
                return 0
            try:
                if path == "$":
                    del store[key]
                    return 1
                jsonpath_expr = jsonpath_ng.parse(path)
                matches = jsonpath_expr.find(store[key])
                for match in matches:
                    match.context.value.pop(match.path[-1])
                return len(matches)
            except Exception as e:
                return f"ERR {str(e)}"

    def handle_command(self, cmd, store, *args):
        cmd = cmd.upper()
        if cmd == "JSON.SET":
            if len(args) != 3:
                return "-ERR wrong number of arguments\r\n"
            key, path, value = args
            result = self.json_set(store, key, path, value)
            return f"+{result}\r\n"
        elif cmd == "JSON.GET":
            if len(args) < 1:
                return "-ERR wrong number of arguments\r\n"
            key = args[0]
            path = args[1] if len(args) > 1 else "$"
            result = self.json_get(store, key, path)
            if result is None:
                return "$-1\r\n"
            json_str = json.dumps(result)
            return f"${len(json_str)}\r\n{json_str}\r\n"
        # ... handle other JSON commands
