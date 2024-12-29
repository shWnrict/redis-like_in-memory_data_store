import json
from typing import Any, Optional, List, Union

class JSONPath:
    @staticmethod
    def parse_path(path: str) -> List[str]:
        """Parse a JSON path into components."""
        if not path or path == '$':
            return []
            
        # Remove $ prefix if present
        if path.startswith('$.'):
            path = path[2:]
        elif path.startswith('$'):
            path = path[1:]
            
        if not path:
            return []
        
        components = []
        current = ''
        in_brackets = False
        
        for char in path:
            if char == '[':
                if current:
                    components.append(current)
                    current = ''
                in_brackets = True
            elif char == ']' and in_brackets:
                if current:
                    components.append(current)
                    current = ''
                in_brackets = False
            elif char == '.' and not in_brackets:
                if current:
                    components.append(current)
                    current = ''
            else:
                current += char
                
        if current:
            components.append(current)
            
        return components

    @staticmethod
    def get_value(obj: Any, path: str) -> Any:
        """Get value at path from object."""
        if not path or path == '$':
            return obj
            
        current = obj
        for component in JSONPath.parse_path(path):
            try:
                if isinstance(current, dict):
                    current = current[component]
                elif isinstance(current, list):
                    idx = int(component)
                    current = current[idx]
                else:
                    return None
            except (KeyError, ValueError, IndexError, TypeError):
                return None
        return current

    @staticmethod
    def set_value(obj: Any, path: str, value: Any) -> bool:
        """Set value at path in object."""
        if not path or path == '$':
            return False
            
        components = JSONPath.parse_path(path)
        if not components:
            return False
            
        current = obj
        for i, component in enumerate(components[:-1]):
            try:
                if isinstance(current, dict):
                    if component not in current:
                        # Create new dict/list based on next component
                        next_comp = components[i + 1]
                        current[component] = [] if next_comp.isdigit() else {}
                    current = current[component]
                elif isinstance(current, list):
                    idx = int(component)
                    while len(current) <= idx:
                        # Extend list if needed
                        next_comp = components[i + 1]
                        current.append([] if next_comp.isdigit() else {})
                    current = current[idx]
                else:
                    return False
            except (ValueError, IndexError):
                return False
                
        # Set final value
        try:
            last = components[-1]
            if isinstance(current, dict):
                current[last] = value
            elif isinstance(current, list):
                idx = int(last)
                while len(current) <= idx:
                    current.append(None)
                current[idx] = value
            else:
                return False
            return True
        except (ValueError, IndexError):
            return False

    @staticmethod
    def delete_value(obj: Any, path: str) -> bool:
        """Delete value at path from object."""
        if not path or path == '$':
            return False
            
        components = JSONPath.parse_path(path)
        if not components:
            return False
            
        current = obj
        for component in components[:-1]:
            try:
                if isinstance(current, dict):
                    current = current[component]
                elif isinstance(current, list):
                    idx = int(component)
                    current = current[idx]
                else:
                    return False
            except (KeyError, ValueError, IndexError):
                return False
                
        # Delete final value
        try:
            last = components[-1]
            if isinstance(current, dict):
                if last in current:
                    del current[last]
                    return True
            elif isinstance(current, list):
                idx = int(last)
                if 0 <= idx < len(current):
                    del current[idx]
                    return True
        except (ValueError, IndexError):
            pass
        return False

class JSONDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_json(self, key: str) -> Any:
        """Ensure value at key is a JSON object."""
        if not self.db.exists(key):
            return None
        value = self.db.get(key)
        return value

    def json_set(self, key: str, path: str, value: str, nx: bool = False, xx: bool = False) -> bool:
        """Set JSON value at path."""
        try:
            json_value = json.loads(value)
            current = self._ensure_json(key)
            
            if path == '$':
                if current is not None:
                    if nx:  # Don't update existing key
                        return False
                elif xx:  # Only update existing key
                    return False
                self.db.store[key] = json_value
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"JSON.SET {key} {path} {value}")
                return True
                
            if current is None:
                if xx:  # Only update existing key
                    return False
                current = {} if not path.split('.')[-1].isdigit() else []
                self.db.store[key] = current
                
            if JSONPath.set_value(current, path, json_value):
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"JSON.SET {key} {path} {value}")
                return True
            return False
        except json.JSONDecodeError:
            return False

    def json_get(self, key: str, path: str = '$') -> Optional[str]:
        """Get JSON value at path."""
        try:
            obj = self._ensure_json(key)
            if obj is None:
                return None
                
            value = JSONPath.get_value(obj, path)
            if value is None:
                return None
                
            # Wrap non-objects in an array for leaf nodes
            if path != '$' and not isinstance(value, (dict, list)):
                value = [value]
                
            return json.dumps(value)
        except (TypeError, ValueError):
            return None

    def json_del(self, key: str, path: str = '$') -> bool:
        """Delete value at path."""
        if path == '$':
            return bool(self.db.delete(key))
            
        obj = self._ensure_json(key)
        if obj is None:
            return False
            
        if JSONPath.delete_value(obj, path):
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"JSON.DEL {key} {path}")
            return True
        return False

    def json_arrappend(self, key: str, path: str, *values: str) -> Optional[int]:
        """Append values to array at path."""
        try:
            obj = self._ensure_json(key)
            if obj is None:
                return None
                
            arr = JSONPath.get_value(obj, path)
            if not isinstance(arr, list):
                return None
                
            json_values = [json.loads(v) for v in values]
            arr.extend(json_values)
            
            if not self.db.replaying:
                values_str = ' '.join(f"'{v}'" for v in values)
                self.db.persistence_manager.log_command(
                    f"JSON.ARRAPPEND {key} {path} {values_str}")
            
            return len(arr)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
