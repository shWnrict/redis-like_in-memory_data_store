import json
from typing import Any, Optional, List, Union

class JSONPath:
    """
    JSONPath is a utility class for parsing, setting, getting, and deleting values in a JSON-like object using JSONPath syntax.
    Methods:
        parse_path(path: str) -> List[str]:
            Parse a JSON path into components.
        set_value(obj: Any, path: str, value: Any) -> bool:
            Set value at path in object.
        _set_recursive(obj: Any, field: str, value: Any) -> bool:
            Recursively set value for all matching fields.
        get_value(obj: Any, path: str) -> Any:
            Get value at path from object.
        _get_recursive(obj: Any, field: str) -> List[Any]:
            Recursively get all values matching the field.
        delete_value(obj: Any, path: str) -> bool:
            Delete value at path from object.
        _delete_recursive(obj: Any, field: str) -> bool:
            Recursively delete all values matching the field.
    """
    @staticmethod
    def parse_path(path: str) -> List[str]:
        """Parse a JSON path into components."""
        if not path or path == '$':
            return []
            
        # Remove $ prefix if present
        if path.startswith('$'):
            path = path[1:]
            if path.startswith('.'):
                path = path[1:]
                
        if not path:
            return []
            
        # Handle ..path (recursive descent)
        if path.startswith('..'):
            return ['..'] + path[2:].split('.')
            
        return path.split('.')

    @staticmethod
    def set_value(obj: Any, path: str, value: Any) -> bool:
        """Set value at path in object."""
        if not path or path == '$':
            return False
            
        try:
            if '..' in path:
                return JSONPath._set_recursive(obj, path[3:], value)  # Skip $.., keep rest of path
                
            current = obj
            components = path.split('.')[1:]  # Skip $ prefix
            
            for i, component in enumerate(components[:-1]):
                if isinstance(current, dict):
                    if component not in current:
                        current[component] = {}
                    current = current[component]
                else:
                    return False
                    
            last = components[-1]
            if isinstance(current, dict):
                try:
                    if isinstance(value, str):
                        try:
                            current[last] = json.loads(value)
                        except json.JSONDecodeError:
                            current[last] = value
                    else:
                        current[last] = value
                    return True
                except Exception:
                    return False
        except Exception:
            return False
        return False

    @staticmethod
    def _set_recursive(obj: Any, field: str, value: Any) -> bool:
        """Recursively set value for all matching fields."""
        changed = False
        
        if isinstance(obj, dict):
            for k, v in list(obj.items()):  # Use list to avoid modification during iteration
                if k == field:
                    try:
                        if isinstance(value, str):
                            try:
                                obj[k] = json.loads(value)
                            except json.JSONDecodeError:
                                obj[k] = value
                        else:
                            obj[k] = value
                        changed = True
                    except Exception:
                        continue
                
                # Recurse into nested objects
                if isinstance(v, (dict, list)):
                    if JSONPath._set_recursive(v, field, value):
                        changed = True
                        
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    if JSONPath._set_recursive(item, field, value):
                        changed = True
                        
        return changed

    @staticmethod
    def get_value(obj: Any, path: str) -> Any:
        """Get value at path from object."""
        if not path or path == '$':
            return obj
            
        try:
            if '..' in path:
                return JSONPath._get_recursive(obj, path[3:])  # Skip $.., keep rest of path
                
            current = obj
            components = path.split('.')[1:]  # Skip $ prefix
            
            for component in components:
                if isinstance(current, dict):
                    current = current.get(component)
                    if current is None:
                        return None
                else:
                    return None
            return current
        except Exception:
            return None

    @staticmethod
    def _get_recursive(obj: Any, field: str) -> List[Any]:
        """Recursively get all values matching the field."""
        results = []
        
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == field:
                    results.append(v)
                if isinstance(v, (dict, list)):
                    results.extend(JSONPath._get_recursive(v, field))
                    
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    results.extend(JSONPath._get_recursive(item, field))
                    
        return results

    @staticmethod
    def delete_value(obj: Any, path: str) -> bool:
        """Delete value at path from object."""
        if not path or path == '$':
            return False
            
        try:
            if '..' in path:
                return JSONPath._delete_recursive(obj, path[3:])  # Skip $.., keep rest of path
                
            current = obj
            components = path.split('.')[1:]  # Skip $ prefix
            
            # Navigate to parent of target
            for component in components[:-1]:
                if isinstance(current, dict):
                    current = current.get(component)
                    if current is None:
                        return False
                else:
                    return False
                    
            # Delete the target
            last = components[-1]
            if isinstance(current, dict) and last in current:
                del current[last]
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def _delete_recursive(obj: Any, field: str) -> bool:
        """Recursively delete all values matching the field."""
        changed = False
        
        if isinstance(obj, dict):
            if field in obj:
                del obj[field]
                changed = True
            
            # Recurse into remaining values
            for v in list(obj.values()):
                if isinstance(v, (dict, list)):
                    if JSONPath._delete_recursive(v, field):
                        changed = True
                        
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    if JSONPath._delete_recursive(item, field):
                        changed = True
                        
        return changed

class JSONDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_json(self, key: str) -> Optional[dict]:
        """Ensure value at key is a JSON object."""
        if not self.db.exists(key):
            return None
        value = self.db.store.get(key)
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return value if isinstance(value, (dict, list)) else None

    def json_set(self, key: str, path: str, value: str) -> str:
        """Set JSON value at path."""
        try:
            json_value = value
            try:
                json_value = json.loads(value)
            except json.JSONDecodeError:
                if path != '$':  # Allow string values for non-root paths
                    json_value = value

            # Handle root path
            if path == '$':
                self.db.store[key] = json_value
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"JSON.SET {key} {path} {value}")
                return "OK"

            # Get existing object or create new
            current = self._ensure_json(key)
            if current is None:
                if '..' in path:  # Can't set recursive path on non-existent object
                    return "ERROR"
                current = {}
                self.db.store[key] = current

            # Set value at path
            if JSONPath.set_value(current, path, json_value):
                if not self.db.replaying:
                    self.db.persistence_manager.log_command(f"JSON.SET {key} {path} {value}")
                return "OK"
            return "ERROR"
        except Exception as e:
            return f"ERROR: {str(e)}"

    def json_get(self, key: str, path: str = '$') -> Optional[str]:
        """Get JSON value at path."""
        try:
            obj = self._ensure_json(key)
            if obj is None:
                return None
                
            if path == '$':
                return json.dumps(obj)
                
            components = JSONPath.parse_path(path)
            value = obj
            for comp in components:
                if isinstance(value, dict):
                    value = value.get(comp)
                else:
                    return None
                    
            return json.dumps(value) if value is not None else None
        except Exception:
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
