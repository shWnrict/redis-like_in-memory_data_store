from typing import Any, List, Dict, Set, Union

class RedisDataTypes:
    def __init__(self):
        self._string_store: Dict[str, str] = {}
        self._list_store: Dict[str, List[Any]] = {}
        self._set_store: Dict[str, Set[Any]] = {}
        self._hash_store: Dict[str, Dict[str, Any]] = {}

    # String Operations
    def string_set(self, key: str, value: str):
        self._string_store[key] = value

    def string_get(self, key: str) -> Union[str, None]:
        return self._string_store.get(key)

    def string_append(self, key: str, value: str):
        if key not in self._string_store:
            self._string_store[key] = ""
        self._string_store[key] += value

    # List Operations
    def list_rpush(self, key: str, value: Any):
        if key not in self._list_store:
            self._list_store[key] = []
        self._list_store[key].append(value)

    def list_lpush(self, key: str, value: Any):
        if key not in self._list_store:
            self._list_store[key] = []
        self._list_store[key].insert(0, value)

    def list_range(self, key: str, start: int, end: int) -> List[Any]:
        return self._list_store.get(key, [])[start:end+1]

    # Set Operations
    def set_add(self, key: str, value: Any):
        if key not in self._set_store:
            self._set_store[key] = set()
        self._set_store[key].add(value)

    def set_members(self, key: str) -> Set[Any]:
        return self._set_store.get(key, set())

    # Hash Operations
    def hash_set(self, key: str, field: str, value: Any):
        if key not in self._hash_store:
            self._hash_store[key] = {}
        self._hash_store[key][field] = value

    def hash_get(self, key: str, field: str) -> Union[Any, None]:
        return self._hash_store.get(key, {}).get(field)