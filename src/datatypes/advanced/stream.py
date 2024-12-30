import time
import collections
from typing import Dict, List, Optional, Tuple

class StreamEntry:
    def __init__(self, id: str, fields: Dict[str, str]):
        self.id = id
        self.fields = fields

class ConsumerGroup:
    def __init__(self, name: str, last_id: str = '0-0'):
        self.name = name
        self.last_delivered_id = last_id
        self.pending = {}  # message_id -> consumer_name
        self.consumers = collections.defaultdict(int)  # consumer_name -> pending count

class StreamDataType:
    """
    StreamDataType provides methods to add, read, and manage stream entries, as well as to handle consumer groups.
    Attributes:
        db (Database): The database instance where the stream data is stored.
        _last_ms (int): The last millisecond timestamp used for generating stream entry IDs.
        _seq_no (int): The sequence number used for generating stream entry IDs.
    Methods:
        __init__(database):
            Initializes the StreamDataType with the given database.
        _ensure_stream(key: str) -> Dict:
            Ensures that the value at the given key is a stream. If not, initializes a new stream.
        _generate_id() -> str:
            Generates a unique stream entry ID based on the current timestamp and sequence number.
        xadd(key: str, fields: Dict[str, str], id: str = '*') -> str:
            Adds an entry to a stream with the given fields and optional ID. If ID is '*', a new ID is generated.
        xread(keys: List[str], ids: List[str], count: Optional[int] = None) -> Dict[str, List[Tuple[str, Dict]]]:
            Reads entries from one or more streams starting from the given IDs. Optionally limits the number of entries.
        xrange(key: str, start: str = '-', end: str = '+', count: Optional[int] = None) -> List[Tuple[str, Dict]]:
            Returns a range of entries from a stream between the given start and end IDs. Optionally limits the number of entries.
        xlen(key: str) -> int:
            Returns the length of a stream (number of entries).
        xgroup_create(key: str, group_name: str, id: str = '$', mkstream: bool = False) -> bool:
            Creates a consumer group for the stream at the given key. Optionally creates the stream if it doesn't exist.
        xreadgroup(group: str, consumer: str, keys: List[str], ids: List[str], count: Optional[int] = None) -> Dict[str, List[Tuple[str, Dict]]]:
            Reads entries from a stream as part of a consumer group. Supports reading new messages or from a specific ID.
        xack(key: str, group: str, *ids: str) -> int:
            Acknowledges consumed messages in a consumer group, removing them from the pending list.
    """
    def __init__(self, database):
        self.db = database
        self._last_ms = 0
        self._seq_no = 0

    def _ensure_stream(self, key: str) -> Dict:
        """Ensure the value at key is a stream."""
        if not self.db.exists(key):
            stream = {
                'entries': collections.OrderedDict(),
                'groups': {},
                'last_id': '0-0'
            }
            self.db.store[key] = stream
            return stream
            
        stream = self.db.store.get(key)
        if not isinstance(stream, dict) or 'entries' not in stream:
            stream = {
                'entries': collections.OrderedDict(),
                'groups': {},
                'last_id': '0-0'
            }
            self.db.store[key] = stream
        return stream

    def _generate_id(self) -> str:
        """Generate a unique stream entry ID."""
        ms = int(time.time() * 1000)
        if ms == self._last_ms:
            self._seq_no += 1
        else:
            self._last_ms = ms
            self._seq_no = 0
        return f"{ms}-{self._seq_no}"

    def xadd(self, key: str, fields: Dict[str, str], id: str = '*') -> str:
        """Add an entry to a stream."""
        try:
            stream = self._ensure_stream(key)
            if id == '*':
                id = self._generate_id()
            elif id in stream['entries']:
                raise ValueError("Duplicate ID")

            entry = StreamEntry(id, fields)
            stream['entries'][id] = entry
            stream['last_id'] = id

            if not self.db.replaying:
                fields_str = ' '.join(f"{k} {v}" for k, v in fields.items())
                self.db.persistence_manager.log_command(f"XADD {key} {id} {fields_str}")

            return id
        except Exception as e:
            return str(e)

    def xread(self, keys: List[str], ids: List[str], count: Optional[int] = None) -> Dict[str, List[Tuple[str, Dict]]]:
        """Read from one or more streams."""
        result = {}
        for key, start_id in zip(keys, ids):
            try:
                stream = self._ensure_stream(key)
                entries = []
                for id, entry in stream['entries'].items():
                    if id > start_id:
                        entries.append((id, entry.fields))
                        if count and len(entries) >= count:
                            break
                if entries:
                    result[key] = entries
            except ValueError:
                continue
        return result

    def xrange(self, key: str, start: str = '-', end: str = '+', count: Optional[int] = None) -> List[Tuple[str, Dict]]:
        """Return a range of entries from a stream."""
        try:
            stream = self._ensure_stream(key)
            result = []
            for id, entry in stream['entries'].items():
                if (start == '-' or id >= start) and (end == '+' or id <= end):
                    result.append((id, entry.fields))
                    if count and len(result) >= count:
                        break
            return result
        except Exception:
            return []

    def xlen(self, key: str) -> int:
        """Return the length of a stream."""
        try:
            stream = self._ensure_stream(key)
            return len(stream['entries'])
        except Exception:
            return 0

    def xgroup_create(self, key: str, group_name: str, id: str = '$', mkstream: bool = False) -> bool:
        """Create a consumer group."""
        try:
            # Create stream if MKSTREAM is specified and stream doesn't exist
            if mkstream and not self.db.exists(key):
                self._ensure_stream(key)

            stream = self._ensure_stream(key)
            if group_name in stream['groups']:
                return False
            
            last_id = stream['last_id'] if id == '$' else id
            stream['groups'][group_name] = ConsumerGroup(group_name, last_id)
            
            if not self.db.replaying:
                mk_param = " MKSTREAM" if mkstream else ""
                self.db.persistence_manager.log_command(f"XGROUP CREATE {key} {group_name} {id}{mk_param}")
            
            return True
        except ValueError:
            return False

    def xreadgroup(self, group: str, consumer: str, keys: List[str], ids: List[str], 
                   count: Optional[int] = None) -> Dict[str, List[Tuple[str, Dict]]]:
        """Read from a stream as part of a consumer group."""
        result = {}
        for key, id in zip(keys, ids):
            try:
                stream = self._ensure_stream(key)
                if group not in stream['groups']:
                    continue

                group_obj = stream['groups'][group]
                entries = []

                if id == '>':  # Read new messages only
                    for entry_id, entry in stream['entries'].items():
                        if entry_id > group_obj.last_delivered_id:
                            entries.append((entry_id, entry.fields))
                            group_obj.pending[entry_id] = consumer
                            group_obj.consumers[consumer] += 1
                            if count and len(entries) >= count:
                                break
                    # Update last delivered ID
                    if entries:
                        group_obj.last_delivered_id = entries[-1][0]
                else:  # Read from specific ID
                    for entry_id, entry in stream['entries'].items():
                        if entry_id > id:
                            entries.append((entry_id, entry.fields))
                            if entry_id not in group_obj.pending:
                                group_obj.pending[entry_id] = consumer
                                group_obj.consumers[consumer] += 1
                            if count and len(entries) >= count:
                                break

                if entries:
                    result[key] = entries
            except ValueError:
                continue
        return result

    def xack(self, key: str, group: str, *ids: str) -> int:
        """Acknowledge consumed messages in a consumer group."""
        try:
            stream = self._ensure_stream(key)
            if group not in stream['groups']:
                return 0

            group_obj = stream['groups'][group]
            ack_count = 0

            for id in ids:
                if id in group_obj.pending:
                    consumer = group_obj.pending[id]
                    group_obj.consumers[consumer] -= 1
                    if group_obj.consumers[consumer] <= 0:
                        del group_obj.consumers[consumer]
                    del group_obj.pending[id]
                    ack_count += 1

            if ack_count and not self.db.replaying:
                self.db.persistence_manager.log_command(f"XACK {key} {group} {' '.join(ids)}")

            return ack_count
        except ValueError:
            return 0
