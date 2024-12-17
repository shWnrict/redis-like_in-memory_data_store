class KeyValueStore:
    def __init__(self, is_master=True):
        self.store = {}
        self.expirations = {}
        self.subscribers = {}  # Dictionary to track subscribers
        self.transaction_queue = []
        self.is_master = is_master
        self.slaves = []  # List of slave instances

    def set(self, key, value):
        self.store[key] = value
        return "OK"

    def get(self, key):
        return self.store.get(key, None)

    def delete(self, key):
        if key in self.store:
            del self.store[key]
            return "OK"
        return "ERR: Key not found"

    def exists(self, key):
        return key in self.store

    # String operations
    def append(self, key, value):
        if key in self.store:
            self.store[key] += value
        else:
            self.store[key] = value
        return len(self.store[key])

    def strlen(self, key):
        if key in self.store:
            return len(self.store[key])
        return 0

    def incr(self, key):
        if key in self.store:
            self.store[key] = str(int(self.store[key]) + 1)
        else:
            self.store[key] = "1"
        return int(self.store[key])

    def decr(self, key):
        if key in self.store:
            self.store[key] = str(int(self.store[key]) - 1)
        else:
            self.store[key] = "-1"
        return int(self.store[key])

    def incrby(self, key, increment):
        if key in self.store:
            self.store[key] = str(int(self.store[key]) + increment)
        else:
            self.store[key] = str(increment)
        return int(self.store[key])

    def decrby(self, key, decrement):
        if key in self.store:
            self.store[key] = str(int(self.store[key]) - decrement)
        else:
            self.store[key] = str(-decrement)
        return int(self.store[key])

# List operations
    def lpush(self, key, value):
        if key not in self.store:
            self.store[key] = []
        self.store[key].insert(0, value)
        return len(self.store[key])

    def rpush(self, key, value):
        if key not in self.store:
            self.store[key] = []
        self.store[key].append(value)
        return len(self.store[key])

    def lpop(self, key):
        if key in self.store and self.store[key]:
            return self.store[key].pop(0)
        return None

    def rpop(self, key):
        if key in self.store and self.store[key]:
            return self.store[key].pop()
        return None

    def lrange(self, key, start, end):
        if key in self.store:
            return self.store[key][start:end]
        return []

    def linsert(self, key, index, value):
        if key not in self.store:
            self.store[key] = []
        self.store[key].insert(index, value)
        return len(self.store[key])
    
    # Hash operations
    def hset(self, key, field, value):
        if key not in self.store:
            self.store[key] = {}
        self.store[key][field] = value
        return True

    def hget(self, key, field):
        if key in self.store and field in self.store[key]:
            return self.store[key][field]
        return None

    def hmset(self, key, field_value_map):
        if key not in self.store:
            self.store[key] = {}
        self.store[key].update(field_value_map)
        return True

    def hgetall(self, key):
        if key in self.store:
            return self.store[key]
        return {}

    def hdel(self, key, field):
        if key in self.store and field in self.store[key]:
            del self.store[key][field]
            return True
        return False

    def hexists(self, key, field):
        return field in self.store.get(key, {})
    
    #Key Expiration
    def set(self, key, value, ttl=None):
        """Store key-value pair and set expiration (optional)."""
        self.store[key] = value
        if ttl:
            self.expirations[key] = time.time() + ttl
        return "OK"

    def get(self, key):
        """Retrieve the value for a given key, considering expiration."""
        if key in self.store and (key not in self.expirations or time.time() < self.expirations[key]):
            return self.store[key]
        if key in self.expirations and time.time() >= self.expirations[key]:
            del self.store[key]
            del self.expirations[key]
        return None

    def delete(self, key):
        """Delete a key-value pair."""
        if key in self.store:
            del self.store[key]
            if key in self.expirations:
                del self.expirations[key]
            return "OK"
        return "ERR: Key not found"
    
    #Transactions
    def multi(self):
        """Begin a transaction."""
        self.transaction_queue = []

    def exec(self):
        """Execute the queued commands."""
        results = []
        for command, args in self.transaction_queue:
            method = getattr(self, command)
            result = method(*args)
            results.append(result)
        self.transaction_queue = []
        return results

    def discard(self):
        """Discard the transaction queue."""
        self.transaction_queue = []

    # Override methods to queue them in transaction
    def set(self, key, value, ttl=None):
        if self.transaction_queue is not None:
            self.transaction_queue.append(('set', (key, value, ttl)))
            return "QUEUED"
        self.store[key] = value
        if ttl:
            self.expirations[key] = time.time() + ttl
        return "OK"

    #Persistence
    def save_to_file(self, filename="dump.json"):
        """Save the store to a file (Snapshot)."""
        with open(filename, "w") as file:
            json.dump(self.store, file)
            json.dump(self.expirations, file)

    def load_from_file(self, filename="dump.json"):
        """Load the store from a file (Snapshot)."""
        try:
            with open(filename, "r") as file:
                self.store = json.load(file)
                self.expirations = json.load(file)
        except FileNotFoundError:
            pass

    #Add Pub/Sub
    def publish(self, channel, message):
        """Publish a message to a channel."""
        if channel in self.subscribers:
            for subscriber in self.subscribers[channel]:
                subscriber(message)

    def subscribe(self, channel, subscriber_callback):
        """Subscribe to a channel with a callback to handle messages."""
        if channel not in self.subscribers:
            self.subscribers[channel] = []
        self.subscribers[channel].append(subscriber_callback)
        return f"Subscribed to {channel}"

    def unsubscribe(self, channel, subscriber_callback):
        """Unsubscribe from a channel."""
        if channel in self.subscribers:
            if subscriber_callback in self.subscribers[channel]:
                self.subscribers[channel].remove(subscriber_callback)
                return f"Unsubscribed from {channel}"
        return f"Not subscribed to {channel}"
    
    #Replication
    def add_slave(self, slave):
        """Add a slave to replicate changes."""
        if self.is_master:
            self.slaves.append(slave)

    def replicate(self, command, *args):
        """Replicate a command to all slaves."""
        for slave in self.slaves:
            getattr(slave, command)(*args)

    def set(self, key, value, ttl=None):
        """Set a key-value pair and replicate to slaves."""
        self.store[key] = value
        if ttl:
            self.expirations[key] = time.time() + ttl
        if self.is_master:
            self.replicate('set', key, value, ttl)
        return "OK"