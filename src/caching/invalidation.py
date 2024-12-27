class CacheInvalidation:
    def __init__(self):
        self.subscribers = {}

    def subscribe(self, key, client):
        if key not in self.subscribers:
            self.subscribers[key] = set()
        self.subscribers[key].add(client)

    def unsubscribe(self, key, client):
        if key in self.subscribers:
            self.subscribers[key].discard(client)
            if not self.subscribers[key]:
                del self.subscribers[key]

    def invalidate(self, key):
        if key in self.subscribers:
            for client in self.subscribers[key]:
                client.invalidate_cache(key)
