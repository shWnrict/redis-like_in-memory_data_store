from collections import defaultdict
from threading import Lock

class PubSubManager:
    """
    PubSubManager is a class that manages a publish-subscribe messaging system.
    
    Attributes:
        channels (defaultdict): A dictionary where keys are channel names and values are sets of client IDs subscribed to those channels.
        client_subscriptions (defaultdict): A dictionary where keys are client IDs and values are sets of channels to which the clients are subscribed.
        lock (Lock): A threading lock to ensure thread-safe operations on the data structures.
    """
    def __init__(self):
        self.channels = defaultdict(set)
        self.client_subscriptions = defaultdict(set)
        self.lock = Lock()

    def subscribe(self, client_id, channel):
        """Subscribe a client to a channel."""
        with self.lock:
            self.channels[channel].add(client_id)
            self.client_subscriptions[client_id].add(channel)
            count = len(self.client_subscriptions[client_id])
            return count

    def unsubscribe(self, client_id, channel=None):
        """Unsubscribe a client from a channel or all channels if channel is None."""
        with self.lock:
            if channel is None:
                channels_to_remove = list(self.client_subscriptions[client_id])
                for chan in channels_to_remove:
                    self.channels[chan].discard(client_id)
                    if not self.channels[chan]:
                        del self.channels[chan]
                self.client_subscriptions[client_id].clear()
            else:
                self.channels[channel].discard(client_id)
                self.client_subscriptions[client_id].discard(channel)
                if not self.channels[channel]:
                    del self.channels[channel]
            
            count = len(self.client_subscriptions[client_id])
            return count

    def publish(self, channel, message):
        """Publish a message to a channel and return the number of receivers."""
        with self.lock:
            subscribers = self.channels.get(channel, set())
            return len(subscribers), list(subscribers)

    def get_client_channels(self, client_id):
        """Get all channels a client is subscribed to."""
        return list(self.client_subscriptions[client_id])

    def remove_client(self, client_id):
        """Remove a client from all subscriptions."""
        self.unsubscribe(client_id)
        if client_id in self.client_subscriptions:
            del self.client_subscriptions[client_id]
