from collections import defaultdict
from threading import Lock
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PubSubManager:
    def __init__(self):
        self.channels = defaultdict(set)
        self.client_subscriptions = defaultdict(set)
        self.lock = Lock()
        self.logger = logging.getLogger('PubSubManager')

    def subscribe(self, client_id, channel):
        """Subscribe a client to a channel."""
        with self.lock:
            self.channels[channel].add(client_id)
            self.client_subscriptions[client_id].add(channel)
            count = len(self.client_subscriptions[client_id])
            self.logger.info(f"Client {client_id} subscribed to {channel}. Total subscriptions: {count}")
            self.logger.debug(f"Channel {channel} subscribers: {self.channels[channel]}")
            return count

    def unsubscribe(self, client_id, channel=None):
        """Unsubscribe a client from a channel or all channels if channel is None."""
        with self.lock:
            if channel is None:
                self.logger.info(f"Client {client_id} unsubscribing from all channels")
                channels_to_remove = list(self.client_subscriptions[client_id])
                for chan in channels_to_remove:
                    self.channels[chan].discard(client_id)
                    self.logger.debug(f"Removed client {client_id} from channel {chan}")
                    if not self.channels[chan]:
                        del self.channels[chan]
                self.client_subscriptions[client_id].clear()
            else:
                self.logger.info(f"Client {client_id} unsubscribing from channel {channel}")
                self.channels[channel].discard(client_id)
                self.client_subscriptions[client_id].discard(channel)
                if not self.channels[channel]:
                    del self.channels[channel]
            
            count = len(self.client_subscriptions[client_id])
            self.logger.debug(f"Client {client_id} remaining subscriptions: {count}")
            return count

    def publish(self, channel, message):
        """Publish a message to a channel and return the number of receivers."""
        with self.lock:
            subscribers = self.channels.get(channel, set())
            self.logger.info(f"Publishing to channel {channel}: {message}")
            self.logger.debug(f"Channel {channel} has {len(subscribers)} subscribers: {subscribers}")
            return len(subscribers), list(subscribers)

    def get_client_channels(self, client_id):
        """Get all channels a client is subscribed to."""
        return list(self.client_subscriptions[client_id])

    def remove_client(self, client_id):
        """Remove a client from all subscriptions."""
        self.unsubscribe(client_id)
        if client_id in self.client_subscriptions:
            del self.client_subscriptions[client_id]
