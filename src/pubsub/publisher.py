# src/pubsub/publisher.py
from src.logger import setup_logger
import threading

logger = setup_logger("pubsub")

class PubSub:
    def __init__(self):
        self.lock = threading.Lock()
        self.channels = {}
        self.patterns = {}

    def subscribe(self, client_id, channel):
        """
        Subscribe a client to a channel.
        """
        with self.lock:
            if channel not in self.channels:
                self.channels[channel] = set()
            self.channels[channel].add(client_id)
            logger.info(f"SUBSCRIBE: Client {client_id} subscribed to channel {channel}")
            return f"Subscribed to {channel}"

    def psubscribe(self, client_id, pattern):
        """
        Subscribe a client to a pattern.
        """
        with self.lock:
            if pattern not in self.patterns:
                self.patterns[pattern] = set()
            self.patterns[pattern].add(client_id)
            logger.info(f"PSUBSCRIBE: Client {client_id} subscribed to pattern {pattern}")
            return f"psubscribed to {pattern}"

    def unsubscribe(self, client_id, channel=None):
        """
        Unsubscribe a client from a channel or all channels.
        """
        with self.lock:
            if channel:
                if channel in self.channels and client_id in self.channels[channel]:
                    self.channels[channel].remove(client_id)
                    if not self.channels[channel]:  # Remove empty channels
                        del self.channels[channel]
                    logger.info(f"UNSUBSCRIBE: Client {client_id} unsubscribed from {channel}")
                    return f"Unsubscribed from {channel}"
            else:
                unsubscribed_channels = []
                for ch, clients in list(self.channels.items()):
                    if client_id in clients:
                        clients.remove(client_id)
                        unsubscribed_channels.append(ch)
                        if not clients:  # Remove empty channels
                            del self.channels[ch]
                logger.info(f"UNSUBSCRIBE: Client {client_id} unsubscribed from all channels")
                return f"Unsubscribed from {', '.join(unsubscribed_channels)}" if unsubscribed_channels else "No subscriptions"

    def publish(self, channel, message):
        """
        Publish a message to all subscribers of a channel.
        """
        with self.lock:
            if channel not in self.channels:
                logger.info(f"PUBLISH: No subscribers for channel {channel}")
                return 0
            subscribers = self.channels[channel]
            for client_id in subscribers:
                self.__send_message(client_id, channel, message)
            logger.info(f"PUBLISH: Sent message to {len(subscribers)} subscribers on {channel}")
            
            # Match patterns
            total_subscribers = len(subscribers)
            for pattern, subs in self.patterns.items():
                if self._matches(pattern, channel):
                    for client_id in subs:
                        self.__send_message(client_id, channel, message)
                    total_subscribers += len(subs)
            return total_subscribers

    def _matches(self, pattern, channel):
        """
        Simple wildcard matching, adapt as needed.
        """
        return True if pattern in channel else False

    def __send_message(self, client_id, channel, message):
        """
        Send a message to a subscriber (mock implementation).
        """
        # Replace this with actual client communication logic.
        logger.info(f"Message sent to Client {client_id} on {channel}: {message}")
