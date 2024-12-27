# src/pubsub/subscriber.py
import socket
import threading
from src.logger import setup_logger
from src.protocol import RESPProtocol
from typing import List, Union

logger = setup_logger("subscriber")

class Subscriber:
    def __init__(self, cache, host="localhost", port=5555):
        self.cache = cache
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.subscribed = False
        self.thread = threading.Thread(target=self.listen)
        self.thread.daemon = True

    def subscribe(self, topic="cache_invalidation"):
        """
        Subscribe to the specified topic for cache invalidation messages.
        """
        try:
            self.socket.connect((self.host, self.port))
            subscribe_message = f"SUBSCRIBE {topic}\n"
            self.socket.sendall(subscribe_message.encode())
            self.subscribed = True
            logger.info(f"Subscribed to topic '{topic}'.")
            self.thread.start()
        except Exception as e:
            logger.error(f"Failed to subscribe to topic '{topic}': {e}")

    def listen(self):
        while self.subscribed:
            try:
                data = self.socket.recv(1024)
                if not data:
                    break
                messages = data.decode().strip().split("\n")
                for message in messages:
                    self.handle_message(message)
            except Exception as e:
                logger.error(f"Error receiving messages: {e}")
                break

    def handle_message(self, message):
        """
        Handle incoming cache invalidation messages.
        """
        parts = message.strip().split()
        if len(parts) == 2 and parts[0] == "INVALIDATE":
            key = parts[1]
            self.cache.invalidate_cache(key)
            logger.info(f"Cache invalidated for key '{key}'.")

    def receive_message(self):
        # Placeholder for receiving messages from Pub/Sub
        # Implement the actual message receiving logic here
        pass
