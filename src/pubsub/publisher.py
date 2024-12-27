from src.logger import setup_logger
import threading
from src.protocol import RESPProtocol
from collections import defaultdict
from typing import Dict, Set, Optional, List, Union
import socket

logger = setup_logger("pubsub")

class PubSub:
    def __init__(self, server):
        self.server = server
        self.lock = threading.RLock()  # Using RLock instead of Lock for nested lock acquisition
        self.channels: Dict[str, Set[socket.socket]] = defaultdict(set)  # Channel -> Set of client sockets
        self.client_channels: Dict[socket.socket, Set[str]] = defaultdict(set)  # Client socket -> Set of subscribed channels
        self.patterns: Dict[str, Set[socket.socket]] = defaultdict(set)  # Pattern -> Set of client sockets
        self.subscribed_clients: Set[socket.socket] = set()  # Set of all subscribed client sockets

    def subscribe(self, client_socket: socket.socket, channel: str) -> List[Union[str, int]]:
        """
        Subscribe a client to a channel.
        
        Args:
            client_socket: The client's socket object
            channel: The channel name to subscribe to
            
        Returns:
            List containing [action, channel, subscriber_count]
        """
        with self.lock:
            if client_socket not in self.channels[channel]:
                self.channels[channel].add(client_socket)
                self.client_channels[client_socket].add(channel)
                self.subscribed_clients.add(client_socket)
                
                subscriber_count = len(self.channels[channel])
                logger.info(f"Client {client_socket} subscribed to channel {channel}. Total subscribers: {subscriber_count}")
                
                response = ["subscribe", channel, subscriber_count]
                self._send_message_to_client(client_socket, response)
                
                return response
            else:
                logger.info(f"Client {client_socket} is already subscribed to channel {channel}")
                return ["subscribe", channel, len(self.channels[channel])]

    def unsubscribe(self, client_socket: socket.socket, channel: Optional[str] = None) -> List[Union[str, int]]:
        """
        Unsubscribe a client from a specific channel or all channels.
        
        Args:
            client_socket: The client's socket object
            channel: Optional channel name. If None, unsubscribe from all channels
            
        Returns:
            List containing [action, channel(s), remaining_subscriptions]
        """
        with self.lock:
            if channel:
                if channel in self.channels:
                    self.channels[channel].discard(client_socket)
                    self.client_channels[client_socket].discard(channel)
                    
                    if not self.channels[channel]:
                        del self.channels[channel]
                    
                    remaining = len(self.client_channels[client_socket])
                    logger.info(f"Client {client_socket} unsubscribed from {channel}. Remaining subscriptions: {remaining}")
                    response = ["unsubscribe", channel, remaining]
                    self._send_message_to_client(client_socket, response)
                    return response
            else:
                unsubscribed = list(self.client_channels[client_socket])
                for ch in unsubscribed:
                    self.channels[ch].discard(client_socket)
                    if not self.channels[ch]:
                        del self.channels[ch]
                
                self.client_channels[client_socket].clear()
                self.subscribed_clients.discard(client_socket)
                
                logger.info(f"Client {client_socket} unsubscribed from all channels: {unsubscribed}")
                response = ["unsubscribe", unsubscribed, 0]
                self._send_message_to_client(client_socket, response)
                return response

    def publish(self, channel: str, message: str) -> int:
        """
        Publish a message to all subscribers of a channel.
        
        Args:
            channel: The channel to publish to
            message: The message to publish
            
        Returns:
            Number of clients that received the message
        """
        with self.lock:
            if channel not in self.channels:
                logger.info(f"No subscribers for channel {channel}")
                return 0

            message_data = ["message", channel, message]
            active_subscribers = set()
            failed_clients = set()

            for client_socket in self.channels[channel]:
                if self._send_message_to_client(client_socket, message_data):
                    active_subscribers.add(client_socket)
                else:
                    failed_clients.add(client_socket)
                    logger.warning(f"Failed to send message to client {client_socket}, marking for cleanup")

            self._cleanup_failed_clients(failed_clients)

            logger.info(f"Published message to {len(active_subscribers)} subscribers on channel {channel}")
            return len(active_subscribers)

    def _send_message_to_client(self, client_socket: socket.socket, message: List[Union[str, int]]) -> bool:
        """
        Send a message to a specific client.
        
        Args:
            client_socket: The client's socket object
            message: The message to send
            
        Returns:
            bool indicating if the message was sent successfully
        """
        try:
            formatted_message = RESPProtocol.format_response(message)
            client_socket.sendall(formatted_message.encode())
            logger.debug(f"Message sent to client {client_socket}: {message}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message to client {client_socket}: {e}")
            return False

    def _cleanup_failed_clients(self, failed_clients: Set[socket.socket]) -> None:
        """
        Clean up state for clients that failed to receive messages.
        
        Args:
            failed_clients: Set of client sockets that failed to receive messages
        """
        with self.lock:
            for client_socket in failed_clients:
                subscribed_channels = self.client_channels.get(client_socket, set())
                for channel in subscribed_channels:
                    self.channels[channel].discard(client_socket)
                    if not self.channels[channel]:
                        del self.channels[channel]
                
                self.client_channels.pop(client_socket, None)
                self.subscribed_clients.discard(client_socket)
                
                logger.info(f"Cleaned up state for failed client {client_socket}")

    def get_client_subscriptions(self, client_socket: socket.socket) -> Set[str]:
        """
        Get all channels a client is subscribed to.
        
        Args:
            client_socket: The client's socket object
            
        Returns:
            Set of channel names
        """
        with self.lock:
            return self.client_channels.get(client_socket, set()).copy()