# src/server.py

import socket
import select
import logging
from core.database import KeyValueStore
from protocol import parse_resp, format_resp, format_pubsub_message
from commands.core_handler import CoreCommandHandler
from commands.string_handler import StringCommandHandler
from commands.transaction_handler import TransactionCommandHandler
from pubsub import PubSubManager
from collections import defaultdict

class TCPServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.db = KeyValueStore()
        self.server_socket = None
        self.shutting_down = False
        self.socket_timeout = 0.1
        self.active_clients = set()
        self.pubsub_manager = PubSubManager()
        self.subscribed_clients = set()
        self.logger = logging.getLogger('TCPServer')
        self.client_sockets = {}  # Map client_id to socket
        self.client_channels = defaultdict(set)  # Track channels per client

        # Initialize command handlers
        self.command_map = {}
        self._init_command_handlers()
        
        # Set command map in database for transaction handling
        self.db.set_command_map(self.command_map)

    def _init_command_handlers(self):
        """Initialize all command handlers and build command map."""
        handlers = [
            CoreCommandHandler(self.db),
            StringCommandHandler(self.db),
            TransactionCommandHandler(self.db),
        ]
        
        for handler in handlers:
            self.command_map.update(handler.get_commands())

    def start(self):
        print(f"Server started on {self.host}:{self.port}")
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(self.socket_timeout)
            
            while not self.shutting_down:
                try:
                    readable, _, _ = select.select([self.server_socket] + list(self.active_clients), [], [], self.socket_timeout)
                    
                    for sock in readable:
                        if sock is self.server_socket:
                            client_socket, address = self.server_socket.accept()
                            client_socket.setblocking(False)
                            print(f"Connection from {address}")
                            self.active_clients.add(client_socket)
                            self.client_sockets[address[1]] = client_socket
                            self.logger.info(f"New client connection: {address[1]}")
                        else:
                            try:
                                self.handle_client_data(sock)
                            except Exception as e:
                                self.logger.error(f"Error handling client data: {e}")
                                self.cleanup_client_by_socket(sock)
                                
                except select.error:
                    if self.shutting_down:
                        break
                except socket.timeout:
                    continue
                except Exception as e:
                    self.logger.error(f"Server error: {e}")
                    if self.shutting_down:
                        break
        finally:
            self.stop()

    def stop(self):
        """Stop the server and cleanup resources."""
        if not self.shutting_down:  # Prevent multiple shutdown attempts
            self.shutting_down = True
            print("Shutting down server...")
            
            # Notify and close all active client connections
            shutdown_message = format_resp("Server shutting down...").encode()
            for client_socket in self.active_clients.copy():
                try:
                    client_socket.send(shutdown_message)
                    client_socket.shutdown(socket.SHUT_RDWR)
                    client_socket.close()
                except (OSError, socket.error):
                    pass
                self.active_clients.remove(client_socket)

            # Close server socket
            if self.server_socket:
                try:
                    self.server_socket.shutdown(socket.SHUT_RDWR)
                    self.server_socket.close()
                except OSError:
                    pass

            self.db.stop()
            print("Server stopped.")

    def handle_client_data(self, client_socket):
        """Handle data from a connected client."""
        try:
            data = client_socket.recv(1024).decode()
            if not data:
                raise ConnectionError("Client disconnected")
                
            client_id = client_socket.getpeername()[1]
            request = parse_resp(data)
            
            if not request:
                return
                
            self.logger.debug(f"Received request from {client_id}: {request}")
            response = self.process_request(request, client_id)
            
            # Handle subscription mode
            if isinstance(response, tuple):
                msg_type, channel = response
                if msg_type == "SUBSCRIBE_MODE":
                    self.logger.debug(f"Subscribing client {client_id} to {channel}")
                    self.subscribed_clients.add(client_id)
                    self.client_channels[client_id].add(channel)
                    subscribe_msg = format_pubsub_message("subscribe", channel)
                    client_socket.sendall(subscribe_msg.encode())
                    return
            
            if response is not None:
                client_socket.sendall(format_resp(response).encode())
                
        except Exception as e:
            self.logger.error(f"Error in handle_client_data: {e}")
            raise

    def cleanup_client_by_socket(self, client_socket):
        """Clean up client resources using socket reference."""
        try:
            client_id = client_socket.getpeername()[1]
            self.logger.info(f"Cleaning up client {client_id}")
            if client_id in self.client_channels:
                channels = self.client_channels.pop(client_id)
                self.logger.debug(f"Removing client {client_id} from channels: {channels}")
            self.pubsub_manager.remove_client(client_id)
            self.subscribed_clients.discard(client_id)
            self.client_sockets.pop(client_id, None)
        except:
            pass
        finally:
            if client_socket in self.active_clients:
                self.active_clients.remove(client_socket)
            try:
                client_socket.close()
            except:
                pass

    def process_request(self, request, client_id):
        if not request:
            return "ERROR: Empty command"
        
        command = request[0].upper()
        args = request[1:]

        self.logger.debug(f"Processing command from client {client_id}: {command} {args}")

        # Handle pub/sub commands first
        if command == "SUBSCRIBE":
            if len(args) != 1:
                return "ERROR: SUBSCRIBE requires channel name"
            channel = args[0]
            self.logger.info(f"Client {client_id} subscribing to channel {channel}")
            count = self.pubsub_manager.subscribe(client_id, channel)
            return ("SUBSCRIBE_MODE", channel)

        if command == "PUBLISH":
            if len(args) != 2:
                return "ERROR: PUBLISH requires channel and message"
            channel, message = args
            receivers, subscriber_list = self.pubsub_manager.publish(channel, message)
            
            self.logger.info(f"Publishing to {len(subscriber_list)} subscribers on channel {channel}")
            delivered = 0
            for subscriber_id in subscriber_list:
                if subscriber_id in self.subscribed_clients:
                    try:
                        subscriber_socket = self.client_sockets.get(subscriber_id)
                        if subscriber_socket:
                            message_resp = format_pubsub_message("message", channel, message)
                            subscriber_socket.sendall(message_resp.encode())
                            delivered += 1
                            self.logger.debug(f"Successfully delivered to subscriber {subscriber_id}")
                    except Exception as e:
                        self.logger.error(f"Failed to deliver to subscriber {subscriber_id}: {e}")
                        continue
            return delivered

        # Handle transaction commands specially
        if command in ["MULTI", "EXEC", "DISCARD"]:
            return self.command_map[command](client_id, *args)

        # Check if in transaction
        if self.db.transaction_manager.is_in_transaction(client_id):
            result = self.db.transaction_manager.queue_command(client_id, command, *args)
            if result is not None:
                return result

        # Normal command execution
        if command in self.command_map:
            return self.command_map[command](client_id, *args)
        return f"ERROR: Unknown command {command}"


