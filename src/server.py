import socket
import select
import pickle
from collections import defaultdict
from core.database import KeyValueStore
from protocol import parse_resp, format_resp, format_pubsub_message
from pubsub import PubSubManager
from commands.core_handler import CoreCommandHandler
from commands.transaction_handler import TransactionCommandHandler
from commands.string_handler import StringCommandHandler
from commands.list_handler import ListCommandHandler
from commands.set_handler import SetCommandHandler
from commands.hash_handler import HashCommandHandler
from commands.zset_handler import ZSetCommandHandler
from commands.stream_handler import StreamCommandHandler
from commands.geo_handler import GeoCommandHandler
from commands.bitmap_handler import BitMapCommandHandler
from commands.bitfield_handler import BitFieldCommandHandler
from commands.probabilistic_handler import ProbabilisticCommandHandler
from commands.timeseries_handler import TimeSeriesCommandHandler
from commands.json_handler import JSONCommandHandler

class TCPServer:
    """
    TCPServer is a class that implements a Redis-like in-memory data store server.
    
    Attributes:
        host (str): The host address of the server.
        port (int): The port number of the server.
        db (KeyValueStore): The key-value store database instance.
        server_socket (socket.socket): The server socket.
        shutting_down (bool): Flag indicating if the server is shutting down.
        socket_timeout (float): The timeout value for socket operations.
        active_clients (set): A set of active client sockets.
        pubsub_manager (PubSubManager): The Pub/Sub manager instance.
        subscribed_clients (set): A set of subscribed client IDs.
        client_sockets (dict): A dictionary mapping client IDs to their sockets.
        client_channels (defaultdict): A dictionary mapping client IDs to their subscribed channels.
        replication_manager (ReplicationManager): The replication manager instance.
        slaves (set): A set of slave client IDs.
        command_map (dict): A dictionary mapping commands to their handlers.
    """
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
        self.client_sockets = {}
        self.client_channels = defaultdict(set)
        
        self.command_map = {}
        self._init_command_handlers()
        self.db.set_command_map(self.command_map)

    def _init_command_handlers(self):
        """Initialize all command handlers and build command map."""
        handlers = [
            CoreCommandHandler(self.db),
            StringCommandHandler(self.db),
            TransactionCommandHandler(self.db),
            ListCommandHandler(self.db),
            SetCommandHandler(self.db),  
            HashCommandHandler(self.db),  
            ZSetCommandHandler(self.db), 
            StreamCommandHandler(self.db),  
            GeoCommandHandler(self.db),  
            BitMapCommandHandler(self.db), 
            BitFieldCommandHandler(self.db),  
            ProbabilisticCommandHandler(self.db), 
            TimeSeriesCommandHandler(self.db),  
            JSONCommandHandler(self.db),  
        ]
        
        for handler in handlers:
            self.command_map.update(handler.get_commands())
        
        self.command_map.update({
            'PING': self.handle_ping,
            'FLUSHDB': self.handle_flushdb,
        })

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
                    sockets_to_monitor = [self.server_socket] + list(self.active_clients)
                    readable, _, _ = select.select(sockets_to_monitor, [], [], self.socket_timeout)
                    
                    for sock in readable:
                        if sock is self.server_socket:
                            client_socket, address = self.server_socket.accept()
                            client_socket.setblocking(False)
                            print(f"Connection from {address}")
                            self.active_clients.add(client_socket)
                            self.client_sockets[address[1]] = client_socket
                        else:
                            try:
                                self.handle_client_data(sock)
                            except Exception:
                                self.cleanup_client_by_socket(sock)
                                
                except select.error:
                    if self.shutting_down:
                        break
                except socket.timeout:
                    continue
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    if self.shutting_down:
                        break
                    print(f"Server error: {e}")
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
            data = client_socket.recv(1024).decode().strip()
            if not data:
                raise ConnectionError("Client disconnected")
                
            client_id = client_socket.getpeername()[1]
            request = parse_resp(data)
            
            if not request:
                return
                
            response = self.process_request(request, client_id)
            
            if isinstance(response, tuple) and len(response) >= 2:
                msg_type = response[0]
                channel = response[1]
                if msg_type == "SUBSCRIBE_MODE":
                    self.subscribed_clients.add(client_id)
                    self.client_channels[client_id].add(channel)
                    client_socket.sendall(format_pubsub_message("subscribe", channel).encode())
                    return
            
            # Format and send response
            formatted_response = format_resp(response)
            if formatted_response:
                client_socket.sendall(formatted_response.encode())
                
        except ConnectionError:
            # Handle normal disconnection
            try:
                addr = client_socket.getpeername()
                print(f"Client disconnected: {addr[0]}:{addr[1]}")
            except:
                print("Client disconnected")
            raise
        except Exception as e:
            print(f"Error handling client data: {str(e)}")
            raise

    def cleanup_client_by_socket(self, client_socket):
        """Clean up client resources using socket reference."""
        try:
            client_id = client_socket.getpeername()[1]
            if client_id in self.client_channels:
                channels = self.client_channels.pop(client_id)
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

    def handle_subscribe(self, client_id, channels):
        """Handle SUBSCRIBE command for multiple channels."""
        if not channels:
            return "ERROR: SUBSCRIBE requires at least one channel name"
            
        responses = []
        for channel in channels:
            self.pubsub_manager.subscribe(client_id, channel)
            self.subscribed_clients.add(client_id)
            self.client_channels[client_id].add(channel)
            responses.append(format_pubsub_message("subscribe", channel))
            
        return ("SUBSCRIBE_MODE", responses)

    def process_request(self, request, client_id):
        if not request:
            return "ERROR: Empty command"
        
        command = request[0].upper()
        args = request[1:]

        # Handle pub/sub commands first
        if command == "SUBSCRIBE":
            response = self.handle_subscribe(client_id, args)
            if isinstance(response, tuple) and response[0] == "SUBSCRIBE_MODE":
                client_socket = self.client_sockets.get(client_id)
                if client_socket:
                    for msg in response[1]:
                        client_socket.sendall(msg.encode())
                return None
            return response

        if command == "PUBLISH":
            if len(args) != 2:
                return "ERROR: PUBLISH requires channel and message"
            channel, message = args
            receivers, subscriber_list = self.pubsub_manager.publish(channel, message)
            
            delivered = 0
            for subscriber_id in subscriber_list:
                if subscriber_id in self.subscribed_clients:
                    try:
                        subscriber_socket = self.client_sockets.get(subscriber_id)
                        if subscriber_socket:
                            message_resp = format_pubsub_message("message", channel, message)
                            subscriber_socket.sendall(message_resp.encode())
                            delivered += 1
                    except:
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

    def handle_ping(self, client_id, *args):
        """Handle PING command."""
        if len(args) > 1:
            return "ERROR: PING takes 0 or 1 arguments"
        return args[0] if args else "PONG"

    def handle_flushdb(self, client_id, *args):
        """Handle FLUSHDB command."""
        if args:
            return "ERROR: FLUSHDB takes no arguments"
        self.db.flush()
        return "OK"


