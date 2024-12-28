# src/server.py

import socket
import select
from core.database import KeyValueStore
from protocol import parse_resp, format_resp
from commands.core_handler import CoreCommandHandler
from commands.string_handler import StringCommandHandler
from commands.transaction_handler import TransactionCommandHandler

class TCPServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.db = KeyValueStore()
        self.server_socket = None
        self.shutting_down = False
        self.socket_timeout = 0.1
        self.active_clients = set()

        # Initialize command handlers
        self.command_map = {}
        self._init_command_handlers()

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
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(self.socket_timeout)
            
            while not self.shutting_down:
                try:
                    readable, _, _ = select.select([self.server_socket], [], [], self.socket_timeout)
                    if self.server_socket in readable:
                        client_socket, address = self.server_socket.accept()
                        client_socket.setblocking(False)  # Make client socket non-blocking
                        print(f"Connection from {address}")
                        self.active_clients.add(client_socket)
                        with client_socket:
                            self.handle_client(client_socket, address)
                except select.error:
                    if self.shutting_down:
                        break
                except socket.timeout:
                    continue  # Check shutdown flag every second
                except OSError as e:
                    if self.shutting_down:
                        break
                    else:
                        raise
        except KeyboardInterrupt:
            print("\nReceived shutdown signal...")
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

    def handle_client(self, client_socket, address):
        client_id = address[1]
        try:
            while not self.shutting_down:  # Check shutdown flag
                try:
                    readable, _, _ = select.select([client_socket], [], [], self.socket_timeout)
                    if not readable:
                        continue
                        
                    data = client_socket.recv(1024).decode()
                    if not data or self.shutting_down:
                        break
                    request = parse_resp(data)
                    response = self.process_request(request, client_id)
                    client_socket.sendall(format_resp(response).encode())
                except (socket.error, ConnectionResetError) as e:
                    print(f"Connection error for {address}: {e}")
                    break
                except Exception as e:
                    error_message = f"ERROR: {str(e)}"
                    try:
                        client_socket.sendall(format_resp(error_message).encode())
                    except:
                        print(f"Could not send error to client {address}: {error_message}")
                    continue
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
        if command in self.command_map:
            return self.command_map[command](client_id, *args)
        return f"ERROR: Unknown command {command}"


