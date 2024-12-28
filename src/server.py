# src/server.py

import socket
from core.database import KeyValueStore

class TCPServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.db = KeyValueStore()
        self.command_map = {
            "SET": self.set_command,
            "GET": self.get_command,
            "DEL": self.del_command,
            "EXISTS": self.exists_command,
        }

    def start(self):
        """Start the TCP server."""
        print(f"Server started on {self.host}:{self.port}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)  # Allow up to 5 concurrent connections
            while True:
                client_socket, address = server_socket.accept()
                print(f"Connection from {address}")
                with client_socket:
                    self.handle_client(client_socket)

    def handle_client(self, client_socket):
        """Handle client requests."""
        while True:
            try:
                data = client_socket.recv(1024).decode().strip()
                if not data:
                    break
                response = self.process_request(data)
                client_socket.sendall(response.encode())
            except Exception as e:
                client_socket.sendall(f"ERROR: {e}".encode())
                break

    def process_request(self, request):
        """Process a single client request."""
        parts = request.split()
        if not parts:
            return "ERROR: Empty command"
        command = parts[0].upper()
        args = parts[1:]
        if command in self.command_map:
            return self.command_map[command](*args)
        return f"ERROR: Unknown command {command}"

    def set_command(self, key, value):
        self.db.set(key, value)
        return "OK"

    def get_command(self, key):
        value = self.db.get(key)
        return value if value else "(nil)"

    def del_command(self, key):
        result = self.db.delete(key)
        return "(1)" if result else "(0)"

    def exists_command(self, key):
        return "(1)" if self.db.exists(key) else "(0)"
