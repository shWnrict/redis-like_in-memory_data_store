import socket
from .error_handling import ConnectionError, ResponseError, InvalidResponse
from src.protocol import RESPProtocol  # Ensure RESPProtocol is imported

class Connection:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.socket = None

    def connect(self):
        if self.socket:
            return
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        except socket.error as e:
            raise ConnectionError(f"Error connecting to {self.host}:{self.port} - {e}")

    def disconnect(self):
        if self.socket:
            try:
                self.socket.close()
            except socket.error as e:
                raise ConnectionError(f"Error disconnecting from {self.host}:{self.port} - {e}")
            finally:
                self.socket = None

    def send_command(self, *args):
        self.connect()
        command = RESPProtocol.format_command(*args)
        try:
            self.socket.sendall(command.encode('utf-8'))
        except socket.error as e:
            raise ConnectionError(f"Error sending command to {self.host}:{self.port} - {e}")

    def send_command_raw(self, command):
        """
        Send a raw RESP-formatted command string.
        """
        self.connect()
        try:
            self.socket.sendall(command.encode('utf-8'))
        except socket.error as e:
            raise ConnectionError(f"Error sending raw command to {self.host}:{self.port} - {e}")

    def read_response(self):
        self.connect()
        try:
            response = self.socket.recv(4096)
            if not response:
                raise InvalidResponse("No response received from server")
            return RESPProtocol.parse_response(response.decode('utf-8').strip())  # Parse response using RESPProtocol
        except socket.error as e:
            raise ResponseError(f"Error reading response from {self.host}:{self.port} - {e}")
