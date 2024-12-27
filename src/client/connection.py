import socket  # Import socket module
from .error_handling import ConnectionError, ResponseError, InvalidResponse
from src.protocol import RESPProtocol  # Ensure RESPProtocol is imported
import logging

logger = logging.getLogger(__name__)

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
            logger.info(f"Connected to {self.host}:{self.port}")
        except socket.error as e:
            logger.error(f"Connection error: {e}")
            self.socket = None
            raise ConnectionError(f"Unable to connect to {self.host}:{self.port}") from e

    def disconnect(self):
        if self.socket:
            try:
                self.socket.close()
                logger.info(f"Disconnected from {self.host}:{self.port}")
            except socket.error as e:
                logger.error(f"Disconnection error: {e}")
            finally:
                self.socket = None

    def send_command(self, *args):
        self.connect()
        command = RESPProtocol.format_command(*args)
        try:
            self.socket.sendall(command.encode())
            logger.debug(f"Sent command: {command.strip()}")
        except socket.error as e:
            logger.error(f"Send command error: {e}")
            raise ConnectionError("Failed to send command") from e

    def send_command_raw(self, command):
        """
        Send a raw RESP-formatted command string.
        """
        self.connect()
        try:
            self.socket.sendall(command.encode())
            logger.debug(f"Sent raw command: {command.strip()}")
        except socket.error as e:
            logger.error(f"Send raw command error: {e}")
            raise ConnectionError("Failed to send raw command") from e

    def read_response(self):
        self.connect()
        try:
            response = self.socket.recv(4096).decode()
            logger.debug(f"Received response: {response.strip()}")
            return response
        except socket.error as e:
            logger.error(f"Read response error: {e}")
            raise ConnectionError("Failed to read response") from e
