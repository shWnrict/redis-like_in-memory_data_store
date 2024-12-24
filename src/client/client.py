# src/client/client.py
import socket
import time

class DataStoreClient:
    def __init__(self, host="127.0.0.1", port=6379, retries=3, retry_delay=2):
        self.host = host
        self.port = port
        self.retries = retries
        self.retry_delay = retry_delay
        self.socket = None
        self.connect()

    def connect(self):
        """
        Establish a connection to the server.
        """
        for attempt in range(self.retries):
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.host, self.port))
                print("Connected to the server")
                return
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                time.sleep(self.retry_delay)
        raise ConnectionError("Failed to connect to the server after multiple attempts")

    def send_command(self, command):
        """
        Send a command to the server and receive the response.
        """
        try:
            if not self.socket:
                self.connect()
            self.socket.sendall(command.encode() + b"\r\n")
            response = self.socket.recv(4096).decode()
            return response.strip()
        except (socket.error, ConnectionResetError):
            self.socket.close()
            self.connect()
            return self.send_command(command)

    def close(self):
        """
        Close the connection to the server.
        """
        if self.socket:
            self.socket.close()
            self.socket = None

    # Helper methods for server operations
    def set(self, key, value):
        return self.send_command(f"SET {key} {value}")

    def get(self, key):
        return self.send_command(f"GET {key}")

    def del_key(self, key):
        return self.send_command(f"DEL {key}")

    def bitfield_get(self, key, offset, size):
        return self.send_command(f"BITFIELD GET {key} {offset} {size}")

    def bitfield_set(self, key, offset, size, value):
        return self.send_command(f"BITFIELD SET {key} {offset} {size} {value}")

    def bitfield_incrby(self, key, offset, size, delta):
        return self.send_command(f"BITFIELD INCRBY {key} {offset} {size} {delta}")
