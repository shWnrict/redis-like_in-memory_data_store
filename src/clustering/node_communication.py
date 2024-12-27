import socket
import threading

class NodeCommunication:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.lock = threading.Lock()

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

    def send_message(self, message):
        self.connect()
        try:
            self.socket.sendall(message.encode('utf-8'))
        except socket.error as e:
            raise ConnectionError(f"Error sending message to {self.host}:{self.port} - {e}")

    def receive_message(self):
        self.connect()
        try:
            response = self.socket.recv(4096)
            if not response:
                raise InvalidResponse("No response received from node")
            return response.decode('utf-8').strip()
        except socket.error as e:
            raise ResponseError(f"Error receiving message from {self.host}:{self.port} - {e}")
