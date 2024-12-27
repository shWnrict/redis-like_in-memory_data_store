import socket
import threading
from src.logger import setup_logger

logger = setup_logger("node_communication")

class NodeCommunication:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.connections = {}
        self.lock = threading.Lock()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = False
        self.thread = threading.Thread(target=self.listen_for_connections)
        self.thread.daemon = True

    def start(self):
        self.running = True
        self.thread.start()
        logger.info(f"NodeCommunication started on {self.host}:{self.port}")

    def stop(self):
        self.running = False
        self.server_socket.close()
        logger.info("NodeCommunication stopped.")

    def listen_for_connections(self):
        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                logger.info(f"Accepted connection from {addr}")
                handler_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                handler_thread.start()
            except Exception as e:
                logger.error(f"Error accepting connections: {e}")

    def handle_client(self, client_socket: socket.socket):
        with client_socket:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        break
                    message = data.decode()
                    logger.info(f"Received message: {message}")
                    # Handle incoming messages (e.g., key distribution, replication commands)
                    response = self.process_message(message)
                    client_socket.sendall(response.encode())
                except Exception as e:
                    logger.error(f"Error handling client: {e}")
                    break

    def process_message(self, message: str) -> str:
        """
        Process incoming messages from other nodes.
        """
        # Placeholder for message processing logic
        logger.info(f"Processing message: {message}")
        return "ACK\n"

    def send_message(self, host: str, port: int, message: str) -> str:
        """
        Send a message to another node.
        """
        try:
            with socket.create_connection((host, port), timeout=5) as sock:
                sock.sendall(message.encode())
                response = sock.recv(1024).decode()
                logger.info(f"Received response: {response}")
                return response
        except Exception as e:
            logger.error(f"Error sending message to {host}:{port} - {e}")
            return "ERR Communication failure\n"

    def add_connection(self, node_id: str, host: str, port: int):
        with self.lock:
            self.connections[node_id] = (host, port)
            logger.info(f"Added connection to node {node_id} at {host}:{port}")

    def remove_connection(self, node_id: str):
        with self.lock:
            if node_id in self.connections:
                del self.connections[node_id]
                logger.info(f"Removed connection to node {node_id}")
