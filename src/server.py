import socket
from src.config import Config
from src.logger import setup_logger

logger = setup_logger(level=Config.LOG_LEVEL)

class Server:
    def __init__(self, host=Config.HOST, port=Config.PORT, max_clients=Config.MAX_CLIENTS):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}")

        while True:
            client_socket, client_address = self.server_socket.accept()
            logger.info(f"New connection from {client_address}")
            self.handle_client(client_socket)

    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            if not data:
                return
            response = self.process_command(data.decode())
            client_socket.sendall(response.encode())
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def process_command(self, command):
        # Placeholder for actual command processing
        return "+OK\r\n"


if __name__ == "__main__":
    server = Server()
    server.start()