# src/server.py
import socket
import threading
from src.config import Config
from src.logger import setup_logger
from src.core.data_store import DataStore
from src.core.memory_manager import MemoryManager
from src.core.expiry_manager import ExpiryManager
from src.protocol import RESPProtocol

logger = setup_logger(level=Config.LOG_LEVEL)

class Server:
    def __init__(self, host=Config.HOST, port=Config.PORT, max_clients=Config.MAX_CLIENTS):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_store = DataStore()
        self.memory_manager = MemoryManager()
        self.expiry_manager = ExpiryManager(self.data_store)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}")

        while True:
            client_socket, client_address = self.server_socket.accept()
            logger.info(f"New connection from {client_address}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            if not data:
                return
            command = RESPProtocol.parse_request(data.decode())
            response = self.process_command(command)
            client_socket.sendall(RESPProtocol.format_response(response).encode())
        except Exception as e:
            logger.error(f"Error handling client: {e}")
            client_socket.sendall(RESPProtocol.format_response("ERR Internal Server Error").encode())
        finally:
            client_socket.close()

    def process_command(self, command):
        try:
            cmd, *args = command.split()
            cmd = cmd.upper()

            if cmd == "SET":
                key, value = args
                if not self.memory_manager.can_store(value):
                    return "ERR Not enough memory"
                self.memory_manager.add(value)
                return self.data_store.set(key, value)

            elif cmd == "GET":
                key = args[0]
                if self.expiry_manager.is_expired(key):
                    return "(nil)"
                return self.data_store.get(key)

            elif cmd == "DEL":
                key = args[0]
                self.memory_manager.remove(self.data_store.get(key))
                return str(self.data_store.delete(key))

            elif cmd == "EXISTS":
                key = args[0]
                return "1" if self.data_store.exists(key) else "0"

            elif cmd == "EXPIRE":
                key, ttl = args
                ttl = int(ttl)
                if self.data_store.exists(key):
                    self.expiry_manager.set_expiry(key, ttl)
                    return "1"
                return "0"

            else:
                return "ERR Unknown command"
        except Exception as e:
            logger.error(f"Command processing error: {e}")
            return "ERR Invalid Command"

if __name__ == "__main__":
    server = Server()
    try: 
        server.start() 
    except KeyboardInterrupt: 
        server.stop()
