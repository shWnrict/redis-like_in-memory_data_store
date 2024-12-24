# src/server.py
import socket
import threading

from src.config import Config
from src.logger import setup_logger
from src.core.data_store import DataStore
from src.core.memory_manager import MemoryManager
from src.core.expiry_manager import ExpiryManager
from src.protocol import RESPProtocol

from src.datatypes.strings import Strings
from src.datatypes.lists import Lists
from src.datatypes.sets import Sets

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

        self.strings = Strings()
        self.lists = Lists()
        self.sets = Sets()

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

            #string
            elif cmd == "APPEND":
                key, value = args
                return str(self.strings.append(self.data_store.store, key, value))

            elif cmd == "STRLEN":
                key = args[0]
                return str(self.strings.strlen(self.data_store.store, key))

            elif cmd == "INCR":
                key = args[0]
                return self.strings.incr(self.data_store.store, key)

            elif cmd == "DECR":
                key = args[0]
                return self.strings.decr(self.data_store.store, key)

            elif cmd == "INCRBY":
                key, increment = args
                return self.strings.incrby(self.data_store.store, key, increment)

            elif cmd == "DECRBY":
                key, decrement = args
                return self.strings.decrby(self.data_store.store, key, decrement)

            elif cmd == "GETRANGE":
                key, start, end = args
                return self.strings.getrange(self.data_store.store, key, start, end)

            elif cmd == "SETRANGE":
                key, offset, value = args
                offset = int(offset)
                return str(self.strings.setrange(self.data_store.store, key, offset, value))

            #lists
            elif cmd == "LPUSH":
                key, *values = args
                return str(self.lists.lpush(self.data_store.store, key, *values))

            elif cmd == "RPUSH":
                key, *values = args
                return str(self.lists.rpush(self.data_store.store, key, *values))

            elif cmd == "LPOP":
                key = args[0]
                return self.lists.lpop(self.data_store.store, key)

            elif cmd == "RPOP":
                key = args[0]
                return self.lists.rpop(self.data_store.store, key)

            elif cmd == "LRANGE":
                key, start, end = args
                return str(self.lists.lrange(self.data_store.store, key, start, end))

            elif cmd == "LINDEX":
                key, index = args
                return self.lists.lindex(self.data_store.store, key, index)

            elif cmd == "LSET":
                key, index, value = args
                return self.lists.lset(self.data_store.store, key, index, value)

            #sets
            elif cmd == "SADD":
                key, *members = args
                return str(self.sets.sadd(self.data_store.store, key, *members))

            elif cmd == "SREM":
                key, *members = args
                return str(self.sets.srem(self.data_store.store, key, *members))

            elif cmd == "SISMEMBER":
                key, member = args
                return str(self.sets.sismember(self.data_store.store, key, member))

            elif cmd == "SMEMBERS":
                key = args[0]
                return str(self.sets.smembers(self.data_store.store, key))

            elif cmd == "SINTER":
                return str(self.sets.sinter(self.data_store.store, *args))

            elif cmd == "SUNION":
                return str(self.sets.sunion(self.data_store.store, *args))

            elif cmd == "SDIFF":
                key1, *keys = args
                return str(self.sets.sdiff(self.data_store.store, key1, *keys))


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
