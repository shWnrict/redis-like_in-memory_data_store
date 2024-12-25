import socket
import threading

from src.config import Config
from src.logger import setup_logger
from src.core.data_store import DataStore
from src.core.memory_manager import MemoryManager
from src.core.expiry_manager import ExpiryManager
from src.protocol import RESPProtocol
from src.core.command_router import CommandRouter

from src.core.transaction_manager import TransactionManager
from src.pubsub.publisher import PubSub
from src.persistence.aof import AOF
from src.persistence.snapshot import Snapshot

from src.datatypes.strings import Strings
from src.datatypes.lists import Lists
from src.datatypes.sets import Sets
from src.datatypes.hashes import Hashes
from src.datatypes.sorted_sets import SortedSets

from src.datatypes.streams import Streams
from src.datatypes.json_type import JSONType
from src.datatypes.bitmaps import Bitmaps
from src.datatypes.bitfields import Bitfields

from src.datatypes.geospatial import Geospatial
from src.datatypes.probabilistic import HyperLogLog
from src.datatypes.timeseries import TimeSeries

from src.datatypes.vectors import Vectors
from src.datatypes.documents import Documents

logger = setup_logger(level=Config.LOG_LEVEL)

class Server:
    def __init__(self, host=Config.HOST, port=Config.PORT, max_clients=Config.MAX_CLIENTS):
        self.running = False
        self.clients = set()
        self.clients_lock = threading.Lock()

        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)  # Enable non-blocking mode
        self.data_store = DataStore()
        self.command_router = CommandRouter(self)

        self.memory_manager = MemoryManager()
        self.expiry_manager = ExpiryManager(self.data_store)
        self.transaction_manager = TransactionManager()
        self.pubsub = PubSub()

        self.aof = AOF()
        self.snapshot = Snapshot()
        self.changes_since_snapshot = 0
        self.snapshot_threshold = Config.SNAPSHOT_THRESHOLD

        self.strings = Strings()
        self.lists = Lists()
        self.sets = Sets()
        self.hashes = Hashes()
        self.sorted_sets = SortedSets()

        self.streams = Streams()
        self.json_type = JSONType()
        self.bitmaps = Bitmaps()
        self.bitfields = Bitfields(self.data_store)

        self.geospatial = Geospatial()
        self.hyperloglogs = {}
        self.timeseries = TimeSeries()

        self.vectors = Vectors()
        self.documents = Documents()

        # Initialize state from persistence mechanisms
        self.data_store.store = self.snapshot.load()
        self.aof.replay(self.process_command, self.data_store.store)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}")

        while True:
            try:
                client_socket, _ = self.server_socket.accept()
                client_socket.setblocking(False)
                threading.Thread(target=self.handle_client, args=(client_socket,)).start()
            except BlockingIOError:
                continue

    def stop(self):
        self.running = False
        with self.clients_lock:
            for client in self.clients:
                try:
                    client.close()
                except:
                    pass
        self.snapshot.save(self.data_store.store)
        self.server_socket.close()
    
    def handle_client(self, client_socket):
        with self.clients_lock:
            self.clients.add(client_socket)

        buffer = b""
        try:
            while self.running:
                try:
                    data = client_socket.recv(1024)
                    if not data:
                        break

                    buffer += data
                    if b"\r\n" not in buffer:
                        continue

                    commands = RESPProtocol.parse_message(buffer.decode())
                    responses = [self.process_command(command) for command in commands]
                    client_socket.send(RESPProtocol.format_response(responses).encode())
                    buffer = b""

                except BlockingIOError:
                    pass
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            with self.clients_lock:
                self.clients.remove(client_socket)
            client_socket.close()

    def process_command(self, command, client_id=None):
        try:
            cmd, *args = command.split()
            cmd = cmd.upper()

            handlers = [
                self.handle_pubsub,
                self.handle_transactions,
                self.handle_persistence,
                self.handle_general_commands,
                self.handle_data_type_commands
            ]

            for handler in handlers:
                result = handler(cmd, args, client_id)
                if result is not None:
                    return result

            return "ERR Unknown command"
        except Exception as e:
            logger.error(f"Command processing error: {e}")
            return "ERR Invalid Command"

    def handle_pubsub(self, cmd, args, client_id):
        if cmd == "SUBSCRIBE":
            return self.pubsub.subscribe(client_id, args[0])
        elif cmd == "UNSUBSCRIBE":
            return self.pubsub.unsubscribe(client_id, args[0] if args else None)
        elif cmd == "PUBLISH":
            return str(self.pubsub.publish(args[0], args[1]))
        return None

    def handle_transactions(self, cmd, args, client_id):
        if cmd == "MULTI":
            return self.transaction_manager.start_transaction(client_id)
        elif cmd == "EXEC":
            return self.transaction_manager.execute_transaction(
                client_id, self.data_store.store, self.process_command
            )
        elif cmd == "DISCARD":
            return self.transaction_manager.discard_transaction(client_id)
        elif client_id in self.transaction_manager.transactions:
            return self.transaction_manager.queue_command(client_id, cmd + " " + " ".join(args))
        return None

    def handle_persistence(self, cmd, args, client_id):
        if cmd == "SAVE":
            return self.snapshot.save(self.data_store.store)
        elif cmd == "RESTORE":
            self.data_store.store = self.snapshot.load()
            return "OK"

        if cmd in {"SET", "DEL", "HSET", "LPUSH", "RPUSH", "ZADD", "GEOADD", "PFADD", "TS.ADD", "DOC.INSERT"}:
            self.aof.log_command(cmd + " " + " ".join(args))
            self.changes_since_snapshot += 1

            if self.changes_since_snapshot >= self.snapshot_threshold:
                self.snapshot.save(self.data_store.store)
                self.aof.truncate(self.data_store.store)
                self.changes_since_snapshot = 0

        return None

    def handle_general_commands(self, cmd, args, client_id):
        if cmd == "SET":
            if len(args) < 2:
                return "-ERR wrong number of arguments for 'SET'\r\n"
            key, value = args
            if not self.memory_manager.can_store(value):
                return "-ERR Not enough memory\r\n"
            self.memory_manager.add(value)
            self.data_store.store[key] = value
            return "+OK\r\n"

        elif cmd == "GET":
            if len(args) < 1:
                return "-ERR wrong number of arguments for 'GET'\r\n"
            key = args[0]
            if self.expiry_manager.is_expired(key):
                return "$-1\r\n"
            value = self.data_store.store.get(key, None)
            if value is None:
                return "$-1\r\n"
            return f"${len(value)}\r\n{value}\r\n"

        elif cmd == "DEL":
            key = args[0]
            self.memory_manager.remove(self.data_store.store.get(key, None))
            removed = self.data_store.store.pop(key, None) is not None
            return f":{1 if removed else 0}\r\n"

        elif cmd == "EXISTS":
            key = args[0]
            return f":{1 if key in self.data_store.store else 0}\r\n"

        elif cmd == "EXPIRE":
            if len(args) < 2:
                return "-ERR wrong number of arguments for 'EXPIRE'\r\n"
            key, ttl = args
            ttl = int(ttl)
            if key in self.data_store.store:
                self.expiry_manager.set_expiry(key, ttl)
                return ":1\r\n"
            return ":0\r\n"

        return None

    def handle_data_type_commands(self, cmd, args, client_id):
        data_type_handlers = {
            "STRINGS": self.strings,
            "LISTS": self.lists,
            "SETS": self.sets,
            "HASHES": self.hashes,
            "SORTED_SETS": self.sorted_sets,
            "STREAMS": self.streams,
            "JSON": self.json_type,
            "BITMAPS": self.bitmaps,
            "BITFIELDS": self.bitfields,
            "GEOSPATIAL": self.geospatial,
            "HYPERLOGLOG": self.hyperloglogs,
            "TIMESERIES": self.timeseries,
            "VECTORS": self.vectors,
            "DOCUMENTS": self.documents
        }

        for data_type, handler in data_type_handlers.items():
            if hasattr(handler, cmd.lower()):
                method = getattr(handler, cmd.lower())
                return method(self.data_store.store, *args)

        return None

if __name__ == "__main__":
    server = Server()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()
