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
from src.datatypes.probabilistic import HyperLogLog, BloomFilter
from src.datatypes.timeseries import TimeSeries

from src.datatypes.vectors import Vectors
from src.datatypes.documents import Documents

logger = setup_logger(level=Config.LOG_LEVEL)

class Server:
    def __init__(self, host=Config.HOST, port=Config.PORT, max_clients=Config.MAX_CLIENTS):
        # Initialize basic server components
        self.running = False
        self.clients = set()
        self.clients_lock = threading.Lock()
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setblocking(False)

        # Initialize data store and managers
        self.data_store = DataStore()
        self.memory_manager = MemoryManager()
        self.expiry_manager = ExpiryManager(self.data_store)
        
        # Initialize data type handlers
        self.strings = Strings(self.expiry_manager)
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
        self.probabilistic = BloomFilter()

        # Initialize persistence and transaction components
        self.aof = AOF()
        self.snapshot = Snapshot()
        self.transaction_manager = TransactionManager()
        self.pubsub = PubSub()
        self.command_router = CommandRouter(self)
        
        # Initialize persistence state
        self.changes_since_snapshot = 0
        self.snapshot_threshold = Config.SNAPSHOT_THRESHOLD

        # Load data from persistence (only once)
        self._load_persistence_data()

    def _load_persistence_data(self):
        """Load data from snapshot and AOF files"""
        try:
            # First load snapshot
            snapshot_data = self.snapshot.load()
            if snapshot_data and isinstance(snapshot_data, dict):
                self.data_store.store = snapshot_data
            else:
                self.data_store.store = {}
        except Exception as e:
            logger.error(f"Error loading snapshot: {e}")
            self.data_store.store = {}

        # Then replay AOF
        try:
            self.aof.replay(self._replay_command, self.data_store.store)
        except Exception as e:
            logger.error(f"Error replaying AOF: {e}")

    def _replay_command(self, command):
        """Special handler for replaying commands from AOF"""
        try:
            if isinstance(command, str):
                parts = command.split()
                cmd = parts[0].upper()
                args = parts[1:]
            else:
                cmd = command[0].upper()
                args = command[1:]

            # Handle string commands directly during replay
            if cmd in {"SET", "GET", "DEL", "EXISTS", "EXPIRE"}:
                return self.strings.handle_command(cmd, self.data_store.store, *args)
            
            return self.process_command([cmd] + args)
        except Exception as e:
            logger.error(f"Error replaying command {command}: {e}")
            return None

    def start(self):
        self.running = True
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}")

        while self.running:
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
        logger.info("Server stopped")

    def handle_client(self, client_socket):
        client_id = id(threading.current_thread())  # Generate unique client ID
        
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

                    command_lists = RESPProtocol.parse_message(buffer.decode())
                    response = self.process_command(command_lists[0], client_id)  # Pass client_id
                    formatted_response = RESPProtocol.format_response(response)
                    client_socket.send(formatted_response.encode())
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
            if not command:
                return "ERR Empty command"

            if isinstance(command, str):
                # Handle string commands from AOF replay
                parts = command.split()
                cmd = parts[0].upper()
                args = parts[1:]
            else:
                # Join split commands (e.g., "S", "ET" -> "SET")
                if len(command) >= 2 and command[0] + command[1] in {"SET", "DEL", "GET"}:
                    cmd = command[0] + command[1]
                    args = command[2:]
                else:
                    cmd = command[0].upper()
                    args = command[1:]

            logger.info(f"Processing command: {cmd} with args: {args} from client: {client_id}")

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

            if cmd in {"GEOADD", "GEODIST", "GEOSEARCH", "GEOHASH"}:
                return self.geospatial.handle_command(cmd, self.data_store.store, *args)
            if cmd in {"SETBIT", "GETBIT", "BITCOUNT", "BITOP", "BITFIELD"}:
                return self.bitfields.handle_command(cmd, self.data_store.store, *args)
            if cmd in {"PFADD", "PFCOUNT", "PFMERGE", "BFADD", "BFEXISTS", "BF.RESERVE"}:
                return self.probabilistic.handle_command(cmd, self.data_store.store, *args)
            if cmd in {"TS.CREATE", "TS.ADD", "TS.GET", "TS.RANGE", "TS.QUERYINDEX"}:
                return self.timeseries.handle_command(cmd, self.data_store.store, *args)

            logger.error(f"Unknown command: {cmd}")
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
        """Handle transaction-related commands"""
        if not client_id:
            client_id = id(threading.current_thread())  # Use thread ID as client ID if none provided
            
        if cmd == "MULTI":
            return self.transaction_manager.start_transaction(client_id)
        elif cmd == "EXEC":
            return self.transaction_manager.execute_transaction(
                client_id,
                self.data_store.store,
                lambda cmd, _: self._execute_transaction_command(cmd, client_id)
            )
        elif cmd == "DISCARD":
            return self.transaction_manager.discard_transaction(client_id)
        elif client_id in self.transaction_manager.transactions:
            full_cmd = f"{cmd} {' '.join(args)}" if args else cmd
            return self.transaction_manager.queue_command(client_id, full_cmd)
        
        return None

    def _execute_transaction_command(self, command, client_id):
        """
        Execute a single command within a transaction without re-queuing it.
        """
        if isinstance(command, str):
            parts = command.split()
            cmd = parts[0].upper()
            args = parts[1:]
        else:
            cmd = command[0].upper()
            args = command[1:]

        logger.info(f"Executing transaction command: {cmd} with args: {args} from client: {client_id}")

        handlers = [
            self.handle_pubsub,
            self.handle_persistence,
            self.handle_general_commands,
            self.handle_data_type_commands
        ]

        for handler in handlers:
            result = handler(cmd, args, client_id)
            if result is not None:
                return result

        if cmd in {"GEOADD", "GEODIST", "GEOSEARCH", "GEOHASH"}:
            return self.geospatial.handle_command(cmd, self.data_store.store, *args)
        if cmd in {"SETBIT", "GETBIT", "BITCOUNT", "BITOP", "BITFIELD"}:
            return self.bitfields.handle_command(cmd, self.data_store.store, *args)
        if cmd in {"PFADD", "PFCOUNT", "PFMERGE", "BFADD", "BFEXISTS", "BF.RESERVE"}:
            return self.probabilistic.handle_command(cmd, self.data_store.store, *args)
        if cmd in {"TS.CREATE", "TS.ADD", "TS.GET", "TS.RANGE", "TS.QUERYINDEX"}:
            return self.timeseries.handle_command(cmd, self.data_store.store, *args)

        logger.error(f"Unknown command: {cmd}")
        return "ERR Unknown command"

    def handle_persistence(self, cmd, args, client_id):
        if cmd == "SAVE":
            return self.snapshot.save(self.data_store.store)
        elif cmd == "RESTORE":
            self.data_store.store = self.snapshot.load()
            return "OK"

        if cmd in {"SET", "DEL", "HSET", "LPUSH", "RPUSH", "ZADD", "GEOADD", "PFADD", "TS.ADD", "DOC.INSERT"}:
            result = self.process_command([cmd] + args, client_id)
            if not result.startswith("ERR"):
                self.aof.log_command(cmd + " " + " ".join(args))
                self.changes_since_snapshot += 1

                if self.changes_since_snapshot >= self.snapshot_threshold:
                    self.snapshot.save(self.data_store.store)
                    self.aof.truncate(self.data_store.store)
                    self.changes_since_snapshot = 0

        return None

    def handle_general_commands(self, cmd, args, client_id):
        if cmd == "SAVE":
            return self.snapshot.save(self.data_store.store)
        elif cmd == "RESTORE":
            self.data_store.store = self.snapshot.load()
            return "OK"
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

        if cmd in {"SET", "GET", "DEL", "EXISTS", "EXPIRE"}:
            return self.strings.handle_command(cmd, self.data_store.store, *args)

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