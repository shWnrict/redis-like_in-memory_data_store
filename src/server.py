import socket
import threading
import time

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
from src.clustering.keyspace_manager import KeyspaceManager  # Import KeyspaceManager
from src.datatypes.base import BaseDataType  # Import BaseDataType
from src.monitoring.stats import Stats  # Import Stats
from src.monitoring.slowlog import SlowLog  # Import SlowLog
from src.scripting.lua_engine import LuaEngine
from src.clustering.hash_slots import HashSlots
from src.clustering.node_communication import NodeCommunication

from src.patterns.distributed_locks import DistributedLock
from src.patterns.rate_limiter import RateLimiter
from src.patterns.message_queue import MessageQueue

from src.clustering.node import Node
from src.clustering.replication import ReplicationManager
from src.core.pipeline_manager import PipelineManager  # Import PipelineManager

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
        self.memory_manager = MemoryManager(self.data_store)
        self.memory_manager.start()
        self.expiry_manager = ExpiryManager(self.data_store)
        self.expiry_manager.start()  # Start ExpiryManager
        
        # Initialize data type handlers with BaseDataType
        self.strings = Strings(self.data_store.store, self.expiry_manager)
        self.lists = Lists(self.data_store.store, self.expiry_manager)
        self.sets = Sets(self.data_store.store, self.expiry_manager)
        self.hashes = Hashes(self.data_store.store, self.expiry_manager)
        self.sorted_sets = SortedSets(self.data_store.store, self.expiry_manager)
        self.streams = Streams(self.data_store.store, self.expiry_manager)
        self.json_type = JSONType(self.data_store.store, self.expiry_manager)
        self.bitmaps = Bitmaps(self.data_store.store, self.expiry_manager)
        self.bitfields = Bitfields(self.data_store.store, self.expiry_manager)
        self.geospatial = Geospatial(self.data_store.store, self.expiry_manager)
        self.hyperloglogs = {}
        self.timeseries = TimeSeries(self.data_store.store, self.expiry_manager)
        self.vectors = Vectors(self.data_store.store, self.expiry_manager)
        self.documents = Documents(self.data_store.store, self.expiry_manager)
        self.probabilistic = BloomFilter(self.data_store.store, self.expiry_manager)

        # Initialize persistence and transaction components
        self.aof = AOF()
        self.snapshot = Snapshot(self.data_store)
        self.transaction_manager = TransactionManager()
        self.pubsub = PubSub(self)  # Pass the server instance to PubSub
        self.command_router = CommandRouter(self)  # Ensure CommandRouter utilizes data type handlers
        
        # Initialize persistence state
        self.changes_since_snapshot = 0
        self.snapshot_threshold = Config.SNAPSHOT_THRESHOLD

        # Initialize KeyspaceManager
        self.keyspace_manager = KeyspaceManager()

        # Pass KeyspaceManager to CommandRouter if needed
        self.command_router.keyspace_manager = self.keyspace_manager

        # Initialize monitoring components
        self.stats = Stats(self.data_store)
        self.slowlog = SlowLog(threshold_ms=Config.SLOWLOG_THRESHOLD_MS)  # Use config for threshold

        # Initialize LuaEngine
        self.lua_engine = LuaEngine(self)
        logger.info("LuaEngine integrated into server.")

        # Initialize clustering components with Config settings
        self.hash_slots = HashSlots()
        self.node_communication = NodeCommunication(Config.CLUSTER_HOST, Config.CLUSTER_PORT)
        self.node_communication.start()
        logger.info("Cluster mode initialized and node communication established.")

        # Initialize ReplicationManager
        self.replication_manager = ReplicationManager(self, self.node_communication)

        # Example: Adding self as a node
        self_node = Node(node_id=Config.CLUSTER_NODE_ID, host=self.host, port=self.port)
        self.hash_slots.add_node(self_node)

        # Connect to other nodes (example)
        other_node = Node(node_id="127.0.0.1:7001", host="127.0.0.1", port=7001)
        self.hash_slots.add_node(other_node)
        self.node_communication.add_connection(other_node.node_id, other_node.host, other_node.port)

        # Initialize pattern handlers
        self.distributed_lock = DistributedLock(self.data_store)
        self.rate_limiter = RateLimiter(self.data_store, max_requests=Config.RATE_LIMIT_MAX_REQUESTS, window_seconds=Config.RATE_LIMIT_WINDOW_SECONDS)
        self.message_queue = MessageQueue(self.data_store, queue_name='default')

        # Initialize PipelineManager
        self.pipeline_manager = PipelineManager(self.command_router)

        # Load data from persistence (only once)
        self._load_persistence_data()

    def _load_persistence_data(self):
        """Load data from snapshot and AOF files"""
        try:
            # First load snapshot
            snapshot_data = self.snapshot.load()
            if snapshot_data and isinstance(snapshot_data, dict):
                with self.data_store.lock:
                    self.data_store.store = snapshot_data
                logger.info("Snapshot data loaded successfully.")
            else:
                logger.info("No snapshot data to load.")
        except Exception as e:
            logger.error(f"Error loading snapshot: {e}")
            self.data_store.store = {}

        # Then replay AOF
        try:
            self.aof.replay(self._replay_command, self.data_store.store)
            logger.info("AOF replayed successfully.")
        except Exception as e:
            logger.error(f"Error replaying AOF: {e}")

    def _replay_command(self, command):
        """Special handler for replaying commands from AOF"""
        try:
            self.command_router.route(command, None)
            logger.debug(f"Replayed command from AOF: {command}")
        except Exception as e:
            logger.error(f"Error replaying command {command}: {e}")

    def start(self):
        self.running = True
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.max_clients)
        logger.info(f"Server started on {self.host}:{self.port}")

        # Start periodic snapshot saving
        self.snapshot.start()

        while self.running:
            try:
                client_socket, addr = self.server_socket.accept()
                logger.info(f"Accepted connection from {addr}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()
            except BlockingIOError:
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error accepting connections: {e}")

    def stop(self):
        self.running = False
        with self.clients_lock:
            for client in self.clients:
                try:
                    client.close()
                except:
                    pass
            self.clients.clear()
        self.snapshot.save(self.data_store)  # Pass the DataStore object directly
        self.aof.truncate(self.data_store.store)
        self.expiry_manager.stop()  # Stop ExpiryManager
        self.memory_manager.stop()  # Stop MemoryManager
        self.snapshot.stop()
        self.server_socket.close()
        self.node_communication.stop()
        logger.info("Server stopped")

    def handle_client(self, client_socket):
        client_id = id(client_socket)  # Use client socket object as client ID
        
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
                    while b"\r\n" in buffer:
                        # Parse commands using RESPProtocol
                        commands = RESPProtocol.parse_message(buffer.decode())
                        if not commands:
                            break
                        for command in commands:
                            cmd = command[0].upper()

                            # Handle transactions
                            if self.transaction_manager.is_in_transaction(client_id):
                                self.transaction_manager.queue_command(client_id, command)
                                client_socket.sendall(b"+QUEUED\r\n")
                                continue

                            # Handle pipeline commands
                            if cmd == "PIPELINE":
                                response = "+OK\r\n"
                                client_socket.sendall(response.encode())
                                continue

                            # Rate limiting
                            if not self.rate_limiter.is_allowed(str(client_id)):
                                response = "-ERR Rate limit exceeded\r\n"
                                client_socket.sendall(response.encode())
                                continue

                            key = command[1] if len(command) > 1 else None
                            if key:
                                forwarded_response = self.distribute_key(key)
                                if forwarded_response:
                                    formatted_response = forwarded_response
                                else:
                                    response = self.command_router.route(command, client_socket)
                                    formatted_response = RESPProtocol.format_response(response)
                            else:
                                response = self.command_router.route(command, client_socket)
                                formatted_response = RESPProtocol.format_response(response)
                            client_socket.sendall(formatted_response.encode())
                        buffer = b""

                except BlockingIOError:
                    continue
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            with self.clients_lock:
                self.clients.remove(client_socket)
            client_socket.close()
            logger.info(f"Client {client_id} disconnected.")
            
    def process_command(self, command, client_socket=None):
        start_time = time.time()
        response = self.command_router.route(command, client_socket)
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Log slow commands
        cmd = command[0].upper() if command else "UNKNOWN"
        args = command[1:] if len(command) > 1 else []
        self.slowlog.add_entry(execution_time_ms, cmd, args)
        
        return response

    def handle_pubsub(self, cmd, args, client_socket):
        if cmd == "SUBSCRIBE":
            # ...existing code...
            pass
        elif cmd == "UNSUBSCRIBE":
            # ...existing code...
            pass
        elif cmd == "PUBLISH":
            # ...existing code...
            pass
        return None
    
    def handle_transactions(self, cmd, args, client_socket):
        """Handle transaction-related commands"""
        client_id = id(client_socket)
            
        if cmd == "MULTI":
            self.transaction_manager.begin_transaction(client_id)
            return "+OK\r\n"
        elif cmd == "EXEC":
            commands = self.transaction_manager.get_transaction_commands(client_id)
            responses = self.pipeline_manager.execute(client_socket)
            self.transaction_manager.end_transaction(client_id)
            formatted_responses = RESPProtocol.format_response(responses)
            return formatted_responses
        elif cmd == "DISCARD":
            self.transaction_manager.discard_transaction(client_id)
            return "+OK\r\n"
        elif self.transaction_manager.is_in_transaction(client_id):
            self.transaction_manager.queue_command(client_id, cmd)
            return "+QUEUED\r\n"
    
        return None

    def _execute_transaction_command(self, command, client_socket):
        """
        Execute a single command within a transaction without re-queuing it.
        """
        if isinstance(command, str):
            parsed_command = RESPProtocol.parse_message(command)
            if parsed_command:
                command = parsed_command[0]
            else:
                return "-ERR Invalid command\r\n"
        else:
            parsed_command = command

        logger.info(f"Executing transaction command: {command} from client: {client_socket}")
        response = self.command_router.route(command, client_socket)
        return RESPProtocol.format_response(response)

    def handle_persistence(self, cmd, args, client_socket):
        if cmd == "SAVE":
            self.snapshot.save(self.data_store)
            return "+OK\r\n"
        elif cmd == "RESTORE":
            # Implement RESTORE logic
            return "-ERR RESTORE not implemented\r\n"

        if cmd in {"SET", "DEL", "HSET", "LPUSH", "RPUSH", "ZADD", "GEOADD", "PFADD", "TS.ADD", "DOC.INSERT"}:
            self.aof.append(cmd, args)
            return None

        return None

    def handle_general_commands(self, cmd, args, client_socket):
        if cmd == "SAVE":
            self.snapshot.save(self.data_store)
            return "+OK\r\n"
        elif cmd == "RESTORE":
            # Implement RESTORE logic
            return "-ERR RESTORE not implemented\r\n"
        elif cmd == "INFO":
            return self.command_router.handle_info(cmd, args)
        return None

    def handle_data_type_commands(self, cmd, args, client_socket):
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
            handler = data_type_handlers["STRINGS"]
            return handler.handle_command(cmd, self.data_store.store, *args)

        for data_type, handler in data_type_handlers.items():
            if cmd.startswith(data_type):
                return handler.handle_command(cmd, self.data_store.store, *args)

        return None

    def get_client_socket(self, client_id):
        """
        Get the client socket by client ID.
        """
        with self.clients_lock:
            for client in self.clients:
                if id(client) == client_id:
                    return client
        return None

    def distribute_key(self, key: str) -> str:
        """
        Determine the node responsible for the given key and forward the command if necessary.
        """
        node = self.hash_slots.get_node(key)
        if node and node.node_id != f"{self.host}:{self.port}":
            connection = self.node_communication.get_connection(node.node_id)
            if connection:
                command = RESPProtocol.format_command(*[key])
                try:
                    connection.send_command_raw(command)
                    response = connection.read_response()
                    logger.debug(f"Forwarded command to {node.node_id}: {command.strip()}")
                    return response
                except ConnectionError as e:
                    logger.error(f"Failed to forward command to {node.node_id}: {e}")
                    return "-ERR Failed to forward command\r\n"
        return ""

if __name__ == "__main__":
    server = Server()
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()