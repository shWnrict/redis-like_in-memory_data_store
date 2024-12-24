# src/server.py
import socket
import threading

from src.config import Config
from src.logger import setup_logger
from src.core.data_store import DataStore
from src.core.memory_manager import MemoryManager
from src.core.expiry_manager import ExpiryManager
from src.protocol import RESPProtocol

from src.core.transaction_manager import TransactionManager
from src.pubsub.publisher import PubSub

from src.datatypes.strings import Strings
from src.datatypes.lists import Lists
from src.datatypes.sets import Sets
from src.datatypes.hashes import Hashes
from src.datatypes.sorted_sets import SortedSets

from src.datatypes.streams import Streams
from src.datatypes.json_type import JSONType
from src.datatypes.bitmaps import Bitmaps

from src.datatypes.geospatial import Geospatial
from src.datatypes.probabilistic import HyperLogLog
from src.datatypes.timeseries import TimeSeries

from src.datatypes.vectors import Vectors
from src.datatypes.documents import Documents

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
        self.transaction_manager = TransactionManager()
        self.pubsub = PubSub()

        self.strings = Strings()
        self.lists = Lists()
        self.sets = Sets()
        self.hashes = Hashes()
        self.sorted_sets = SortedSets()

        self.streams = Streams()
        self.json_type = JSONType()
        self.bitmaps = Bitmaps()

        self.geospatial = Geospatial()
        self.hyperloglogs = {}
        self.timeseries = TimeSeries()

        self.vectors = Vectors()
        self.documents = Documents()

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

    def process_command(self, command, client_id=None):
        try:
            cmd, *args = command.split()
            cmd = cmd.upper()

            #Pub/Sub
            if cmd == "SUBSCRIBE":
                channel = args[0]
                return self.pubsub.subscribe(client_id, channel)

            elif cmd == "UNSUBSCRIBE":
                channel = args[0] if args else None
                return self.pubsub.unsubscribe(client_id, channel)

            elif cmd == "PUBLISH":
                channel, message = args
                return str(self.pubsub.publish(channel, message))

            #Transactions
            if cmd == "MULTI":
                return self.transaction_manager.start_transaction(client_id)

            elif cmd == "EXEC":
                return self.transaction_manager.execute_transaction(client_id, self.data_store.store, self.process_command)

            elif cmd == "DISCARD":
                return self.transaction_manager.discard_transaction(client_id)

            elif client_id in self.transaction_manager.transactions:
                return self.transaction_manager.queue_command(client_id, command)

            #General
            elif cmd == "SET":
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

            #Hashes
            elif cmd == "HSET":
                key, field, value = args
                return str(self.hashes.hset(self.data_store.store, key, field, value))

            elif cmd == "HMSET":
                key, *field_value_pairs = args
                field_value_dict = dict(zip(field_value_pairs[::2], field_value_pairs[1::2]))
                return self.hashes.hmset(self.data_store.store, key, field_value_dict)

            elif cmd == "HGET":
                key, field = args
                return self.hashes.hget(self.data_store.store, key, field)

            elif cmd == "HGETALL":
                key = args[0]
                return str(self.hashes.hgetall(self.data_store.store, key))

            elif cmd == "HDEL":
                key, *fields = args
                return str(self.hashes.hdel(self.data_store.store, key, *fields))

            elif cmd == "HEXISTS":
                key, field = args
                return str(self.hashes.hexists(self.data_store.store, key, field))

            #Sorted Sets
            elif cmd == "ZADD":
                key, *rest = args
                return str(self.sorted_sets.zadd(self.data_store.store, key, *rest))

            elif cmd == "ZRANGE":
                key, start, end = args[:3]
                with_scores = "WITHSCORES" in args
                return str(self.sorted_sets.zrange(self.data_store.store, key, start, end, with_scores))

            elif cmd == "ZRANK":
                key, member = args
                return str(self.sorted_sets.zrank(self.data_store.store, key, member))

            elif cmd == "ZREM":
                key, *members = args
                return str(self.sorted_sets.zrem(self.data_store.store, key, *members))

            elif cmd == "ZRANGEBYSCORE":
                key, min_score, max_score = args[:3]
                with_scores = "WITHSCORES" in args
                return str(self.sorted_sets.zrangebyscore(self.data_store.store, key, min_score, max_score, with_scores))

            #Streams
            elif cmd == "XADD":
                key, entry_id, *field_value_pairs = args
                fields = dict(zip(field_value_pairs[::2], field_value_pairs[1::2]))
                return self.streams.xadd(self.data_store.store, key, entry_id, **fields)

            elif cmd == "XREAD":
                key, count = args[:2]
                last_id = args[2] if len(args) > 2 else "0-0"
                return str(self.streams.xread(self.data_store.store, key, count, last_id))

            elif cmd == "XRANGE":
                key, start, end = args[:3]
                count = args[3] if len(args) > 3 else None
                return str(self.streams.xrange(self.data_store.store, key, start, end, count))

            elif cmd == "XLEN":
                key = args[0]
                return str(self.streams.xlen(self.data_store.store, key))


            elif cmd == "XGROUP":
                subcmd = args[0].upper()
                if subcmd == "CREATE":
                    key, group_name, start_id = args[1:]
                    return self.streams.xgroup_create(self.data_store.store, key, group_name, start_id)
                else:
                    return "ERR Unknown XGROUP subcommand"

            elif cmd == "XREADGROUP":
                group_name, consumer_name, key, count = args[:4]
                last_id = args[4] if len(args) > 4 else ">"
                return str(self.streams.xreadgroup(self.data_store.store, group_name, consumer_name, key, count, last_id))

            elif cmd == "XACK":
                key, group_name, *entry_ids = args
                return str(self.streams.xack(self.data_store.store, key, group_name, *entry_ids))
            
            #JSON
            elif cmd == "JSON.SET":
                key, path, value = args
                return self.json_type.json_set(self.data_store.store, key, path, value)

            elif cmd == "JSON.GET":
                key, path = args
                return self.json_type.json_get(self.data_store.store, key, path)

            elif cmd == "JSON.DEL":
                key, path = args
                return str(self.json_type.json_del(self.data_store.store, key, path))

            elif cmd == "JSON.ARRAPPEND":
                key, path, *values = args
                return str(self.json_type.json_arrappend(self.data_store.store, key, path, *values))
            
            #Bitmaps
            elif cmd == "SETBIT":
                key, offset, value = args
                return str(self.bitmaps.setbit(self.data_store.store, key, int(offset), value))

            elif cmd == "GETBIT":
                key, offset = args
                return str(self.bitmaps.getbit(self.data_store.store, key, int(offset)))

            elif cmd == "BITCOUNT":
                key, *rest = args
                start, end = (int(rest[0]), int(rest[1])) if len(rest) == 2 else (None, None)
                return str(self.bitmaps.bitcount(self.data_store.store, key, start, end))

            elif cmd == "BITOP":
                operation, destkey, *sourcekeys = args
                return str(self.bitmaps.bitop(self.data_store.store, operation, destkey, *sourcekeys))
            
            #Geospatial
            elif cmd == "GEOADD":
                key, *rest = args
                return str(self.geospatial.geoadd(self.data_store.store, key, *rest))

            elif cmd == "GEODIST":
                key, member1, member2, *unit = args
                unit = unit[0] if unit else "m"
                return str(self.geospatial.geodist(self.data_store.store, key, member1, member2, unit))

            elif cmd == "GEOSEARCH":
                key, lat, lon, radius, *unit = args
                unit = unit[0] if unit else "km"
                return str(self.geospatial.geosearch(self.data_store.store, key, float(lat), float(lon), float(radius), unit))

            #HyperLogLog
            elif cmd == "PFADD":
                key, *values = args
                if key not in self.hyperloglogs:
                    self.hyperloglogs[key] = HyperLogLog()
                for value in values:
                    self.hyperloglogs[key].add(value)
                return "1"

            elif cmd == "PFCOUNT":
                key = args[0]
                if key not in self.hyperloglogs:
                    return "0"
                return str(self.hyperloglogs[key].count())

            elif cmd == "PFMERGE":
                destkey, *sourcekeys = args
                if destkey not in self.hyperloglogs:
                    self.hyperloglogs[destkey] = HyperLogLog()
                for sourcekey in sourcekeys:
                    if sourcekey not in self.hyperloglogs:
                        return f"ERR {sourcekey} does not exist"
                    self.hyperloglogs[destkey].merge(self.hyperloglogs[sourcekey])
                return "OK"

            #Timeseries
            elif cmd == "TS.CREATE":
                key = args[0]
                return self.timeseries.create(self.data_store.store, key)

            elif cmd == "TS.ADD":
                key, timestamp, value = args
                return self.timeseries.add(self.data_store.store, key, timestamp, value)

            elif cmd == "TS.GET":
                key = args[0]
                return str(self.timeseries.get(self.data_store.store, key))

            elif cmd == "TS.RANGE":
                key, start, end, *aggregation = args
                aggregation = aggregation[0] if aggregation else None
                return str(self.timeseries.range(self.data_store.store, key, start, end, aggregation))

            #Vectors
            elif cmd == "VECTOR.ADD":
                key = args[0]
                vector = list(map(float, args[1:]))
                return self.vectors.add_vector(self.data_store.store, key, vector)

            elif cmd == "VECTOR.SIMILARITY":
                metric = args[0].lower()
                query_vector = list(map(float, args[1:-1]))
                top_k = int(args[-1])
                return str(self.vectors.similarity_search(self.data_store.store, query_vector, metric, top_k))

            elif cmd in {"VECTOR.ADD", "VECTOR.SUB", "VECTOR.DOT"}:
                op = cmd.split(".")[-1].lower()
                vector1 = list(map(float, args[:len(args) // 2]))
                vector2 = list(map(float, args[len(args) // 2:]))
                return str(self.vectors.vector_operation(self.data_store.store, op, vector1, vector2))
            
            #Documents
            elif cmd == "DOC.INSERT":
                key, document = args
                return self.documents.insert(self.data_store.store, key, document)

            elif cmd == "DOC.FIND":
                key, query = args
                return str(self.documents.find(self.data_store.store, key, query))

            elif cmd == "DOC.UPDATE":
                key, query, update_fields = args
                return str(self.documents.update(self.data_store.store, key, query, update_fields))

            elif cmd == "DOC.DELETE":
                key, query = args
                return str(self.documents.delete(self.data_store.store, key, query))

            elif cmd == "DOC.AGGREGATE":
                key, operation, field = args
                return str(self.documents.aggregate(self.data_store.store, key, operation, field))


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
