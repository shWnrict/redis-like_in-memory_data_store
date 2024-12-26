from .strings import Strings
from .json_type import JSONType
from .lists import Lists
from .sets import Sets
from .hashes import Hashes
from .sorted_sets import SortedSets
from .streams import Streams
from .geospatial import Geospatial
from .bitmaps import Bitmaps
from .bitfields import Bitfields
from .probabilistic import HyperLogLog
from .timeseries import TimeSeries
from .vectors import Vectors
from .documents import Documents

# Command mapping for easy lookup
COMMAND_MAP = {
    # String commands
    "SET": ("strings", Strings.set),
    "GET": ("strings", Strings.get),
    "APPEND": ("strings", Strings.append),
    "STRLEN": ("strings", Strings.strlen),
    "INCR": ("strings", Strings.incr),
    "DECR": ("strings", Strings.decr),
    "INCRBY": ("strings", Strings.incrby),
    "DECRBY": ("strings", Strings.decrby),
    "GETRANGE": ("strings", Strings.getrange),
    "SETRANGE": ("strings", Strings.setrange),

    # JSON commands
    "JSON.SET": ("json", JSONType.json_set),
    "JSON.GET": ("json", JSONType.json_get),
    "JSON.DEL": ("json", JSONType.json_del),
    "JSON.ARRAPPEND": ("json", JSONType.json_arrappend),

    # List commands
    "LPUSH": ("lists", Lists.lpush),
    "RPUSH": ("lists", Lists.rpush),
    "LPOP": ("lists", Lists.lpop),
    "RPOP": ("lists", Lists.rpop),
    "LRANGE": ("lists", Lists.lrange),
    "LINDEX": ("lists", Lists.lindex),
    "LSET": ("lists", Lists.lset),

    # Set commands
    "SADD": ("sets", Sets.sadd),
    "SREM": ("sets", Sets.srem),
    "SISMEMBER": ("sets", Sets.sismember),
    "SMEMBERS": ("sets", Sets.smembers),
    "SINTER": ("sets", Sets.sinter),
    "SUNION": ("sets", Sets.sunion),
    "SDIFF": ("sets", Sets.sdiff),

    # Hash commands
    "HSET": ("hashes", Hashes.hset),
    "HGET": ("hashes", Hashes.hget),
    "HMSET": ("hashes", Hashes.hmset),
    "HGETALL": ("hashes", Hashes.hgetall),
    "HDEL": ("hashes", Hashes.hdel),
    "HEXISTS": ("hashes", Hashes.hexists),

    # Sorted Set commands
    "ZADD": ("sorted_sets", SortedSets.zadd),
    "ZRANGE": ("sorted_sets", SortedSets.zrange),
    "ZRANK": ("sorted_sets", SortedSets.zrank),
    "ZREM": ("sorted_sets", SortedSets.zrem),
    "ZRANGEBYSCORE": ("sorted_sets", SortedSets.zrangebyscore),

    # Stream commands
    "XADD": ("streams", Streams.xadd),
    "XREAD": ("streams", Streams.xread),
    "XRANGE": ("streams", Streams.xrange),
    "XLEN": ("streams", Streams.xlen),
    "XGROUP": ("streams", Streams.xgroup_create),
    "XREADGROUP": ("streams", Streams.xreadgroup),
    "XACK": ("streams", Streams.xack),

    # Geospatial commands
    "GEOADD": ("geospatial", Geospatial.geoadd),
    "GEOSEARCH": ("geospatial", Geospatial.geosearch),
    "GEODIST": ("geospatial", Geospatial.geodist),

    # Bitmap commands
    "SETBIT": ("bitmaps", Bitmaps.setbit),
    "GETBIT": ("bitmaps", Bitmaps.getbit),
    "BITCOUNT": ("bitmaps", Bitmaps.bitcount),
    "BITOP": ("bitmaps", Bitmaps.bitop),

    # Bitfield commands
    "BITFIELD": ("bitfields", Bitfields.handle_command),

    # Probabilistic commands
    "PFADD": ("hyperloglog", HyperLogLog.add),
    "PFCOUNT": ("hyperloglog", HyperLogLog.count),
    "PFMERGE": ("hyperloglog", HyperLogLog.merge),

    # Time Series commands
    "TS.CREATE": ("timeseries", TimeSeries.create),
    "TS.ADD": ("timeseries", TimeSeries.add),
    "TS.RANGE": ("timeseries", TimeSeries.range),
    "TS.GET": ("timeseries", TimeSeries.get),

    # Vector commands
    "VECTOR.ADD": ("vectors", Vectors.add_vector),
    "VECTOR.SEARCH": ("vectors", Vectors.similarity_search),
    "VECTOR.OP": ("vectors", Vectors.vector_operation),

    # Document commands
    "DOC.INSERT": ("documents", Documents.insert),
    "DOC.FIND": ("documents", Documents.find),
    "DOC.UPDATE": ("documents", Documents.update),
    "DOC.DELETE": ("documents", Documents.delete),
    "DOC.AGGREGATE": ("documents", Documents.aggregate),
}
