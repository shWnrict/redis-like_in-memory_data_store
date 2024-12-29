import socket
import time
from protocol import parse_resp, format_resp
from config.settings import (
    SERVER_HOST, SERVER_PORT,
    CLIENT_TIMEOUT, CLIENT_RETRY_INTERVAL,
    CLIENT_MAX_RETRIES
)
from utils.logger import setup_logger

class RedisClient:
    def __init__(self, 
                 host=SERVER_HOST, 
                 port=SERVER_PORT,
                 timeout=CLIENT_TIMEOUT,
                 retry_interval=CLIENT_RETRY_INTERVAL,
                 max_retries=CLIENT_MAX_RETRIES):
        self.logger = setup_logger('client')
        self.host = host
        self.port = port
        self.timeout = timeout
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self._socket = None
        self.connected = False

    def connect(self):
        """Establish connection to server with retry logic."""
        self.logger.info(f"Connecting to {self.host}:{self.port}")
        retries = 0
        while retries < self.max_retries:
            try:
                if self._socket:
                    self._socket.close()
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self.timeout)
                self._socket.connect((self.host, self.port))
                self.connected = True
                return True
            except socket.error as e:
                retries += 1
                if retries == self.max_retries:
                    raise ConnectionError(f"Failed to connect after {self.max_retries} attempts: {e}")
                time.sleep(self.retry_interval)
        return False

    def disconnect(self):
        """Close connection to server."""
        if self._socket:
            self._socket.close()
            self._socket = None
        self.connected = False

    def _execute_command(self, *args):
        """Execute a command and return the response."""
        if not self.connected:
            self.connect()

        try:
            command = format_resp(args)
            self.logger.debug(f"Executing command: {' '.join(str(arg) for arg in args)}")
            self._socket.sendall(command.encode())
            response = self._socket.recv(4096).decode()
            return parse_resp(response)
        except (socket.error, ConnectionError) as e:
            self.connected = False
            self.logger.error(f"Command execution error: {e}")
            raise ConnectionError(f"Connection error: {e}")

    # Basic Commands
    def ping(self, message=None):
        """Send PING command."""
        args = ['PING']
        if message:
            args.append(message)
        return self._execute_command(*args)

    def set(self, key, value):
        """Set key to value."""
        return self._execute_command('SET', key, value)

    def get(self, key):
        """Get value for key."""
        return self._execute_command('GET', key)

    def delete(self, key):
        """Delete key."""
        return self._execute_command('DEL', key)

    def flushdb(self):
        """Clear all keys from the database."""
        return self._execute_command('FLUSHDB')

    # String Operations
    def append(self, key, value):
        """Append value to key."""
        return self._execute_command('APPEND', key, value)

    def incr(self, key):
        """Increment value of key."""
        return self._execute_command('INCR', key)

    def decr(self, key):
        """Decrement value of key."""
        return self._execute_command('DECR', key)

    # List Operations
    def lpush(self, key, *values):
        """Push values to the head of list."""
        return self._execute_command('LPUSH', key, *values)

    def rpush(self, key, *values):
        """Push values to the tail of list."""
        return self._execute_command('RPUSH', key, *values)

    def lpop(self, key):
        """Pop value from the head of list."""
        return self._execute_command('LPOP', key)

    def rpop(self, key):
        """Pop value from the tail of list."""
        return self._execute_command('RPOP', key)

    # Set Operations
    def sadd(self, key, *members):
        """Add members to set."""
        return self._execute_command('SADD', key, *members)

    def srem(self, key, *members):
        """Remove members from set."""
        return self._execute_command('SREM', key, *members)

    def smembers(self, key):
        """Get all members of set."""
        return self._execute_command('SMEMBERS', key)

    # Hash Operations
    def hset(self, key, field, value):
        """Set hash field to value."""
        return self._execute_command('HSET', key, field, value)

    def hget(self, key, field):
        """Get value of hash field."""
        return self._execute_command('HGET', key, field)

    def hdel(self, key, *fields):
        """Delete hash fields."""
        return self._execute_command('HDEL', key, *fields)

    # Sorted Set Operations
    def zadd(self, key, score, member):
        """Add member with score to sorted set."""
        return self._execute_command('ZADD', key, score, member)

    def zrem(self, key, *members):
        """Remove members from sorted set."""
        return self._execute_command('ZREM', key, *members)

    # Pub/Sub Operations
    def publish(self, channel, message):
        """Publish message to channel."""
        return self._execute_command('PUBLISH', channel, message)

    def subscribe(self, channel):
        """Subscribe to channel and return generator for messages."""
        self._execute_command('SUBSCRIBE', channel)
        while True:
            try:
                response = self._socket.recv(4096).decode()
                if response:
                    yield parse_resp(response)
            except socket.error:
                self.connected = False
                break

    # Transaction Operations
    def multi(self):
        """Start transaction."""
        return self._execute_command('MULTI')

    def exec(self):
        """Execute transaction."""
        return self._execute_command('EXEC')

    def discard(self):
        """Discard transaction."""
        return self._execute_command('DISCARD')

    # Advanced Data Type Operations
    def json_set(self, key, path, value):
        """Set JSON value at path in key."""
        return self._execute_command('JSON.SET', key, path, value)

    def json_get(self, key, path='.'):
        """Get JSON value at path from key."""
        return self._execute_command('JSON.GET', key, path)

    def ts_add(self, key, timestamp, value):
        """Add time series data point."""
        return self._execute_command('TS.ADD', key, timestamp, value)

    def ts_range(self, key, from_ts, to_ts):
        """Query time series range."""
        return self._execute_command('TS.RANGE', key, from_ts, to_ts)

    # Context manager support
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
