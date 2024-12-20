import socket
import threading
import logging
from src.protocol import RESPParser
from src.core.keyspace_manager import KeyspaceManager

# Server Configuration
from src.utils.config import Config  # Centralized configuration

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format="[%(asctime)s] %(message)s")

# Initialize Keyspace Manager
keyspace_manager = KeyspaceManager()

def handle_client(client_socket, addr):
    """Handle individual client connections."""
    logging.info(f"Client connected: {addr}")
    parser = RESPParser()

    try:
        while True:
            # Receive data from the client
            data = client_socket.recv(1024)
            if not data:
                break  # Client disconnected

            # Parse RESP command
            try:
                command = parser.parse(data)
                response = route_command(command)
            except Exception as e:
                response = f"-ERROR {str(e)}\r\n".encode()

            # Send response back to the client
            client_socket.sendall(response)
    except Exception as e:
        logging.error(f"Error with client {addr}: {e}")
    finally:
        logging.info(f"Client disconnected: {addr}")
        client_socket.close()

def route_command(command):
    """Route RESP commands to the appropriate handler."""
    if not isinstance(command, list) or len(command) < 1:
        return b"-ERROR Invalid command format\r\n"

    cmd = command[0].upper()
    args = command[1:]

    try:
        if cmd == "SET":
            return handle_set(args)
        elif cmd == "GET":
            return handle_get(args)
        elif cmd == "DEL":
            return handle_del(args)
        elif cmd == "EXISTS":
            return handle_exists(args)
        elif cmd == "EXPIRE":
            return handle_expire(args)
        elif cmd == "TTL":
            return handle_ttl(args)
        elif cmd == "PERSIST":
            return handle_persist(args)
        elif cmd == "SELECT":
            return handle_select(args)
        elif cmd == "FLUSHDB":
            return handle_flushdb()
        elif cmd == "RANDOMKEY":
            return handle_randomkey()
        else:
            return b"-ERROR Unknown command\r\n"
    except Exception as e:
        return f"-ERROR {str(e)}\r\n".encode()

# Command Handlers
def handle_set(args):
    if len(args) != 2:
        return b"-ERROR SET requires 2 arguments\r\n"
    key, value = args
    store = keyspace_manager.get_current_store()
    return f"+{store.set(key, value)}\r\n".encode()

def handle_get(args):
    if len(args) != 1:
        return b"-ERROR GET requires 1 argument\r\n"
    key = args[0]
    store = keyspace_manager.get_current_store()
    value = store.get(key)
    if value is None:
        return b"$-1\r\n"  # Null bulk string
    return f"${len(value)}\r\n{value}\r\n".encode()

def handle_del(args):
    if len(args) < 1:
        return b"-ERROR DEL requires at least 1 argument\r\n"
    store = keyspace_manager.get_current_store()
    count = sum(store.delete(key) for key in args)
    return f":{count}\r\n".encode()

def handle_exists(args):
    if len(args) != 1:
        return b"-ERROR EXISTS requires 1 argument\r\n"
    key = args[0]
    store = keyspace_manager.get_current_store()
    exists = store.exists(key)
    return f":{1 if exists else 0}\r\n".encode()

def handle_expire(args):
    if len(args) != 2:
        return b"-ERROR EXPIRE requires 2 arguments\r\n"
    key, ttl = args
    ttl = int(ttl)
    store = keyspace_manager.get_current_store()
    store.expiry_manager.set_expiry(key, ttl)
    return f":1\r\n".encode()

def handle_ttl(args):
    if len(args) != 1:
        return b"-ERROR TTL requires 1 argument\r\n"
    key = args[0]
    store = keyspace_manager.get_current_store()
    ttl = store.expiry_manager.ttl(key)
    return f":{ttl}\r\n".encode()

def handle_persist(args):
    if len(args) != 1:
        return b"-ERROR PERSIST requires 1 argument\r\n"
    key = args[0]
    store = keyspace_manager.get_current_store()
    result = store.expiry_manager.persist(key)
    return f":{result}\r\n".encode()

def handle_select(args):
    if len(args) != 1:
        return b"-ERROR SELECT requires 1 argument\r\n"
    db_index = int(args[0])
    return f"+{keyspace_manager.select(db_index)}\r\n".encode()

def handle_flushdb():
    return f"+{keyspace_manager.flushdb()}\r\n".encode()

def handle_randomkey():
    random_key = keyspace_manager.randomkey()
    if random_key is None:
        return b"$-1\r\n"  # Null bulk string
    return f"${len(random_key)}\r\n{random_key}\r\n".encode()

# Start the server
if __name__ == "__main__":
    start_server()
