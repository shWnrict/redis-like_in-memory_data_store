# src/server.py

import socket
from core.database import KeyValueStore

class TCPServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.db = KeyValueStore()
        self.command_map = {
            "SET": self.set_command,
            "GET": self.get_command,
            "DEL": self.del_command,
            "EXISTS": self.exists_command,
            # Expiration commands   
            "EXPIRE": self.expire_command,
            "TTL": self.ttl_command,
            "PERSIST": self.persist_command,
            # Transaction commands
            "MULTI": self.multi_command,
            "EXEC": self.exec_command,
            "DISCARD": self.discard_command,
            # String-specific commands
            "APPEND": self.append_command,
            "STRLEN": self.strlen_command,
            "INCR": self.incr_command,
            "DECR": self.decr_command,
            "INCRBY": self.incrby_command,
            "DECRBY": self.decrby_command,
            "GETRANGE": self.getrange_command,
            "SETRANGE": self.setrange_command,
        }

    def start(self):
        print(f"Server started on {self.host}:{self.port}")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(5)
                while True:
                    client_socket, address = server_socket.accept()
                    print(f"Connection from {address}")
                    with client_socket:
                        self.handle_client(client_socket)
        finally:
            self.db.stop()  # Ensure AOF logger stops on exit

    def handle_client(self, client_socket, address):
        client_id = address[1]
        while True:
            try:
                data = client_socket.recv(1024).decode().strip()
                if not data:
                    break
                response = self.process_request(data, client_id)
                client_socket.sendall(response.encode())
            except Exception as e:
                client_socket.sendall(f"ERROR: {e}".encode())
                break

    def process_request(self, request, client_id):
        parts = request.split()
        command = parts[0].upper()
        args = parts[1:]
        if command in self.command_map:
            return self.command_map[command](client_id, *args)
        return f"ERROR: Unknown command {command}"

    # Core operations
    def set_command(self, key, value):
        self.db.set(key, value)
        return "OK"

    def get_command(self, key):
        value = self.db.get(key)
        return value if value else "(nil)"

    def del_command(self, key):
        result = self.db.delete(key)
        return "(1)" if result else "(0)"

    def exists_command(self, key):
        return "(1)" if self.db.exists(key) else "(0)"

    # Expiration commands
    def expire_command(self, key, ttl):
        result = self.db.expiry_manager.set_expiry(key, int(ttl))
        return "(1)" if result else "(0)"

    def ttl_command(self, key):
        ttl = self.db.expiry_manager.ttl(key)
        return str(ttl)

    def persist_command(self, key):
        result = self.db.expiry_manager.persist(key)
        return "(1)" if result else "(0)"
    
    # Transaction commands
    def multi_command(self, client_id):
        result = self.db.transaction_manager.start_transaction(client_id)
        return result if result else "OK"

    def exec_command(self, client_id):
        results = self.db.transaction_manager.execute_transaction(client_id)
        return "\n".join(map(str, results))

    def discard_command(self, client_id):
        result = self.db.transaction_manager.discard_transaction(client_id)
        return result if result else "OK"
    
    # String-specific operations
    def append_command(self, key, value):
        return str(self.db.string.append(key, value))

    def strlen_command(self, key):
        return str(self.db.string.strlen(key))

    def incr_command(self, key):
        return str(self.db.string.incr(key))

    def decr_command(self, key):
        return str(self.db.string.decr(key))

    def incrby_command(self, key, increment):
        return str(self.db.string.incrby(key, int(increment)))

    def decrby_command(self, key, decrement):
        return str(self.db.string.decrby(key, int(decrement)))

    def getrange_command(self, key, start, end):
        return self.db.string.getrange(key, int(start), int(end))

    def setrange_command(self, key, offset, value):
        return str(self.db.string.setrange(key, int(offset), value))


