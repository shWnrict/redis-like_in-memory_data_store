# src/server.py

import socket
from core.database import KeyValueStore
from protocol import parse_resp, format_resp

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
                        self.handle_client(client_socket, address)
        except KeyboardInterrupt:
            print("Server shutting down...")
        finally:
            self.db.stop()  # Ensure AOF logger stops on exit

    def handle_client(self, client_socket, address):
        client_id = address[1]
        try:
            while True:
                try:
                    data = client_socket.recv(1024).decode()
                    if not data:
                        break
                    request = parse_resp(data)
                    response = self.process_request(request, client_id)
                    client_socket.sendall(format_resp(response).encode())
                except ConnectionResetError:
                    print(f"Connection reset by {address}")
                    break
                except Exception as e:
                    client_socket.sendall(format_resp(f"ERROR: {e}").encode())
                    break
        except KeyboardInterrupt:
            print("Server shutting down...")
        finally:
            client_socket.close()

    def process_request(self, request, client_id):
        if not request:
            return "ERROR: Empty command"
        command = request[0].upper()
        args = request[1:]
        if command in self.command_map:
            return self.command_map[command](client_id, *args)
        return f"ERROR: Unknown command {command}"

    # Core operations
    def set_command(self, client_id, key, value):
        self.db.set(key, value)
        return "OK"

    def get_command(self, client_id, key):
        value = self.db.get(key)
        return value if value else "(nil)"

    def del_command(self, client_id, key):
        return "(1)" if self.db.delete(key) else "(0)"

    def exists_command(self, client_id, key):
        return "(1)" if self.db.exists(key) else "(0)"

    # Expiration commands
    def expire_command(self, client_id, key, ttl):
        return "(1)" if self.db.expiry_manager.set_expiry(key, int(ttl)) else "(0)"

    def ttl_command(self, client_id, key):
        return str(self.db.expiry_manager.ttl(key))

    def persist_command(self, client_id, key):
        return "(1)" if self.db.expiry_manager.persist(key) else "(0)"
    
    # Transaction commands
    def multi_command(self, client_id):
        return self.db.transaction_manager.start_transaction(client_id) or "OK"

    def exec_command(self, client_id):
        results = self.db.transaction_manager.execute_transaction(client_id)
        return results

    def discard_command(self, client_id):
        return self.db.transaction_manager.discard_transaction(client_id) or "OK"
    
    # String-specific operations
    def append_command(self, client_id, key, value):
        return str(self.db.string.append(key, value))

    def strlen_command(self, client_id, key):
        return str(self.db.string.strlen(key))

    def incr_command(self, client_id, key):
        return str(self.db.string.incr(key))

    def decr_command(self, client_id, key):
        return str(self.db.string.decr(key))

    def incrby_command(self, client_id, key, increment):
        return str(self.db.string.incrby(key, int(increment)))

    def decrby_command(self, client_id, key, decrement):
        return str(self.db.string.decrby(key, int(decrement)))

    def getrange_command(self, client_id, key, start, end):
        return self.db.string.getrange(key, int(start), int(end))

    def setrange_command(self, client_id, key, offset, value):
        return str(self.db.string.setrange(key, int(offset), value))


