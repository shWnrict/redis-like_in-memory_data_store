# src/server.py

import socket
import select
from core.database import KeyValueStore
from protocol import parse_resp, format_resp

class TCPServer:
    def __init__(self, host='127.0.0.1', port=6379):
        self.host = host
        self.port = port
        self.db = KeyValueStore()
        self.server_socket = None
        self.shutting_down = False
        self.socket_timeout = 0.1  # Reduced timeout for faster response
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
        self.active_clients = set()  # Track active client connections

    def start(self):
        print(f"Server started on {self.host}:{self.port}")
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.server_socket.settimeout(self.socket_timeout)
            
            while not self.shutting_down:
                try:
                    readable, _, _ = select.select([self.server_socket], [], [], self.socket_timeout)
                    if self.server_socket in readable:
                        client_socket, address = self.server_socket.accept()
                        client_socket.setblocking(False)  # Make client socket non-blocking
                        print(f"Connection from {address}")
                        self.active_clients.add(client_socket)
                        with client_socket:
                            self.handle_client(client_socket, address)
                except select.error:
                    if self.shutting_down:
                        break
                except socket.timeout:
                    continue  # Check shutdown flag every second
                except OSError as e:
                    if self.shutting_down:
                        break
                    else:
                        raise
        except KeyboardInterrupt:
            print("\nReceived shutdown signal...")
        finally:
            self.stop()

    def stop(self):
        """Stop the server and cleanup resources."""
        if not self.shutting_down:  # Prevent multiple shutdown attempts
            self.shutting_down = True
            print("Shutting down server...")
            
            # Notify and close all active client connections
            shutdown_message = format_resp("Server shutting down...").encode()
            for client_socket in self.active_clients.copy():
                try:
                    client_socket.send(shutdown_message)
                    client_socket.shutdown(socket.SHUT_RDWR)
                    client_socket.close()
                except (OSError, socket.error):
                    pass
                self.active_clients.remove(client_socket)

            # Close server socket
            if self.server_socket:
                try:
                    self.server_socket.shutdown(socket.SHUT_RDWR)
                    self.server_socket.close()
                except OSError:
                    pass

            self.db.stop()
            print("Server stopped.")

    def handle_client(self, client_socket, address):
        client_id = address[1]
        try:
            while not self.shutting_down:  # Check shutdown flag
                try:
                    readable, _, _ = select.select([client_socket], [], [], self.socket_timeout)
                    if not readable:
                        continue
                        
                    data = client_socket.recv(1024).decode()
                    if not data or self.shutting_down:
                        break
                    request = parse_resp(data)
                    response = self.process_request(request, client_id)
                    client_socket.sendall(format_resp(response).encode())
                except (socket.error, ConnectionResetError) as e:
                    print(f"Connection error for {address}: {e}")
                    break
                except Exception as e:
                    error_message = f"ERROR: {str(e)}"
                    try:
                        client_socket.sendall(format_resp(error_message).encode())
                    except:
                        print(f"Could not send error to client {address}: {error_message}")
                    continue
        finally:
            if client_socket in self.active_clients:
                self.active_clients.remove(client_socket)
            try:
                client_socket.close()
            except:
                pass

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


