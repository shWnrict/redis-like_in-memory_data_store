import socket
import threading
from key_value_store import KeyValueStore

class RedisServer:
    def __init__(self, host='127.0.0.1', port=6379, password=None):
        self.host = host
        self.port = port
        self.password = password  # Password for authentication
        self.store = KeyValueStore(is_master=True)  # Initialize store as master

    def handle_client(self, client_socket):
        """Handle communication with a connected client."""
        authenticated = False

        while True:
            try:
                # Receive the entire request from the client
                request = client_socket.recv(1024).decode('utf-8').strip()
                if not request:
                    break
                
                # Split the request into commands if it contains multiple commands
                requests = request.split('\r\n')
                
                responses = []
                for command in requests:
                    parts = command.split()
                    cmd = parts[0].upper()

                    if cmd == "AUTH":
                        # Handle authentication command
                        if len(parts) != 2:
                            responses.append("ERR: Invalid AUTH command")
                        else:
                            password = parts[1]
                            if password == self.password:
                                authenticated = True
                                responses.append("OK")
                            else:
                                responses.append("ERR: Invalid password")
                        continue

                    if not authenticated:
                        responses.append("ERR: Authentication required")
                        continue

                    # Process other commands if authenticated
                    response = self.process_command(cmd, parts)
                    responses.append(response)

                # Send all responses at once
                client_socket.send('\r\n'.join(responses).encode('utf-8'))

            except Exception as e:
                client_socket.send(f"ERROR: {str(e)}".encode('utf-8'))
                break

        client_socket.close()

    def process_command(self, command, parts):
        """Parse and process the client command."""
        if command == 'SET':
            return self.store.set(parts[1], parts[2])
        elif command == 'GET':
            return self.store.get(parts[1]) or "ERR: Key not found"
        elif command == 'PUBLISH':
            return self.store.publish(parts[1], parts[2])
        else:
            return "ERR: Unknown command"

    def start(self):
        """Start the Redis server."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}...")

        while True:
            client_socket, client_address = server_socket.accept()
            print(f"Connection from {client_address} has been established.")
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()

# Run the server
if __name__ == "__main__":
    server = RedisServer(password="password123")  # Set the password
    server.start()
