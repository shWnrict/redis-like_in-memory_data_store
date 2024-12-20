import socket
import threading
import logging
from src.protocol import RESPParser
from src.utils.config import Config  # Import configurations

# Configure logging
logging.basicConfig(level=Config.LOG_LEVEL, format="[%(asctime)s] %(message)s")

def handle_client(client_socket, addr):
    """Handle an individual client connection."""
    logging.info(f"Client connected: {addr}")
    parser = RESPParser()

    try:
        while True:
            # Receive data from the client
            data = client_socket.recv(1024)
            if not data:
                break  # Client disconnected

            # Parse and handle the RESP command
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

def start_server():
    """Start the TCP server."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((Config.HOST, Config.PORT))
    server_socket.listen(Config.MAX_CONNECTIONS)
    logging.info(f"Server started on {Config.HOST}:{Config.PORT}")

    try:
        while True:
            client_socket, addr = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
            client_thread.start()
    except KeyboardInterrupt:
        logging.info("Shutting down server.")
    finally:
        server_socket.close()

def route_command(command):
    """Route a command to the appropriate handler (placeholder)."""
    return f"+Received: {command}\r\n".encode()

if __name__ == "__main__":
    start_server()
