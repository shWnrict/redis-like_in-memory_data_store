# src/main.py

from src.server import TCPServer

if __name__ == "__main__":
    server = TCPServer()
    server.start()
