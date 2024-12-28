import sys
import os

# Add the src directory to the module search path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from server import TCPServer

if __name__ == "__main__":
    server = TCPServer()
    server.start()
