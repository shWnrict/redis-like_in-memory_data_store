import sys
import os
import argparse

# Add the src directory to the module search path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from server import TCPServer

def parse_args():
    parser = argparse.ArgumentParser(description='Redis-like server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=6379, help='Port to bind to')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    server = TCPServer(host=args.host, port=args.port)
    print(f"Starting server on {args.host}:{args.port}")
    server.start()
