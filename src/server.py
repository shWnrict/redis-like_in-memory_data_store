import socket
import threading
from typing import Dict
from .data_structures.data_types import RedisDataTypes
from .protocol import RESPProtocol

class RedisServer:
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.data_store = RedisDataTypes()
        self.commands: Dict[str, callable] = self._register_commands()

    def _register_commands(self):
        return {
            'SET': self._set_command,
            'GET': self._get_command,
            'LPUSH': self._lpush_command,
            'RPUSH': self._rpush_command,
            'SADD': self._sadd_command,
            'SMEMBERS': self._smembers_command,
        }

    def _set_command(self, *args):
        if len(args) != 2:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key, value = args
        self.data_store.string_set(key, value)
        return RESPProtocol.encode_simple_string("OK")

    def _get_command(self, *args):
        if len(args) != 1:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key = args[0]
        value = self.data_store.string_get(key)
        return RESPProtocol.encode_bulk_string(value)

    def _lpush_command(self, *args):
        if len(args) < 2:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key, *values = args
        for value in values:
            self.data_store.list_lpush(key, value)
        return RESPProtocol.encode_integer(len(values))

    def _rpush_command(self, *args):
        if len(args) < 2:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key, *values = args
        for value in values:
            self.data_store.list_rpush(key, value)
        return RESPProtocol.encode_integer(len(values))

    def _sadd_command(self, *args):
        if len(args) < 2:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key, *values = args
        for value in values:
            self.data_store.set_add(key, value)
        return RESPProtocol.encode_integer(len(values))

    def _smembers_command(self, *args):
        if len(args) != 1:
            return RESPProtocol.encode_error("Wrong number of arguments")
        key = args[0]
        members = self.data_store.set_members(key)
        return RESPProtocol.encode_bulk_string(str(members))

    def start(self):
        self.socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")
        
        while True:
            client_socket, address = self.socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()
    
    def handle_client(self, client_socket):
        while True:
            try:
                # Receive data
                data = client_socket.recv(1024)
                if not data:
                    break

                # Parse and execute command
                command_parts = self._parse_command(data)
                if command_parts:
                    command, *args = command_parts
                    response = self._execute_command(command.upper(), *args)
                    client_socket.send(response)

            except Exception as e:
                print(f"Error handling client: {e}")
                break

        client_socket.close()

    def _parse_command(self, data: bytes):
        # Very basic command parsing
        # TODO: Implement full RESP protocol parsing
        try:
            decoded = data.decode().strip().split()
            return decoded
        except Exception:
            return None

    def _execute_command(self, command: str, *args):
        cmd_func = self.commands.get(command)
        if cmd_func:
            return cmd_func(*args)
        return RESPProtocol.encode_error(f"Unknown command: {command}")

if __name__ == "__main__":
    server = RedisServer()
    server.start()