class RESPProtocol:
    @staticmethod
    def encode_simple_string(message: str) -> bytes:
        return f"+{message}\r\n".encode()

    @staticmethod
    def encode_error(message: str) -> bytes:
        return f"-{message}\r\n".encode()

    @staticmethod
    def encode_integer(number: int) -> bytes:
        return f":{number}\r\n".encode()

    @staticmethod
    def encode_bulk_string(message: str) -> bytes:
        if message is None:
            return b"$-1\r\n"
        return f"${len(message)}\r\n{message}\r\n".encode()

    @staticmethod
    def decode(data: bytes):
        # Basic RESP protocol decoding
        message_type = chr(data[0])
        if message_type == '+':
            return data[1:].split(b'\r\n')[0].decode()
        elif message_type == '-':
            return f"Error: {data[1:].split(b'\r\n')[0].decode()}"
        elif message_type == ':':
            return int(data[1:].split(b'\r\n')[0])
        elif message_type == '$':
            # Handle bulk strings
            length = int(data[1:].split(b'\r\n')[0])
            if length == -1:
                return None
            return data.split(b'\r\n')[1].decode()