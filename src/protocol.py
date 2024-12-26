# src/protocol.py
class RESPProtocol:
    @staticmethod
    def parse_message(data):
        try:
            lines = iter(data.splitlines())
            commands = []

            while True:
                try:
                    line = next(lines)
                    if not line.startswith("*"):
                        raise ValueError("Expected array length marker")
                    count = int(line[1:])

                    if count < 1:
                        raise ValueError("Invalid command length")

                    command = []
                    for _ in range(count):
                        line = next(lines)
                        if not line.startswith("$"):
                            raise ValueError("Expected bulk string marker")

                        length = int(line[1:])
                        bulk_data = next(lines)
                        if len(bulk_data) != length:
                            raise ValueError(f"Bulk string length mismatch. Expected {length}, got {len(bulk_data)}")
                        command.append(bulk_data)

                    commands.append(command)
                except StopIteration:
                    break  # End of input data
            return commands

        except (ValueError, StopIteration) as e:
            raise ProtocolError(f"Protocol parsing error: {e}")

    @staticmethod
    def format_response(response):
        if isinstance(response, list):
            resp = f"*{len(response)}\r\n"
            for item in response:
                resp += RESPProtocol.format_response(item)
            return resp
        elif response is None:
            return "$-1\r\n"  # Null Bulk String
        elif isinstance(response, str):
            return f"${len(response)}\r\n{response}\r\n"
        elif isinstance(response, int):
            return f":{response}\r\n"
        elif isinstance(response, bool):
            return ":1\r\n" if response else ":0\r\n"  # Boolean as integer
        elif isinstance(response, float):
            return f"${len(str(response))}\r\n{response}\r\n"  # Float as bulk string
        else:
            raise ValueError(f"Unsupported response type: {type(response)}")

class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(f"RESP Protocol Error: {message}")
